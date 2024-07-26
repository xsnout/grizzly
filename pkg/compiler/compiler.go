//
// This program translates a UQL query string and generates
//
//   1. A text Cap'n Proto file (plan.capnp) with the schema (mostly fields and types) of
//      each node in the query plan.
//
//   2. A binary Cap'n Proto query plan file according to the grizzly schema (grizzly.capnp)
//
// There are 2 different parameters:
//
//   1. compile:  Given a UQL query, generate the binary query plan
//
//   2. show:     Given a binary query plan, generate a JSON representation of the query plan
//
// Compilation:
//
// stdin (UQL query)  --->  ./grizzlyc compile  -+->  stdout (binary Cap'n Proto stream)
//                                               |
//                                               +->  file with Cap'n Proto schemas (schemas.capnp)
//                                                    (this file is a side effect)
// Example:
//
//   echo "from table1 where x >= 5 project a, b" | ./compiler compile > ./plan.bin
//   cat ./plan.bin | ./compiler show | jq . | tee ./plan_pretty.json
//

//
// Data flow between nodes:
//
// "A --> B": "rows from A feed into B" (data flow)
//
//     ingressNode --> ...
// --> ingressFilterNode
// --> windowNode
// --> aggregateNode
// --> groupFilterNode
// --> projectNode
// --> projectFilterNode
// --> egressNode
//
// ingressNode:          Reads from some input source, like a CSV file from STDIN
// ingressFilterNode:    Optionally removes rows based on a condition
// windowNode:           Groups rows into time intervals, e.g., all rows that arrived in the last 5 seconds
// aggregateNode:        Applies aggregate functions to the rows in a group and outputs exactly one result row per group
// aggregateFilterNode:  Optionally removes rows based on a condition
// projectNode:          Optionally removes fields from the input row and applies basic expressions like "(a + b) as x"
// projectFilterNode:    Optionally removes rows based on a condition
// egressNode:           Transforms an input row into a format that can be read by the user of the query, e.g., CSV or JSON format
//

package compiler

import (
	"errors"
	"fmt"
	"io"
	"os"
	"strconv"

	"capnproto.org/go/capnp/v3"
	"github.com/antlr4-go/antlr/v4"
	"github.com/rs/zerolog"
	"github.com/xsnout/grizzly/capnp/grizzly"
	"github.com/xsnout/grizzly/pkg/_out/query/parser"
	"github.com/xsnout/grizzly/pkg/catalog"
	"github.com/xsnout/grizzly/pkg/codegen"
	_ "github.com/xsnout/grizzly/pkg/plan"
	"github.com/xsnout/grizzly/pkg/utility"
)

const (
	functionsPath   = "_out/functions_code_snippet.cc"
	CatalogFilePath = "_out/catalog.bin"
)

const (
	WindowType            = "window_type"
	IntervalType          = "interval_type"
	IntervalAmount        = "interval_amount"
	IntervalUnit          = "interval_unit"
	SessionCloseInclusive = "session_close_inclusive"
	SequenceFieldName     = "sequence_field_name"
)

const (
	WindowTypeSession    = "session"
	WindowTypeSlice      = "slice"
	IntervalTypeDistance = "distance"
	IntervalTypeTime     = "time"
)

var (
	log zerolog.Logger
)

type queryListener struct {
	*parser.BaseUQLListener

	queryPlan QueryPlan

	capnpCode codegen.CapnpCode
	goCode    codegen.GoCode

	sessionOpenTuple  codegen.GoExpression
	sessionCloseTuple codegen.GoExpression

	hasIngressFilter            bool
	hasAggregateFilter          bool
	hasProjectFilter            bool
	hasSessionWindow            bool
	sliceIntervalTypeIsDistance bool

	inputTableFullName      string
	aggregateAliasFieldName string
	sequenceFieldName       string
	groupFieldNames         []string

	filterType codegen.FilterType
	calls      []grizzly.Call
}

func NewQueryPlanTemplate(seg *capnp.Segment, msg *capnp.Message, QueryPlan *QueryPlan) {
	var err error
	if QueryPlan.root, err = grizzly.NewRootNode(seg); err != nil {
		panic(err)
	}

	var children grizzly.Node_List
	var parent, this grizzly.Node

	{
		//
		// EGRESS
		//
		parent = QueryPlan.root
		parent.SetType(grizzly.OperatorType_egress)
		parent.SetLabel("Egress")
		parent.SetId(0)
		if children, err = parent.NewChildren(1); err != nil {
			panic(err)
		}
	}
	{
		//
		// PROJECT FILTER
		//
		this = children.At(0)
		parent.SetChildren(children)
		this.SetType(grizzly.OperatorType_projectFilter)
		this.SetLabel("Project Filter")
		this.SetId(1)
		if children, err = this.NewChildren(1); err != nil {
			panic(err)
		}
		parent = this
	}
	{
		//
		// PROJECT
		//
		this = children.At(0)
		parent.SetChildren(children)
		this.SetType(grizzly.OperatorType_project)
		this.SetLabel("Project")
		this.SetId(2)
		if children, err = this.NewChildren(1); err != nil {
			panic(err)
		}
		parent = this
	}
	{
		//
		// AGGREGATE FILTER
		//
		this = children.At(0)
		parent.SetChildren(children)
		this.SetType(grizzly.OperatorType_aggregateFilter)
		this.SetLabel("Aggregate Filter")
		this.SetId(3)
		if children, err = this.NewChildren(1); err != nil {
			panic(err)
		}
		parent = this
	}
	{
		//
		// AGGREGATE
		//
		this = children.At(0)
		parent.SetChildren(children)
		this.SetType(grizzly.OperatorType_aggregate)
		this.SetLabel("Aggregate")
		this.SetId(4)
		if children, err = this.NewChildren(1); err != nil {
			panic(err)
		}
		parent = this
	}
	{
		//
		// WINDOW
		//
		this = children.At(0)
		parent.SetChildren(children)
		this.SetType(grizzly.OperatorType_window)
		this.SetLabel("Window")
		this.SetId(5)
		if children, err = this.NewChildren(1); err != nil {
			panic(err)
		}
		parent = this
	}
	{
		//
		// INGRESS FILTER
		//
		this = children.At(0)
		parent.SetChildren(children)
		this.SetType(grizzly.OperatorType_ingressFilter)
		this.SetLabel("Ingress Filter")
		this.SetId(6)
		if children, err = this.NewChildren(1); err != nil {
			panic(err)
		}
		parent = this
	}
	{
		//
		// INGRESS
		//
		this = children.At(0)
		parent.SetChildren(children)
		this.SetType(grizzly.OperatorType_ingress)
		this.SetLabel("Ingress")
		this.SetId(7)
		parent = this
	}
}

func (l *queryListener) ExitQueryClause(ctx *parser.QueryClauseContext) {
	copyGroupFields(l.ingressNode(), l.ingressFilterNode())
	copyGroupFields(l.ingressNode(), l.windowNode())
	copyGroupFields(l.ingressNode(), l.aggregateNode())
	copyGroupFields(l.ingressNode(), l.aggregateFilterNode())
	copyGroupFields(l.ingressNode(), l.projectNode())
	copyGroupFields(l.ingressNode(), l.projectFilterNode())
	copyGroupFields(l.ingressNode(), l.egressNode())

	l.goCode.IngressFilter.Functions = append(l.goCode.IngressFilter.Functions, codegen.GoTranslate("Ingress", l.ingressNode(), &l.queryPlan.root))
	l.goCode.AggregateFilter.Functions = append(l.goCode.AggregateFilter.Functions, codegen.GoTranslate("Aggregate", l.aggregateNode(), &l.queryPlan.root))
	l.goCode.ProjectFilter.Functions = append(l.goCode.ProjectFilter.Functions, codegen.GoTranslate("Egress", l.egressNode(), &l.queryPlan.root))

	l.goCode.IngressFilter.Functions = append(l.goCode.IngressFilter.Functions, codegen.GoFilter("Ingress", "Ingress"))
	l.goCode.IngressFilter.Functions = append(l.goCode.IngressFilter.Functions, codegen.GoFilter("SessionOpen", "Ingress"))
	l.goCode.IngressFilter.Functions = append(l.goCode.IngressFilter.Functions, codegen.GoFilter("SessionClose", "Ingress"))
	l.goCode.AggregateFilter.Functions = append(l.goCode.AggregateFilter.Functions, codegen.GoFilter("Aggregate", "Aggregate"))
	l.goCode.ProjectFilter.Functions = append(l.goCode.ProjectFilter.Functions, codegen.GoFilter("Project", "Egress"))

	l.goCode.IngressFilter.Types = append(l.goCode.IngressFilter.Types, codegen.GoInternalPayload("Ingress", l.ingressNode(), grizzly.OperatorType_ingress, &l.queryPlan.root))
	l.goCode.AggregateFilter.Types = append(l.goCode.AggregateFilter.Types, codegen.GoInternalPayload("Aggregate", l.aggregateNode(), grizzly.OperatorType_aggregate, &l.queryPlan.root))
	l.goCode.ProjectFilter.Types = append(l.goCode.ProjectFilter.Types, codegen.GoInternalPayload("Egress", l.egressNode(), grizzly.OperatorType_egress, &l.queryPlan.root))

	if l.hasIngressFilter {
		l.goCode.IngressFilter.Functions = append(l.goCode.IngressFilter.Functions, codegen.GoEval("Ingress", "Ingress", l.goCode.IngressFilter.Definitions, l.goCode.IngressFilter.Condition))
	} else {
		l.goCode.IngressFilter.Functions = append(l.goCode.IngressFilter.Functions, codegen.GoPassthroughEvalFunction("Ingress", "Ingress"))
	}

	if l.hasSessionWindow {
		l.goCode.IngressFilter.Functions = append(l.goCode.IngressFilter.Functions, codegen.GoEval("SessionOpen", "Ingress", l.goCode.IngressFilter.Definitions, l.goCode.SessionOpen.Condition))
		l.goCode.IngressFilter.Functions = append(l.goCode.IngressFilter.Functions, codegen.GoEval("SessionClose", "Ingress", l.goCode.IngressFilter.Definitions, l.goCode.SessionClose.Condition))
	} else {
		l.goCode.IngressFilter.Functions = append(l.goCode.IngressFilter.Functions, codegen.GoPassthroughEvalFunction("SessionOpen", "Ingress"))
		l.goCode.IngressFilter.Functions = append(l.goCode.IngressFilter.Functions, codegen.GoPassthroughEvalFunction("SessionClose", "Ingress"))
	}

	if l.hasAggregateFilter {
		l.goCode.AggregateFilter.Functions = append(l.goCode.AggregateFilter.Functions, codegen.GoEval("Aggregate", "Aggregate", l.goCode.AggregateFilter.Definitions, l.goCode.AggregateFilter.Condition))
	} else {
		l.goCode.AggregateFilter.Functions = append(l.goCode.AggregateFilter.Functions, codegen.GoPassthroughEvalFunction("Aggregate", "Aggregate"))
	}

	if l.hasProjectFilter {
		l.goCode.ProjectFilter.Functions = append(l.goCode.ProjectFilter.Functions, codegen.GoEval("Project", "Egress", l.goCode.ProjectFilter.Definitions, l.goCode.ProjectFilter.Condition))
	} else {
		l.goCode.ProjectFilter.Functions = append(l.goCode.ProjectFilter.Functions, codegen.GoPassthroughEvalFunction("Project", "Egress"))
	}

	var fields capnp.StructList[grizzly.Field]
	var err error
	if fields, err = l.ingressNode().Fields(); err != nil {
		panic(err)
	}
	l.capnpCode.Body += codegen.CapnpStructGroup(&l.queryPlan.root, fields, l.groupFieldNames)
	l.capnpCode.Body += codegen.CapnpStructIngressRow(&l.queryPlan.root, fields)

	codegen.GoCodeCreateFile(l.goCode)
	codegen.CapnpCreateDataFile(l.capnpCode)
}

func (l *queryListener) ingressNode() *grizzly.Node {
	return findNode(l, grizzly.OperatorType_ingress)
}

func (l *queryListener) ingressFilterNode() *grizzly.Node {
	return findNode(l, grizzly.OperatorType_ingressFilter)
}

func (l *queryListener) windowNode() *grizzly.Node {
	return findNode(l, grizzly.OperatorType_window)
}

func (l *queryListener) aggregateNode() *grizzly.Node {
	return findNode(l, grizzly.OperatorType_aggregate)
}

func (l *queryListener) aggregateFilterNode() *grizzly.Node {
	return findNode(l, grizzly.OperatorType_aggregateFilter)
}

func (l *queryListener) projectNode() *grizzly.Node {
	return findNode(l, grizzly.OperatorType_project)
}

func (l *queryListener) projectFilterNode() *grizzly.Node {
	return findNode(l, grizzly.OperatorType_projectFilter)
}

func (l *queryListener) egressNode() *grizzly.Node {
	return findNode(l, grizzly.OperatorType_egress)
}

func findNode(l *queryListener, typ grizzly.OperatorType) (node *grizzly.Node) {
	var found bool
	if node, found = utility.FindFirstNodeByType(&l.queryPlan.root, typ); !found {
		panic(fmt.Errorf("could not find operator %v", typ.String()))
	}
	return
}

type QueryPlan struct {
	msg  *capnp.Message
	seg  *capnp.Segment
	root grizzly.Node
}

func Init() {
	//zerolog.SetGlobalLevel(zerolog.Disabled)
	//zerolog.SetGlobalLevel(zerolog.InfoLevel)
	log = zerolog.New(os.Stderr).With().Caller().Timestamp().Logger()
	codegen.Init()
	utility.Init()
}

func Compile() {
	var query string
	var bytes []byte
	var err error
	if bytes, err = io.ReadAll(os.Stdin); err != nil {
		panic(err)
	}
	query = string(bytes)

	log.Info().Msgf("query: %s", query)

	var msg *capnp.Message
	var seg *capnp.Segment
	if msg, seg, err = capnp.NewMessage(capnp.SingleSegment(nil)); err != nil {
		log.Error().Err(err)
		panic(err)
	}

	_ = parseQuery(msg, seg, query)

	utility.WriteBinary(msg, os.Stdout)
}

func parseQuery(msg *capnp.Message, seg *capnp.Segment, query string) grizzly.Node {
	listener := queryListener{
		queryPlan: QueryPlan{
			msg: msg,
			seg: seg,
		},
	}
	var err error
	if listener.queryPlan.root, err = grizzly.NewRootNode(seg); err != nil {
		log.Error().Err(err)
		panic(err)
	}

	is := antlr.NewInputStream(query)
	lexer := parser.NewUQLLexer(is)
	tokenStream := antlr.NewCommonTokenStream(lexer, antlr.TokenDefaultChannel)

	parser := parser.NewUQLParser(tokenStream)

	NewQueryPlanTemplate(seg, msg, &listener.queryPlan)
	antlr.ParseTreeWalkerDefault.Walk(&listener, parser.Start_())

	return listener.queryPlan.root
}

func (l *queryListener) push(tuple codegen.GoExpression) {
	l.goCode.ExprStack = append(l.goCode.ExprStack, tuple)
}

func (l *queryListener) pop() codegen.GoExpression {
	if len(l.goCode.ExprStack) < 1 {
		panic("stack is empty; unable to pop")
	}

	// Get the last value from the stack.
	result := l.goCode.ExprStack[len(l.goCode.ExprStack)-1]

	// Remove the last element from the stack.
	l.goCode.ExprStack = l.goCode.ExprStack[:len(l.goCode.ExprStack)-1]

	return result
}

func (l *queryListener) ExitIngressWhereClause(c *parser.IngressWhereClauseContext) {
	l.hasIngressFilter = true
}

func (l *queryListener) ExitAggregateWhereClause(c *parser.AggregateWhereClauseContext) {
	l.hasAggregateFilter = true
}

func (l *queryListener) ExitProjectWhereClause(c *parser.ProjectWhereClauseContext) {
	l.hasProjectFilter = true
}

func (l *queryListener) ExitEquation(c *parser.EquationContext) {
	right, left := l.pop(), l.pop()

	var code string
	var kind codegen.Kind
	if left.Kind == codegen.Timestamp || right.Kind == codegen.Timestamp {
		code = timeCompare(c.GetOp(), left.Code, right.Code)
		kind = codegen.Timestamp
	} else {
		code = defaultCompare(c.GetOp(), left.Code, right.Code)
		kind = left.Kind
	}

	t := codegen.GoExpression{
		Code: code,
		Kind: kind,
	}
	l.push(t)
}

func (l *queryListener) ExitConnection(c *parser.ConnectionContext) {
	right, left := l.pop(), l.pop()
	var t codegen.GoExpression

	t.Kind = right.Kind

	switch c.GetOp().GetTokenType() {
	case parser.UQLParserAND:
		t.Code = left.Code + " && " + right.Code
	case parser.UQLParserOR:
		t.Code = left.Code + " || " + right.Code
	default:
		panic(fmt.Errorf("unexpected op: %s", c.GetOp().GetText()))
	}

	l.push(t)
}

func (l *queryListener) ExitParenthesis(c *parser.ParenthesisContext) {
	term := l.pop()
	tuple := codegen.GoExpression{
		Code: "(" + term.Code + ")",
		Kind: term.Kind,
	}
	l.push(tuple)
}

func (l *queryListener) ExitNegation(c *parser.NegationContext) {
	term := l.pop()
	tuple := codegen.GoExpression{
		Code: "!(" + term.Code + ")",
		Kind: term.Kind,
	}
	l.push(tuple)
}

func (l *queryListener) ExitMulDivMod(c *parser.MulDivModContext) {
	right, left := l.pop(), l.pop()

	var code string
	switch c.GetOp().GetTokenType() {
	case parser.UQLParserMUL:
		code = left.Code + " * " + right.Code
	case parser.UQLParserDIV:
		code = left.Code + " / " + right.Code
	case parser.UQLParserMOD:
		code = left.Code + " % " + right.Code
	default:
		panic(fmt.Sprintf("unexpected op: %s", c.GetOp().GetText()))
	}

	t := codegen.GoExpression{
		Code: code,
		Kind: right.Kind,
	}
	l.push(t)
}

func (l *queryListener) ExitAddSub(c *parser.AddSubContext) {
	right, left := l.pop(), l.pop()

	var code string
	var kind codegen.Kind

	if left.Kind != codegen.Duration && right.Kind == codegen.Duration {
		kind = codegen.Timestamp
		code = timeAddSub(c.GetOp(), left.Code, right.Code)
	} else if left.Kind == codegen.Duration && right.Kind != codegen.Duration {
		kind = codegen.Timestamp
		code = timeAddSub(c.GetOp(), right.Code, left.Code)
	} else {
		kind = right.Kind

		switch c.GetOp().GetTokenType() {
		case parser.UQLParserADD:
			code = left.Code + " + " + right.Code
		case parser.UQLParserSUB:
			code = left.Code + " - " + right.Code
		default:
			panic(fmt.Sprintf("unexpected op: %s", c.GetOp().GetText()))
		}
	}

	t := codegen.GoExpression{
		Code: code,
		Kind: kind,
	}
	l.push(t)
}

func timeAddSub(token antlr.Token, timestamp string, duration string) (code string) {
	var method string

	switch token.GetTokenType() {
	case parser.UQLParserADD:
		method = "Add"
	case parser.UQLParserSUB:
		method = "Sub"
	default:
		panic(fmt.Sprintf("unexpected op: %s", token.GetText()))
	}

	code = timestamp + "." + method + "(" + duration + ")"
	return
}

func timeCompare(token antlr.Token, timestamp1 string, timetamp2 string) (code string) {
	var cmp string

	switch token.GetTokenType() {
	case parser.UQLParserLT:
		cmp = " == -1"
	case parser.UQLParserLT_EQ:
		cmp = " <= 0"
	case parser.UQLParserEQ:
		cmp = " == 0"
	case parser.UQLParserNOT_EQ:
		cmp = " != 0"
	case parser.UQLParserGT_EQ:
		cmp = " >= 0"
	case parser.UQLParserGT:
		cmp = " > 0"
	default:
		panic(fmt.Sprintf("unexpected comparison operator: %s", token.GetText()))
	}
	code = timestamp1 + ".Compare(" + timetamp2 + ")" + cmp
	return
}

func defaultCompare(token antlr.Token, left string, right string) (code string) {
	var cmp string

	switch token.GetTokenType() {
	case parser.UQLParserLT:
		cmp = " < "
	case parser.UQLParserLT_EQ:
		cmp = " <= "
	case parser.UQLParserEQ:
		cmp = " == "
	case parser.UQLParserNOT_EQ:
		cmp = " != "
	case parser.UQLParserGT_EQ:
		cmp = " >= "
	case parser.UQLParserGT:
		cmp = " > "
	default:
		panic(fmt.Sprintf("unexpected comparison operator: %s", token.GetText()))
	}
	code = left + cmp + right
	return
}

func (l *queryListener) ExitFloat(c *parser.FloatContext) {
	tuple := codegen.GoExpression{
		Code: c.GetText(),
		Kind: codegen.Float,
	}
	l.push(tuple)
}

func (l *queryListener) ExitInteger(c *parser.IntegerContext) {
	tuple := codegen.GoExpression{
		Code: c.GetText(),
		Kind: codegen.Integer,
	}
	l.push(tuple)
}

func (l *queryListener) ExitString(c *parser.StringContext) {
	tuple := codegen.GoExpression{
		Code: c.GetText(),
		Kind: codegen.String,
	}
	l.push(tuple)
}

func (l *queryListener) ExitVariable(c *parser.VariableContext) {
	var node *grizzly.Node
	switch l.filterType {
	case codegen.IngressFilterType:
		node = l.ingressNode()
	case codegen.AggregateFilterType:
		node = l.aggregateNode()
	case codegen.ProjectFilterType:
		node = l.projectNode()
	default:
		panic(fmt.Errorf("unknown filter type: %v", l.filterType))
	}

	var fields capnp.StructList[grizzly.Field]
	var err error
	if fields, err = node.Fields(); err != nil {
		panic(err)
	}

	foundVariable := false
	isTimeType := false
	variableName := c.GetText()
	for i := 0; i < fields.Len(); i++ {
		field := fields.At(i)
		var name string
		if name, err = field.Name(); err != nil {
			panic(err)
		}
		if name == variableName {
			foundVariable = true
			if field.Usage() == grizzly.FieldUsage_time {
				isTimeType = true
			}
			break
		}
	}
	if !foundVariable {
		var label string
		if label, err = node.Label(); err != nil {
			panic(err)
		}
		panic(fmt.Errorf("could not find variable name %v in node %v", variableName, label))
	}

	var kind codegen.Kind
	if isTimeType {
		kind = codegen.Timestamp
	} else {
		kind = codegen.Variable // any other type
	}

	tuple := codegen.GoExpression{
		Code: codegen.GoCodeVariablePrefix + "." + c.GetText(),
		Kind: kind,
	}
	l.push(tuple)
}

func (l *queryListener) ExitTimestamp(c *parser.TimestampContext) {
	variable := "timestamp" + strconv.Itoa(l.goCode.VariableCounter)
	l.goCode.VariableCounter++

	tuple := codegen.GoExpression{
		Code: variable,
		Kind: codegen.Timestamp,
	}
	l.push(tuple)

	s := c.GetText()
	s = "\"" + s[1:len(s)-1] + "\"" // replace single-quotes with double-quotes
	head := "var " + variable + " time.Time\n"
	head += "if " + variable + ", err = time.Parse(time.RFC3339Nano, " + s + "); err != nil {\n"
	head += "\tpanic(err)\n"
	head += "}\n"

	l.goCode.Definitions = append(l.goCode.Definitions, head)
}

func (l *queryListener) ExitDistance(ctx *parser.DistanceContext) {
	l.sliceIntervalTypeIsDistance = true
}

func (l *queryListener) ExitDuration(ctx *parser.DurationContext) {
	quantity := ctx.GetAmount().GetText()
	unit := ctx.GetUnit().GetText()

	var timeUnit string
	switch unit {
	case "minutes":
		timeUnit = "time.Minute"
	case "seconds":
		timeUnit = "time.Second"
	default:
		panic(fmt.Errorf("unknown time unit: %v", unit))
	}

	variable := "duration" + strconv.Itoa(l.goCode.VariableCounter)
	l.goCode.VariableCounter++

	tuple := codegen.GoExpression{
		Code: variable,
		Kind: codegen.Duration,
	}
	l.push(tuple)

	head := variable + " := time.Duration(" + quantity + " * " + timeUnit + ")\n"
	l.goCode.Definitions = append(l.goCode.Definitions, head)
}

func (l *queryListener) ExitFromClause(ctx *parser.FromClauseContext) {
	node := l.ingressNode()

	// Look up fields from catalog
	l.inputTableFullName = ctx.TableName().GetText()
	var msg *capnp.Message
	var table grizzly.Table
	var err error
	if msg, table, err = catalog.FindTable(CatalogFilePath, l.inputTableFullName); err != nil {
		panic(err)
	}
	// FIXME: Why do I need to read msg?
	log.Info().Msgf("ExitFromClause: msg: %v", msg)

	var fields capnp.StructList[grizzly.Field]
	if fields, err = table.Fields(); err != nil {
		panic(err)
	}

	if err = node.SetFields(fields); err != nil {
		panic(err)
	}

	//
	// Add details for the WHERE clause
	//
	copyFields(l.ingressNode(), l.ingressFilterNode())
	copyFields(l.ingressNode(), l.windowNode())
	l.filterType = codegen.IngressFilterType
}

func (l *queryListener) ExitGroupClause(ctx *parser.GroupClauseContext) {
	allGroups := ctx.Groups().AllGroupName()
	for i := 0; i < len(allGroups); i++ {
		group := allGroups[i]
		fieldName := group.GetText()
		l.groupFieldNames = append(l.groupFieldNames, fieldName)
	}

	node := l.ingressNode()

	var fields capnp.StructList[grizzly.Field]
	var err error
	if fields, err = node.Fields(); err != nil {
		panic(err)
	}

	var groupFields capnp.StructList[grizzly.Field]
	if groupFields, err = node.NewGroupFields(int32(len(l.groupFieldNames))); err != nil {
		panic(err)
	}

	for g, groupFieldName := range l.groupFieldNames {
		for i := 0; i < fields.Len(); i++ {
			field := fields.At(i)
			var name string
			if name, err = field.Name(); err != nil {
				panic(err)
			}
			if name == groupFieldName {
				if err = groupFields.Set(g, field); err != nil {
					panic(err)
				}
			}
		}
	}

	if err = node.SetGroupFields(groupFields); err != nil {
		panic(err)
	}
}

func (l *queryListener) ExitAggregations(ctx *parser.AggregationsContext) {
	aggregations := ctx.AllAggregation()
	n := len(aggregations)

	var err error
	if n != len(l.calls) {
		err = errors.New("number of aggregations are inconsistent")
		panic(err)
	}

	node := l.aggregateNode()

	var calls capnp.StructList[grizzly.Call]
	if calls, err = node.NewCalls(int32(n)); err != nil {
		panic(err)
	}
	for i, v := range l.calls {
		if err = calls.Set(i, v); err != nil {
			panic(err)
		}
	}
	if err = node.SetCalls(calls); err != nil {
		panic(err)
	}

	// Copy each outfield and add it to the fields of this node
	var fields capnp.StructList[grizzly.Field]
	if fields, err = node.NewFields(int32(calls.Len())); err != nil {
		panic(err)
	}

	if calls, err = node.Calls(); err != nil {
		panic(err)
	}
	for i := 0; i < calls.Len(); i++ {
		call := calls.At(i)
		var field grizzly.Field
		if field, err = call.OutputField(); err != nil {
			panic(err)
		}
		if err = fields.Set(i, field); err != nil {
			panic(err)
		}
	}

	if node.SetFields(fields); err != nil {
		panic(err)
	}

	l.capnpCode.Body += codegen.CapnpStructAggregateRow(fields)
	l.capnpCode.Body += codegen.CapnpStructEgressRow(fields)
}

func (l *queryListener) EnterAggregateClause(ctx *parser.AggregateClauseContext) {
}

func (l *queryListener) ExitAggregateClause(ctx *parser.AggregateClauseContext) {
	copyFields(l.aggregateNode(), l.aggregateFilterNode())
	l.filterType = codegen.AggregateFilterType
}

func (l *queryListener) EnterAggregation(ctx *parser.AggregationContext) {
	l.aggregateAliasFieldName = ctx.FieldName().GetText()
}

func (l *queryListener) ExitAggregateAverage(ctx *parser.AggregateAverageContext) {
	outputType := grizzly.FieldType_float64
	l.addAggregateFunction("average", ctx.FieldName().GetText(), &outputType)
}

func (l *queryListener) ExitAggregateCount(ctx *parser.AggregateCountContext) {
	outputType := grizzly.FieldType_integer64
	//l.addAggregateFunction("count", ctx.FieldName().GetText(), &outputType)
	l.addAggregateFunction("count", "N/A -- count(*)", &outputType)
}

func (l *queryListener) ExitAggregateSum(ctx *parser.AggregateSumContext) {
	l.addAggregateFunction("sum", ctx.FieldName().GetText(), nil)
}

func (l *queryListener) ExitAggregateFirst(ctx *parser.AggregateFirstContext) {
	l.addAggregateFunction("first", ctx.FieldName().GetText(), nil)
}

func (l *queryListener) ExitAggregateLast(ctx *parser.AggregateLastContext) {
	l.addAggregateFunction("last", ctx.FieldName().GetText(), nil)
}

func (l *queryListener) ExitSequenceFieldClause(ctx *parser.SequenceFieldClauseContext) {
	l.sequenceFieldName = ctx.FieldName().GetText()
}

// window session begin when c == "a" end when c == "b" expire after 5 sesonds
// window slice 2 seconds
func (l *queryListener) ExitSessionOpen(ctx *parser.SessionOpenContext) {
	l.sessionOpenTuple = l.pop()

	code := l.sessionOpenTuple.Code
	l.goCode.SessionOpen.Condition = code
	//SetWindowNodeProperties(l.windowNode(), "session", "N/A", "N/A", "N/A")
}

func (l *queryListener) ExitSessionClose(ctx *parser.SessionCloseContext) {
	l.sessionCloseTuple = l.pop()

	code := l.sessionCloseTuple.Code
	l.goCode.SessionClose.Condition = code

	var sessionCloseInclusive string
	switch ctx.GetClusivity().GetTokenType() {
	case parser.UQLParserINCLUSIVE:
		sessionCloseInclusive = "true"
	case parser.UQLParserEXCLUSIVE:
		sessionCloseInclusive = "false"
	default:
		panic(fmt.Errorf("unexpected clusivity: %s", ctx.GetClusivity().GetText()))
	}

	SetWindowNodeProperties(l.windowNode(), "session", "N/A", "N/A", "N/A", sessionCloseInclusive, l.sequenceFieldName)
}

func (l *queryListener) EnterSessionWindow(ctx *parser.SessionWindowContext) {
	l.hasSessionWindow = true
}

func (l *queryListener) ExitSessionWindow(ctx *parser.SessionWindowContext) {
	l.goCode.Definitions = []string{} // flush the list
}

func (l *queryListener) ExitSliceWindow(ctx *parser.SliceWindowContext) {
	if l.sliceIntervalTypeIsDistance {
		distance := ctx.Distance()

		windowType := WindowTypeSlice
		intervalType := IntervalTypeDistance
		intervalAmount := distance.GetAmount().GetText()
		intervalUnit := distance.GetUnit().GetText()
		sessionCloseInclusive := "false"

		SetWindowNodeProperties(l.windowNode(), windowType, intervalType, intervalAmount, intervalUnit, sessionCloseInclusive, l.sequenceFieldName)
	} else {
		durationText := l.pop() // flush the stack
		theList := l.goCode.Definitions
		log.Printf("ExitSliceWindowClause: durationText: %v", durationText)
		log.Printf("ExitSliceWindowClause: theList: %v", theList)
		l.goCode.Definitions = []string{} // flush the list

		duration := ctx.Duration()
		intervalAmount := duration.GetAmount().GetText()
		intervalUnit := duration.GetUnit().GetText()
		var intervalType string
		switch intervalUnit {
		case "milliseconds", "seconds", "minutes":
			intervalType = IntervalTypeTime
		case "rows":
			intervalType = IntervalTypeDistance
		default:
			panic(fmt.Errorf("cannot find appropriate interval type for unit: %v", intervalUnit))
		}
		sessionCloseInclusive := "false"
		windowType := WindowTypeSlice

		SetWindowNodeProperties(l.windowNode(), windowType, intervalType, intervalAmount, intervalUnit, sessionCloseInclusive, l.sequenceFieldName)
	}
}

func SetWindowNodeProperties(
	windowNode *grizzly.Node,
	windowType string,
	intervalType string,
	intervalAmount string,
	intervalUnit string,
	sessionCloseInclusive string,
	sequenceFieldName string) {

	var properties capnp.StructList[grizzly.OperatorProperty]
	var err error
	if properties, err = windowNode.NewProperties(7); err != nil {
		panic(err)
	}

	property := properties.At(0)
	property.SetKey(WindowType)
	property.SetValue(windowType)
	if err = properties.Set(0, property); err != nil {
		panic(err)
	}

	property = properties.At(1)
	property.SetKey(IntervalType)
	property.SetValue(intervalType)
	if err = properties.Set(1, property); err != nil {
		panic(err)
	}

	property = properties.At(2)
	property.SetKey(IntervalAmount)
	property.SetValue(intervalAmount)
	if err = properties.Set(2, property); err != nil {
		panic(err)
	}

	property = properties.At(3)
	property.SetKey(IntervalUnit)
	property.SetValue(intervalUnit)
	if err = properties.Set(3, property); err != nil {
		panic(err)
	}

	property = properties.At(5)
	property.SetKey(SessionCloseInclusive)
	property.SetValue(sessionCloseInclusive)
	if err = properties.Set(5, property); err != nil {
		panic(err)
	}

	property = properties.At(6)
	property.SetKey(SequenceFieldName)
	property.SetValue(sequenceFieldName)
	if err = properties.Set(6, property); err != nil {
		panic(err)
	}

	if err = windowNode.SetProperties(properties); err != nil {
		panic(err)
	}
}

func (l *queryListener) EnterAppendClause(ctx *parser.AppendClauseContext) {
	allProjections := ctx.Projections().AllProjectionName()

	node := l.projectNode()
	var fields capnp.StructList[grizzly.Field]
	var err error
	if fields, err = node.NewFields(int32(len(allProjections))); err != nil {
		panic(err)
	}

	for i := 0; i < len(allProjections); i++ {
		projection := allProjections[i]
		fieldName := projection.GetText()

		var otherFields capnp.StructList[grizzly.Field]
		if otherFields, err = l.aggregateFilterNode().Fields(); err != nil {
			panic(err)
		}
		for j := 0; j < otherFields.Len(); j++ {
			otherField := otherFields.At(j)
			newField := fields.At(i)

			var name string
			if name, err = otherField.Name(); err != nil {
				panic(err)
			}

			if name == fieldName {
				if err = newField.SetName(name); err != nil {
					panic(err)
				}
				newField.SetType(otherField.Type())

				if err = fields.Set(i, newField); err != nil {
					panic(err)
				}
			}
		}
	}

	if err = node.SetFields(fields); err != nil {
		panic(err)
	}
}

func (l *queryListener) ExitAppendClause(ctx *parser.AppendClauseContext) {
	copyFields(l.projectNode(), l.projectFilterNode())
	l.filterType = codegen.ProjectFilterType
}

func (l *queryListener) EnterToClause(ctx *parser.ToClauseContext) {
	copyFields(l.projectFilterNode(), l.egressNode())
}

func (l *queryListener) ExitWhereClause(ctx *parser.WhereClauseContext) {
	code := l.pop().Code

	switch l.filterType {
	case codegen.IngressFilterType:
		l.goCode.IngressFilter.Condition = code //codegen.GoCondition("Ingress", l.list, code)
	case codegen.AggregateFilterType:
		l.goCode.AggregateFilter.Condition = code //codegen.GoCondition("Aggregate", l.list, code)
	case codegen.ProjectFilterType:
		l.goCode.ProjectFilter.Condition = code //codegen.GoCondition("Project", l.list, code)
	default:
		panic(fmt.Errorf("unknown filter type: %v", l.filterType))
	}
}

func (l *queryListener) addAggregateFunction(functionName string, inputFieldName string, outputType *grizzly.FieldType) {
	var function grizzly.Function
	var err error
	if function, err = grizzly.NewFunction(l.queryPlan.seg); err != nil {
		log.Error().Err(err)
		panic(err)
	}
	function.SetIsAggregate(true)
	function.SetIsBuiltIn(true)
	function.SetName(functionName)

	var msg *capnp.Message
	var field grizzly.Field
	if msg, field, err = catalog.FindField(CatalogFilePath, l.inputTableFullName, inputFieldName); err != nil {
		panic(err)
	}
	inputFieldType := field.Type()
	// FIXME: Why do I need to read msg?
	log.Info().Msgf("ExitWhereClause: msg: %v", msg)

	var outputFieldType grizzly.FieldType
	if outputType != nil {
		// This is mostly for the "average" function:  The average output should always be float64,
		// independent if the input is an integer or a float.
		// E.g. avg{1, 4} = 2.5 (a float), avg{1.5, 1.7} = 1.6 (a float as well)
		outputFieldType = *outputType
	} else {
		outputFieldType = inputFieldType
	}
	function.SetOutputType(outputFieldType)

	if err = function.SetOutputName(l.aggregateAliasFieldName); err != nil {
		panic(err)
	}

	var inputFieldTypes capnp.EnumList[grizzly.FieldType]
	if inputFieldTypes, err = grizzly.NewFieldType_List(l.queryPlan.seg, 1); err != nil {
		panic(err)
	}
	inputFieldTypes.Set(0, inputFieldType)
	if err = function.SetInputTypes(inputFieldTypes); err != nil {
		panic(err)
	}

	var inputFields capnp.StructList[grizzly.Field]
	if inputFields, err = grizzly.NewField_List(l.queryPlan.seg, 1); err != nil {
		panic(err)
	}
	if err = inputFields.Set(0, field); err != nil {
		panic(err)
	}

	var call grizzly.Call
	if call, err = grizzly.NewCall(l.queryPlan.seg); err != nil {
		panic(err)
	}
	if err = call.SetInputFields(inputFields); err != nil {
		panic(err)
	}
	if err = call.SetFunction(function); err != nil {
		panic(err)
	}

	var outputField grizzly.Field
	if outputField, err = call.NewOutputField(); err != nil {
		panic(err)
	}
	if err = outputField.SetName(l.aggregateAliasFieldName); err != nil {
		panic(err)
	}
	outputField.SetType(outputFieldType)
	if err = call.SetOutputField(outputField); err != nil {
		panic(err)
	}

	l.calls = append(l.calls, call)
}

func copyFields(from *grizzly.Node, to *grizzly.Node) {
	if !from.HasFields() {
		return
	}

	var err error
	var oldFields, newFields capnp.StructList[grizzly.Field]

	if oldFields, err = from.Fields(); err != nil {
		panic(err)
	}
	if newFields, err = to.NewFields(int32(oldFields.Len())); err != nil {
		panic(err)
	}

	copyFieldsHelper(&oldFields, &newFields)

	if err = to.SetFields(newFields); err != nil {
		panic(err)
	}
}

func copyGroupFields(from *grizzly.Node, to *grizzly.Node) {
	if !from.HasGroupFields() {
		return
	}

	var err error
	var oldFields, newFields capnp.StructList[grizzly.Field]

	if oldFields, err = from.GroupFields(); err != nil {
		panic(err)
	}
	if newFields, err = to.NewGroupFields(int32(oldFields.Len())); err != nil {
		panic(err)
	}

	copyFieldsHelper(&oldFields, &newFields)

	if err = to.SetGroupFields(newFields); err != nil {
		panic(err)
	}
}

func copyFieldsHelper(oldFields *capnp.StructList[grizzly.Field], newFields *capnp.StructList[grizzly.Field]) {
	for i := 0; i < (*oldFields).Len(); i++ {
		oldField := (*oldFields).At(i)
		newField := (*newFields).At(i)

		newField.SetType(oldField.Type())
		newField.SetUsage(oldField.Usage())

		var err error
		var name string
		if name, err = oldField.Name(); err != nil {
			panic(err)
		}
		if err = newField.SetName(name); err != nil {
			panic(err)
		}

		var oldProperties, newProperties capnp.StructList[grizzly.FieldProperty]
		if oldProperties, err = oldField.Properties(); err != nil {
			panic(err)
		}
		if newProperties, err = newField.NewProperties(int32(oldProperties.Len())); err != nil {
			panic(err)
		}
		for j := 0; j < oldProperties.Len(); j++ {
			oldProperty := oldProperties.At(j)
			newProperty := newProperties.At(j)

			if key, err := oldProperty.Key(); err != nil {
				panic(err)
			} else if err = newProperty.SetKey(key); err != nil {
				panic(err)
			}

			if value, err := oldProperty.Value(); err != nil {
				panic(err)
			} else if err = newProperty.SetValue(value); err != nil {
				panic(err)
			}
		}
		if err = newField.SetProperties(newProperties); err != nil {
			panic(err)
		}
	}
}
