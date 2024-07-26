package codegen

import (
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"

	"capnproto.org/go/capnp/v3"
	"github.com/rs/zerolog"
	"github.com/xsnout/grizzly/capnp/grizzly"
	"github.com/xsnout/grizzly/pkg/utility"
)

const (
	CapnpCodeFilePath    = "capnp/data/data.capnp"
	GoCodeFilePath       = "pkg/_out/functions/functions.go"
	GoCodeVariablePrefix = "p"
)

type CapnpCode struct {
	Body string
}

type GoCode struct {
	ExprStack       []GoExpression // Golang snippets to generate a single expression
	Definitions     []string       // Golang code literals definitions
	VariableCounter int

	IngressFilter   goCodeItem
	AggregateFilter goCodeItem
	ProjectFilter   goCodeItem
	SessionOpen     goCodeItem
	SessionClose    goCodeItem
}

type GoExpression struct {
	Code string
	Kind Kind
}

type Kind int

const (
	Boolean Kind = iota
	Duration
	Float
	Integer
	String
	Timestamp
	Variable
)

type goCodeItem struct {
	Imports     []string
	Types       []string
	Functions   []string
	Condition   string   // The actual expression, like "x.a >= y.b"
	Definitions []string // Any declarations needed for the Condition
}

func GoCodeCreateFile(code GoCode) {
	var imports []string
	imports = append(imports, goDefaultImports())
	imports = append(imports, code.IngressFilter.Imports...)
	imports = append(imports, code.AggregateFilter.Imports...)
	imports = append(imports, code.ProjectFilter.Imports...)
	imports = append(imports, code.SessionOpen.Imports...)
	imports = append(imports, code.SessionClose.Imports...)

	var types []string
	types = append(types, goFilterType())
	types = append(types, code.IngressFilter.Types...)
	types = append(types, code.AggregateFilter.Types...)
	types = append(types, code.ProjectFilter.Types...)
	types = append(types, code.SessionOpen.Types...)
	types = append(types, code.SessionClose.Types...)
	types = removeDuplicates[string](types)

	var functions []string
	functions = append(functions, goInitFunction())
	functions = append(functions, code.IngressFilter.Functions...)
	functions = append(functions, code.AggregateFilter.Functions...)
	functions = append(functions, code.ProjectFilter.Functions...)
	functions = append(functions, code.SessionOpen.Functions...)
	functions = append(functions, code.SessionClose.Functions...)
	functions = removeDuplicates[string](functions)

	imports = addTimeImportIfMissing(imports, types)
	imports = addTimeImportIfMissing(imports, functions)
	imports = removeDuplicates[string](imports)

	s := goPackage()
	s += strings.Join(imports[:], "\n")
	s += strings.Join(types[:], "\n")
	s += strings.Join(functions[:], "\n")

	bytes := []byte(s)
	var err error
	if err = os.WriteFile(GoCodeFilePath, bytes, 0644); err != nil {
		panic(err)
	}
}

func addTimeImportIfMissing(imports []string, code []string) []string {
	found := false
	for _, v := range code {
		if strings.Contains(v, "time.Time") {
			found = true
			break
		}
	}
	if found {
		imports = append(imports, "import \"time\"")
	}
	return imports
}

func removeDuplicates[T comparable](values []T) (result []T) {
	allKeys := make(map[T]bool)
	for _, value := range values {
		if _, key := allKeys[value]; !key {
			allKeys[value] = true
			result = append(result, value)
		}
	}
	return result
}

func goPackage() string {
	return `
package functions
`
}

func goDefaultImports() string {
	return `
import "os"
import "github.com/xsnout/grizzly/capnp/data"
import "github.com/rs/zerolog"
`
}

func goFilterType() string {
	return `
type Filterer interface {
  EvalIngressFilter(row data.IngressRow) (pass bool)
  EvalAggregateFilter(row data.AggregateRow) (pass bool)
  EvalSessionOpenFilter(row data.IngressRow) (pass bool)
  EvalSessionCloseFilter(row data.IngressRow) (pass bool)
  EvalProjectFilter(row data.EgressRow) (pass bool)
}

type Filter struct{}
`
}

func goInitFunction() string {
	return `
var (
	log zerolog.Logger
)

func init() {
	log = zerolog.New(os.Stderr).With().Caller().Timestamp().Logger()
}
`
}

type FilterType int

const (
	IngressFilterType FilterType = iota
	AggregateFilterType
	ProjectFilterType
)

var (
	log zerolog.Logger
)

func Init() {
	//zerolog.SetGlobalLevel(zerolog.Disabled)
	//zerolog.SetGlobalLevel(zerolog.InfoLevel)
	log = zerolog.New(os.Stderr).With().Caller().Timestamp().Logger()
}

func GoInternalPayload(nodeName string, node *grizzly.Node, operatorType grizzly.OperatorType, rootNode *grizzly.Node) (code string) {
	code += "type Internal" + nodeName + "Payload struct {\n"

	var err error
	var fields capnp.StructList[grizzly.Field]
	if fields, err = node.Fields(); err != nil {
		panic(err)
	}

	var name string
	for i := 0; i < fields.Len(); i++ {
		if name, err = fields.At(i).Name(); err != nil {
			panic(err)
		}
		field := fields.At(i)
		catalogUsage := field.Usage()
		goType := FindCatalogFieldType(rootNode, name, operatorType)
		code += GoFieldDeclaration(name, goType, catalogUsage)
	}

	code += "}\n"
	return
}

func GoUnusedInternalPayload(name string) (code string) {
	code += "// Internal" + name + "Payload is never called because the query has no " + name + " filter.\n"
	code += "type Internal" + name + "Payload struct {}\n"
	return
}

func GoTranslate(nodeName string, node *grizzly.Node, root *grizzly.Node) (code string) {
	var fields capnp.StructList[grizzly.Field]
	var err error
	if fields, err = node.Fields(); err != nil {
		panic(err)
	}

	code += "func Translate" + nodeName + "Payload(in data." + nodeName + "Payload) (out Internal" + nodeName + "Payload) {\n"

	var name string
	for i := 0; i < fields.Len(); i++ {
		if name, err = fields.At(i).Name(); err != nil {
			panic(err)
		}
		field := fields.At(i)
		catalogUsage := field.Usage()
		goType := FindCatalogFieldType(root, name, node.Type())
		code += GoFieldMapping(name, goType, catalogUsage) + "\n"
	}

	code += "return\n"
	code += "}\n"

	return
}

func GoFilter(filterName string, payloadName string) (code string) {
	code += "func (f *Filter) Eval" + filterName + "Filter(row data." + payloadName + "Row) (pass bool) {\n"
	code += "var err error\n"
	code += "var payload data." + payloadName + "Payload\n"
	code += "if payload, err = row.Payload(); err != nil {\n"
	code += "panic(err)\n"
	code += "}\n"
	code += "internalPayload := Translate" + payloadName + "Payload(payload)\n"
	code += "pass = eval" + filterName + "Filter(internalPayload)\n"
	code += "return\n"
	code += "}\n"
	return
}

func GoEval(filterName string, payloadName string, list []string, body string) (code string) {
	var header string
	foundErr := false
	for _, v := range list {
		if strings.Contains(v, "err != nil") {
			foundErr = true
		}
		header += v
	}
	if foundErr {
		header = "var err error\n" + header
	}

	code = "\nfunc eval" + filterName + "Filter(" + GoCodeVariablePrefix + " Internal" + payloadName + "Payload) (pass bool) {\n"
	code += header
	code += "pass = " + body + "\n"
	code += "return\n"
	code += "}\n"

	return
}

func GoPassthroughEvalFunction(filterName string, payloadName string) (code string) {
	code += "// eval" + filterName + "Filter never blocks a row in the " + filterName + " filter.\n"
	code += "func eval" + filterName + "Filter(payload Internal" + payloadName + "Payload) (pass bool) {\n"
	code += "return true\n"
	code += "}\n"
	return
}

func GoFieldDeclaration(fieldName string, fieldType grizzly.FieldType, fieldUsage grizzly.FieldUsage) (code string) {
	var goTypeName string
	switch fieldType {
	case grizzly.FieldType_boolean:
		goTypeName = "bool"
	case grizzly.FieldType_float64:
		goTypeName = "float64"
	case grizzly.FieldType_integer64:
		goTypeName = "int64"
	case grizzly.FieldType_text:
		if fieldUsage == grizzly.FieldUsage_time {
			goTypeName = "time.Time"
		} else {
			goTypeName = "string"
		}
	default:
		panic(fmt.Errorf("cannot find field type %v", fieldType))
	}

	return fieldName + " " + goTypeName + "\n"
}

func GoFieldMapping(fieldName string, fieldType grizzly.FieldType, fieldUsage grizzly.FieldUsage) (code string) {
	methodName := utility.UpcaseFirstLetter(fieldName) + "()"
	switch fieldType {
	case grizzly.FieldType_boolean, grizzly.FieldType_float64, grizzly.FieldType_integer64:
		code = "out." + fieldName + " = in." + methodName
	case grizzly.FieldType_text:
		if fieldUsage == grizzly.FieldUsage_time {
			code = "if value, err := in." + methodName + "; err != nil {\n"
			code += "panic(err)\n"
			code += "} else if out." + fieldName + ", err = time.Parse(time.RFC3339Nano, value); err != nil {\n"
			code += "panic(err)\n"
			code += "}"
		} else {
			code = "if value, err := in." + methodName + "; err != nil {\n"
			code += "panic(err)\n"
			code += "} else {\n"
			code += "out." + fieldName + " = value\n"
			code += "}"
		}
	default:
		panic(fmt.Errorf("cannot find field type %v", fieldType))
	}
	return
}

func CapnpCreateDataFile(code CapnpCode) {
	code.Body = CapnpDataCodePreamble() + code.Body
	bytes := []byte(code.Body)
	var err error
	if err = os.WriteFile(CapnpCodeFilePath, bytes, 0644); err != nil {
		panic(err)
	}
}

func CapnpStructGroup(rootNode *grizzly.Node, fields capnp.StructList[grizzly.Field], fieldNames []string) (code string) {
	code += "\nstruct Group {\n"
	var name string
	var err error
	for i, fieldName := range fieldNames {
		for j := 0; j < fields.Len(); j++ {
			if name, err = fields.At(j).Name(); err != nil {
				panic(err)
			}
			if fieldName == name {
				theType := FindCatalogFieldType(rootNode, name, grizzly.OperatorType_ingress)
				code += CapnpFieldDeclaration(fieldName, i, theType, 1)
			}
		}
	}

	code += "}\n"
	return
}

func CapnpStructIngressRow(rootNode *grizzly.Node, fields capnp.StructList[grizzly.Field]) (code string) {
	code += "\nstruct IngressRow {\n"
	code += "\tgroup @0 :Group;\n"
	code += "\tpayload @1 :IngressPayload;\n"
	code += "}\n"
	code += "\nstruct IngressPayload {\n"
	var name string
	var err error
	for i := 0; i < fields.Len(); i++ {
		if name, err = fields.At(i).Name(); err != nil {
			panic(err)
		}
		catalogType := fields.At(i).Type().String()
		log.Printf("1 %v (%v)", name, catalogType)
		capnpType := FindCatalogFieldType(rootNode, name, grizzly.OperatorType_ingress)
		log.Printf("2 %v (%v)", name, capnpType)
		code += CapnpFieldDeclaration(name, i, capnpType, 1)
	}
	code += "}\n"

	return
}

func CapnpStructAggregateRow(fields capnp.StructList[grizzly.Field]) (code string) {
	code += "\nstruct AggregateRow {\n"
	code += "\tgroup @0 :Group;\n"
	code += "\tpayload @1 :AggregatePayload;\n"
	code += "}\n"
	code += "\nstruct AggregatePayload {\n"
	for i := 0; i < fields.Len(); i++ {
		field := fields.At(i)
		var name string
		var err error
		if name, err = field.Name(); err != nil {
			panic(err)
		}
		typ := field.Type()
		code += CapnpFieldDeclaration(name, i, typ, 1)
	}
	code += "}\n"
	return
}

func CapnpStructEgressRow(fields capnp.StructList[grizzly.Field]) (code string) {
	code += "\nstruct EgressRow {\n"
	code += "\tgroup @0 :Group;\n"
	code += "\tpayload @1 :EgressPayload;\n"
	code += "}\n"
	code += "\nstruct EgressPayload {\n"
	for i := 0; i < fields.Len(); i++ {
		field := fields.At(i)
		var name string
		var err error
		if name, err = field.Name(); err != nil {
			panic(err)
		}
		typ := field.Type()
		code += CapnpFieldDeclaration(name, i, typ, 1)
	}
	code += "}\n"

	return
}

func CapnpDataCodePreamble() (code string) {
	code = fmt.Sprintf("using Go = import \"/go.capnp\";\n%s;\n$Go.package(\"data\");\n$Go.import(\"github.com/xsnout/grizzly/capnp/data\");\n", utility.CreateCapnpId())
	return
}

func CapnpFieldDeclaration(fieldName string, index int, fieldType grizzly.FieldType, indent int) string {
	var typ string
	switch fieldType {
	case grizzly.FieldType_boolean:
		typ = "Bool"
	case grizzly.FieldType_float64:
		typ = "Float64"
	case grizzly.FieldType_integer64:
		typ = "Int64"
	case grizzly.FieldType_text:
		typ = "Text"
	default:
		panic(errors.New("cannot find field type"))
	}

	indentation := ""
	for i := 0; i < indent; i++ {
		indentation += "\t"
	}

	return indentation + fieldName + " @" + strconv.Itoa(index) + " :" + typ + ";\n"
}

func FindCatalogFieldType(rootNode *grizzly.Node, name string, operatorType grizzly.OperatorType) (typ grizzly.FieldType) {
	log.Info().Msgf("FindCatalogFieldType: name: %v\n", name)

	// Find the ingressNode node that has the information about all fields
	var ingressNode *grizzly.Node
	var ok bool
	if ingressNode, ok = utility.FindFirstNodeByType(rootNode, operatorType); !ok {
		panic(errors.New("cannot find field type"))
	}

	// Go through the list of fields
	var fields capnp.StructList[grizzly.Field]
	var err error
	if fields, err = ingressNode.Fields(); err != nil {
		panic(err)
	}
	for i := 0; i < fields.Len(); i++ {
		field := fields.At(i)
		var fieldName string
		if fieldName, err = field.Name(); err != nil {
			panic(err)
		}
		//zlog.Printf("getFieldType 1: (%v,%v)", name, fieldName)
		if fieldName == name {
			return field.Type()
		}
	}
	panic(errors.New("cannot find desired field"))
}
