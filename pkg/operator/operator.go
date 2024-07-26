package operator

import (
	"fmt"
	"os"
	"reflect"
	"strconv"
	"time"

	"capnproto.org/go/capnp/v3"
	"github.com/rs/zerolog"
	"github.com/xsnout/grizzly/capnp/data"
	"github.com/xsnout/grizzly/capnp/grizzly"
	"github.com/xsnout/grizzly/pkg/compiler"
	"github.com/xsnout/grizzly/pkg/functor"
	"github.com/xsnout/grizzly/pkg/utility"
)

var (
	log zerolog.Logger
)

func Init() {
	//zerolog.SetGlobalLevel(zerolog.Disabled)
	//zerolog.SetGlobalLevel(zerolog.InfoLevel)
	log = zerolog.New(os.Stderr).With().Caller().Timestamp().Logger()
}

type Operator struct {
	OutputFieldNames        []string
	OutputFieldTypes        []grizzly.FieldType
	OutputFieldNamesToTypes map[string]grizzly.FieldType
	GroupFieldNames         []string
	GroupFieldTypes         []grizzly.FieldType
	GroupFieldNamesToTypes  map[string]grizzly.FieldType
}

func (s *Operator) Init(node *grizzly.Node) {
	var err error

	var fields capnp.StructList[grizzly.Field]
	if fields, err = node.Fields(); err != nil {
		panic(err)
	}

	s.OutputFieldNamesToTypes = make(map[string]grizzly.FieldType)
	for i := 0; i < fields.Len(); i++ {
		f := fields.At(i)
		var name string
		if name, err = f.Name(); err != nil {
			panic(err)
		}

		typ := f.Type()
		s.OutputFieldNames = append(s.OutputFieldNames, name)
		s.OutputFieldTypes = append(s.OutputFieldTypes, typ)
		s.OutputFieldNamesToTypes[name] = typ
	}

	var groupFields capnp.StructList[grizzly.Field]
	if groupFields, err = node.GroupFields(); err != nil {
		panic(err)
	}

	s.GroupFieldNamesToTypes = make(map[string]grizzly.FieldType)
	for i := 0; i < groupFields.Len(); i++ {
		f := groupFields.At(i)
		var name string
		if name, err = f.Name(); err != nil {
			panic(err)
		}

		typ := f.Type()
		s.GroupFieldNames = append(s.GroupFieldNames, name)
		s.GroupFieldTypes = append(s.GroupFieldTypes, typ)
		s.GroupFieldNamesToTypes[name] = typ
	}
}

type Filter struct {
	Operator

	conditions []Condition
	values     []string
	//connectors []grizzly.Connector
}

func (o *Filter) Init(node *grizzly.Node) {
	o.Operator.Init(node)

	conditions, err := node.FieldConstantConditions()
	if err != nil {
		panic(err)
	}
	for i := 0; i < conditions.Len(); i++ {
		c := conditions.At(i)
		fieldName, err := c.FieldName()
		if err != nil {
			panic(err)
		}
		value, err := c.Constant()
		if err != nil {
			panic(err)
		}
		o.values = append(o.values, value)

		o.conditions = append(o.conditions,
			Condition{
				fieldName:  fieldName,
				comparator: c.Comparator(),
				value:      value,
			},
		)
	}
}

type Condition struct {
	fieldName  string
	comparator grizzly.Comparator
	value      string
}

type Window struct {
	Operator

	WindowType               string
	IntervalField            string
	IntervalType             string
	IntervalUnit             string
	IntervalAmount           string
	SequenceField            string
	IntervalRows             int64
	TickerSeconds            float64
	SessionIncludeClosingRow bool // if true, the row that fulfills the END condition is added to the window
}

func (op *Window) Init(node *grizzly.Node) {
	op.Operator.Init(node)
	properties, err := node.Properties()
	if err != nil {
		panic(err)
	}

	if op.WindowType, err = properties.At(0).Value(); err != nil {
		panic(err)
	}
	if op.IntervalType, err = properties.At(1).Value(); err != nil {
		panic(err)
	}
	if op.IntervalAmount, err = properties.At(2).Value(); err != nil {
		panic(err)
	}
	if op.IntervalUnit, err = properties.At(3).Value(); err != nil {
		panic(err)
	}
	if op.IntervalField, err = properties.At(4).Value(); err != nil {
		panic(err)
	}
	if inclusiveText, err := properties.At(5).Value(); err != nil {
		panic(err)
	} else if op.SessionIncludeClosingRow, err = strconv.ParseBool(inclusiveText); err != nil {
		panic(err)
	}
	if op.SequenceField, err = properties.At(6).Value(); err != nil {
		panic(err)
	}

	switch op.IntervalType {
	case compiler.IntervalTypeTime:
		if op.TickerSeconds, err = strconv.ParseFloat(op.IntervalAmount, 64); err != nil {
			panic(err)
		}
	case compiler.IntervalTypeDistance:
		if op.IntervalRows, err = strconv.ParseInt(op.IntervalAmount, 10, 64); err != nil {
			panic(err)
		}
	case "N/A":
		// Do nothing; it's a session window.
	default:
		panic(fmt.Errorf("illegal interval type: %v", op.IntervalType))
	}
}

type Ingress struct {
	Operator
}

func (o *Ingress) Init(node *grizzly.Node) {
	o.Operator.Init(node)
}

func (o *Ingress) Ingress(record []string, row *data.IngressRow) {
	var err error

	var payload data.IngressPayload
	if payload, err = row.NewPayload(); err != nil {
		panic(err)
	}
	var group data.Group
	if group, err = row.NewGroup(); err != nil {
		panic(err)
	}

	for i := 0; i < len(record); i++ {
		theType := stringToType(record[i], o.OutputFieldTypes[i])
		InvokeWithParameters(payload, "Set"+utility.UpcaseFirstLetter(o.OutputFieldNames[i]), theType)

		for g := 0; g < len(o.GroupFieldNames); g++ {
			if o.GroupFieldNames[g] == o.OutputFieldNames[i] {
				InvokeWithParameters(group, "Set"+utility.UpcaseFirstLetter(o.GroupFieldNames[g]), theType)
				break
			}
		}
	}

	if err = row.SetPayload(payload); err != nil {
		panic(err)
	}
	if err = row.SetGroup(group); err != nil {
		panic(err)
	}
}

type Aggregate struct {
	Operator
	inputNames []string
	inputTypes []grizzly.FieldType
	functors   []functor.Functor
}

func (o *Aggregate) Init(node *grizzly.Node) {
	o.Operator.Init(node)

	var err error
	var calls capnp.StructList[grizzly.Call]
	if calls, err = node.Calls(); err != nil {
		panic(err)
	}

	for i := 0; i < calls.Len(); i++ {
		var function grizzly.Function
		if function, err = calls.At(i).Function(); err != nil {
			panic(err)
		}

		var name string
		if name, err = function.Name(); err != nil {
			panic(err)
		}

		var inputFields capnp.StructList[grizzly.Field]
		if inputFields, err = calls.At(i).InputFields(); err != nil {
			panic(err)
		}

		var inputName string
		if inputName, err = inputFields.At(0).Name(); err != nil {
			panic(err)
		}
		o.inputNames = append(o.inputNames, inputName)

		inputType := inputFields.At(0).Type()
		o.inputTypes = append(o.inputTypes, inputType)

		switch name {
		case "average":
			var f functor.Averager
			f.Init(&inputType)
			o.functors = append(o.functors, &f)
		case "count":
			var f functor.Counter
			f.Init(nil) // count() has no input field
			o.functors = append(o.functors, &f)
		case "distinctcount": // Similar to "unique" but precise
			var f functor.DistinctCounter
			f.Init(&inputType)
			o.functors = append(o.functors, &f)
		case "maximum":
			var f functor.Maximizer
			f.Init(&inputType)
			o.functors = append(o.functors, &f)
		case "minimum":
			var f functor.Minimizer
			f.Init(&inputType)
			o.functors = append(o.functors, &f)
		case "group":
			var f functor.NoOp
			f.Init(&inputType)
			o.functors = append(o.functors, &f)
		case "sum":
			var f functor.Summer
			f.Init(&inputType)
			o.functors = append(o.functors, &f)
		case "unique": // Similar to "distinctcount" but approximate due to use of a sketch
			var f functor.Uniquer
			f.Init(&inputType)
			o.functors = append(o.functors, &f)
		case "first":
			var f functor.First
			f.Init(&inputType)
			o.functors = append(o.functors, &f)
		case "last":
			var f functor.Last
			f.Init(&inputType)
			o.functors = append(o.functors, &f)
		default:
			panic(fmt.Errorf("unknown function name: %s", name))
		}
	}
}

func (o *Aggregate) Value(outRow *data.AggregateRow) {
	var err error
	var payload data.AggregatePayload
	if payload, err = outRow.NewPayload(); err != nil {
		panic(err)
	}

	for i := 0; i < len(o.OutputFieldNames); i++ {
		outputName := o.OutputFieldNames[i]
		outputType := o.OutputFieldTypes[i]

		value := o.functors[i].Value()

		switch outputType {
		case grizzly.FieldType_boolean:
			var v bool
			if v, err = strconv.ParseBool(fmt.Sprintf("%v", value)); err != nil {
				panic(err)
			}
			InvokeWithParameters(payload, "Set"+utility.UpcaseFirstLetter(outputName), v)
		case grizzly.FieldType_float64:
			var f float64
			if f, err = strconv.ParseFloat(fmt.Sprintf("%v", value), 64); err != nil {
				panic(err)
			}
			v := float64(f)
			InvokeWithParameters(payload, "Set"+utility.UpcaseFirstLetter(outputName), v)
		case grizzly.FieldType_integer64:
			var i int64
			if i, err = strconv.ParseInt(fmt.Sprintf("%v", value), 10, 64); err != nil {
				panic(err)
			}
			v := int64(i)
			InvokeWithParameters(payload, "Set"+utility.UpcaseFirstLetter(outputName), v)
		case grizzly.FieldType_text:
			//v := value.(string)
			v := fmt.Sprintf("%v", value)
			InvokeWithParameters(payload, "Set"+utility.UpcaseFirstLetter(outputName), v)
		default:
			panic(fmt.Errorf("cannot find field type %v", outputType))
		}
	}

	if err = outRow.SetPayload(payload); err != nil {
		panic(err)
	}
}

func (o *Aggregate) Update(inRow data.IngressRow) {
	var err error
	var payload data.IngressPayload
	if payload, err = inRow.Payload(); err != nil {
		panic(err)
	}

	for i := 0; i < len(o.inputNames); i++ {
		// Example: For "avg(foo) as avgFoo", "foo" is the inputName and "avgFoo" is the outputName.
		inputName := o.inputNames[i]
		getMethodName := utility.UpcaseFirstLetter(inputName)
		values := InvokeWithoutParameters(payload, getMethodName)
		value := typeCast(values[0], o.inputTypes[i])
		o.functors[i].Update(value)
	}
}

func (o *Aggregate) Reset() {
	for _, f := range o.functors {
		f.Reset()
	}
}

type Project struct {
	Operator
}

func (o *Project) Init(node *grizzly.Node) {
	o.Operator.Init(node)
}

func (o *Project) Project(inRow *data.AggregateRow, outRow *data.EgressRow) {
	var inPayload data.AggregatePayload
	var err error
	if inPayload, err = inRow.Payload(); err != nil {
		panic(err)
	}

	var outPayload data.EgressPayload
	if outPayload, err = outRow.NewPayload(); err != nil {
		panic(err)
	}

	for i := 0; i < len(o.OutputFieldNames); i++ {
		getMethodName := o.OutputFieldNames[i]
		values := InvokeWithoutParameters(inPayload, getMethodName)
		value := values[0]
		setMethodName := "Set" + utility.UpcaseFirstLetter(o.OutputFieldNames[i])
		arg := typeCast(value, o.OutputFieldTypes[i])
		InvokeWithParameters(outPayload, setMethodName, arg)
	}

	if err = outRow.SetPayload(outPayload); err != nil {
		panic(err)
	}

	var group data.Group
	if group, err = inRow.Group(); err != nil {
		panic(err)
	}
	if err = outRow.SetGroup(group); err != nil {
		panic(err)
	}
}

type Egress struct {
	Operator
}

func (o *Egress) Init(node *grizzly.Node) {
	o.Operator.Init(node)

	//o.Operator.
}

func stringToType(value string, t grizzly.FieldType) interface{} {
	switch t {
	case grizzly.FieldType_text:
		return value
	case grizzly.FieldType_boolean:
		b, err := strconv.ParseBool(value)
		if err != nil {
			panic(err)
		}
		return b
	case grizzly.FieldType_float64:
		f, err := strconv.ParseFloat(value, 64)
		if err != nil {
			panic(err)
		}
		return float64(f)
	case grizzly.FieldType_integer64:
		i, err := strconv.ParseInt(value, 10, 64)
		if err != nil {
			panic(err)
		}
		return int64(i)
	}
	panic(fmt.Errorf("cannot cast string value \"%s\" to type %s", value, t.String()))
}

func InvokeWithoutParameters(any interface{}, methodName string) []reflect.Value {
	value := reflect.ValueOf(any)
	upperCaseMethodName := utility.UpcaseFirstLetter(methodName)
	method := value.MethodByName(upperCaseMethodName)
	results := method.Call(nil)
	return results
}

func InvokeWithParameters(any interface{}, methodName string, args ...interface{}) []reflect.Value {
	if args == nil {
		log.Info().Msgf("InvokeWithParameters: args == nil: %v", args)
	}
	inputs := make([]reflect.Value, len(args))
	//log.Info().Msgf("Invoke: methodName: %v, len(args): %v, len(inputs): %v, inputs: %v\n", methodName, len(args), len(inputs), inputs)

	for i, arg := range args {
		inputs[i] = reflect.ValueOf(arg)
	}

	value := reflect.ValueOf(any)
	method := value.MethodByName(methodName)
	results := method.Call([]reflect.Value{reflect.ValueOf(args[0])}) // panic: reflect: call of reflect.Value.Call on zero Value

	return results
}

func typeCast(value reflect.Value, t grizzly.FieldType) interface{} {
	switch t {
	case grizzly.FieldType_boolean:
		return value.Bool()
	case grizzly.FieldType_float64:
		return value.Float()
	case grizzly.FieldType_integer64:
		return value.Int()
	case grizzly.FieldType_text:
		return value.String()
	}
	panic(fmt.Errorf("cannot find field type %v", t))
}

func Timestamp(ingressRow *data.IngressRow, timeFieldName string) (timestamp time.Time) {
	var payload data.IngressPayload
	var err error
	if payload, err = ingressRow.Payload(); err != nil {
		panic(err)
	}
	getMethodName := utility.UpcaseFirstLetter(timeFieldName)
	values := InvokeWithoutParameters(payload, getMethodName)
	value := fmt.Sprintf("%v", values[0])
	if timestamp, err = time.Parse(time.RFC3339Nano, value); err != nil {
		panic(err)
	}
	return
}

func Rowstamp(ingressRow *data.IngressRow, rowFieldName string) (rowstamp int) {
	var payload data.IngressPayload
	var err error
	if payload, err = ingressRow.Payload(); err != nil {
		panic(err)
	}
	getMethodName := utility.UpcaseFirstLetter(rowFieldName)
	values := InvokeWithoutParameters(payload, getMethodName)
	value := fmt.Sprintf("%v", values[0])
	if rowstamp, err = strconv.Atoi(value); err != nil {
		panic(err)
	}
	return
}
