package plan

import (
	"capnproto.org/go/capnp/v3"
	"github.com/xsnout/grizzly/capnp/grizzly"
)

// Used to print a JSON version of the execution plan
type PlanNode struct {
	Id                 int64                  `json:"id"`
	Label              string                 `json:"label"`
	Description        string                 `json:"description"`
	Type               string                 `json:"type"`
	Fields             []PlanField            `json:"fields"`
	GroupFields        []PlanField            `json:"groupFields"`
	OperatorProperties []PlanOperatorProperty `json:"properties"`
	Calls              []PlanCall             `json:"calls"`
	Children           []PlanNode             `json:"children"`
}

type PlanField struct {
	Name        string `json:"name"`
	Description string `json:"description"`
	Type        string `json:"type"`
	Usage       string `json:"usage"`
}

type PlanCall struct {
	Function    PlanFunction `json:"function"`
	InputFields []PlanField  `json:"infields"`
	OutputField PlanField    `json:"outfield"`
}

type PlanFunction struct {
	Id          int64    `json:"id"`
	Name        string   `json:"name"`
	Description string   `json:"description"`
	IsAggregate bool     `json:"isAggregate"`
	IsBuiltIn   bool     `json:"isBuiltIn"`
	InputTypes  []string `json:"inputTypes"`
	OutputType  string   `json:"outputType"`
	OutputName  string   `json:"outputName"`
	LibraryPath string   `json:"libraryPath"`
	Properties  []string `json:"properties"`
}

type PlanOperatorProperty struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

func GrizzlyNodeToPlan(node grizzly.Node) (p PlanNode) {
	var err error
	p.Id = node.Id()
	if p.Label, err = node.Label(); err != nil {
		panic(err)
	}

	p.Type = node.Type().String()

	{
		var fields capnp.StructList[grizzly.Field]
		if fields, err = node.Fields(); err != nil {
			panic(err)
		}
		for i := 0; i < fields.Len(); i++ {
			field := fields.At(i)
			var name, typ, description, usage string

			if description, err = field.Description(); err != nil {
				panic(err)
			}
			if name, err = field.Name(); err != nil {
				panic(err)
			}
			typ = field.Type().String()
			usage = field.Usage().String()

			p.Fields = append(p.Fields, PlanField{
				Name:        name,
				Description: description,
				Type:        typ,
				Usage:       usage,
			})
		}
	}

	{
		var fields capnp.StructList[grizzly.Field]
		if fields, err = node.GroupFields(); err != nil {
			panic(err)
		}
		for i := 0; i < fields.Len(); i++ {
			field := fields.At(i)
			var name, typ, description, usage string

			if description, err = field.Description(); err != nil {
				panic(err)
			}
			if name, err = field.Name(); err != nil {
				panic(err)
			}
			typ = field.Type().String()
			usage = field.Usage().String()

			p.GroupFields = append(p.GroupFields, PlanField{
				Name:        name,
				Description: description,
				Type:        typ,
				Usage:       usage,
			})
		}
	}
	{
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

			var description string
			if description, err = function.Description(); err != nil {
				panic(err)
			}

			var libraryPath string
			if libraryPath, err = function.LibraryPath(); err != nil {
				panic(err)
			}

			var inputTypes capnp.EnumList[grizzly.FieldType]
			if inputTypes, err = function.InputTypes(); err != nil {
				panic(err)
			}

			var inTypes []string
			for i := 0; i < inputTypes.Len(); i++ {
				typ := inputTypes.At(i).String()
				inTypes = append(inTypes, typ)
			}

			var properties capnp.StructList[grizzly.FunctionProperty]
			if properties, err = function.Properties(); err != nil {
				panic(err)
			}
			var props []string
			for i := 0; i < properties.Len(); i++ {
				property := properties.At(i)
				props = append(props, property.String())
			}

			var outputName string
			if outputName, err = function.OutputName(); err != nil {
				panic(err)
			}

			planFunction := PlanFunction{
				Id:          function.Id(),
				Name:        name,
				Description: description,
				IsAggregate: function.IsAggregate(),
				IsBuiltIn:   function.IsBuiltIn(),
				InputTypes:  inTypes,
				OutputType:  function.OutputType().String(),
				OutputName:  outputName,
				LibraryPath: libraryPath,
				Properties:  props,
			}

			var inputFields capnp.StructList[grizzly.Field]
			if inputFields, err = calls.At(i).InputFields(); err != nil {
				panic(err)
			}
			var inFields []PlanField
			for j := 0; j < inputFields.Len(); j++ {
				field := inputFields.At(j)

				if name, err = field.Name(); err != nil {
					panic(err)
				}

				if description, err = field.Description(); err != nil {
					panic(err)
				}

				planField := PlanField{
					Name:        name,
					Type:        field.Type().String(),
					Description: description,
					Usage:       field.Usage().String(),
				}
				inFields = append(inFields, planField)
			}

			var outputField grizzly.Field
			if outputField, err = calls.At(i).OutputField(); err != nil {
				panic(err)
			}
			if name, err = outputField.Name(); err != nil {
				panic(err)
			}
			outField := PlanField{
				Name: name,
				Type: outputField.Type().String(),
			}

			p.Calls = append(
				p.Calls, PlanCall{
					Function:    planFunction,
					InputFields: inFields,
					OutputField: outField,
				})
		}
	}

	var properties capnp.StructList[grizzly.OperatorProperty]
	if properties, err = node.Properties(); err != nil {
		panic(err)
	}
	for i := 0; i < properties.Len(); i++ {
		var key string
		if key, err = properties.At(i).Key(); err != nil {
			panic(err)
		}

		var value string
		if value, err = properties.At(i).Value(); err != nil {
			panic(err)
		}

		p.OperatorProperties = append(p.OperatorProperties, PlanOperatorProperty{
			Key:   key,
			Value: value,
		})
	}

	// var fieldConstantConditions capnp.StructList[grizzly.FieldConstantCondition]
	// if fieldConstantConditions, err = node.FieldConstantConditions(); err != nil {
	// 	panic(err)
	// }
	// for i := 0; i < fieldConstantConditions.Len(); i++ {
	// 	var name, value string

	// 	if name, err = fieldConstantConditions.At(i).FieldName(); err != nil {
	// 		panic(err)
	// 	}
	// 	comparator := grizzlyComparatorToString(fieldConstantConditions.At(i).Comparator())

	// 	if value, err = fieldConstantConditions.At(i).Constant(); err != nil {
	// 		panic(err)
	// 	}
	// 	p.FieldConstantConditions = append(p.FieldConstantConditions, PlanFieldConstantCondition{
	// 		Name:       name,
	// 		Comparator: comparator,
	// 		Constant:   value,
	// 	})
	// }

	// fieldFieldCondition, err := node.FieldFieldConditions()
	// if err != nil {
	// 	panic(err)
	// }
	// for i := 0; i < fieldFieldCondition.Len(); i++ {
	// 	var name1, name2 string

	// 	if name1, err = fieldFieldCondition.At(i).FieldName1(); err != nil {
	// 		panic(err)
	// 	}
	// 	if name2, err = fieldFieldCondition.At(i).FieldName2(); err != nil {
	// 		panic(err)
	// 	}

	// 	comparator := grizzlyComparatorToString(fieldFieldCondition.At(i).Comparator())

	// 	p.FieldFieldConditions = append(p.FieldFieldConditions, PlanFieldFieldCondition{
	// 		Name1:      name1,
	// 		Comparator: comparator,
	// 		Name2:      name2,
	// 	})
	// }

	var children capnp.StructList[grizzly.Node]
	if children, err = node.Children(); err != nil {
		panic(err)
	}
	for i := 0; i < children.Len(); i++ {
		child := children.At(i)
		p.Children = append(p.Children, GrizzlyNodeToPlan(child))
	}

	return
}
