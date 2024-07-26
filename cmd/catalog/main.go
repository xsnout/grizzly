package main

import (
	"fmt"
	"os"

	"github.com/xsnout/grizzly/pkg/catalog"
)

func main() {
	args := os.Args
	if len(args) != 7 {
		err := fmt.Errorf("missing arguments")
		fmt.Println(err)
		return
	} else if args[1] != "-i" && args[3] != "-o" && args[5] != "-t" {
		err := fmt.Errorf("must specify input and output and CSV template file path")
		fmt.Println(err)
		return
	}

	c := catalog.NewCatalog(os.Stdin, os.Stdout)

	switch args[2] {
	case "capnp":
		c.ReadCapnp()
	case "json":
		c.ReadJson()
	case "example":
		catalog.Example()
	}

	csvTemplateFilePath := args[6]

	switch args[4] {
	case "capnp":
		c.WriteCapnp(csvTemplateFilePath)
	case "json":
		c.WriteJson()
	}

	// args := os.Args
	// if len(args) != 2 {
	// 	err := fmt.Errorf("missing argument; valid arguments: write")
	// 	fmt.Println(err)
	// 	return
	// } else if args[1] != "read" && args[1] != "write" && args[1] != "example" {
	// 	err := fmt.Errorf("wrong argument; valid arguments: read, write, example")
	// 	fmt.Println(err)
	// 	return
	// }

	// var c catalog.Catalog
	// switch args[1] {
	// case "read":
	// 	c.ReadCapnp(os.Stdin)
	// case "write":
	// 	c.WriteCapnp(os.Stdout)
	// case "example":
	// 	catalog.Example()
	// }
}
