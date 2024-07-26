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
// stdin (UQL query)  --->  ./compiler compile  --->  stdout (binary Cap'n Proto stream)
//                                               |
//                                               +->  file with Cap'n Proto schemas (schemas.capnp)
//                                                    (this file is a side effect)
// Example:
//
//   echo "from table1 where x >= 5 project a, b" | ./compiler compile > ./plan.bin
//   cat ./plan.bin | ./compiler show | jq . | tee ./plan_pretty.json
//

package main

import (
	"errors"
	"fmt"
	"os"

	"github.com/rs/zerolog"

	_ "github.com/xsnout/grizzly/pkg/plan"
	"github.com/xsnout/grizzly/pkg/utility"

	"github.com/xsnout/grizzly/pkg/compiler"
)

func main() {
	log := zerolog.New(os.Stderr).With().Caller().Logger()
	log.Info().Msg("Compiler says welcome!")

	err := errors.New("unknown or missing argument\nusage: grizzlyc [compile|show]")

	if len(os.Args) != 2 {
		panic(err)
	}

	cmdArgs := os.Args[1]

	log.Info().Msgf("command used: %s", cmdArgs)

	compiler.Init()
	switch cmdArgs {
	case "compile":
		compiler.Compile()
	case "show":
		utility.ShowPlan()
	default:
		err := errors.New("unknown or missing argument/nusage: z [compile|show]")
		fmt.Print(err)
		log.Error().Err(err)
		os.Exit(0)
	}

	log.Info().Msg("Compiler says good-bye!")
}
