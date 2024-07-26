package main

import (
	"bufio"
	"fmt"
	"os"
	"strconv"

	_ "net/http/pprof"

	engine "github.com/xsnout/grizzly/pkg/engine"
)

func main() {
	/*
		  go func() {
				mux := http.NewServeMux()

				mux.HandleFunc("/debug/pprof/", pprof.Index)
				mux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
				mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
				mux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
				mux.HandleFunc("/debug/pprof/trace", pprof.Trace)

				// ...

				server := &http.Server{
					Addr:    ":8081",
					Handler: mux,
				}
				server.ListenAndServe()
			}()
	*/

	args := os.Args
	var err error
	if len(args) != 5 {
		err = fmt.Errorf("missing arguments")
		fmt.Println(err)
		return
	} else if args[1] != "-p" {
		err = fmt.Errorf("must specify binary input plan file")
		fmt.Println(err)
		return
	} else if args[3] != "-x" {
		err = fmt.Errorf("must specify integer number of seconds")
		fmt.Println(err)
		return
	}

	planFilePath := args[2]
	var planFile *os.File

	if planFile, err = os.Open(planFilePath); err != nil {
		panic(err)
	}
	defer planFile.Close()
	planReader := bufio.NewReader(planFile)

	var exitAfterSeconds int
	if exitAfterSeconds, err = strconv.Atoi(args[4]); err != nil {
		panic(err)
	}

	//reader := bufio.NewReader(csvFile)
	dataReader := bufio.NewReader(os.Stdin)
	dataWriter := os.Stdout

	e := engine.NewEngine(dataReader, dataWriter, planReader, exitAfterSeconds)
	e.Run()
}
