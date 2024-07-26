##
## Call this Makefile as shown in run-example.sh
##

THROTTLE_MILLISECONDS  := 20
EXIT_AFTER_SECONDS     := 3600

REPO                   := github.com/xsnout/grizzly
EXAMPLES_DIR           := examples
CAPNP_DIR              := $(HOME)/git/capnproto
CATALOG_DIR            := cmd/catalog
COMPILER_DIR           := cmd/grizzlyc
ENGINE_DIR             := cmd/grizzly
PKG_OUT_DIR            := pkg/_out
QUERY_DIR              := $(PKG_OUT_DIR)/query
FUNCTIONS_DIR          := $(PKG_OUT_DIR)/functions
OUT_DIR                := _out
PLAN_DIR               := $(OUT_DIR)
CATALOG_OUT_DIR        := $(OUT_DIR)/catalog
CSV_DATA_DIR           := $(OUT_DIR)/csv_data
CSV_TEMPLATE_DIR       := $(OUT_DIR)/csv_templates
EXAMPLE_QUERY_PATH     := $(JOB_DIR)/query.uql
TEMPLATE_DIR           := templates

LOG                    := grizzly.log
CATALOGJ_MASTER        := $(JOB_DIR)/catalog.json
CATALOGB               := $(OUT_DIR)/catalog.bin
CATALOGJ               := $(OUT_DIR)/catalog.json
PLANB                  := $(PLAN_DIR)/plan.bin
PLANJ                  := $(PLAN_DIR)/plan.json
TABLE_NAME             := $(shell head -1 $(EXAMPLE_QUERY_PATH) | awk '{print $$2}')
JOB_DATA               := $(JOB_DIR)/$(TABLE_NAME).csv
JOB_ENGINE             := $(JOB_DIR)/grizzly
JOB_PLANB              := $(JOB_DIR)/plan.bin
JOB_PLANJ              := $(JOB_DIR)/plan.json
JOB_LOG                := $(JOB_DIR)/grizzly.log

ANTLRGEN               := BaseListener Lexer Listener Parser
GRAMMAR_QUERY          := UQL
OS_NAME                := $(shell uname -s)

CATALOG                := $(CATALOG_DIR)/catalog
COMPILER               := $(COMPILER_DIR)/grizzlyc
ENGINE                 := $(ENGINE_DIR)/grizzly

ifeq ($(OS_NAME), Darwin)
ANTLR4                 := antlr
else ifeq ($(OS_NAME), Linux)
ANTLR4                 := java -Xmx500m -cp "jars/antlr-4.13.1-complete.jar:CLASSPATH" org.antlr.v4.Tool
else
ANTLR4                 := "Unknown operating system name: $(OS_NAME)"
endif

all:
	./run-example.sh


build: clean prepare build_compiler build_engine

all_standalone: clean prepare build_compiler setup_example build_engine

again: clean_log mini_build run

setup_example:
	mkdir -p /tmp/jobs
	cp -r examples/$(EXAMPLE_NAME) /tmp/jobs

doc_run:
	go install golang.org/x/tools/cmd/godoc@latest
	godoc -http=:6060

doc_view:
	open http://localhost:6060/pkg/github.com/xsnout/grizzly/pkg/compiler/

mini_build:
	$(ANTLR4) -Dlanguage=Go -o $(QUERY_DIR) $(GRAMMAR_QUERY).g4
	go build -o $(CATALOG) $(CATALOG_DIR)/main.go
	go build -o $(COMPILER) $(COMPILER_DIR)/main.go

prepare:
	mkdir -p $(JOB_DIR)
	mkdir -p $(CAPNP_DIR)
	mkdir -p $(OUT_DIR)
	mkdir -p $(FUNCTIONS_DIR)
	mkdir -p $(QUERY_DIR)
	mkdir -p $(PLAN_DIR)
	mkdir -p $(CSV_DATA_DIR)
	mkdir -p $(CSV_TEMPLATE_DIR)
	go mod init $(REPO)
#	go mod tidy
	go get github.com/antlr4-go/antlr/v4
	go get zombiezen.com/go/capnproto2
	go get capnproto.org/go/capnp/v3
	go install capnproto.org/go/capnp/v3/capnpc-go@latest
	go get github.com/rs/zerolog
	go get github.com/DataDog/hyperloglog
	cd $(CAPNP_DIR); git clone https://github.com/capnproto/go-capnproto2.git
	cd capnp/grizzly; go generate
	go mod edit -require=$(REPO)/capnp/data@v0.0.0-unpublished
	go mod edit -replace=$(REPO)/capnp/data@v0.0.0-unpublished=./capnp/data

build_compiler:
	$(ANTLR4) -Dlanguage=Go -o $(QUERY_DIR) $(GRAMMAR_QUERY).g4
	cd $(QUERY_DIR); go mod init $(REPO)/$(QUERY_DIR); go mod tidy
	cd $(FUNCTIONS_DIR); go mod init $(REPO)/$(FUNCTIONS_DIR); go mod tidy
	go mod edit -require=$(REPO)/pkg/_out/query/parser@v0.0.0-unpublished
	go mod edit -replace=$(REPO)/pkg/_out/query/parser@v0.0.0-unpublished=./pkg/_out/query
	go mod edit -require=$(REPO)/pkg/_out/functions@v0.0.0-unpublished
	go mod edit -replace=$(REPO)/pkg/_out/functions@v0.0.0-unpublished=./pkg/_out/functions
	go build -o $(CATALOG) $(CATALOG_DIR)/main.go
	go build -o $(COMPILER) $(COMPILER_DIR)/main.go

build_engine:
	@cat $(CATALOGJ_MASTER) | $(CATALOG) -i json -o capnp -t $(CSV_TEMPLATE_DIR) 2>> $(LOG) > $(CATALOGB)
#	@cat $(CATALOGB) | $(CATALOG) -i capnp -o jmson -t $(CSV_TEMPLATE_DIR) 2>> $(LOG) | tee $(CATALOGJ) | jq '.' --tab
	@cat $(CATALOGB) | $(CATALOG) -i capnp -o json -t $(CSV_TEMPLATE_DIR) 2>> $(LOG) > $(CATALOGJ)
	mkdir -p $(PLAN_DIR)
	@cat $(EXAMPLE_QUERY_PATH) | $(COMPILER) compile > $(PLANB) 2>> $(LOG)
	cp $(PLANB) $(JOB_DIR)
	gofmt -w $(FUNCTIONS_DIR)/functions.go
	@cat $(PLANB) | $(COMPILER) show > $(PLANJ)
	cp $(PLANJ) $(JOB_DIR)
#	@cat $(PLANJ) | jq '.' --indent 4
	cd capnp/data; go generate
	go build $(FUNCTIONS_DIR)/functions.go
	go build -o $(ENGINE) $(ENGINE_DIR)/main.go
	cp $(ENGINE) $(JOB_DIR)
	go mod tidy

run:
#	@cat $(TABLE_NAME_CSV) | $(ENGINE) -p $(PLANB) 2>> $(LOG)
#	@cat $(JOB_DATA) | $(THROTTLE) --milliseconds 100 --append-timestamp false | $(ENGINE) -p $(PLANB) -x $(EXIT_AFTER_SECONDS) 2>> $(LOG)
	@cat $(JOB_DATA) | $(THROTTLE) --milliseconds 100 --append-timestamp false | $(JOB_ENGINE) -p $(JOB_PLANB) -x $(EXIT_AFTER_SECONDS) 2>> $(JOB_LOG)

rerun: # Don't remove the go.mod or install software again
	rm -f $(LOG)
	go build -o $(CATALOG) $(CATALOG_DIR)/main.go
	go build -o $(COMPILER) $(COMPILER_DIR)/main.go
	go build -o $(ENGINE) $(ENGINE_DIR)/main.go

run_syslog:
	$(SYSLOG) | $(ENGINE) -p $(PLANB) -x $(EXIT_AFTER_SECONDS) 2>> $(LOG)

# run_condition:
# 	$(CODEGEN_CONDITION) $(CONDITION)

run_fast:
	@cat $(JOB_DATA) | $(ENGINE) -p $(PLANB) -x $(EXIT_AFTER_SECONDS) 2>> $(LOG)

clean_log:
	rm -f $(LOG)

clean:
	rm -rf .antlr
	rm -f *.log
	rm -f $(LOG)
	rm -f $(CATALOG)
	rm -f $(COMPILER)
	rm -f $(ENGINE)
	rm -f go.mod
	rm -f go.sum
	rm -f go.work.sum
	rm -rf $(PKG_OUT_DIR)
	rm -rf $(CAPNP_DIR)/go-capnproto2
	rm -rf $(OUT_DIR)
	rm -f ./capnp/books/*.capnp.go
	rm -f ./capnp/data/data.capnp
	rm -f ./capnp/data/*.capnp.go
	rm -f ./capnp/foo/*.capnp.go
	rm -f ./capnp/grizzly/*.capnp.go
	rm -f ./capnp/person/*.capnp.go

showc: # Show catalog
	@cat $(CATALOGB) | $(CATALOG) -i capnp -o json | jq '.' --tab

showp: # Show plan
	@cat $(PLANB) | $(COMPILER) show | jq . --tab

tiny:
	./$(ENGINE) tiny > $(PLAN_DIR)/tiny.bin
	cat $(PLAN_DIR)/tiny.bin | ./$(ENGINE) show

example:
	mkdir -p $(PLAN_DIR)
	$(ENGINE) example > $(PLAN_DIR)/example.bin
	cat $(PLAN_DIR)/example.bin | $(ENGINE) show | jq .

justrun:
	echo $(QUERY) | ./$(COMPILER) compile | $(ENGINE_DIR)/$(ENGINE) example

test:
	go test -v
