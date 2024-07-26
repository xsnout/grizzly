using Go = import "/go.capnp";
@0xde128f6e36f039ab;
$Go.package("grizzly");
$Go.import("github.com/xsnout/grizzly/capnp/grizzly");

struct System {
    id          @0 :Int64;
    name        @1 :Text;
    description @2 :Text;
    databases   @3 :List(Database);
    properties  @4 :List(SystemProperty);
}

struct Database {
    id          @0 :Int64;
    name        @1 :Text;
    description @2 :Text;
    schemas     @3 :List(Schema);
    properties  @4 :List(DatabaseProperty);
}

struct Schema {
    id          @0 :Int64;
    name        @1 :Text;
    description @2 :Text;
    tables      @3 :List(Table);
    functions   @4 :List(Function);
    properties  @5 :List(SchemaProperty);
}

struct Table {
    id          @0 :Int64;
    name        @1 :Text;
    description @2 :Text;
    fields      @3 :List(Field);
    properties  @4 :List(TableProperty);
}

struct Function {
    id          @0 :Int64;
    name        @1 :Text;
    description @2 :Text;
    isAggregate @3 :Bool; # False if it's a scalar function
    isBuiltIn   @4 :Bool; # Built-in functions are :count, min, max; all else are user-defined
    inputTypes  @5 :List(FieldType);
    outputType  @6 :FieldType;
    outputName  @7 :Text;
    libraryPath @8 :Text; # DLL, dynamic library, written in C++, for external functions
    properties  @9 :List(FunctionProperty);
}

struct Field {
    name        @0 :Text;
    description @1 :Text;
    type        @2 :FieldType;
    usage       @3 :FieldUsage;
    properties  @4 :List(FieldProperty);
}

enum FieldType {
    boolean   @0;
    float64   @1;
    integer64 @2;
    text      @3;
}

enum FieldUsage {
    data     @0; # Normal field to store information
    time     @1; # Reference to express the sequence of rows (typically for a text FieldType used as a timestamp)
    group    @2;
    sequence @3;
}

struct Node {
    id                      @0  :Int64;
    label                   @1  :Text; # For debugging for now
    description             @2  :Text;
    type                    @3  :OperatorType;
    properties              @4  :List(OperatorProperty);
    fields                  @5  :List(Field);
    groupFields             @6  :List(Field);
    calls                   @7  :List(Call);
    fieldConstantConditions @8  :List(FieldConstantCondition);
    fieldFieldConditions    @9  :List(FieldFieldCondition);
    parent                  @10 :Node;
    children                @11 :List(Node);
}

struct Call {
    function    @0 :Function;
    inputFields @1 :List(Field);
    outputField @2 :Field; # Output field is the alias "x" in a function call like "count() as x"
}

 enum OperatorType {
    ingress         @0; # from clause, receives input rows
    ingressFilter   @1; # where clause after from clause
    window          @2; # window clause
    aggregate       @3; # aggregate clause
    aggregateFilter @4; # where clause after aggregate clause
    project         @5; # append clause
    projectFilter   @6; # where clause after append clause
    egress          @7; # transform data according to output schema
}

# a >= 0.5
struct FieldConstantCondition {
    fieldName  @0 :Text;
    comparator @1 :Comparator;
    constant   @2 :Text;
}

# a >= b
struct FieldFieldCondition {
    fieldName1 @0 :Text;
    comparator @1 :Comparator;
    fieldName2 @2 :Text;
}

enum Comparator {
    eq   @0;
    nEq  @1;
    lt   @2;
    ltEq @3;
    gt   @4;
    gtEq @5;
}

enum Connector {
    and @0;
    or  @1;
}

struct OperatorProperty {
    key   @0 :Text;
    value @1 :Text;
}

struct TableProperty {
    key   @0 :Text;
    value @1 :Text;
}

struct FieldProperty {
    key   @0 :Text;
    value @1 :Text;
}

struct FunctionProperty {
    key   @0 :Text;
    value @1 :Text;
}

struct DatabaseProperty {
    key   @0 :Text;
    value @1 :Text;
}

struct SchemaProperty {
    key   @0 :Text;
    value @1 :Text;
}

struct SystemProperty {
    key   @0 :Text;
    value @1 :Text;
}
