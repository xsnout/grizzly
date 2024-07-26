# Grizzly&mdash;Preview (don't use yet)

```ascii
  ·
· * ·
 · ·
                    _        _____
            bd,   b888d   .b8888888d
          *.888888888888888888888888d
        o8888888 88 88888888888 888888
        ~~`8888 8888 888888888 8888888
               88888 888888888 8888888
               8888 8888888888 888888
              8888  ## 88888  88888 ##
             8888   ###      88888 ####
           ,,,88  ,,,##    ,,,888 ,,,###
```

Grizzly is a small **data stream processor** that aggregates and filters time-stamped data such as `syslog` using a simple query language. Similar to other command-line data transformation commands like `grep` and `cut`, the `grizzly` utility ...

- reads data from `stdin`,
- writes results to `stdout`, and
- logs errors and status information to `stderr`.

This repository contains the code to build the compiler and engine, i.e., the core tool to manage data streams. Docker deployment, tools and examples that help playing with Grizzly can be found in the [ursa](http://github.com/xsnout/grizzly) repository.

## Motivation

Many database systems, spreadsheets, and statistical tools calculate and crunch static data, and can do so quickly and efficiently but they often lack a simple way to process live data. In the past decades, dozens of data stream systems emerged that can deal with complex event and stream processing challenges.

In contrast, `grizzly` is a _small_ and _simple_ tool that can process temporal data — covering both data sequences in files as well as real-time data such as sensor signals. It is basic but hopefully comprehensive enough to serve as a potential seed for a bigger project that uses distributed computing for complex queries. Therefore, we aimed for a query execution plan format using Cap'n Proto that is easily extensible such that plans could in the future be transferred over the internet.

> This is a tiny project, hence the name [_Grizzly_](https://en.wikipedia.org/wiki/Flag_of_California). Like a 🐻 standing in a stream catching salmon, Grizzly might help you pick and consume tasty data rows and hopefully produce some insightful nuggets in one form or another.

### Acknowledgment

The Grizzly project rests on the awesomeness of the following technologies and the people behind it to whom I am grateful:

- [ANTLR](https://www.antlr.org)
- [Cap'n Proto](https://capnproto.org)
- [Golang](https://go.dev)
- [zombiezen](https://pkg.go.dev/zombiezen.com/go/capnproto2)

## Getting started

Grizzly works in two stages:

1. we compile a query into an execution plan and generated Go code, then
2. an engine processes input data and produces results according to the plan.

## Usage

```sh
make grizzlyc
make grizzly
make syslog-example
```

- `make grizzlyc` creates the compiler that is based on the UQL grammar described below. The command is used to translate a UQL query into a file with the query execution plan.

- `make grizzly` creates the engine. It uses the plan file created by `grizzlyc` and waits for data on `stdin` that is compatible with the schema named in the query's `from` clause as defined in the `catalog.json` file described below.

- `make syslog-example` runs a simple UQL query over live `syslog` data on your system (Linux or MacOS).

## Example

With the _Ursa Query Language_ (UQL) we can specify a task in an intuitve manner. Imagine, we want to process time-stamped CSV data like the following from the file [foo.csv](data/foo.csv):

|   x |             t             |
| --: | :-----------------------: |
|   1 | 2030-01-01T17:00:01−07:00 |
|   2 | 2030-01-01T17:00:04−07:00 |
|   3 | 2030-01-01T17:00:11−07:00 |
|   4 | 2030-01-01T17:00:12−07:00 |
|   5 | 2030-01-01T17:00:17−07:00 |
|   6 | 2030-01-01T17:00:26−07:00 |
|   7 | 2030-01-01T17:00:40−07:00 |
|   8 | 2030-01-01T17:00:43−07:00 |
|   9 | 2030-01-01T17:00:49−07:00 |

This query computes aggregate results every 10 seconds on the wall clock:

```ascii
from instance1.database1.schema1.table1 \
group by g2, g1 \
window slice 20 rows \
based on rowid \
aggregate count(a) as aCount, avg(a) as aAvg, avg(b) as bAvg, sum(a) as aSum, first(c) as cFirst, last(c) as cLast, first(t2) as t2First, last(t2) as t2Last, first(rowid) as rowidFirst, last(rowid) as rowidLast \
append t2First, t2Last, rowidFirst, rowidLast, aCount, aSum, aAvg, bAvg, cFirst, cLast, bAvg, aCount \
where aAvg <= 12 \
to xxx \
```

```ascii
from
  foo
window
  slice
  10 seconds
  based on t
aggregate
  avg(x) as avg,
  sum(x) as total,
  count() as n,
  first(t) as begin,
  last(t) as end
append
  avg,
  total,
  n,
  seconds(end - begin) as duration,
  end as close
to
  bar
```

```ascii
every
    10 wall clock seconds
    chunking
    based on t
from
    foo
where
    x < 10
aggregate
    avg(x) as avg,
    sum(x) as total,
    count(*) as n,
    first(t) as begin,
    last(t) as end
where
    n < 20 and total > 0
append
    avg,
    total,
    n,
    seconds(end - begin) as duration,
    end as close
where
    duration % 3 = 0
to
    bar
```

The `where` clauses are shown only for illustration purposes, they have no influence on the result becasue the conditions are all `true`.

The result is also in the file [bar.csv](data/bar.csv):

| avg | total |   n | duration |           close           |
| --: | ----: | --: | -------: | :-----------------------: |
| 1.5 |     3 |   2 |        3 | 2030-01-01T17:00:04−07:00 |
|   4 |    12 |   3 |        6 | 2030-01-01T17:00:17−07:00 |
|   6 |     6 |   1 |        0 | 2030-01-01T17:00:26−07:00 |
|   8 |    24 |   3 |        9 | 2030-01-01T17:00:49−07:00 |

For the 10-second time period between 17:00:30 and 17:00:40 there is no input data. Therefore, we won't output any result row for that time window.

## Query language

The [Query.g4](Query.g4) grammar file contains the language details using [ANTLR](https://www.antlr.org).

The order of the clauses is:

1. `from`
2. `group by` (optional)
3. `where` (optional)
4. `window`
5. `based on` (optional)
6. `aggregate`
7. `where` (optional)
8. `append`
9. `where` (optional)
10. `to`

### The `from` clause

### The `group by` clause

### The `where` clause

### The `window` clause

A window specifies the properties of the sub-sequence of rows in the input data.

A window can be described by

- time or
- row count.

There are several forms of windows which can be regarded on a spectrum of flexibility. In Grizzly, we give the following names to 3 degrees of flexibility:

- `session` (most general),
- `slide`, and
- `slice` (most restrictive).

#### The `slice` window

This is the simplest window and it is a special case of the `slide` window described next. A slice is an equal width window (duration or number of rows) and the slices are contiguous, one next to the other.

#### The `slide` window

An extreme case of a slide window is where the start of the window remains unchanged. You can think of it as a "rubber band" behavior.

#### The `session` window

This is the most general kind of window. Instead of having a simple start and end duration, the start and end of a window are defined by a condition, respectively.

The name derives from the textbook example of session window:

A user's online session, where a user

- logs in
- logs out
- or doesn't she log out but her session is timed out eventually.

The syntax for a `session` could be used to achieve the same behavior as `slide` and `slice`. And `slide` can be used to emulate a `slice` window. Here is an example. The following `window` clauses achieve the same result:

```sql
slice "10 seconds" based on t
slide "10 seconds" advance every "3 seconds" based on t
session begin when t mod "10 seconds" == "0 seconds" expire after "10 seconds" based on t
// end when t >= t.start + "10 seconds" based on t

session begin when t mod "3 seconds" == "0 seconds" end t == t.start + "10 seconds" based on t
```

### The `based on` clause

If this clause is present, it specifies the field used to divide the flow of time into intervals. If the field of type `timestamp`, we compare its values based on `time` intervals. If it is of type `int64`, we use the difference in integer values as the distance in number of rows.

### The `aggregate` clause

### The `append` clause

### The `to` clause

On a high level, a UQL query consists of the following clauses that are named by its first keyword.

- `every` specifies the window size and how the window moves along the input data.
- `from` references the input schema in the [catalog.json](cmd/catalog/catalog.json) file.
- `to` specifies an the name of the output schema that may or may not exist in the catalog.
- `aggregate` is a list of aggregate function calls, and is the main processing of a window is specified.
- `append` can be thought of as the `SELECT` clause in SQL; it allows for projections and simple calculations over scalar values.
- `where` uses Boolean expressions to remove rows of the previous clause that we're no longer interested in.

## Windows

We implemented three types of window behaviors explained below.

### Slice window

Example:

```txt
session
  begin  when action = "login"
  end    when action = "logout"
  expire after 30 minutes
group by
  userid
```

## Aggregate functions

Grizzly comes with a few typical aggregate functions out-of-the-box.

| Function   | Description          |
| ---------- | -------------------- |
| `count()`  | Number of input rows |
| `avg(x)`   | Average value of `x` |
| `sum(x)`   | Total value of `x`   |
| `min(x)`   | Minimum value of `x` |
| `max(x)`   | Maximum value of `x` |
| `first(x)` | First value of `x`   |
| `last(x)`  | Last value of `x`    |
| `hll(x)`   | HyperLogLog          |
| `cms(x)`   | CountMin Sketch      |

## Aggregate function extensions

You can extend the family of aggregate functions by:

- either adding your own implementation in the source code
- or by using Grizzly's facility to add implementations by dynamically linking your code. The implementation of HLL and CMS follows this approach.

## Schemas

The `grizzly` command processes data continuously. It needs to know the format and meaning of the input rows. These details are defined in a [catalog.json](cmd/catalog/catalog.json) file.

Here is an excerpt of the catalog that can be used for our example:

```json
{
  "name": "my cool catalog",
  "schemas": [
    {
      "name": "foo",
      "format": "csv",
      "fields": [
        {
          "name": "a",
          "type": "integer16",
          "usage": "data"
        },
        {
          "name": "t",
          "type": "timestamp",
          "usage": "time"
        }
      ]
    },
    ...
  ],
  "functions": [
    ...
  ]
}
```

Schema `foo` describe the query's input. If we wanted to use the output of the query as input to another query, we could add its schema to the catalog as well.

The `usage` attribute of a field has two possible values

- `data` means that the attribute is treated like normal input
- `time` means that this attribute serves as the reference to base window calculations on. There may be several timestamp attributes in the input but only one of them can serve as the `time` attribute.

## Behind the scenes

We use data structures called _operators_ that form a pipelined execution plan like the following:

`cat foo.csv` → ingress → filter → window → aggregate → filter → append → filter → Egress → `| tee bar.csv`

> One of my original aims of the design was to have each operator run in a different thread and/or perhaps on different compute nodes. This is a linear pipeline but certainly sharding and other data distribution techniques are thinkable for the future that would make such a pipeline more bushy.

The correspondencs between Ursa query clauses and Grizzly plan operators are shown below:

| Query clause | Plan operator | Description                                                   |
| ------------ | ------------- | ------------------------------------------------------------- |
| from         | ingress       | reads rows from a data source                                 |
| to           | egress        | transforms the result rows in a certain format (e.g., CSV)    |
| every        | window        | reads a certain amount of rows from the ingress filter output |
| aggregate    | aggregate     | aggregates the goup of rows from the window operator          |
| append       | project       | applies expressions and removes fields from a row             |
| where        | filter        | removes rows from the previous operator's output              |

Internally, we use further operators for each of the different aggregate functions, i.e., instead of a single `aggregate` operator, there may be several different kinds.
