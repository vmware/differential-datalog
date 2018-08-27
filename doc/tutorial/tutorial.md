# Tutorial: Differential Datalog

**Note:**: All examples from this tutorial can be found in the
[`test/datalog_tests/tutorial.dl`](test/datalog_tests/tutorial.dl)
file.  Test inputs are in
[`test/datalog_tests/tutorial.dat`](test/datalog_tests/tutorial.dat)
and outputs are in
[`test/datalog_tests/tutorial.dump.expected`](test/datalog_tests/tutorial.dump.expected).

## Introduction

Differential Datalog, or **DDlog** for short, is a *bottom-up*,
*incremental*, *in-memory* Datalog engine for
writing *application-integrated* deductive database engines.  Let's
decipher some of these keywords:

1. **Bottom-up**: DDlog starts from a set of *ground facts* (i.e.,
facts provided by the user) and computes *all* possible derived
facts by following Datalog rules in a bottom-up fashion.  This is in
contrast to top-down engines that are optimized to answer individual
user queries without computing all possible facts ahead of time.  For
example, given a Datalog program that computes pairs of connected
vertices in a graph, a bottom-up engine maintains the set of all such
pairs.  A top-down engine, on the other hand, is triggered by a user
query to determine whether a pair of vertices is connected and
handles the query by searching for a derivation chain back to ground
facts.  The bottom-up approach is preferable in applications where all
derived facts must be computed ahead of time and in applications
where the cost of initial computation is amortized across a large
number of queries.

1. **Incremental**: whenever the set of ground facts changes, DDlog
updates derived facts incrementally, avoiding complete recomputation
as much as possible.  In practical terms, the means that a small
change to ground facts often triggers only a trivial amount of
computation.

1. **In-memory**: DDlog stores and processes data in memory.  In a
typical use case, a DDlog program is used in conjunction with a
persistent database, with database records being fed to DDlog as
grounds, and derived facts computed by DDlog being written back to
the database.

At the moment, DDlog can only operate on databases that completely
fit in memory of a single host. (This may change in the future,
as DDlog builds on the differential dataflow library that supports
distributed computation over partitioned data).

1. **Integrated**: while DDlog programs can be run
interactively via a command line interface, its primary use
case is to integrate with other applications that require deductive
database functionality.  A DDlog program is compiled into a Rust
library that can be linked against a Rust or C/C++ program (bindings
for other languages can be easily added, but Rust and C are the only
ones we support at the moment).  This enables good performance, but
somewhat limits the flexibility, as changes to the relational
schema or rules require re-compilation.

### Not your textbook Datalog

Although Datalog is typically described as a programming language, in
its classical textbook form it is more of a mathematical formalism
than a practical tool for programmers.  In particular, pure Datalog
does not have concepts like data types, arithmetics, strings or
functions.  To facilitate writing of safe, clear, and concise code,
DDlog extends pure Datalog with these concepts, including:

1. A powerful type system, including Booleans, integers,
bitvectors, strings, tuples, and tagged unions.
1. Standard integer and bitvector arithmetic.
1. A simple procedural language that allows expressing many
computations natively in DDlog without resorting to external
functions.
1. String operations, including string concatenation and
interpolation.

## Installing DDlog

**TODO: document proper installation procedure when we have one.**

Follow instructions in Installation sections of the
[README](README.md).

## "Hello, World!"

We start with the obligatory "Hello, World!" example.  Lets create a
file with `.dl` extension, say `playpen.dl` in the
`test/datalog_tests` directory.  Next time you run `stack test`, the
test framework will pickup the program, and automatically compile and
run it.  Alternatively, running `stack test --ta '-p playpen'` will
execute just your test instead of the entire test framework.

**TODO: replace these instructions once issue #64 has been fixed**


```
typedef Category = CategoryStarWars
                 | CategoryOther

input relation Word1(word: string, cat: Category)
input relation Word2(word: string, cat: Category)

relation Phrases(phrase: string)

Phrases(w1 ++ " " ++ w2) :- Word1(w1, cat), Word2(w2, cat).
```

// Declare two input relations (the `input` keyword indicates that
// these relations can only be populated by input facts and cannot
// appear in the head of a rule).
// `string` is a primitive type in ddlog.

// Computed relation populated by facts derived from rules.

// Produce phrases by combining pairs of words from the same category.
// The `++` operator is string concatenation.

## Compiling and running the "Hello, World!" example

*Explain the three interfaces to the Datalog program*
*Named vs positional arguments*

## Expressions

*Extract subnet mask from IP address*
*wildcard*
*Lower case/upper case rules*

### String concatenation and interpolation

### Arithmetic expressions

*Link to language reference*

### Control flow and variables

*if-then-else, sequencing, assignments, var*

## Functions

### Extern functions

## Advanced rule syntax

### Filter literals

### Assignment literals

### FlatMap literals

### Rules with multiple heads

### Recursive rules

### Antijoins

### Aggregation

## The type system

### Tagged unions

### Filtering relations with structural matching

### match expressions

### Tuples

### Type synonyms

### Generic types

### Built-in types

## Explicit relation types

## Flow Template Language
