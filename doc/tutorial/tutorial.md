# Tutorial: Differential Datalog

**Note:**: All examples from this tutorial can be found in the
[`test/datalog_tests/tutorial.dl`](../../test/datalog_tests/tutorial.dl)
file.  Test inputs are in
[`test/datalog_tests/tutorial.dat`](../../test/datalog_tests/tutorial.dat)
and outputs are in
[`test/datalog_tests/tutorial.dump.expected`](../../test/datalog_tests/tutorial.dump.expected).

## Introduction

Differential Datalog, or **DDlog** for short, is a *bottom-up*,
*incremental*, *in-memory* Datalog engine for writing
*application-integrated* deductive database engines.  Let's decipher
some of these keywords:

1. **Bottom-up**: DDlog starts from a set of *ground facts* (i.e.,
facts provided by the user) and computes *all* possible derived facts
by following Datalog rules in a bottom-up fashion.  This is in
contrast to top-down engines that are optimized to answer individual
user queries without computing all possible facts ahead of time.  For
example, given a Datalog program that computes pairs of connected
vertices in a graph, a bottom-up engine maintains the set of all such
pairs.  A top-down engine, on the other hand, is triggered by a user
query to determine whether a pair of vertices is connected and handles
the query by searching for a derivation chain back to ground facts.
The bottom-up approach is preferable in applications where all derived
facts must be computed ahead of time and in applications where the
cost of initial computation is amortized across a large number of
queries.

1. **Incremental**: whenever the set of ground facts changes, DDlog
updates derived facts incrementally, avoiding complete recomputation
as much as possible.  In practical terms, the means that a small
change to ground facts often triggers only a trivial amount of
computation.

1. **In-memory**: DDlog stores and processes data in memory.  In a
typical use case, a DDlog program is used in conjunction with a
persistent database, with database records being fed to DDlog as
grounds, and derived facts computed by DDlog being written back to the
database.

At the moment, DDlog can only operate on databases that completely fit
in memory of a single host. (This may change in the future, as DDlog
builds on the differential dataflow library that supports distributed
computation over partitioned data).

1. **Integrated**: while DDlog programs can be run interactively via a
command line interface, its primary use case is to integrate with
other applications that require deductive database functionality.  A
DDlog program is compiled into a Rust library that can be linked
against a Rust or C/C++ program (bindings for other languages can be
easily added, but Rust and C are the only ones we support at the
moment).  This enables good performance, but somewhat limits the
flexibility, as changes to the relational schema or rules require
re-compilation.

### Not your textbook Datalog

Although Datalog is typically described as a programming language, in
its classical textbook form it is more of a mathematical formalism
than a practical tool for programmers.  In particular, pure Datalog
does not have concepts like data types, arithmetics, strings or
functions.  To facilitate writing of safe, clear, and concise code,
DDlog extends pure Datalog with these concepts, including:

1. A powerful type system, including Booleans, integers, bitvectors,
   strings, tuples, and tagged unions.
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

We start with the obligatory "Hello, World!" example.  Let's create a
file with `.dl` extension, say `playpen.dl`, in the
`test/datalog_tests` directory.  Next time you run `stack test`, the
test framework will pickup the program, and automatically compile and
run it.  Alternatively, running `stack test --ta '-p playpen'` will
execute just your test instead of the entire test framework.

**TODO: replace these instructions once issue #64 has been fixed**

Copy the following code to `playpen.dl`:

```
/* First DDlog example */

typedef Category = CategoryStarWars
                 | CategoryOther

input relation Word1(word: string, cat: Category)
input relation Word2(word: string, cat: Category)

relation Phrases(phrase: string)

// Rule
Phrases(w1 ++ " " ++ w2) :- Word1(w1, cat), Word2(w2, cat).
```

This may not look like your typical "Hello, world!" program, yet that
is exactly what it is.  Let's dissect this code snippet.  First, note
the use of C++ style comments (`//` and `/* */`).

Comments aside, this program contains several DDlog declarations: a
data type declaration, three relations, and a rule.  We discuss the
type system in more detail below.  For the moment, think of the
`Category` type as a C-style enum with two possible values:
`CategoryStarWars` and `CategoryOther`.

Datalog relations are similar to database tables in that they store
sets of records.  `Word1` and `Word2` are two *input* relations in our
example.  Input relations (aka *extensional* relations in the Datalog
world) are populated by user-provided ground facts.  The other kind of
relations are *derived* (or *intensional*) relations that are computed
based on rules.  The two kinds do not overlap: a relation must be
populated exclusively by either ground or derived facts.  The `input`
qualifier is used to declare an input relation; relations declared
without this qualifier are derived relations.

`Word1` and `Word2` relations have the same record type, including a
`word` field of type `string` and a `cat` field of type `Category`.
We would like to combine strings from `Word1` and `Word2` relations
into phrases and store them in the `Phrases` relation.  However, we
only want to combine words that belong to the same category.  This is
captured by the DDlog rule in the end of the listing.  The rule can be
read as follows: *for every `w1`, `w2`, and `cat`, such that `(w1,
cat)` is in `Word1` and `(w2, cat)` is in `Word2`, there is a record
`w1 ++ " " ++ w2` in Phrases*, where, `++` is a string concatenation
operator.

Clauses of the form `<Relation_name>(<arguments>)` are called *atoms*.
An atom is true iff the relation contains the specified record.  The
atom to the left of `:-` is the *head* of the rule; atoms to the right
of `:-` form the *body* of the rule.  Commas in the body of the rule
represent logical conjunctions, i.e., if all atoms in the body are
true, then the head of the rule holds.

The above rule computes an inner join of `Word1` and `Word2` relations
on the `cat` field.

### Capitalization rules

1. A relation name must start with an upper-case letter.

1. A variable name must start with a lower-case letter or underscore
(`_`).

1. A type name can start with an upper-case or a lower-case letter or
an underscore.

## Running the "Hello, World!" example

`playpen.dat` file.

Command language described in ...

output in `playpen.dump`

set semantics.

*Explain the three interfaces to the Datalog program*

## Expressions

*Extract subnet mask from IP address*

*wildcard*

*Named vs positional arguments*

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
