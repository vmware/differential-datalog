# Tutorial: Differential Datalog

## Introduction

Differential Datalog, or **ddlog** for short is a *bottom-up*,
*incremental*, *in-memory* Datalog engine for
writing *application-integrated* deductive database engines.  Let's
decipher some of these keywords:

1. **Bottom-up**: ddlog starts from a set of *ground facts* (i.e.,
facts provided by the user) and computes *all* possible derived
facts by following Datalog rules in a bottom-up fashion.  This is in
contrast to top-down engines that are optimized to answer individual
user queries without computing all possible facts ahead of time.  For
example, given a Datalog program that computes pairs of connected
vertices in a graph, a bottom-up engine maintains the set of all such
pairs.  A top-down engine, on the other hand, is triggered by a user
query to determine whether a pair of vertices is connected and
handles the query by searching for a derivation chain back to ground
facts.

  The bottom-up approach is preferable in applications where all
  derived facts must be computed ahead of time and in applications
  where the cost of initial computation is amortized across a large
  number of queries.

1. **Incremental**: whenever the set of ground facts changes, ddlog
updates derived facts incrementally, avoiding complete recomputation
as much as possible.  In practical terms, the means that a small
change to ground facts often triggers only a trivial amount of
computation.

1. **In-memory**: ddlog stores and processes data in memory.  In a
typical use case, a ddlog program is used in conjunction with a
persistent database, with database records being fed to ddlog as
grounds, and derived facts computed by ddlog being written back to
the database.

  At the moment, ddlog can only operate on databases that completely
  fit in memory of a single host. (This may change in the future,
  as ddlog builds on the differential dataflow library that supports
  distributed computation over partitioned data).

1. **Integrated**: while ddlog programs can be run
interactively via a command line interface, its primary use
case is to integrate with other applications that require deductive
database functionality.  A ddlog program is compiled into a Rust
library that can be linked against a Rust or C/C++ program (bindings
for other languages can be easily added, but Rust and C are the only
ones we support at the moment).  This enables good performance, but
somewhat limits the flexibility, as any changes to the relational
schema or Datalog rules require re-compilation.

### Not your textbook Datalog

## Installing

## "Hello, World!" in Datalog

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
