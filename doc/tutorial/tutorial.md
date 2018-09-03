# Tutorial: Differential Datalog

**Note:**: All examples from this tutorial can be found in
[`test/datalog_tests/tutorial.dl`](../../test/datalog_tests/tutorial.dl).  Test inputs are in
[`test/datalog_tests/tutorial.dat`](../../test/datalog_tests/tutorial.dat) and outputs are in
[`test/datalog_tests/tutorial.dump.expected`](../../test/datalog_tests/tutorial.dump.expected).

## Introduction

Differential Datalog, or **DDlog** for short, is a *bottom-up*, *incremental*, *in-memory* Datalog
engine for writing *application-integrated* deductive database engines.  Let's decipher some of
these keywords:

1. **Bottom-up**: DDlog starts from a set of *ground facts* (i.e., facts provided by the user) and
computes *all* possible derived facts by following Datalog rules in a bottom-up fashion.  This is in
contrast to top-down engines that are optimized to answer individual user queries without computing
all possible facts ahead of time.  For example, given a Datalog program that computes pairs of
connected vertices in a graph, a bottom-up engine maintains the set of all such pairs.  A top-down
engine, on the other hand, is triggered by a user query to determine whether a pair of vertices is
connected and handles the query by searching for a derivation chain back to ground facts.  The
bottom-up approach is preferable in applications where all derived facts must be computed ahead of
time and in applications where the cost of initial computation is amortized across a large number of
queries.

1. **Incremental**: whenever the set of ground facts changes, DDlog updates derived facts
incrementally, avoiding complete recomputation as much as possible.  In practical terms, the means
that a small change to ground facts often triggers only a trivial amount of computation.

1. **In-memory**: DDlog stores and processes data in memory.  In a typical use case, a DDlog program
is used in conjunction with a persistent database, with database records being fed to DDlog as
grounds, and derived facts computed by DDlog being written back to the database.

At the moment, DDlog can only operate on databases that completely fit in memory of a single host.
(This may change in the future, as DDlog builds on the differential dataflow library that supports
distributed computation over partitioned data).

1. **Integrated**: while DDlog programs can be run interactively via a command line interface, its
primary use case is to integrate with other applications that require deductive database
functionality.  A DDlog program is compiled into a Rust library that can be linked against a Rust or
C/C++ program (bindings for other languages can be easily added, but Rust and C are the only ones we
support at the moment).  This enables good performance, but somewhat limits the flexibility, as
changes to the relational schema or rules require re-compilation.

### Not your textbook Datalog

Although Datalog is typically described as a programming language, in its classical textbook form it
is more of a mathematical formalism than a practical tool for programmers.  In particular, pure
Datalog does not have concepts like data types, arithmetics, strings or functions.  To facilitate
writing of safe, clear, and concise code, DDlog extends pure Datalog with these concepts, including:

1. A powerful type system, including Booleans, integers, bitvectors, strings, tuples, and tagged
unions.

1. Standard integer and bitvector arithmetic.

1. A simple procedural language that allows expressing many computations natively in DDlog without
resorting to external functions.

1. String operations, including string concatenation and interpolation.

## Installing DDlog

**TODO: document proper installation procedure when we have one.**

Follow instructions in Installation sections of the
[README](README.md).

## "Hello, World!"

We start with the obligatory "Hello, World!" example.  Let's create a file with `.dl` extension, say
`playpen.dl`, in the `test/datalog_tests` directory.  Next time you run `stack test`, the test
framework will pickup the program, and automatically compile and run it.  Alternatively, running
`stack test --ta '-p playpen'` will execute just your test instead of the entire test framework.

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

This may not look like a typical "Hello, world!" program, yet that is exactly what it is.  Let's
dissect this code snippet.  First, note the use of C++ style comments (`//` and `/* */`).

Comments aside, this program contains several DDlog declarations: a data type declaration, three
relations, and a rule.  We discuss the type system in more detail below.  For the moment, think of
the `Category` type as a C-style enum with two possible values: `CategoryStarWars` and
`CategoryOther`.

Datalog relations are similar to database tables in that they store sets of records.  `Word1` and
`Word2` are two *input* relations in our example.  Input relations (aka *extensional* relations in
the Datalog world) are populated by user-provided ground facts.  The other kind of relations are
*derived* (or *intensional*) relations that are computed based on rules.  The two kinds do not
overlap: a relation must be populated exclusively by either ground or derived facts.  The `input`
qualifier is used to declare an input relation; relations declared without this qualifier are
derived relations.

`Word1` and `Word2` relations have the same record type, including a `word` field of type `string`
and a `cat` field of type `Category`.  We would like to combine strings from `Word1` and `Word2`
relations into phrases and store them in the `Phrases` relation.  However, we only want to combine
words that belong to the same category.  This is captured by the DDlog rule in the end of the
listing.  The rule can be read as follows: *for every `w1`, `w2`, and `cat`, such that `(w1, cat)`
is in `Word1` and `(w2, cat)` is in `Word2`, there is a record `w1 ++ " " ++ w2` in Phrases*, where,
`++` is a string concatenation operator.

Clauses of the form `<Relation_name>(<arguments>)` are called *atoms*.  An atom is true iff the
relation contains the specified record.  The atom to the left of `:-` is the *head* of the rule;
atoms to the right of `:-` form the *body* of the rule.  Commas in the body of the rule represent
logical conjunctions, i.e., if all atoms in the body are true, then the head of the rule holds.
Finally, note the mandatory dot in the end of the rule.

The above rule computes an inner join of `Word1` and `Word2` relations on the `cat` field.

### Capitalization rules

1. A relation name must start with an upper-case letter.

1. A variable name must start with a lower-case letter or underscore
(`_`).

1. A type name can start with an upper-case or a lower-case letter or
an underscore.

### Ordering of declarations

DDlog does not care about the order in which various program entities (types, relations, rules,
functions) are declared.  Reordering declarations in the above program does not have any effect on
program semantics.

## Compiling the example

Run `stack test --ta '-p playpen'` to compile the "Hello, world!" program.  This command performs
two actions.  First, it runs the DDlog compiler to generate a Rust library that implements the
semantics of your DDlog program, along with C bindings to invoke the library from C/C++ programs.
Second, it invokes the Rust compiler to compile the library.  The latter step is unfortunately quite
slow at the moment, typically taking a couple of minutes.  Once the `stack test` command completes,
you should find the following artifacts in the same directory with the `playpen.dl` file.

1. The DDlog compiler creates three Rust packages (or "crates") that comprise a Rust library that
implements your DDlog relations and rules:
    * `./differential_dataflow/`
    * `./cmd_parser/`
    * `./playpen/` (this is the main crate, which imports the two
      others)

1. If you plan to use this library directly from a Rust program, have a look at the
`./playpen/lib.rs` file, which contains the Rust API to DDlog.

**TODO: link to a separate document explaining the structure and API of the Rust project**

1. If you plan to use the library from a C/C++ program, your program must link against the
`./playpen/target/release/libplaypen.so` library, which wraps the DDlog program into a C API.  This
API is declared in the auto-generated `./playpen/playpen.h` header file.

**TODO: link to a separate document explaining the use of the C FFI**

1. Throughout this tutorial, we will use DDlog via its text-based interface.  This interface is
primarily meant for testing and debugging purposes, as it does not offer the same performance and
flexibility as the API-based interfaces.  The text-based interface is implemented by an
auto-generated executable `./playpen/target/release/playpen_cli`.

## Running the example

What kind of a "Hello, world!" program does not contain "Hello" or "world" strings anywhere in the
code?  Well, those strings are data, and DDlog offers several ways to feed data to the program:

1. Statically, by listing ground facts as part of the program.

1. Via text-based interface.

1. From a Rust program.

1. From a C or C++ program.

In the following sections, we expand on each method.

### Specifying ground facts statically in the program source code

**TODO: This does not currently work: DDlog will accept such ground facts, but they will not
actually be added to the DB, see issue #55**

This method is useful for specifying ground facts that are guaranteed to hold in every instantiation
of the program.  Such facts can be specified statically to save the hassle of adding them manually
on every program instantiation.  A ground fact is just a rule without a body.  Add the following
declarations to the program to pre-populate `Word1` and `Word2` relations:

```
Word1("Hello,", CategoryOther).
Word2("world!", CategoryOther).
```

### Feeding data through text-based interface

The text-based interface offers a convenient way to interact with the DDlog program during
development and debugging.  Let's try this interface in the command-line mode first.  Execute the
`./playpen/target/release/playpen_cli` program from terminal and enter the following commands in its
command line:

```
start;

insert Word1("Hello,", CategoryOther),
insert Word2("world!", CategoryOther);

commit;
dump Phrases;
```

You should see the following output:

```
>> start;
>>
>> insert Word1("Hello,", CategoryOther),
>> insert Word2("world!", CategoryOther);
>>
>> commit;
insert 30 Word1(Word1{"Hello,",Category::CategoryOther{}})
insert 31 Word2(Word2{"world!",Category::CategoryOther{}})
insert 21 Phrases(Phrases{"Hello, world!"})
>> dump Phrases;
Phrases{"Hello, world!"}
>>
```

Note the three lines printed by the program in response to the `commit;` command.  At commit time,
DDlog reports incremental changes to all of its relations, including input and derived relations.
Use the `--no-print` command-line option to disable this behavior.  See [this
document](../testing/testing.md#command-reference) for a complete list of commands supported by the
tool.

You won't get very far by typing commands manually every time.  Fortunately, you can also run the
program in batch mode, feeding commands via a UNIX pipe from a file or another program.  Create a
file called `playpen.dat` with the following content:

```
start;

insert Word1("Hello,"      , CategoryOther),
insert Word1("Goodbye,"    , CategoryOther),
insert Word2("World"       , CategoryOther),
insert Word2("Ruby Tuesday", CategoryOther);

insert Word1("Help me,"      , CategoryStarWars),
insert Word1("I am your"     , CategoryStarWars),
insert Word2("Obi-Wan Kenobi", CategoryStarWars),
insert Word2("father"        , CategoryStarWars);

commit;

echo Phrases:;
dump Phrases;
```

You can run the program either manually:

```
playpen/target/release/playpen_cli < playpen.dat
```

or using DDlog's test framework:
```
`stack test --ta '-p playpen'`
```

The latter will automatically recompile the program if it has changed, check if a file named
`playpen.dat` exists under `tests/datalog_tests` and invoke `playpen_cli` on this file, dumping its
output to `playpen.dump`.  In this example, it will produce the following output:

```
Phrases:
Phrases{"Goodbye, Ruby Tuesday"}
Phrases{"Goodbye, World"}
Phrases{"Hello, Ruby Tuesday"}
Phrases{"Hello, World"}
Phrases{"Help me, Obi-Wan Kenobi"}
Phrases{"Help me, father"}
Phrases{"I am your Obi-Wan Kenobi"}
Phrases{"I am your father"}
```

### Set semantics

DDlog implements set semantics for relations, i.e., multiple identical records are merged into a
single record.  Try modifying the `.dat` file to add the same input record twice and dump the
relation to make sure that it contains one instance of the record.  The same is true for derived
relations: when the same record can be derived in multiple ways, the resulting relation will still
contain a single copy.

### Incremental evaluation

Before moving on to more interesting programs, we use the "Hello, world!" example to demonstrate
the incremental aspect of DDlog.  Add the following commands to the `.dat` file to delete one of the
input records.  All phrases ending with "World" should disappear from `Phrases`.  DDlog performs
this computation incrementally, without recomputing the entire relation from scratch.

```
start;
delete Word2("World", CategoryOther);
commit;

echo Phrases:;
dump Phrases;
```

## Expressions

DDlog features a powerful expression language.  Expressions are used inside DDlog rules to filter
input records and compute derived records.  In this section of the tutorial, we introduce the use of
expressions with a series of examples.  See the [language
reference](../language_reference/language_reference.md#expressions) for a complete description.

The following example illustrates the use of expressions to filter records in the body of a rule
along with several other features of DDlog.  Given a set of IP hosts and subnets, this DDlog program
computes a host-to-subnet mapping by matching IP addresses against subnet masks.

```
// Type aliases improve readability.
typedef UUID    = bit<128>
typedef IPAddr  = bit<32>
typedef NetMask = bit<32>

//IP host specified by its name and address.
input relation Host(id: UUID, name: string, ip: IPAddr)

//IP subnet specified by its IP prefix and mask
input relation Subnet(id: UUID, prefix: IPAddr, mask: NetMask)

// HostInSubnet relation maps hosts to known subnets
relation HostInSubnet(host: UUID, subnet: UUID)

HostInSubnet(host_id, subnet_id) :- Host(host_id, _, host_ip),
                                    Subnet(subnet_id, subnet_prefix, subnet_mask),
                                    ((host_ip & subnet_mask) == subnet_prefix). // filter condition
```

First, note the three *type alias* declarations in the first lines of the example.  A type alias is
a shortcut that can be used interchangeably with the type it aliases.  Type aliases improve code
readability and simplify refactoring.  For example, while in this example `IPAddr` and `bit<32>`
refer to the exact same type (a 32-bit unsigned integer), the former makes it explicit that the
corresponding variable or field stores an IP address value.

Next we declare the two input relations that store known IP hosts and subnets respectively, followed
by the derived `HostInSubnet` relation.  `HostInSubnet` is computed by filtering all host-subnet
pairs where host address matches subnet prefix and mask.  This is captured by the rule in the end of
the program.  The first two clauses use already familiar syntax two *bind* `host_id`, `host_ip`,
`subnet_id`, `subnet_prefix`, `subnet_mask` variables to records from `Host` and `Subnet` relations.

The first two clauses iterate over all host-subnet pairs.  We would like to narrow this set down to
only those pairs where the host's IP address belongs to the subnet.  This is captured by the
expression in the third clause of the rule: `(host_ip & subnet_mask) == subnet_prefix`, where `&` is
the bitwise "and" operaror, and `==` is the equality operator.  In DDlog, a clause that is a Boolean
expression is a *filter* clause that eliminates all tuples that do not satisfy the condition.

Note that this rule does not use the `name` field of the `Host` relation.  We do not want to pollute
the name space with an unused variable and instead use the *wildcard* symbol (`_`) in the first
clause of the rule to indicate that the value of the field is immaterial:

```
Host(host_id, _, host_ip)
```

DDlog supports an alternative syntax for atoms, identifying relation arguments by name instead of by
position:

```
Host(.id=host_id, .ip=host_ip)
```

While more verbose than the original version, this syntax is less error-prone, especially for
relations with many arguments.  In addition, it only requires the programmer to list arguments that
are used in the rule.  Omitted arguments are automaticaly wildcarded.

### String concatenation and interpolation

### Arithmetic expressions

*Bitvector constants*

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

**TODO**

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
