# A Differential Datalog (DDlog) tutorial

**Note:** All examples from this tutorial can be found in
[`test/datalog_tests/tutorial.dl`](../../test/datalog_tests/tutorial.dl).
The examples can be executed using test inputs from
[`test/datalog_tests/tutorial.dat`](../../test/datalog_tests/tutorial.dat)
and the expected outputs are in
[`test/datalog_tests/tutorial.dump.expected`](../../test/datalog_tests/tutorial.dump.expected).

## Introduction

Differential Datalog or **DDlog** is an enhanced
version of the [Datalog](https://en.wikipedia.org/wiki/Datalog)
programming language.  A DDlog program can be used in two ways:

- integrated in other applications as a library.  This is the main use case that DDlog is optimized for.
- as a standalone program with a command-line interface (CLI)

In this tutorial we use DDlog using the CLI.  The
following diagram shows how we will use DDlog in the tutorial.  The
DDlog compiler generates [Rust](https://www.rust-lang.org/en-US/)
code, which is compiled by the Rust compiler to an x86 executable,
which provides a text-based interface to push relation updates to
the program and query relation state.

```
                                                 input data
           ------------           ------------       |
  DDlog    | DDlog    |   Rust    | Rust     |       V
program -> | compiler |-> code -> | compiler |--> executable
           ------------           ------------       |
                         runtime         ^           V
                         libraries ------/       output data

```

## Installing DDlog

Installation instructions are found in the [README](../../README.md#Installation).
Don't forget to set the `$DDLOG_HOME` environment variable to point to the root of the repository
or the DDlog installation directory (when using a binary release of DDlog).

## "Hello, World!" in DDlog

Files storing DDlog programs have the `.dl` suffix.
[dl.vim](../../tools/vim/syntax/dl.vim) is a file that offers syntax
highlighting support for the vim editor.

Create a file called `playpen.dl` with the following text in an empty directory.

```
/* First DDlog example */

typedef Category = CategoryStarWars
                 | CategoryOther

input relation Word1(word: string, cat: Category)
input relation Word2(word: string, cat: Category)

output relation Phrases(phrase: string)

// Rule
Phrases(w1 ++ " " ++ w2) :- Word1(w1, cat), Word2(w2, cat).
```

DDlog comments are like C++ and Java comments (`//` and `/* */`),
except that `/* ... */` comments nest to make it easy to comment out
blocks of code that themselves contain comments.

This program contains several declarations:
- a data type declaration, `Category`
- two input relations (collections) called `Word1` and `Word2`
- an output relation called `Phrases`
- a rule describing how to compute `Phrases`

When this program is executed it expects to be given as input the
contents of collections `Word1` and `Word2` and it produces as output the contents
of `Phrases`.

The `Category` type is similar to a C or Java `enum`, and has only two
possible values: `CategoryStarWars` and `CategoryOther`.

DDlog relations are similar to database tables (the term 'relation'
also comes from databases): a relation is a set of records.  Records
are like C `struct`s: objects with multiple named and fields.  Relation names must
start with an uppercase letter.

Relations without an `input` qualifier are also called *derived* relations. They are
computed from input relations using rules.
Output relations (marked by the `output` qualifier) are derived relations whose content
can be observed by the user (see below).

The declaration of `Word1`:
`input relation Word1(word: string, cat: Category)` indicates the type of its records: each
record has a `word` field of type `string`, and a `cat` field of type `Category`.
The records of `Word2` happen to have the same type.

From this declaration: `output relation Phrases(phrase: string)`
we know that he output relation `Phrases` has records with a single field named `phrase`,
which is a `string`.

Finally, the rule

```
Phrases(w1 ++ " " ++ w2) :- Word1(w1, cat), Word2(w2, cat).
^^^^^^^^^^^^^^^^^^^^^^^^    ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
          head                           body             dot
```

shows how to compute the records that belong to `Phrases`: for every
`w1`, `w2`, and `cat`, such that `(w1, cat)` is an element of `Word1` and `(w2,
cat)` is an element of `Word2`, there is a record in Phrases obtained by
concatenating the strings `w1`, a space and `w2`: `w1 ++ " " ++ w2`.
The rule's body shows how to compute the rule's head.
A comma in a rule is read as "and".  Rules end with a mandatory dot.

In database terminology the above rule computes an inner join of
the `Word1` and `Word2` relations on the `cat` field.

### Computation
In DDlog, the two main compute constructs are
[rules](../language_reference/language_reference.md#rules) and
[functions](../language_reference/language_reference.md#functions).

Rules describe how to compute an output relation from its input relations.
The available computations expressable in rules are similar to common queries
in traditional relational databases,
including select, join, filter, map, groupby, and aggregates.

Functions in DDlog are similar to user-defined functions in relational languages
and enable imperative expression of computation via traditional constructs
such as control flow and complex data types. As with rules, functions are designed
to operate on values that come from some relation.

### Capitalization rules

1. Identifiers start with letters or underscore `_`, and may contain digits.

1. Relation names must be capitalized (start with an uppercase symbol).

1. Variable names must start with a lowercase symbol or `_`.

1. Type names have no restrictions.

1. Type constructors have to be uppercase.

### Declaration order

The order of declarations in a DDlog program is unimportant; you
should use the order that makes programs easiest to read.

### Compiling the "Hello, world!" program

Compiling a DDlog program consists of two steps.
First, run the DDlog compiler to generate the Rust program:
```
ddlog -i playpen.dl
```
DDlog will search for its standard libraries inside the directory specified
in the `$DDLOG_HOME` variable.  If this variable is not set, add the `-L <ddlog/lib>`
switch, where `<ddlog/lib>` is the path to the `lib`
directory inside the DDlog installation directory (when using a binary release) or
inside the DDlog repository (when building DDlog from source).

Second, run the Rust compiler to generate a binary executable:
```
(cd playpen_ddlog && cargo build --release)
```

#### Compilation speed

Compiling the Rust project generated by DDlog can take a long time.
The following steps may help:

- Decompose your DDlog program into [modules](#modules) without circular
  dependencies between them.  This will cause DDlog to generate
  a separate Rust crate for each module and thus speed up the
  re-compilation of your program.

- Do not delete the `playpen_ddlog` directory containing Rust compiler
  artifacts when you change your DDlog program (unless you upgrade the
  DDlog compiler itself).

- Set the following environment variable when running `cargo`:
  `CARGO_PROFILE_RELEASE_OPT_LEVEL="z"`.  This will speed-up the Rust
  compilation while generating code that is 50% slower.

  **Note:** Disable this setting during benchmarking or when creating
  a production build of your code.

- The Rust debug compilation (`cargo build` without `--release`) will
  be even faster, but will produce very large and slow binaries.

### Running the "Hello, world!" program

The text-based interface offers a convenient way to interact with the
DDlog program during development and debugging.  In your favorite Unix
shell start the program that was generated by the compiler `./playpen_ddlog/target/release/playpen_cli`.
The CLI shows you a prompt `>>`.  Now you can type some commands that are executed by the datalog engine.

CLI commands can be used to show the contents of output relations, or to
insert or delete records from input relations.

We can display the contents of the `Phrases` relation:

```
>> dump Phrases;
>>
```

Initially all relations are empty, so nothing is printed.  Records can
be added or removed from relations.  All insertion and deletion
operations must be part of a *transaction*.  We start a transaction
with `start`:

```
>> start;
>>
```

Then we insert some records:

```
>> insert Word1("Hello,", CategoryOther);
>> insert Word2("world!", CategoryOther);
```

And finally we complete the transaction with `commit dump_changes`:

```
>> commit dump_changes;
Phrases:
Phrases{.phrase = "Hello, world!"}: +1
```

The CLI has executed all the operations that we have requested.  It
has inserted records in relations `Word1` and `Word2`, and the rule
for `Phrases` has inferred that a new record should be added to
this relation.  When committing a transaction DDlog reports all
*changes* to derived relations.

We can now again inspect the contents of `Phrases`:

```
>> dump Phrases;
Phrases{.phrase = "Hello, world!"}
>>
```

### Relations are sets

DDlog relations declared using `relation` keyword are sets, i.e., a
relation cannot contain multiple identical records.  Try adding an
existing record to `Word1` and check that it only appears once.
[Later](#multisets), we will introduce relations that have multiset
semantics.

### Incremental evaluation

We can delete records from input relations:

```
start;
delete Word2("world!", CategoryOther);
commit;

dump Phrases;
```

`Phrases` should be empty again.  DDlog only computes the change of `Phrases`
incrementally, without recomputing all records.

See [Command Language Reference](../command_reference/command_reference.md) for a complete list of commands
supported by the CLI tool.

### Running DDlog programs in batch mode

Typing commands interactively quickly gets tedious.  Therefore, throughout the rest
of this tutorial we will run DDlog in batch mode, feeding commands from a file.
Create a file called `playpen.dat` with the following contents:

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

Feed the file to the compiled DDlog program:

```
./playpen_ddlog/target/release/playpen_cli < playpen.dat
```

In this mode, it will only produce output in response to commands such as `dump` or
`commit dump_changes` (see [command reference](../command_reference/command_reference.md)).

In this example, it will produce the following output:

```
Phrases:
Phrases{.phrase = "Goodbye, Ruby Tuesday"}
Phrases{.phrase = "Goodbye, World"}
Phrases{.phrase = "Hello, Ruby Tuesday"}
Phrases{.phrase = "Hello, World"}
Phrases{.phrase = "Help me, Obi-Wan Kenobi"}
Phrases{.phrase = "Help me, father"}
Phrases{.phrase = "I am your Obi-Wan Kenobi"}
Phrases{.phrase = "I am your father"}
```

> ## :heavy_exclamation_mark: Pro tip: automating your DDlog workflow
>
> At this point you may want to create a small shell script to automate compiling and
> running DDlog. It will save you some typing and will make sure you don't forget to
> recompile your program after making changes to it:
>
> ```
> #!/bin/bash
>
> ddlog -i playpen.dl
> (cd playpen_ddlog && cargo build --release)
> ./playpen_ddlog/target/release/playpen_cli < playpen.dat
> ```
>
> If you are using [Nix shell](../../README.md#installing-dependencis-using-nix), the
> script already exists and can be invoked as:
> ```
> ddlog-run playpen.dl
> ```
> which will recompile the program and feed `playpen.dat` as input, if that file exists.

## DDlog language basics

In the rest of this document we introduce other features of DDlog through simple examples.

### Expressions

Expressions can be used to filter records and to compute derived
records.  In the previous example we have used a string
concatenation expression.

Let us write a program to manipulate a network information database.
Given a set of IP hosts and subnets, the following DDlog program
computes a host-to-subnet mapping by matching IP addresses against subnet masks.

We use type aliases to give an alternative names to types:

```
// Type aliases
typedef UUID    = bit<128>
typedef IP4     = bit<32>
typedef NetMask = bit<32>
```

In this example `IP4` and `bit<32>` refer to the exact same type, a
32-bit unsigned integer.

```
//IP host specified by its name and address.
input relation Host(id: UUID, name: string, ip: IP4)

//IP subnet specified by its IP prefix and mask
input relation Subnet(id: UUID, prefix: IP4, mask: NetMask)

// HostInSubnet relation maps hosts to known subnets
output relation HostInSubnet(host: UUID, subnet: UUID)

HostInSubnet(host_id, subnet_id) :- Host(host_id, _, host_ip),
                                    Subnet(subnet_id, subnet_prefix, subnet_mask),
                                    ((host_ip & subnet_mask) == subnet_prefix). // filter condition
```

Two input relations store known IP hosts and subnets respectively.
The `HostInSubnet` relation is computed by filtering all host-subnet
pairs where the host address matches the subnet prefix and mask.

This clause:

```
Host(host_id, _, host_ip)
```

*binds* variables `host_id` and `host_ip`, to corresponding fields of records from
the `Host` relation.  So the first two lines of the rule iterate over
all host-subnet pairs.  The expression on the third line `(host_ip &
subnet_mask) == subnet_prefix` uses `&` (bitwise "and"), and `==`
(equality test).  This expression acts as a *filter* that eliminates
all tuples that do not satisfy the condition that the host subnet
(computed using the mask) is the same as the `subnet_prefix`.

This rule does not care about the `name` field of the `Host` relation,
so it uses the *"don't care"* symbol (`_`) for the second field:

```
Host(host_id, _, host_ip)
              ^ don't care
```

In these examples relation field names are identified by position, but
they can also be specified by name:

```
Host(.id=host_id, .ip=host_ip)
```

Omitted fields (e.g., `name` in this example) are don't cares.

### String constants (literals)

DDlog strings are sequences of UTF-8 symbols.  Two forms of string constants are supported:

1. *Quoted strings* are Unicode strings enclosed in quotes,  and use the same single-character
escapes as in C, e.g.:
    ```
    "bar", "\tbar\n", "\"buzz", "\\buzz", "barΔ"
    ```
    Long strings can be broken into multiple lines using a pair of backslash characters; the spaces
    between backslashes are ignored:
    ```
    "foo\
       \bar"
    ```
    is equivalent to `"foobar"`

2. *Raw strings*, enclosed in `[| |]`, can contain arbitrary Unicode characters, (except the `|]`
sequence), including newlines and backslashes, e.g.,
    ```
    [|
    foo\n
    buzz
    bar|]
    ```
    is the same string as `"foo\\n\nbuzz\nbar"`.

### String concatenation and interpolation

Adjacent string literals (of both kinds) are automatically concatenated, e.g., `"foo" "bar"` is
converted to `"foobar"`.

*String interpolation* allows evaluating arbitrary DDlog expressions
to construct a string (this feature is inspired by JavaScript).
Embedded expressions
are wrapped in `${}`.  The following program defines relation
`Pow2` to contain strings that enumerate the squares of all numbers in relation `Number`.

```
input relation Number(n: bigint)
output relation Pow2(p: string)
Pow2("The square of ${x} is ${x*x}") :- Number(x).
```

Built-in types (`bool`, `bit<>`, `signed<>`, `bigint`, `string`, `float`, `double`)
have built-in conversions to strings.  To enable the compiler to automatically convert
a user-defined type to a string, the user must implement a
function named `to_string` that takes an instance of the type and returns a `string`
(functions are described [below](#functions)):

```
funtion to_string(x: Category): string {
    match (x) {
        CategoryStarWars -> "Star Wars",
        CategoryOther -> "other"
    }
}
```

A `to_string()` implementation can be provided for structs and
[`extern` types](#extern-types), but not tuples.  The latter must be converted
to strings manually.

### String library functions

Other string operations are implemented as library
[functions](#functions) in DDlog.  Many of these functions are part of the
[standard library](#the-standard-library), e.g.,

```
extern function len(s: string): usize
extern function contains(s1: string, s2: string): bool
extern function join(strings: Vec<string>, sep: string): string
...
```

In addition, the [regex](../../lib/regex.dl) library contains functions for
matching strings against regular expressions.

### Creating new types

Let's say we want to display IP and Ethernet
addresses in a standard format, e.g., `"192.168.0.1"` and
`"aa:b6:d0:11:ae:c1"` respectively.  Since `IP4` is an alias to 32-bit
numbers, in an interpolated string an IP address is formatted as a
decimal integer.  We can create a custom formatting function for IP
addresses called `format_bit32_as_ip`, which we can then invoke: e.g.,
`"IP address: ${format_bit32_as_ip(addr)}"`, but
this is error-prone, since it is easy to forget to call the
formatting function when printing IP addresses.  An alternative is to declare a new type for IP addresses:

```
typedef ip_addr_t = IPAddr{addr: bit<32>}
```

This declares a new type named `ip_addr_t`, having a single *type constructor* called `IPAddr`.
Think of this declaration as a C struct with
a single field of type `bit<32>`.  We can write a user-defined formatting method:

```
function to_string(ip: ip_addr_t): string {
    "${ip.addr[31:24]}.${ip.addr[23:16]}.${ip.addr[15:8]}.${ip.addr[7:0]}"
}
```

We have used two new operators: dot to access a field:
`ip.addr`, and bit slicing: `ip.addr[31:24]` to select a contiguous
range of bits from a bit vector.  Bits are numbered from the
least-significant, which is bit 0.  The slice boundaries are both
inclusive, so the result of `ip.addr[31:24]` has 8 bits.  Using our new
type `ip_addr_t` prevents one from accidentally mixing IP addresses and numbers.

We do the same for Ethernet addresses; for formatting we invoke the built-in
`hex()` function to convert a number to a hexadecimal string
representation:

```
typedef mac_addr_t = MACAddr{addr: bit<48>}

function to_string(mac: mac_addr_t): string {
    "${hex(mac.addr[47:40])}:${hex(mac.addr[39:32])}:${hex(mac.addr[31:24])}:\
     \${hex(mac.addr[23:16])}:${hex(mac.addr[15:8])}:${hex(mac.addr[7:0])}"
}
```

The `NHost` type describes a network host and contains the host's IP and MAC
addresses:

```
typedef nethost_t = NHost {
    ip:  ip_addr_t,
    mac: mac_addr_t
}

function to_string(h: nethost_t): string {
    "Host: IP=${h.ip}, MAC=${h.mac}"
}
```

DDlog will automatically invoke the user-defined string conversion
functions to format the `ip` and `mac` fields when it needs to produce strings.

Here is a program that computes a derived relation storing string
representation of hosts:

```
input relation NetHost(id: bigint, h: nethost_t)
output relation NetHostString(id: bigint, s: string)

NetHostString(id, "${h}") :- NetHost(id, h).
```

Try feeding the following `.dat` file to this program:

```
start;

insert NetHost(1, NHost{IPAddr{0xaabbccdd}, MACAddr{0x112233445566}}),
insert NetHost(2, NHost{IPAddr{0xa0b0c0d0}, MACAddr{0x102030405060}});

commit;

dump NetHostString;
```

You should obtain the following output:

```
NetHostString{.id = 1, .s = "Host: IP=170.187.204.221, MAC=11:22:33:44:55:66"}
NetHostString{.id = 2, .s = "Host: IP=160.176.192.208, MAC=10:20:30:40:50:60"}
```

### Bit vectors and integers

The type `bigint` describes arbitrary-precision (unbounded) integers.
`var x: bigint = 125` declares a variable `x` with type integer and
initial value 125.

> ### :heavy_exclamation_mark: Pro tip: Use `bigint` sparingly
>
> The `bigint` type is backed by a dynamically allocated array.  It is
> therefore quite expensive and should only be used when more efficient
> fixed-width integer types are not sufficient.

Signed integers are written as `signed<N>`, where `N` is the width of
the integer in bits.  Currently DDlog only supports signed integers with
widths that are supported by the native machine type (e.g., 8, 16, 32,
or 64 bits).  DDlog supports all standard arithmetic operators over
integers.

Bit-vector types are described by `bit<N>`, where `N` is the
bit-vector width.  Bit-vectors are unsigned integers when performing
arithmetic.  DDlog supports all standard arithmetic and bitwise
operators over bit-vectors.  Concatenation of bit-vectors is
expressed with the `++` operator (same notation as string
concatenation).

```
// Form IP address from bytes using bit vector concatenation
function ip_from_bytes(b3: bit<8>, b2: bit<8>, b1: bit<8>, b0: bit<8>)
    : ip_addr_t
{
    IPAddr{.addr = b3 ++ b2 ++ b1 ++ b0}
}

// Check for multicast IP address using bit slicing
function is_multicast_addr(ip: ip_addr_t): bool { ip.addr[31:28] == 14 }

input relation Bytes(b3: bit<8>, b2: bit<8>, b1: bit<8>, b0: bit<8>)

// convert bytes to IP addresses
output relation Address(addr: ip_addr_t)
Address(ip_from_bytes(b3,b2,b1,b0)) :- Bytes(b3,b2,b1,b0).

// filter multicast IP addresses
output relation MCastAddress(addr: ip_addr_t)
MCastAddress(a) :- Address(a), is_multicast_addr(a).
```

The DDlog [standard library](#the-standard-library) declares shortcuts for
common signed and unsigned integer types: `u8`, `u16`, `u32`, `u64`, `u128`,
`s8`, `s16`, `s32`, `s64`, and `s128`.  It also declares the `usize` that
represents lengths and sizes and is currently an alias to `bit<64>`.

Bit vector constants can be written as decimal numbers.  If the width is not specified
DDlog computes the width using *type inference*.

Bit vector constants can also specify the width explicitly and be written in
binary, octal or hexadecimal:
`[<width>]<radix_specifier><value>` syntax, where `<radix>` is one of
`'d`, `'h`, `'o`, `'b` for decimal, hexadecimal, octal and binary
numbers respectively:

```
32'd15              // 32-bit decimal
48'haabbccddeeff    // 48-bit hexadecimal
3'b101              // 3-bit binary
'd15                // bit vector whose value is 15 and whose width is determined from the context
```

DDlog does not automatically coerce different integer types to each other, e.g.,
it is an error for a function that is declared to return a `bit<64>` value to
return `bit<32>`.  Instead, the programmer must use the type cast operator `as` to
perform type conversion explicitly:

```
32'd15 as bit<64>    // type-cast bit<32> to bit<64>
32'd15 as signed<32> // type-cast bit<32> to signed<32>
32'd15 as bigint     // type-cast bit<32> to bigint
```

The following type conversions are currently supported:
- `bit<N>` to `bit<M>` for any `N` and `M`
- `signed<N>` to `signed<M>` for any `N` and `M`
- `bit<N>` to `signed<N>`
- `signed<N>` to `bit<N>`
- `bit<N>` to `bigint`
- `signed<N>` to `bigint`

Note that converting between signed and unsigned bit vectors of different width
requires chaining two type casts: `bit<32> as signed<32> as signed<64>`.

DDlog uses two's complement representation for bit vectors.  Casting a signed or
unsigned bit vector to a narrower bit vector type truncates higher-order bits.
Casting to a wider type sign extends (for signed bit vectors) or zero-extends
(for unsigned bit vectors) the value to the new width.  Finally, converting
between signed and unsigned bitvectors does not change the underlying
representation.

### Comparisons

All datatypes support comparison for equality (`==` and difference
`!=`) and ordering comparisons (`>`, `>=`, `<=`, `<`).  Only values
with the same type can be compared.  Comparison of complex types
(strings, tuples, types with constructors) is done lexicographically.

### Type inference and type annotations

DDlog is equipped with a type inference engine that is able to
deduce types of variables and expressions at compile time.  For instance, in the
following example the compiler knows that the type of `s` is `string` and the
type of `strs` is `Vec<string>`:

```
function ti_f(value: Option<string>): Option<Vec<string>> {
    var strs = match (value) {
        Some{s} -> string_split(s, ":"),
        None -> return None
    };
    Some{strs}
}
```

The user almost never needs to explicitly specify variable types.
However, they may do so to improve code readability or when the
inference engine is unable to deduce the type automatically.  In fact, type
annotations can be attached to any expression in DDlog:

```
function ti_f(value: Option<string>): Option<Vec<string>> {
    var strs: Vec<string> = match (value) {
        Some{s: string} -> string_split(s: string, ":": string): Vec<string>,
        None: Option<string> -> return (None: Option<Vec<string>>)
    }: Vec<string>;
    Some{strs}
}
```


### Control flow

DDlog functions are written using an *expression-oriented language*,
where all statements are expressions.
Evaluation order can be controlled using several constructs:

1. Semicolon is used to separate expressions that are evaluated in sequence, from left to right.

1. *`if (cond) e1 [else e2]`* evaluates one of two subexpressions depending on the value of `cond`.
   If the `else` is missing, the value `()` (empty tuple) is used for `e2`

1. *Matching* is a generalization of C/Java `switch` statements.

1. A new block (scope) can be created with curly braces `{ }`

1. A `for`-loop

1. `continue` terminates current loop iteration

1. `break` terminates a loop

1. `return [expr]` returns from a function

The following example illustrates the first four of these constructs.  Loops are explained [below](#container-types-flatmap-and-for-loops).

```
function addr_port(ip: ip_addr_t, proto: string, preferred_port: bit<16>): string
{
    var port: bit<16> = match (proto) {  // match protocol string
        "FTP"   -> 20,  // default FTP port
        "HTTPS" -> 443, // default HTTP port
        _       -> {    // other protocol
            if (preferred_port != 0)
                preferred_port // return preferred_port if specified
            else
                return "${ip}:80" // assume HTTP otherwise
        }
    };  // semicolon required for sequencing
    // Return the address:port string
    "${ip}:${port}"
}
```

The result computed by a function is the value of the last expression evaluated
or the value produced by the first `return` statement encountered when
evaluating the function.  A semicolon at the end of a sequence of expressions
discards the value of the last expressions and produces an empty tuple.  For
instance, expressions `{ var x: u32 = y + 1; x }` and
`{ var x: u32 = y + 1; x; }` have different meaning.  The former returns the
32-bit value of variable `x`, whereas the latter is equivalent to
`{ var x: u32 = y + 1; x; () }`, which produces an empty tuple `()` as
described [below](#tuples).

If the `else` clause of an `if-else` expression is missing, the value `()`
is used for the `else` branch.  In DDlog, `match` expressions are exhaustive -
i.e., the patterns must cover all possible cases (for instance, the match
expression above would not be correct without the last "catch-all" (`_`) case).

DDlog variables must always be initialized when declared.  In this
example, the `port` variable is assigned the result of the `match`
expression.  A variable can be assigned multiple times, overwriting
previous values.

DDlog assignments cannot be chained like in C; although an assignment
is an expression, it produces an empty tuple.

`match` matches the value of its argument against a series of
*patterns*.  The simplest pattern is a constant value, e.g., `"FTP"`.
The don't care pattern (`_`) matches any value.  More
complex patterns are shown [below](#variant-types).

In the following example the input relation stores a set of network endpoints
represented by their IP addresses, protocols, and preferred port
numbers.  The `EndpointString` relation contains a string
representation of the endpoints, computed using the function
`addr_port()`, which is invoked in the head of the rule:

```
input relation Endpoint(ip: ip_addr_t,
                        proto: string,
                        preferred_port: bit<16>)

output relation EndpointString(s: string)

EndpointString(addr_port(ip, proto, preferred_port)) :-
    Endpoint(ip, proto, preferred_port).
```

#### Container types, FlatMap, and for-loops

DDlog supports three built-in container data types: `Vec`, `Set`, and `Map`.  These
are *generic* types that can be parameterized by any other
DDlog types, e.g., `Vec<string>` is a vector of strings, `Map<string,bool>` is
a map from strings to Booleans.

We will use a DDlog standard library function that splits a string
into a list of substrings according to a separator:

```
function split(s: string, sep: string): Vec<string>
```

We define a DDlog function which splits IP addresses at spaces:

```
function split_ip_list(x: string): Vec<string> {
   split(x, " ")
}
```

Consider an input relation `HostAddress` associating each host with a
string of all its IP addresses, separated by whitespaces.  We want to
compute a `HostIP` relation that consists of (host, ip) pairs:

```
input relation HostAddress(host: bit<64>, addrs: string)
output relation HostIP(host: bit<64>, addr: string)
```

For this purpose we can use `FlatMap`: this operator applies a
function that produces a set or a vector for each value in a relation, and creates a
result that is the union of all the resulting sets:

```
HostIP(host, addr) :- HostAddress(host, addrs),
                      var addr = FlatMap(split_ip_list(addrs)).
```

You can read this rule as follows:

1. For every `(host, addrs)` pair in the `HostAddress` relation, use `split_ip_list()`
to split `addrs` into individual addresses.

1. Bind each address in the resulting set to the `addr` variable, producing a new set of `(host,
addr)` records.

1. Store the resulting records in the `HostIP` relation.

For-loops allow manipulating container types in a more procedural fashion, without first flattening them.
The following function concatenates a vector of strings, each starting at a new line.

```
function vsep(strs: Vec<string>): string {
    var res = "";
    for (s in strs) {
        res = res ++ s ++ "\n"
    };
    res
}
```

Loops can only iterate over container types: sets, maps, and vectors.  When iterating over sets
and vectors, the loop variable (`s` in the above example) has the same type as elements of the container
(e.g., `string`).  When iterating over maps, the loop variable is a 2-tuple `(key,value)`.

A `continue` statement used anywhere inside the body of a loop terminates the
current loop iteration:

```
// Returns only even elements of the vector.
function evens(vec: Vec<bigint>): Vec<bigint> {
    var res: Vec<bigint> = vec_empty();
    for (x in vec) {
        if (x % 2 != 0) { continue };
        vec_push(res, x)
    };
    res
}
```

A `break` statement used anywhere inside the body of a loop terminates the loop:

```
// Returns prefix of `vec` before the first occurrence of value `v`.
function prefixBefore(vec: Vec<'A>, v: 'A): Vec<'A> {
    var res: Vec<'A> = vec_empty();
    for (x in vec) {
        if (x == v) { break };
        vec_push(res, x)
    };
    res
}
```

#### Vector and map literals

Let's say we wanted to create a vector with three elements: `0`, `x`, and
`f(y)`.  This can be achieved by repeatedly calling the `push()` function
from the standard library:

```
var v = vec_with_capacity(3);
v.push(0);
v.push(x);
v.push(f(y));
...
```

The above code can be simplified using **vector literals** that allow
creating a vector by listing its elements in square brackets:

```
var v = [0, x, f(y)];
```

Likewise, **map literals** can be used to create maps by listing their
key-value pairs:

```
var m = ["foo" -> 0,
         "bar" -> 1,
         "foobar" -> 2];
...
```

### Functions

We have already encountered several functions in this tutorial.  This section
gives some additional details on writing DDlog functions.

#### Polymorphic functions

DDlog supports two forms of polymorphism: parametric and ad hoc polymorphism.
The following declarations from [`ddlog_std.dl`](../../lib/ddlog_std.dl) illustrate both:

```
function size(s: Set<'X>): usize {...}
function size(m: Map<'K, 'V>): usize {...}
```

Parametric polymorphism allows declaring functions generic over their argument
types.  The `size` functions above work for sets and maps that store values of
arbitrary types.  This is indicated by using type arguments (`'X`, `'K`, `'V`)
instead of concrete argument types.

Ad hoc polymorphism allows multiple functions with the same name but different
arguments.  The two `size()` functions above do not introduce any ambiguity,
since the compiler is able to infer the correct function to call in each case
from the type of the argument.  Specifically, the compiler uses a combination of
the number and types of arguments and the return type to disambiguate between
multiple candidates when compiling a function call, and generates an error when
sufficient type information to perform the disambiguation cannot be inferred
from the context.

#### Object-oriented function call syntax

In addition to the C-style function invocation syntax, e.g., `f(x,y)`,
DDlog supports an alternative notation, where the function name is prefixed by
its first argument, similar to object-oriented languages like Java: `x.f(y)`.
This often leads to more readable code.  Compare for example:

```
contains(to_string(unwrap_or_default(x)),"ddlog")
```

and

```
x.unwrap_or_default().to_string().contains("ddlog")
```

#### Modifying function arguments

By default, function arguments cannot be modified inside the function.  Writable
arguments can be declared using the `mut` qualifier:

```
// This function modifies its first argument.
function insert(m: mut Map<'K,'V>, k: 'K, v: 'V): () { ... }
```

> #### Legacy function syntax
>
> DDlog supports an alternative syntax for functions with equality sign between
> function declaration and its body, which does not require curly braces:
>
> ```
> function myfunc(x: string): string = x
> ```
>
> However, this syntax can cause parsing ambiguities in some circumstances;
> therefore the newer syntax is preferred:
>
> ```
> function myfunc(x: string): string { x }
> ```


#### Extern functions

Functions that cannot be easily expressed in DDlog can be implemented as
*extern* functions.  Currently these must be written in Rust; the Rust
implementation may in turn invoke implementations in C or any other language.

Example:

```
extern function string_slice(x: string, from: bit<64>, to: bit<64>): string
```

We can invoke `string_slice()` just like a normal DDlog function, e.g.,

```
output relation First5(str: string)
First5(string_slice(p, 0, 5)) :- Phrases(p).
```

To build this program we must provide a Rust implementation of
`string_slice()`.  DDlog generates a commented out function prototype
in `playpen_ddlog/types/lib.rs`.  Do not modify this file; instead, create a new
file called `playpen.rs` in the same directory:

```
pub fn string_slice(x: &String, from: &u64, to: &u64) -> String {
    x.as_str()[(*from as usize)..(*to as usize)].to_string()
}
```

DDlog will automatically pickup this file and inline its contents in the
generated `lib.rs`.

See [more detailed discussion](#implementing-extern-functions-and-types-in-Rust)
on integrating Rust and DDlog code below.


#### Functions with side effects

Functions implemented completely in DDlog without calls to any extern functions
are pure (side-effect-free) computations.  It is however possible to declare
extern functions with side effects.  The DDlog compiler needs to know about these
side effects, as they may interfere with its optimizations.  The programmer is
responsible for labeling such functions with the `#[has_side_effects]` attribute,
e.g., the following function is defined in the [`log.dl`](../../lib/log.dl) library:

```
#[has_side_effects]
extern function log(module: module_t, level: log_level_t, msg: string): ()
```

The compiler automatically infers these annotations for non-extern functions
that invoke extern functions with side effects, so only extern functions must
be annotated.

#### Higher-order functions, lambda expressions, and closures

DDlog allows functions to take other functions as arguments.  This is
particularly useful when working with containers like vectors and maps.
Consider, for example, the `map` function from the [`vec.dl`](../../lib/vec.dl)
library that applies a user-defined transformation to each element of a vector:

```
// Apply `f` to all elements of `v`.
function map(v: Vec<'A>, f: function('A): 'B): Vec<'B> {
    var res = vec_with_capacity(v.len());
    for (x in v) {
        res.push(f(x))
    };
    res
}
```

The second argument of this function has type `function('A): 'B`, i.e., it is
a function that takes an argument of type `'A` and returns a value of
type `'B`.  How do we create a value of this type?  One option is to use
a function whose signature matches the type of `f`, e.g.:

```
import vec

function times2(x: s64): s64 {
    x << 1
}

function vector_times2(v: Vec<s64>): Vec<s64> {
    // Pass function `times2` as an argument to `map`.
    v.map(times2)
}
```

This works well when a suitable transformer function exists, but it is sometimes
convenient or even necessary to create one on the fly.  Consider the following
function that multiplies each element of an input vector by a number that is
also provided as an argument:

```
function vector_times_n(v: Vec<s64>, n: s64): Vec<s64> {
    v.map(|x| x * n)
}
```

Here, `|x| x * n` is a **lambda expression**, or anonymous function, that multiplies
its argument `x` by `n`.  The type of this function is `function(s64): s64`,
i.e., it matches the type signature expected by `map`, with type arguments `'A` and
`'B` bound to `s64`.

One interesting thing about this lambda expression is that it accesses variable `n`
that is not one of its arguments.  This variable comes from the local scope where the
lambda expression is defined.  Such functions that capture variables from their
environment are known as **closures**, thus technically a lambda expression (which is
a syntactic construct) generates a closure (a runtime object) of type `function (..):..`.

Lambda expressions can have optional type annotations on their arguments and return values:

```
v.map(|x: s64|:s64 { x * n })
```

When type annotations are missing, the compiler relies on
[type inference](#type-inference-and-type-annotations) to determine the type of the
expression.  DDlog allows writing lambda expressions using the `function` keyword
instead of vertical bars (`||`):

```
v.map(function(x: s64):s64 { x * n })
```

Conversely, function types can be written using vertical bars.  The following
declarations are equivalent:

```
function map(v: Vec<'A>, f: function('A): 'B): Vec<'B>
```

```
function map(v: Vec<'A>, f: |'A|: 'B): Vec<'B>
```

Closures are first-class objects in DDlog and can be used as fields in structs or
tuples and stored in relations.  In the following example we declare two relations
containing closures and values respectively and apply all closures in the former
to all values in the latter:

```
relation Closures(f: |u64|: string)

Closures(|x| "closure1: ${x}").
Closures(|x| "closure2: ${x}").

relation Arguments(arg: u64)

// Apply all closures in `Closures` to all values in `Arguments`.
output relation ClosuresXArguments(arg: u64, res: string)

ClosuresXArguments(arg, f(arg)) :- Closures(f), Arguments(arg).
```

Similar to regular functions, closures can take mutable arguments:

```
// Applies specified transformer function to `x`, modifying `x` in-place.
function apply_transform(x: mut 'A, f: |mut 'A|) {
    f(x)
}

function test_apply_transform() {
    var s = "foo";
    apply_transform(s, |x|{ x = "${x}_${x}" });
    // The value of `s` is now "foo_foo".
}
```

Note, however, that closures cannot currently modify captured variables.

Among other applications, higher-order functions enable the creation of
reusable primitives for working with container types.  DDlog provides
some of these as part of [`vec.dl`](../../lib/vec.dl), [`set.dl`](../../lib/set.dl)
and [`map.dl`](../../lib/map.dl) libraries,
including functions for mapping, filtering, searching, and reducing
collections.  Here are a few examples:

```
/* Returns a vector containing only those elements in `v` that satisfy predicate `f`,
 * preserving the order of the elements in the original vector. */
function filter(v: Vec<'A>, f: function('A): bool): Vec<'A>

/* Applies closure `f` to each element of the vector and concatenates the
 * resulting vectors, returning a flat vector. */
function flatmap(v: Vec<'A>, f: function('A): Vec<'B>): Vec<'B>

/* Iterates over the vector, aggregating its contents using `f`. */
function fold(v: Vec<'A>, f: function('B, 'A): 'B, initializer: 'B): 'B
```

These functions enable more concise and clear code than for-loops.
Besides, they compose nicely into complex processing pipelines:

```
var vec = [[1,2,3], [4,5,6], [7]];

/* Remove entries in with less than 2 elements;
 * truncate remaining entries and flatten them into a 1-dimensional
 * vector; compute the sum of elements in the resulting vector.
 *
 * Comments in the end of each line show the output of
 * each transformation. */
vec.filter(|v| v.len() > 1)   // [[1,2,3], [4,5,6]]
   .flatmap(|v| {
       var res = v;
       res.truncate(2);
       res
    }) // [1,2,4,5]
   .fold(|acc, x| acc + x, 0) // 12
```

### Advanced rules

#### Negations and antijoins

Let's assume we want to compute a relation `SanitizedEndpoint`
containing the endpoints that do not appear in a `Blocklisted`
relation:

```
input relation Blocklisted(ep: string)
output relation SanitizedEndpoint(ep: string)

SanitizedEndpoint(endpoint) :-
    EndpointString(endpoint),
    not Blocklisted(endpoint).
```

The `not` operator in the last line eliminates all endpoint values
that appear in `Blocklisted`.  In database terminology this is known
as *antijoin*.

#### Left join

A *left join* includes all of the records in one relation plus fields
from matching records from a second relation.  In DDlog, we can build
left joins using negation.  For example, we can extend the previous
example with a new relation `EndpointSanitization` that includes every
endpoint plus whether it is sanitized:

```
output relation EndpointSanitization(ep: string, sanitized: bool)
EndpointSanitization(endpoint, false) :- Blocklisted(endpoint).
EndpointSanitization(endpoint, true) :- SanitizedEndpoint(endpoint).
```

However, nothing above prevents `Blocklisted`, and therefore
`EndpointSanitization`, from containing endpoints that do not appear
in `EndpointString`.  If that is undesirable, we can replace the first
rule above by:

```
EndpointSanitization(endpoint, false) :-
    EndpointString(endpoint),
    Blocklisted(endpoint).
```

#### Assignments in rules

We can directly use assignments in rules:

```
SanitizedEndpoint(endpoint) :-
    Endpoint(ip, proto, preferred_port),
    var endpoint = addr_port(ip, proto, preferred_port),
    not Blocklisted(endpoint).
```

#### Binding variables to rows

It is sometimes convenient to refer to a table row as a whole.
The following binds the `ep` variable to a row of the `Endpoint` relation.

```
SanitizedEndpoint(endpoint) :-
    ep in Endpoint(),
    var endpoint = addr_port(ep.ip, ep.proto, ep.preferred_port),
    not Blocklisted(endpoint).
```

Here we filter the `Endpoint` relation to select HTTP endpoints only
and bind resulting rows to `ep`:

```
SanitizedHTTPEndpoint(endpoint) :-
    ep in Endpoint(.proto = "HTTP"),
    var endpoint = addr_port(ep.ip, ep.proto, ep.preferred_port),
    not Blocklisted(endpoint).
```

#### `@`-bindings

When performing pattern matching inside rules, it is often helpful to
simultaneously refer to a struct and its individual fields by name.  This can be
achieved using `@`-bindings:

```
typedef Book = Book {
    author: string,
    title: string
}

input relation Library(book: Book)
input relation Author(name: string, born: u32)

output relation BookByAuthor(book: Book, author: Author)

BookByAuthor(b, author) :-
    // Variable `b` will be bound to the entire `Book` struct;
    // `author_name` will be bound to `b.author`.
    Library(.book = b@Book{.author = author_name}),
    author in Author(.name = author_name).
```

#### Rules with multiple heads

The following program computes the sums and products of pairs of values from `X`:

```
input relation X(x: bit<16>)

output relation Sum(x: bit<16>, y: bit<16>, sum: bit<16>)
output relation Product(x: bit<16>, y: bit<16>, prod: bit<16>)

Sum(x,y,x+y) :- X(x), X(y).
Product(x,y,x*y) :- X(x), X(y).
```

The last two rules can be written more compactly as a rule with multiple heads:

```
Sum(x,y,x+y), Product(x,y,x*y) :- X(x), X(y).
```

#### Recursive rules

The same relation can appear both in the head and the body of a rule.  The
following program computes pairs of connected nodes in a graph given
the set of edges:

```
input relation Edge(s: node, t: node)

output relation Path(s1: node, s2: node)

Path(x, y) :- Edge(x,y).
Path(x, z) :- Path(x, w), Edge(w, z).
```

This is an example of a *recursive* program, where a relation depends
on itself, either directly or via a chain of rules.  The [language
reference](../language_reference/language_reference.md#constraints-on-dependency-graph)
describes constraints on the recursive programs accepted by DDlog.

#### Grouping

The `group_by` operator groups records that share common values of a subset of
variables (group-by variables).
The following program groups the `Price` relation by `item` and selects the minimal
price for each item:

```
input relation Price(item: string, vendor: string, price: u64)
output relation BestPrice(item: string, price: u64)

BestPrice(item, best_price) :-
    Price(.item = item, .price = price),
    var group: Group<string, u64> = price.group_by(item),
    var best_price = group.min().
```

The `group_by` operator first selects the `price` variable from each record and
then groups the resulting table of prices such that all records in a group share the
same value of the `item` variable.  It yields a new table with one record per group.
The contents of the group is bound to a variable of type `Group<string,u64>`,
where `string` is the type of group key, i.e., the variable(s) that we group by
(in this case, `item`), and `u64` is the type of values in the group.

In general, the `group_by` operator has the following syntax:

```
<select clause>.group_by(<group-by vars>)
```

where `<select clause>` is an arbitrary expression that projects records
from the input relation into values to be grouped. `<group-by vars>` is a single
variable or a tuple of variables to be used as the key to group by:

```
/* Group by an empty tuple: aggregates the entire relation into a single group. */
var group: Group<(), usize> = (x: usize).group_by(())

/* Group by a single variable.  Generates one group per each unique value
 * of y. */
var group: Group<string, usize> = (x: usize).group_by(y: string)

/* Group by multiple variables.  Generates a group per each unique combination
 * of `y` and `z`. */
var group: Group<(string, bool), usize> = x.group_by((y: string, z: bool))
```

The following rule groups prices by both item name and vendor in order to compute
the lowest price that each vendor charges for each item:

```
BestPricePerVendor(item, vendor, best_price) :-
    Price(.item = item, .vendor = vendor, .price = price),
    var group = price.group_by((item, vendor)),
    var best_price = group.min().
```

Typically, the group returned by `group_by` is immediately
reduced to a single value, e.g., by computing its minimal element as in the
`BestPrice` example.  In this case, we can pass the group directly
to the reduction function without having to first store it in a variable:

```
BestPrice(item, best_price) :-
    Price(.item = item, .price = price),
    // Group and immediately reduce.
    var best_price = price.group_by(item).min().
```

In fact, the `group_by` operator can be used as any normal expression, e.g.:

```
// Items under 100 dollars.
Under100(item) :-
    Price(.item = item, .price = price),
    // Group and immediately reduce.
    price.group_by(item).min() < 100.
```

`min()` is one of several standard reduction functions defined in
[`lib/ddlog_std.dl`](../../lib/ddlog_std.dl).  Below we list few of the others:

```
/**/
function key(g: Group<'K, 'V>): 'K

/* The number of elements in the group.  The result is always greater than 0. */
function size(g: Group<'K, 'V>): usize

/* The first element of the group.  This operation is well defined,
 * as a group returned by `group-by` cannot be empty. */
function first(g: Group<'K, 'V>): 'V

/* Nth element of the group. */
function nth(g: Group<'K,'V>, n: usize): Option<'V>

/* Convert group to a vector of its elements.  The resulting
 * vector has the same size as the group. */
function to_vec(g: Group<'K, 'V>): Vec<'V>
```

What if we wanted to return the name of the vendor along with the
lowest price for each item?  The following naive attempt will **not work**:

```
output relation BestVendor(item: string, vendor: string, price: bit<64>)

BestVendor(item, vendor /*unknown variable*/, best_price) :-
    Price(.item = item, .vendor = vendor, .price = price),
    var best_price = price.group_by(item).min().
```

DDlog will complain that `vendor` is unknown in the head of the rule. What
happens here is that we first group records with the same `item` value
and then reduce each group to a single value, `best_price`.  All other
variables (except `item` and `best_price`) disappear from the scope and cannot
be used in the body of the rule following the `group_by` operator or in
its head.

A correct solution requires a different function that takes a
group of `(vendor, price)` tuples and picks one with the smallest price.
This can be achieved using the `arg_min` function,
that takes a group and a closure that maps an element of the group into
an integer value and returns the first element in the group that minimizes
the value returned by the closure:

```
// Defines `arg_min` and other higher-order functions over groups.
import group

BestVendor(item, best_vendor, best_price) :-
    Price(item, vendor, price),
    (var best_vendor, var best_price) = (vendor, price).group_by(item).arg_min(|vendor_price| vendor_price.1).
```

`arg_min`, along with other useful higher-order functions over groups,
is defined in [`lib/group.dl`](../../lib/group.dl).

You will occasionally find that you want to write a custom reduction that is
not found in one of standard libraries.  The following custom reduction
function computes the cheapest vendor for each item and returns a string
containing item name, vendor, and price:

```
function best_vendor_string(g: Group<string, (string, bit<64>)>): string
{
    var min_vendor = "";
    var min_price = 'hffffffffffffffff;
    for (((vendor, price), w) in g) {
        if (price < min_price) {
            min_vendor = vendor;
            min_price = price;
        }
    };
    "Best deal for ${group_key(g)}: ${min_vendor}, $${min_price}"
}

output relation BestDeal(best: string)
BestDeal(best) :-
    Price(item, vendor, price),
    var best = (vendor, price).group_by(item).best_vendor_string().
```

Similar to vectors, sets, and other container types, groups are iterable
objects, and can be iterated over in a for-loop of flattened by a `FlatMap`
operator.  Let us have a closer look at the for-loop in the last code snippet:

```
for (((vendor, price), w) in g) { .. }
```

The iterator yields a 2-tuple, where the first component `(vendor, price)` of
type `(string, bit<64>)` is the value stored in the group, and the second
component, `w` of type `DDWeight`, is the **weight** associated with this value.
Internally DDlog tracks the number of times each record has been derived.  For
example, records in input relations have weight 1.  An intermediate relation can
contain values with weights greater than 1, e.g., if the same value has been
derived by multiple rules.  The complete set of rules for computing weights is
quite complex and is not yet fixed as part of the language semantics.
Fortunately, in most cases, the aggregate function does not depend on weights,
and they can be safely ignored, as in this example (`w` is never used in the
body of the loop, and can be replaced by the `_` placeholder).

#### Debugging and tracing using `Inspect`

The `Inspect` operator offers visibility into the flow of data inside DDlog.
As the name suggests, it allows the programmer to inspect the values of
variables inside a rule without affecting how the rule is evaluated.
It can be inserted after any operator.  When triggered, `Inspect` evaluates a
specified expression, which can include side effects such as writing a log
message:

```
// The `inspect_log.dl` module in the `datalog_tests` folder implements
// a `log()` function convenient for testing.
import inspect_log as log

output relation BestDeal(best: string)
BestDeal(best) :-
    Price(item, vendor, price),
    Inspect log::log("../tutorial.log", "ts:${ddlog_timestamp}, w:${ddlog_weight}: Price(item=\"${item}\", vendor=\"${vendor}\", price=${price})"),
    var best = Aggregate((item), best_vendor_string((vendor, price))),
    Inspect log::log("../tutorial.log", "ts:${ddlog_timestamp}, w:${ddlog_weight}: best(\"${item}\")=\"${best}\"").
```

The `Inspect` expression can access all variables defined at the current point
in the rule (e.g., `item`, `vendor`, and `best` in the above example), and two
additional variables that expose two pieces of internal metadata DDlog
associates with each record: **timestamp** and **weight**.  The timestamp,
stored in the `ddlog_timestamp` special variable, represents the logical time
when the record was created.  For non-recursive rules, the timestamp consists of
a global epoch, i.e., the serial number of the transaction that created the
record (see `DDEpoch` type declared in [`lib/ddlog_std.dl`](../../lib/ddlog_std.dl)).
For recursive rules, the timestamp additionally includes iteration number of the inner
fixed point computation when the record was produced (see `DDNestedTS` type in
[`lib/ddlog_std.dl`](../../lib/ddlog_std.dl)).

The `ddlog_weight` variable stores the weight of a record, i.e., the number of
times the record has been derived at the current logical timestamp.  Newly
derived records have positive weights, while records being deleted have negative
weights.  Weights aggregate over time.  For example, a record can appear with
weight `+1` during the first transaction, and then again with weight `+1` during
the second transaction, bringing the total weight to `+2`.  If the record
appears with negative weight `-2` in a subsequent transaction, its aggregated
weight becomes `0`, effectively removing the record from the collection.

The following log fragment was generated by `Inspect` operators in the above example.

```
ts:18, w:1: Price(item="A", vendor="Overpriced Inc", price=120)
ts:18, w:1: Price(item="A", vendor="Scrooge Ltd", price=110)
ts:18, w:1: Price(item="A", vendor="Discount Corp", price=100)
ts:18, w:1: Price(item="B", vendor="Overpriced Inc", price=123)
ts:18, w:1: Price(item="B", vendor="Scrooge Ltd", price=150)
ts:18, w:1: Price(item="B", vendor="Discount Corp", price=111)
ts:18, w:1: best("A")="Best deal for A: Discount Corp, $100"
ts:18, w:1: best("B")="Best deal for B: Discount Corp, $111"
ts:19, w:-1: Price(item="A", vendor="Discount Corp", price=100)
ts:19, w:-1: Price(item="B", vendor="Discount Corp", price=111)
ts:19, w:-1: best("A")="Best deal for A: Discount Corp, $100"
ts:19, w:1: best("A")="Best deal for A: Scrooge Ltd, $110"
ts:19, w:-1: best("B")="Best deal for B: Discount Corp, $111"
ts:19, w:1: best("B")="Best deal for B: Overpriced Inc, $123"
```

The inspect operator is typically used in conjunction with a logging library, such
as the one in [`lib/log.dl`](../../lib/log.dl).

#### Relation transformers

Transformers are a way to invoke incremental computations implemented in Rust
directly on top of differential dataflow. This is useful for those computations
that cannot be expressed in DDlog, but can be implemented in differential. A
transformer takes one or more relations and returns one or more relations.  A
transformer can be additionally parameterized by one or more **projection**
functions that extract values from input relations.  The following strongly
connected component transformer takes `from` and `to` functions that extract
source and destination node id's from the input relation that stores graph
edges:

```
/* Compute strongly connected components of a directed graph:
 *
 * Type variables:
 * - `'E` - type that represents graph edge
 * - `'N` - type that represents graph node
 *
 * Arguments:
 * - `Edges` - relation that stores graph edges
 * - `from`  - extracts the source node of an edge
 * - `to`    - extracts the destination node of an edge
 *
 * Output:
 * - `Labels` - partitions a subset of graph nodes into strongly
 *    connected components represented by labeling these nodes
 *    with SCC ids.  An SCC id is id of the smallest node in the SCC.
 *    A node not covered by the partitioning belongs to an SCC
 *    with a single element (itself).
 */
extern transformer SCC(Edges:   relation['E],
                       from:    function(e: 'E): 'N,
                       to:      function(e: 'E): 'N)
    -> (SCCLabels: relation [('N, 'N)])
```

Only extern transformers are supported at the moment. Native/non-extern transformers, i.e.,
transformers implemented in DDlog, may be introduced in the future as a form of
code reuse (a transformer can be used to create a computation that can be instantiated
multiple times for different input/output relations).

Several useful graph transformers are declared in
[`lib/graph.dl`](../../lib/graph.dl) and implemented in
[`lib/graph.rs`](../../lib/graph.rs)

### Variable mutability

We summarize variable mutability rules in DDlog.  Variables declared in the
context of a rule are immutable: they are bound once and do not change their
value within the rule.  Mutability of variables in an expression is determined
according to the following rules:

* Local variables declared using `var v` are mutable.

* Function arguments with `mut` qualifier are mutable.

* Other function arguments are immutable.

* Loop variables are automatically modified at each iteration of the loop, but
  cannot be modified in the body of the loop.

* Variables declared in the rule scope and used inside an expression (e.g., in
  an assignment clause) are immutable.

* Variables declared inside a match pattern are mutable if and only if the expression
  being matched is mutable:

Mutable variables and their fields can be assigned to or passed as mutable
arguments to functions.

```
function f(x: mut string, y: u32) {
    var z = 0;

    // ok: z is a local variable.
    z = y;

    // ok: x is a `mut` argument.
    x = "foo"

    // error: y is a read-only argument.
    y = 0;

    var vec = [0, 1, 2];
    for (val in vec) {
        // error: loop variable cannot be modified in the body of the loop.
        val = y
    };

    var opt: Option<u32> = Some{0};

    match (opt) {
        Some{u} -> {
            // ok: `opt` is mutable; hence it components are mutable.
            u = 5
        },
        _ -> ()
    }
}
```

There are several exceptions to the above rules:

* Field of variables of type `Ref<>` or `Intern<>` are immutable.

* Variables used in the expression that the loop iterates over cannot be modified
  in the body of the loop.

* In addition, to prevent errors, DDlog disallows modifying any variables inside
  `if`-conditions, or inside operands of binary or unary operators.

```
typedef TestStruct = TestStruct {
    f1: string,
    f2: bool
}

function f() {
    var test: TestStruct = TestStruct{"foo", false};
    var interned: Intern<TestStruct> = TestStruct{"foo", false}.intern();

    // ok: modifying field of a local variable.
    test.f1 = "bar";

    // ok: overwriting the entire interned variable.
    interned = TestStruct{"bar", true}.intern();

    // error: cannot modify the contents of an interned value.
    interned.f1 = "bar";

    var vec = [0, 1, 2];
    for (val in vec) {
        // error: cannot modify a variable while iterating over it.
        vec.push(val)
    };

    // error: attempt to modify a variable inside an if-condition.
    if ({vec.push(3); vec.len()} > 5) {
        ...
    }
}
```

## Primary keys

We have so far used `insert` and `delete` commands to make changes to input
relations.  Both commands take a complete value to be added or removed from
the relation.  It is often more convenient to refer to records in the relation by
a unique key if such a key exists for the given relation.  E.g., one may wish
to delete a value associated with a specified key by specifying
only the key and not the entire value.  DDlog supports three commands for
accessing input relations by key: `delete_key`, `insert_or_update`, and `modify`
(see [Command Language Reference](../command_reference/command_reference.md)
for details).  These commands are only applicable to input relations declared with a
**primary key**.

A primary key typically consists of one or more columns, but in general it can be
an arbitrary function of the record.  In the following example, we define the
primary key as a tuple consisting of `author` and `title` columns.  We specify
it as a closure that takes a record `x` and returns the `(author, title)`
pair:

```
input relation Article(author: string, title: string, year: u16, pages: usize)
primary key (x) (x.author, x.title)
```

We can now access records in the `Article` relation by key:

```
start;

insert Article("D. L. Parnas", "On the Criteria To Be Used in Decomposing System into Modules", 1974, 6),
insert Article("C. A. R. Hoare", "Communicating Sequential Processes", 1980, 40),
insert Article("Test Author", "Test Title", 2020, 10),

# Delete test record by only specifying its primary key.
delete_key Article ("Test Author", "Test Title"),

# We accidentally specified incorrect year for Parnas's article.
# Modify the record by only specifying values of field(s) we want to change.
modify Article ("D. L. Parnas", "On the Criteria To Be Used in Decomposing System into Modules") <- Article{.year = 1972},

# Delete Hoare's paper and replace it with a new record with the same primary key.
insert_or_update Article("C. A. R. Hoare", "Communicating Sequential Processes", 1978, 30),

commit;
```

In addition to enabling new commands, defining a primary key
changes the semantics of `insert` and `delete` commands.  The `insert` command
fails when a record with the same primary key exists in the relation with the
same or different value.  Likewise, `delete` fails if the specified record does
not exist.  In contrast, duplicate insertions and deletions are ignored for
regular relations.

## Multisets

All DDlog relations considered so far implement set semantics.  A set can contain
at most one instance of each value.  If the same value is inserted in the relation
multiple times, all but the last insertion are ignored (or fail if the relation
has a primary key).  Likewise, repeated deletions are ignored or fail.  Similarly,
DDlog ensures that output relations behave as sets, e.g., the same value cannot
be inserted multiple times, unless it is deleted in between.

The following table illustrates the set semantics by showing a sequence of
commands that insert and delete the same value in a relation (without a primary
key)

| # | command | multiplicity | comment                                                               |
|---|---------|:------------:|-----------------------------------------------------------------------|
| 1 | insert  |       1      | record added to input table                                           |
| 2 | insert  |       1      | second insert is ignored (or fails if the relation has a primary key) |
| 3 | delete  |       0      | delete the record from the table                                      |
| 4 | insert  |       1      | insert it again                                                       |


While this behavior matches the requirements of most applications, there are
cases when relations with non-unit multiplicities are useful.  Consider, for
example, an input relation that receives inputs from two sources, that can both
produce the same value (at which point its multiplicity is 2).  The value should only be
removed from the relation once it has been removed from both sources, i.e., its
multiplicity drops to 0.  In other words, we would like the relation to have
*multiset*, rather than set semantics.  Such a relation can be declared by using
the `multiset` keyword instead of `relation`:

```
input multiset MSetIn(x: u32)
```

Values in a multiset relation can have both positive and negative
multiplicities: deleting a non-existent value introduces the value with
multiplicity `-1`.  Thus *multisets* can be seen as maps from elements to
integer counts.

| # | command | multiplicity | comment                                                               |
|---|---------|:------------:|-----------------------------------------------------------------------|
| 1 | insert  |       1      | record added to input table                                           |
| 2 | insert  |       2      | second insert increases the multiplicity to 2                         |
| 3 | delete  |       1      | delete decrements the multiplicity by one                             |
| 4 | insert  |       2      |                                                                       |
| 5 | delete  |       1      |                                                                       |
| 6 | delete  |       0      | multiplicity drops to 0, the record gets deleted from the relation    |

An output relation can also be declared as `multiset`, in which case DDlog
can derive the same output value multiple times, so that `dump` and
`commit dump_changes` commands will output records with non-unit multiplicities:

```
output multiset MSetOut(x: u32)
MSetOut(x) :- MSetIn(x).
```

The following scenario illustrates the semantics of multisets:

```
start;

insert MSetIn(0),
# Insert the same value twice; due to the multiset semantics, it will appear twice in MSetOut.
insert MSetIn(1),
insert MSetIn(1),
commit dump_changes;
# expected output:
# MSetOut:
# MSetOut{.x = 0}: +1
# MSetOut{.x = 1}: +2

start;
# Add one more instance of the same record.
insert MSetIn(1),
commit dump_changes;

# expected output:
# MSetOut:
# MSetOut{.x = 1}: +1

start;
# Delete one instance of the record; we're down to 2.
delete MSetIn(1),
commit dump_changes;
# expected output:
# MSetOut:
# MSetOut{.x = 1}: -1

dump MSetOut;
# expected output:
# MSetOut{.x = 0} +1
# MSetOut{.x = 1} +2
```

Output `multiset`s are more memory-efficient than `relation`s: internally the
implementation uses only multisets, and converts them to sets when needed using
an expensive Differential Dataflow `distinct` operator.

## Streams

Incremental computation is often used in stream analytics in order to update
results of a query as new data arrives in real time.  Such applications
typically operate on a mix of persistent data that, once inserted in a relation,
remains there until explicitly deleted, and **ephemeral data** that gets
processed and instantly discarded.

As a case in point, the `ZipCodes` relation below, that maps a zip code to the
city where this zip is located, only changes occasionally.  In contrast, the
`Parcels` relation, that contains information about parcels received by the
postal service, may ingest thousands of records per second.  We join the two
relations and output a copy of records from `Parcels` enriched with the name of
the city where each parcel must be sent.

```
input relation ZipCodes(zip: u32, city: string)
input relation Parcel(zip: u32, weight: usize)

// Add the name of the destination city to each parcel.
output relation ParcelCity(zip: u32, city: string, weight: usize)
ParcelCity(zip, city, weight) :-
    Parcel(zip, weight),
    ZipCodes(zip, city).
```

At runtime, the client inserts a bunch of records in the `Parcel` relation,
reads the corresponding outputs from `ParcelCity` and clears the `Parcel`
relation since its contents is no longer needed and to avoid running out of
memory.  This is cumbersome and expensive.

A better solution is to use the DDlog **stream** abstraction, intended to
simplify working with ephemeral data, i.e., data that gets discarded after being
used.  A stream is a relation that only contains records added during the
current transaction.  When the transaction completes, all previously inserted
records as well as any records derived from them disappear without a trace and
the memory footprint of all streams in the program drops to zero.

We re-declare the `Parcel` relation as a stream (by using the `stream` keyword
instead of `relation`).  Any rule that uses a stream yields a stream; hence
`ParcelCity` must also be declared as a stream.

```
input relation ZipCodes(zip: u32, city: string)
input stream Parcel(zip: u32, weight: usize)

// Add the name of the destination city to each parcel.
output stream ParcelCity(zip: u32, city: string, weight: usize)
ParcelCity(zip, city, weight) :-
    Parcel(zip, weight),
    ZipCodes(zip, city).
```

No other changes to the program are required, and we can now observe how
streams behave at runtime:

```
echo Populating the zip code table.;
start;
insert ZipCodes(94022, "Los Altos"),
insert ZipCodes(94035, "Moffett Field"),
insert ZipCodes(94039, "Mountain View"),
insert ZipCodes(94085, "Sunnyvale"),
commit dump_changes;

echo Feeding data to the Parcel stream;
start;
insert Parcel(94022, 2),
insert Parcel(94085, 6),
commit dump_changes;

# DDlog output:
#   Populating the zip code table.
#   Feeding data to the Parcel stream
#   ParcelCity:
#   ParcelCity{.zip = 94022, .city = "Los Altos", .weight = 2}: +1
#   ParcelCity{.zip = 94085, .city = "Sunnyvale", .weight = 6}: +1

echo Send more parcels!;
start;
insert Parcel(94039, 10),
insert Parcel(94022, 1),
commit dump_changes;

# DDlog output:
#   Send more parcels!
#   ParcelCity:
#   ParcelCity{.zip = 94022, .city = "Los Altos", .weight = 1}: +1
#   ParcelCity{.zip = 94039, .city = "Mountain View", .weight = 10}: +1
```

Note that although the stream becomes empty at the start of every transaction,
DDlog does not output delete records (i.e., records with negative weights) for
streams.

In general, streams can be used in rules just like normal relations.  They can
be joined with other relations, filtered, flat-mapped, and grouped.  There are also some
restrictions on streams.  Most importantly, streams cannot be part of a
recursive computation.  In addition streams currently cannot be used as one of
the inputs to an antijoin.  These constraints may be removed in future versions
of DDlog.

Consider the following transaction that simultaneously changes the contents of
the `ZipCode` relation and the `Parcel` stream:

```
echo Modifying both inputs in the same transaction.;
start;
insert ZipCodes(94301, "Palo Alto"),
insert Parcel(94301, 4),
insert Parcel(94035, 1),
commit dump_changes;

# DDlog output:
#   Modifying both inputs in the same transaction.
#   ParcelCity:
#   ParcelCity{.zip = 94035, .city = "Moffett Field", .weight = 1}: +1
#   ParcelCity{.zip = 94301, .city = "Palo Alto", .weight = 4}: +1
```

Note that changes to `ZipCode` are reflected in the output of the transaction.
Generally speaking, the output of each transaction is computed based on the
contents of all input relations and streams right before the commit.

As far as the API is concerned, streams behave as multisets in that one can push
duplicate records to a stream.  Since the contents of a stream is discarded
after each transaction, the `clear` command is not defined for streams.


### Streaming aggregation; differentiation and delay operators

> ## :heavy_exclamation_mark: Unstable features: this section describes experimental features that are likely to change in future DDlog releases.

Next we would like to extend the parcel example to keep track of the total
weight of parcels sent to each Zip code.  This sounds like a job for the
`group_by` operator.  Let's see if it will work for streams:

```
output stream ParcelWeight(zip: u32, total_weight: usize)

// Streaming group_by: aggregates parcel weights for each individual transaction.
ParcelWeight(zip, total_weight) :-
    Parcel(zip, weight),
    var total_weight = weight.group_by(zip).sum_with_multiplicities().

function sum_with_multiplicities(g: Group<u32, usize>): usize {
    var s = 0;
    for ((w, m) in g) {
        s = s + w * (m as usize)
    };
    s
}
```

We group records in the `Parcel` stream by `zip`, extract the `weight` field
from each record and sum up the weights using the `sum_with_multiplicities` function.  This
function differs from the `group_sum` function from the standard library in
that it takes into account multiplicities of records in the group.  Recall that
each record in the group has a multiplicity (we use the term "multiplicity"
instead of "weight" here to avoid confusion with parcel weights), which indicates
how many times the record appears in the input relation or stream.
Multiplicities are important when dealing with streams, since streams can
contain multiple identical values.  In our example, multiple parcels with
identical weights can be sent to the same zip code.  Within the group, the
weight will appear once with multiplicity equal to the number of
occurrences of this weight in the stream.  Each iteration of the for-loop over
the group yields a 2-tuple consisting of parcel weight `w` and
multiplicity `m`.  We multiply `w` by `m` and aggregate across all elements
in the group to compute the total weight of parcels in the stream.

This above code compiles, **but the result is not what we intended**:

```
echo Feeding data to the Parcel stream;
start;
insert Parcel(94022, 2),
insert Parcel(94085, 6),
commit dump_changes;

# DDlog output:
#  ParcelWeight:
#  ParcelWeight{.zip = 94022, .total_weight = 2}: +1
#  ParcelWeight{.zip = 94085, .total_weight = 6}: +1

echo Send more parcels!;
start;
insert Parcel(94022, 8),
insert Parcel(94085, 10),
insert Parcel(94022, 1),
commit dump_changes;

# DDlog output:
#  ParcelWeight:
#  ParcelWeight{.zip = 94022, .total_weight = 9}: +1
#  ParcelWeight{.zip = 94085, .total_weight = 10}: +1
```

When applied to a stream, the `group_by` operator groups records for each
individual transaction ignoring all previous inputs.  This shouldn't come as
a surprise: streams contain ephemeral data that disappears without a trace
after each transaction.  Note that the output of `group_by` is also a
stream: it contains a new set of values for each transaction, while old values
are not explicitly retracted (we don't see events with negative weights
in the output of DDlog).

Fortunately, DDlog offers a way to aggregate the contents of a stream
over time.  Moreover it can do so efficiently using bounded CPU and
memory:

```
/* Aggregate the contents of the Parcel stream over time. */

// The ParcelFold relation contains aggregated parcel weights from all earlier
// transactions and the new Parcel records add by the last transaction.
relation ParcelFold(zip: u32, weight: usize)

// Add aggregated parcel weight from previous transactions.
ParcelFold(zip, total_past_weight) :- ParcelWeightAggregated-1(zip, total_past_weight).
// Add new parcels from the last transaction.
ParcelFold(zip, weight) :- Parcel'(zip, weight).

// Group and aggregate weights in the ParcelFold relation.
output relation ParcelWeightAggregated(zip: u32, weight: usize)

ParcelWeightAggregated(zip, total_weight) :-
    ParcelFold(zip, weight),
    var total_weight = weight.group_by(zip).sum_with_multiplicities().
```

This code uses two new DDlog language features in order to implement the new
**stream fold** idiom.  The idea is simple: we would like to aggregate weights
across all transactions without storing all individual weights.  To this end,
we replace the entire past history of the `Parcel` stream with a single value
per Zip code that stores the sum of all weights encountered before the current
transaction.  Each transaction updates this aggregate with the sum of new
weights added during the current transaction.  To this end we construct the
`ParcelFold` relation that consists of the previously aggregated weight and new
parcels added during the current transaction.

How do we refer to the previous contents of a relation?  All DDlog constructs
introduced so far operate on the current snapshot of relations.  We introduce
a new **delay** operator that refers to the contents of a relation from `N`
transactions ago, where `N` can be any positive integer constant.  Given a
relation `R` and a number `N`, `R-N` is a relation that contains the exact same
records as `R` did `N` transactions ago.  In particular `R-1` is the contents
of `R` in the end of the previous transaction.

Delayed relations can be used in DDlog rules just like any normal relation names.
In our example we add the values in `ParcelWeightAggregated` delayed by `1` to
the `ParcelFold` relation:

```
// Add aggregated parcel weight from previous transactions.
ParcelFold(zip, total_past_weight) :- ParcelWeightAggregated-1(zip, total_past_weight)
```

Next we need to add all new parcels inserted by the current transaction to
`ParcelFold`.  Our first instinct is to write `ParcelFold(zip, weight) :-
Parcel(zip, weight)`, which won't work, since `Parcel` is a stream and
`ParcelFold` is a relation.  Streams are ephemeral while records in
relations persist until explicitly removed.  Mixing the two can easily lead to
surprising outcomes; therefore DDlog will refuse to automatically
convert a stream into a relation.  Instead, it offers the *differentiation*
operator to do this explicitly.  The differentiation operator takes
a stream and returns a relation that contains exactly the values added to the
stream by the last transaction.  Intuitively, if we think about a stream `S`
as an infinitely growing table, the differentiation operator returns the
difference between the current and previous snapshots of the stream:
`S' = S \ (S-1)`, where the prime (`'`) symbol represent differentiation.
In our example we use the differentiation operator as follows.

```
// Add new parcels from the last transaction.
ParcelFold(zip, weight) :- Parcel'(zip, weight).
```

Now that we have constructed the `ParcelFold` relation, we are ready to group
and aggregate it just like any normal relation:

```
ParcelWeightAggregated(zip, total_weight) :-
    ParcelFold(zip, weight),
    var total_weight = weight.group_by(zip).group_sum().
```

Let's run the new code.  In the following output listing we show the output of
`ParcelWeightAggregated` and `ParcelWeight` next to each other for comparison.
The former is a relation that aggregates parcel weights across the entire
history, while the latter is a stream that aggregates weights in the last
transaction only:

```
echo Feeding data to the Parcel stream;
start;
insert Parcel(94022, 2),
insert Parcel(94085, 6),
commit dump_changes;

# DDlog output:
#  ParcelWeight:
#  ParcelWeight{.zip = 94022, .total_weight = 2}: +1
#  ParcelWeight{.zip = 94085, .total_weight = 6}: +1
#  ParcelWeightAggregated:
#  ParcelWeightAggregated{.zip = 94022, .weight = 2}: +1
#  ParcelWeightAggregated{.zip = 94085, .weight = 6}: +1

echo Send more parcels!;
start;
insert Parcel(94022, 8),
insert Parcel(94085, 10),
insert Parcel(94022, 1),
commit dump_changes;

# DDlog output:
#  ParcelWeight:
#  ParcelWeight{.zip = 94022, .total_weight = 9}: +1
#  ParcelWeight{.zip = 94085, .total_weight = 10}: +1
#  ParcelWeightAggregated:
#  ParcelWeightAggregated{.zip = 94022, .weight = 2}: -1
#  ParcelWeightAggregated{.zip = 94022, .weight = 11}: +1
#  ParcelWeightAggregated{.zip = 94085, .weight = 6}: -1
#  ParcelWeightAggregated{.zip = 94085, .weight = 16}: +1
```

After the first transaction, `ParcelWeight` and `ParcelWeightAggregated`
look the same; however after the second transaction `ParcelWeightAggregated`
retracts old aggregate values and replaces them with new aggregates
that sum up weights from both transactions.

## Advanced types

### Variant types

`EthPacket` below is a data type modeling the essential contents of
Ethernet packets, including source and destination addresses, and the
payload:

```
typedef eth_pkt_t = EthPacket {
    src     : bit<48>,
    dst     : bit<48>,
    payload : eth_payload_t
}
```

A payload can be one of several things: an IPv4 packet, an IPv6
packet, or some other level-3 protocol:

```
typedef eth_payload_t = EthIP4   {ip4 : ip4_pkt_t}
                      | EthIP6   {ip6 : ip6_pkt_t}
                      | EthOther
```

This declaration is an example of a *variant type*.  `EthIP4`,
`EthIP6`, and `EthOther` are the three *type constructors* and
variables in curly braces are *arguments* of type constructor.
`Category` from ["Hello, world!"](#hello-world) was also a variant
type, without arguments. `ip_addr_t` is another variant type, but with a
single constructor.

We continue by defining types holding important fields of IP4, IP6,
and transport protocol packets:

```
typedef ip4_pkt_t = IP4Pkt { ttl      : bit<8>
                           , src      : ip4_addr_t
                           , dst      : ip4_addr_t
                           , payload  : ip_payload_t}

typedef ip6_pkt_t = IP6Pkt { ttl     : bit<8>
                           , src     : ip6_addr_t
                           , dst     : ip6_addr_t
                           , payload : ip_payload_t}

typedef ip_payload_t = IPTCP   { tcp : tcp_pkt_t}
                     | IPUDP   { udp : udp_pkt_t}
                     | IPOther

typedef tcp_pkt_t = TCPPkt { srcPort : bit<16>
                           , dst{prt : bit<16>
                           , flags   : bit<9> }

typedef udp_pkt_t = UDPPkt { srcPort : bit<16>
                           , dstPort : bit<16>
                           , len     : bit<16>}
```

The following function creates a packet with Ethernet, IPv6 and TCP headers:

```
function tcp6_packet(ethsrc: bit<48>, ethdst: bit<48>,
                     ipsrc: ip6_addr_t, ipdst: ip6_addr_t,
                     srcport: bit<16>, dstport: bit<16>): eth_pkt_t
{
    EthPacket {
        // Explicitly name constructor arguments for clarity
        .src = ethsrc,
        .dst = ethdst,
        .payload = EthIP6 {
            // Omit argument name here
            IP6Pkt {
                .ttl = 10,
                .src = ipsrc,
                .dst = ipdst,
                .payload = IPTCP {
                    TCPPkt {
                        .src = srcport,
                        .dst = dstport,
                        .flags = 0
                    }
                }
            }
        }
    }
}
```

As with relations, type constructor arguments can be identified by name or by position in the
argument list.  The above function uses both notations.

How does one access the content of a variant type?  Let's say we have a variable `pkt` of type
`eth_pkt_t`.  We can access `pkt` fields just like a C struct, e.g., `pkt.ttl`, `pkt.src`.  This is
safe, as `eth_pkt_t` has a single type constructor and hence every instance of this type has this
field.  In contrast, `pkt.payload.ip4` is unsafe, since `eth_payload_t` has multiple constructors,
and the packet may or may not have the `ip4` field depending on its specific constructor.  DDlog
will therefore reject this expression.  Instead, a safe way to access variant types is using match
expressions.  The following function extracts IPv4 header from a packet or returns a default value
if the packet is not of type IPv4 (we will see a nicer way to deal with non-existent values
[below](#generic-types)):

```
function pkt_ip4(pkt: eth_pkt_t): ip4_pkt_t {
    match (pkt) {
        EthPacket{.payload = EthIP4{ip4}} -> ip4,
        _                                 -> IP4Pkt{0,0,0,IPOther}
    }
}
```

Note how the match pattern simultaneously constrains the shape of the packet and binds its relevant
fields to fresh variable names (`ip4` in this case).

Structural matching can be performed up to arbitrary depth.  For example, the following function
matches both level-3 and level-4 protocol headers to extract destination UDP port number from a
packet.

```
function pkt_udp_port(pkt: eth_pkt_t): bit<16> {
    match (pkt) {
        EthPacket{.payload = EthIP4{IP4Pkt{.payload = IPUDP{UDPPkt{.dst = port}}}}} -> port,
        EthPacket{.payload = EthIP6{IP6Pkt{.payload = IPUDP{UDPPkt{.dst = port}}}}} -> port,
        _ -> 0
    }
}
```

Structural matching can also be performed directly in the body of a rule.  The following program
extracts all TCP destination port numbers from an input relation that stores a set of packets.

```
input relation Packet(pkt: eth_pkt_t)
output relation TCPDstPort(port: bit<16>)

TCPDstPort(port) :- Packet(EthPacket{.payload = EthIP4{IP4Pkt{.payload = IPTCP{TCPPkt{.dst = port}}}}}).
TCPDstPort(port) :- Packet(EthPacket{.payload = EthIP6{IP6Pkt{.payload = IPTCP{TCPPkt{.dst = port}}}}}).
```

Consider, the first of these rules.  DDlog interprets it as follows: "for every packet in the
`Packet` relation that matches the specified pattern, bind a fresh variable `port` to the value of
the TCP destination port of the packet and add a record with this value to the `TCPDstPort`
relation".  Note that this example requires two rules to capture two separate patterns (for IPv4 and
IPv6 packets).

It is possible to pattern-match the structure and content of a value at the same time.  For
instance, we can modify the last rule above to match only TCP packets whose source port number
is 100:

```
TCPDstPort(port) :- Packet(EthPacket{.payload = EthIP6{IP6Pkt{.payload = IPTCP{TCPPkt{.src=100, .dst=port}}}}}).
```

### Tuples

Imagine that we wanted to write a function that takes a 32-bit IP address and splits it into
individual bytes.  How do we define a function that returns multiple values?  In languages like C or
Java this can be achieved by modifying function arguments passed by reference.  Another option is to
define a new type with four fields, e.g., `typedef four_bytes_t = FourBytes{b3: bit<8>, b2: bit<8>,
b1: bit<8>, b0: bit<8>}`.  DDlog offers a nicer solution based on *tuple types*.  A tuple is just a
group of related values of possibly different types.  A tuple type lists the types of its values.
For example, our IP address splitting function could return a tuple with four 8-bit fields:

```
function addr_to_tuple(addr: bit<32>): (bit<8>, bit<8>, bit<8>, bit<8>)
{
    // construct an instance of a tuple
    (addr[31:24], addr[23:16], addr[15:8], addr[7:0])
}
```

We use this function to compute a subset of IP addresses on the `192.168.*.*` subnet:

```
input relation KnownHost(addr: ip4_addr_t)
output relation IntranetHost(addr: ip4_addr_t)

IntranetHost(addr) :- KnownHost(addr),
                      (var b3, var b2, _, _) = addr_to_tuple(addr),
                      b3 == 192,
                      b2 == 168.
```

Note the assignment that binds variables `b2` and `b3` to individual fields of the tuple returned by
`addr_to_tuple()`.  DDlog allows an even more compact and intuitive way to express this rule by
combining assignment and pattern matching:

```
IntranetHost(addr) :- KnownHost(addr),
                      (192, 168, _, _) = addr_to_tuple(addr).
```

The fields of a tuple can be accessed using the record field access
syntax but using numbers instead of field names: `.0` is the 0-th
field of the tuple (reading from the left).  The previous rule can
be also written as:

```
IntranetHost(addr) :- KnownHost(addr),
                      var t = addr_to_tuple(addr),
                      t.0 == 192, t.1 == 168.
```

### Generic types

Let's revise the `pkt_ip4()` function defined above.  The function extracts IPv4 header from a
packet.  If the packet does not have an IPv4 header, it returns a default value with all fields set
to 0:

```
function pkt_ip4(pkt: eth_pkt_t): ip4_pkt_t {
    match (pkt) {
        EthPacket{.payload = EthIP4{ip4}} -> ip4,
        _                                 -> IP4Pkt{0,0,0,IPOther}
    }
}
```

This does not feel satisfactory.  A well-designed interface should give an explicit indication that
the requested header is missing.  One solution would be to return a variant type with two
constructors:

```
// A type that represents an IP4 header or its absence.
typedef option_ip4_pkt_t = IP4Some{ p: ip4_pkt_t }
                         | IP4None
```

However, defining an extra variant type for each type in the program quickly becomes a burden on
the programmer.  Fortunately, DDlog allows us to define a generic option type that can be
instantiated for any concrete type.  This type is defined in the DDlog standard library as follows:

```
typedef Option<'A> = None
                   | Some {value : 'A}
```

Here `'A` is a *type argument* that must be replaced with a concrete type to create a concrete
instantiation of `Option`.  We can now rewrite the `pkt_ip4()` function using `Option`:

```
function pkt_ip4(pkt: eth_pkt_t): Option<ip4_pkt_t> {
    match (pkt) {
        EthPacket{.payload = EthIP4{ip4}} -> Some{ip4},
        _                                 -> None
    }
}
```

The standard library defines several useful functions for working with optional values:

```
/* `x` is a `None`. */
function is_none(x: Option<'A>): bool
/* `x` is a `Some{}`. */
function is_some(x: Option<'A>): bool
/* Applies transformation `f` to the value stored inside `Option`. */
function map(opt: Option<'A>, f: function('A): 'B): Option<'B> {
```

### `Result<>` type and error handling

In addition to the `Option` type introduced above, the standard library defines
another type that describes the result of a computation that can either succeed
and produce a value of type `'V` or fail and return an error of type `'E`:

```
typedef Result<'V,'E> = Ok{res: 'V}
                      | Err{err: 'E}
```

We recommend that all functions that can fail return either `Result` or
`Option`.  The DDlog type system protects the programmer from accidentally
ignoring an error: before accessing a value returned by a function, the program
must pattern match it against `Some` and `None` (or `Ok` and `Err`), and handle
the error case explicitly, e.g.:

```
/* Lookup item in the inventory and return its price in cents. */
function get_price_in_cents(inventory: Map<string, string>, item: string): Option<u64> {
    match (inventory.get(item)) {
        None -> None,
        Some{price} -> match (parse_dec_u64(price)) {
                           None    -> None,
                           Some{p} -> Some{100 * p}
                       }
    }
}
```

As a result, bugs like NULL pointer dereferences and unhandled exceptions are
impossible in DDlog.  On the flip side, explicitly checking the result of every
function call can be burdensome.  DDlog offers several mechanisms for handling
errors in a concise and safe manner:

1. `map` and `map_error` functions

1. `unwrap_` functions

1. The `?` operator

`map` and `map_error` functions, defined in the standard library, convert the
`Result<>` type into a different `Result<>` type:

```
/* Maps a `Result<'V1, 'E>` to `Result<'V2, 'E>` by applying a function to a
 * contained `Ok` value, leaving an `Err` value untouched.  */
function map(res: Result<'V1, 'E>, f: function('V1): 'V2): Result<'V2, 'E>

/* Maps a `Result<'V, 'E1>` to `Result<'V, 'E2>` by applying a function to a
 * contained `Err` value, leaving an `Ok` value untouched. */
function map_err(res: Result<'V, 'E1>, f: function('E1): 'E2): Result<'V, 'E2>
```

`unwrap_` functions, also defined in the standard library, unwrap the `Option` or
`Result` type, returning their inner value on success or a default value on
error:

```
/* Returns the contained `Some` value or a provided default. */
function unwrap_or(x: Option<'A>, def: 'A): 'A

/* Returns the default value for the given type if `opt` is `None`. */
function unwrap_or_default(opt: Option<'A>): 'A

/* Returns the contained Ok value or a provided default. */
function unwrap_or(res: Result<'V,'E>, def: 'V): 'V

/* Returns the default value for the given type if `res` is an error. */
function unwrap_or_default(res: Result<'V,'E>): 'V
```

`unwrap_or_default()` returns the default value of its return type on error:
`0` for numeric types, `false` for Booleans, `""` for strings, etc.  Default
values of structs and tuples are constructed recursively out of default values
of their fields.

Here is a version of `get_price_in_cents()` that uses the `unwrap_` functions
to handle errors by replacing missing or invalid values with `0`:

```
/* As above, but returns 0 if the item is missing from the inventory or the price
 * string is invalid. */
function get_price_in_cents_unwrap(inventory: Map<string, string>, item: string): u64 {
    inventory.get(item).unwrap_or("0").parse_dec_u64().unwrap_or(0) * 100
}
```

Often, however, we want to send the error up the call stack instead of
suppressing it.  This can be achieved using the `?` operator, placed after an
expression that returns `Option<>` or `Result<>` inside a function scope.
Similar to `unwrap_`, it extracts the inner value on success.  On error, it
returns the error value (or `None`) from the function.  The `?` operator can
be used to implement `get_price_in_cents` as a one-liner:

```
/* get_price_in_cents written more concisely with the help of the `?` operator. */
function get_price_in_cents_(inventory: Map<string, string>, item: string): Option<u64> {
    Some{ inventory.get(item)?.parse_dec_u64()? * 100 }
}
```

The `?` operator can only be used inside a function whose return type is
`Option` or `Result`.  The following table summarizes the behavior of `?`
for various combinations of expression type and function return type.

| Function return type |     Expression type    | Expression value |  ? behaves as  |
|:--------------------:|:----------------------:|------------------|:--------------:|
| Option<T1>           | Option<T2>             | None             |   return None  |
| Option<T1>           | Option<T2>             | Some{x}          |        x       |
| Option<T1>           | Result<V,E>            | Err{e}           |   return None  |
| Option<T1>           | Result<V,E>            | Ok{v}            |        v       |
| Result<X,E>          | Result<Y,E>            | Err{e}           | return Err{e}  |
| Result<X,E>          | Result<Y,E>            | Ok{v}            |        v       |
| Result<V,E>          | Option<T>              |                  |     invalid    |
| Result<X,E1>         | Result<Y,E2>, E2 != E1 |                  |     invalid    |

### Extern types

Similar to extern functions, extern types are types implemented outside of
DDlog, in Rust, that are accessible to DDlog programs.  We already encountered
examples of extern types like `Ref<>` and `Intern<>`.  Their implementation
can be found in [`ddlog_std.dl`](../../lib/ddlog_std.dl) &
[`ddlog_std.rs`](../../lib/ddlog_std.rs) and
[`internment.dl`](../../lib/internment.dl) &
[`internment.rs`](../../lib/internment.rs) respectively.

All extern types are required to implement a number of generic and DDlog-specific
traits: `Default`, `Eq`, `PartialEq`, `Clone`, `Hash`, `PartialOrd`, `Ord`,
`Display`, `Debug`, `Serialize`, `Deserialize`, `FromRecord`, `IntoRecord`,
`Mutator`, `ToFlatBuffer`, `ToFlatBufferTable`, `ToFlatBufferVectorElement`.

Outside of DDlog libraries, extern types should only be defined when
absolutely necessary, as they are not easy to implement correctly, and their
API is not yet fully standardized.

See [more detailed discussion](#implementing-extern-functions-and-types-in-Rust)
on integrating Rust and DDlog code below.

## Explicit relation types

Consider the following relation:

```
input relation Person (name: string, nationality: string, occupation: string)
```

We would like to define a function that filters this relation based on some criteria, e.g., selects
all US nationals who are students.  Such a function could take all fields of the relation as
arguments; however a better software engineering practice is to pass the entire `Person` record to
the function:

```
function is_target_audience(person: Person): bool {
    (person.nationality == "USA") and
    (person.occupation == "student")
}
```

Note how we use relation name as a type name here.  In fact, the above relation declaration is merely
syntactic sugar that expands into the following:

```
typedef Person = Person{name: string, nationality: string, occupation: string}
input relation Person [Person]
```

The square bracket syntax declares a relation by specifying its record type rather that a list of
fields.  Using this syntax, we declare the `TargetAudience` relation that has the same record type
as `Person` and apply the `is_target_audience()` function to filter the `Person` relation.

```
output relation TargetAudience[Person]

TargetAudience[person] :- Person[person], is_target_audience(person).
```

In the context of a rule, square brackets are used to select or assign the entire record rather than
its individual fields.

## Reference type (`Ref<>`)

DDlog's rich type system makes it easy to store complex data structures in
relations.  As these data structures are copied to other relations, their
contents is duplicated, potentially wasting memory.  An alternative is to copy
such values by reference instead.

The reference type is declared in the standard library along with two functions
that manipulate references.

```
extern type Ref<'A>

extern function ref_new(x: 'A): Ref<'A>
extern function deref(x: Ref<'A>): 'A
```

Objects wrapped in references are automatically reference counted and
deallocated when the last reference is dropped.  The programmer must however
**avoid creating cyclic references**, as those will form garbage that will stay
in memory forever.  References can be compared using all comparison operators
(`==`, `!=`, `<`, `<=`, `>`, `>=`).  The operators compare the objects wrapped in
references rather than their addresses.  Two references are equal if and only
if the objects they point to are equal, whether they are the same object or not.

The `ref_new()` and `deref()` functions are in principle sufficient
to work with references, but they would be cumbersome to use without
additional syntactic support, described below.

Consider two relations that store information about schools and students:

```
typedef student_id = bit<64>

input relation &School(name: string, address: string)
input relation &Student(id: student_id, name: string, school: string, sat_score: bit<16>)
```

We are planning to copy records from these relations around and therefore wrap them
in `Ref<>`'s.  The ampersand (`&`) in these declaration is a shortcut notation for:

```
typedef School{name: string, address: string}
input relation School[Ref<School>]
```

We now compute a new relation that stores the details of a student and their
school in each record:

```
relation StudentInfo(student: Ref<Student>, school: Ref<School>)

StudentInfo(student, school) :-
    student in &Student(.school = school_name),
    school in &School(.name = school_name).
```

Note the use of the `Ref` type in relation declaration.  Note also how `&` is
used to "open up" the relation and pattern match its fields without
calling `deref` on it.  Here is an equivalent rule written without `&`:

```
StudentInfo(student, school) :-
    Student[student],
    Student{.school = var school_name} = deref(student),
    School[school],
    deref(school).name == school_name.
```

Next we use the `StudentInfo` relation to select the top SAT score for each school:

```
output relation TopScore(school: string, top_score: bit<16>)

TopScore(school, top_score) :-
    StudentInfo(&Student{.sat_score = sat}, &School{.name = school}),
    var top_score = Aggregate((school), group_max(sat)).
```

We again use `&` to pattern match values stored by reference.

As another syntactic convenience, DDlog allows accessing fields of structs and
tuples wrapped in references directly, without dereferencing them first.
The following rule is equivalent to the one above, but instead of opening up the
`student` record, we use `.`-notation to access the `sat_score` field:

```
TopScore(school, top_score) :-
    StudentInfo(student, &School{.name = school}),
    var top_score = Aggregate((school), group_max(student.sat_score)).
```

### Performance pitfalls with pattern matching references

`&` can pattern match a value stored by reference, so it is tempting
to use it to extract the value of a `Ref<>` column.  For example, we
could replace `student` by `&student` in the `StudentInfo` clause just
above, like this:

```
TopScore(school, top_score) :-
    StudentInfo(&student, &School{.name = school}),
    var top_score = Aggregate((school), group_max(student.sat_score)).
```

This produces the same output as the former rule.  It does not depend
on automatic dereferencing behavior; instead, the `StudentInfo` clause
binds `student` to a copy of the dereferenced value rather than to the
reference itself.

However, this is a performance anti-pattern.  DDlog copies by value
any types not wrapped in `Ref<>` (or in `Intern<>`, described later),
so this copies the whole `Student` record every time it executes.
This is a waste of memory and CPU and it does not happen in the former
version of the rule.

`&` is still useful and does not incur any overhead when used to
deconstruct an object wrapped in a reference and refer to its fields,
as in `&School{.name = school}` in the second part of the
`StudentInfo` clause above.

On top of this, the `@` operator can be used to simultaneously bind
the entire value stored in a column and its individual fields.  For
example, one could write:

```
    StudentInfo(stu @ &Student{.sat_score = sat},
                sch @ &School{.name = school}),
```

The `&`s in this rule deconstruct the value inside the reference;
however, since the binding operator `@` precedes `&`, we bind `stu`
and `sch` to the references themselves, not the values that they wrap.

## Interned values (`Intern<>`, `istring`)

Interned values offer another way to save memory when working with large data
objects by storing exactly one copy of each distinct object.  Consider the
following program that takes a table of online orders as input and outputs
a re-arranged representation of the table where every record lists all orders
that contain one specific item.

```
input relation OnlineOrder(order_id: u64, item: string)
output relation ItemInOrders(item: string, orders: Vec<u64>)

ItemInOrders(item, orders) :-
    OnlineOrder(order, item),
    var orders = Aggregate((item), group2vec(order)).
```

Popular items like milk will occur in many orders, causing the string `"milk"`
to be allocated and stored many times.  Interning avoids wasting memory by
making sure that all identical strings point to the same memory location.
Internally, interned objects are kept in a hash table.  When a new interned object is
created, DDlog looks it up in the hash table.  If an identical object exists, it
simply returns a new reference to it; otherwise a new object with reference
count of 1 is created.  An interned object is deallocated when the last reference
to it is dropped.  All this happens behind the scenes; the programmer simply has
to replace `string` with `istring` in relation declarations to take advantage
of interning:

```
input relation OnlineOrder(order_id: u64, item: istring)
output relation ItemInOrders(item: istring, orders: Vec<u64>)
```

Interned strings are input and output by DDlog just like normal strings.  Inputs:

```
start;
insert OnlineOrder(1, "milk"),
insert OnlineOrder(1, "eggs"),
insert OnlineOrder(1, "jackfruit"),
insert OnlineOrder(2, "sursild"),
insert OnlineOrder(2, "milk"),
insert OnlineOrder(2, "eggs"),
commit dump_changes;
```

Outputs:
```
ItemInOrders{.item = "eggs", .orders = [1, 2]}: +1
ItemInOrders{.item = "jackfruit", .orders = [1]}: +1
ItemInOrders{.item = "milk", .orders = [1, 2]}: +1
ItemInOrders{.item = "sursild", .orders = [2]}: +1
```

DDlog implements two functions to manipulate interned objects. `ival()`
takes an interned object (e.g., an `istring`) and returns the
value it points to (e.g., `string`).  `intern()` is the inverse of
`ival()` -- it interns its argument and returns a reference to the interned
object.  Both functions are declared in [`internment.dl`](../../lib/internment.dl),
a standard library automatically imported by all DDlog programs.

In the following example, we pretty-print `OnlineOrder` records by first using `ival()`
to extract the value of an item and then using `intern()` to intern the
formatted string:

```
output relation OrderFormatted(order: istring)

OrderFormatted(formatted) :-
    OnlineOrder(order, item),
    var formatted: istring = intern("order: ${order}, item: ${ival(item)}").
```

Just like with normal strings, it is often necessary to compare interned
strings, e.g., we may want to filter out all orders containing milk:

```
output relation MilkOrders(order: u64)
MilkOrders(order) :- OnlineOrder(order, i"milk").
```

Here we construct an interned string by prepending `i` to string literal `"milk"`.
This works for any form of [string literals](#string-constants-literals).  The
compiler expands `i"milk"` to `intern("milk")`, which gets evaluated statically.

Interned strings are just one important special case of interned objects.  The
general interned object type is declared in [`internment.dl`](../../lib/internment.dl)
as `extern type Intern<'A>`.  In the following example, we intern the user-defined
type `StoreItem`:

```
typedef StoreItem = StoreItem {
    name: string,
    description: istring
}
typedef IStoreItem = Intern<StoreItem>

input relation StoreInventory(item: IStoreItem)
output relation InventoryItemName(name: istring)

/* `ival()` and `intern()` functions work for all interned
 * objects, not just strings. */
InventoryItemName(name) :-
    StoreInventory(item),
    var name = intern(ival(item).name).
```

The `internment.dl` library contains other useful declarations, which you may
want to check out if you are working with interned values.

## A more imperative syntax

Some people dislike Datalog because the evaluation order of rules is not always obvious.  DDlog
provides an alternative syntax for writing rules which resembles more traditional imperative
languages.  For example, the [previous program](#explicit-relation-types) can be written as follows:

```
for (person in Person if is_target_audience(person))
    TargetAudience(person)
```

Yet another way to write this program is:

```
for (person in Person)
   if (is_target_audience(person))
       TargetAudience(person)
```

Using this syntax, joins are expressed with nested for-loops; for example the following computes
an inner join of `Region` and `Person` relations over the `region.id` field:

```
for (region in Region) {
    for (person in Person(.region = region.id))
       if (is_target_audience(person))
           TargetAudience(person, region)
}
```

One can also define variables:

```
for (person in Person)
    var is_audience: bool = is_target_audience(person) in
    if (is_audience)
        TargetAudience(person)
```

Here is a more complex example that simultaneously filters out records that do not contain
a "router-port" option and assign the value of this option to the `lrport_name` variable.

```
for (lsp in Logical_Switch_Port) {
    Some{var lrport_name} = map_get(lsp.options, "router-port") in
    ...
}
```

One can also use `match` statements:

```
for (person in Person)
   var is_audience = is_target_audience(person) in
   match (is_audience) {
       true -> TargetAudience(person)
       _    -> skip
   }
```

These 5 kinds of statements (`for`, `if`, `match`, `var`, blocks enclosed in braces) can be nested
in arbitrary ways.  When using this syntax, semicolons must be used as separators between statements.
The DDlog compiler translates such programs into regular DDlog programs.

## Avoiding weight overflow

Each DDlog record has an associated weight, a 32-bit signed integer
that counts the number of times the record occurs.  In a multiset, the
count's value is significant.  In a relation, DDlog hides the count,
acting as if there is only a single copy of any given record, but it
still maintains it internally.

Most of the time, a DDlog developer may ignore record weights.  In
unusual circumstances, though, weights can overflow the 32-bit range.
If this happens, the program malfunctions, so it's important for DDlog
developers to understand when overflow is likely to occur so that they
can avoid the problem.

Joining records with weights `x` and `y` yields a record with weight
`x*y`.  When the records being joined both have weight 1, which is the
most common case, the result also has weight 1 and no problem can
result.  However, a series of joins of input records with weights
greater than 1 can quickly overflow: with weight 1000, it only takes 4
joins to overflow, and even with weight 2, it still takes only 31.

The question, then, is what leads to weights greater than 1?  One
common cause is projection, that is, using only some of the data in a
record.  When the fields that are used have duplicates, the result is
a record weighted with the duplicate count.  For example, if we want a
relation that contains the nationality of each Person, we can declare
it and populate it as follows.  The weight of each record in
Nationality is the number of Person records with that given
nationality.  We can similarly define Occupation:

```
relation Nationality[string]
Nationality[nationality] :- Person(.nationality = nationality).

relation Occupation[string].
Occupation[occupation] :- Person(.occupation = occupation).
```

If we joined `Nationality` and `Occupation` in a rule, then the output
record's weight would be the product of the input records' weights.
In a pathological case where all `N` Persons had a single nationality
and occupation, the output record would have weight `N^2`.

Consider the following additional example, which is abstracted from an
actual bug in a system built with DDlog.  Suppose that a networking
system has a collection of switches, each of which may have ports with
different types and names, as well as access control lists (ACLs) and
load balancers.  The management system might provide the configuration
through a set of input relations, like this:

```
input relation Switch(name: string, description: string)
primary key (x) (x.name)

input relation Port(sw: string, port: string, ptype: string)
primary key (x) ((x.sw, x.port))

input relation ACL(sw: string, is_stateful: bool, details: string)

input relation LoadBalancer(sw: string, name: string)
```

As part of the implementation, the system might derive an
`AnnotatedSwitch` relation that associates each switch's configuration
with information about whether the switch has any ACLs (stateful or
stateless), load balancers, and router ports, like this:

```
relation AnnotatedSwitch(
    name: string,
    details: string,
    has_acl: bool,
    has_stateful_acl: bool,
    has_load_balancer: bool,
    has_router_port: bool)
```

We can calculate the `has_acl` column using a projection, as shown
below.  The `SwitchHasACL` relation associates each switch's name with
`true` if at least one ACL is defined for the switch, projecting away
everything but the switch name.  This means that, if a switch has `N`
ACLs, then its record in `SwitchHasACL` has weight `N`:

```
relation SwitchHasACL(sw: string, has_acl: bool)
SwitchHasACL(sw, true) :- ACL(sw, _, _).
SwitchHasACL(sw, false) :- Switch(sw, _), not ACL(sw, _, _).
```

We can define the other properties similarly:

```
relation SwitchHasStatefulACL(sw: string, has_stateful_acl: bool)
SwitchHasStatefulACL(sw, true) :- ACL(sw, true, _).
SwitchHasStatefulACL(sw, false) :- Switch(sw, _), not ACL(sw, true, _).

relation SwitchHasLoadBalancer(sw: string, has_load_balancer: bool)
SwitchHasLoadBalancer(sw, true) :- LoadBalancer(sw, _).
SwitchHasLoadBalancer(sw, false) :- Switch(sw, _), not LoadBalancer(sw, _).

relation SwitchHasRouterPort(sw: string, has_router_port: bool)
SwitchHasRouterPort(sw, true) :- Port(sw, _, "router").
SwitchHasRouterPort(sw, false) :- Switch(sw, _), not Port(sw, _, "router").
```

To populate `AnnotatedSwitch`, we join all of these relations, like this:

```
AnnotatedSwitch(sw, details, has_acl, has_stateful_acl, has_load_balancer, has_router_port) :-
    Switch(sw, details),
    SwitchHasACL(sw, has_acl),
    SwitchHasStatefulACL(sw, has_stateful_acl),
    SwitchHasLoadBalancer(sw, has_load_balancer),
    SwitchHasRouterPort(sw, has_router_port).
```

Since joins multiply weights, if a given switch has 100 ACLs, 50 of
which are stateful, 15 load balancers, and 10 router ports, then its
`AnnotatedSwitch` record will have weight 750,000.  In the real case
this example is based on, overflow in fact occurred with only 8 ACLs
because of additional multiplicative factors and layers.

This issue is arguably a bug in DDlog (see [Issue
#878](https://github.com/vmware/differential-datalog/issues/878)).  If,
for example, DDlog used `bigint` weights, then it would avoid the
problem entirely.  Future versions of DDlog might adopt this or
another strategy to avoid weight overflow, or at least to report
overflow when it occurs.

Until then, DDlog provides a workaround.  Relations marked as `output`
pass through a Differential Datalog `distinct` operator, which reduces
every positive weight to 1.  Thus, if we change `relation` to `output
relation` in each `SwitchHas<x>` declaration above, all of the records
in `AnnotatedSwitch` will have weight 1.  This has some runtime cost,
but it is reasonable to use this approach for all the projective
relations in a DDlog program.

For finding this kind of problem in a program, one can use the
`Inspect` operator to report weights.  For example, one might use it
in our example by adding `import print` to the `.dl` file and
appending the following clause to the `AnnotatedSwitch` rule:

```
    Inspect print("AnnotatedSwitch ${sw} weight=${ddlog_weight}").
```

If mysterious issues make one wonder whether weight overflow could be
the issue, another approach is to invoke `ddlog` with
`--output-internal-relations`.  This option transforms every internal
relation into an output relation, fixing all ordinary weight overflow
problems.

## Modules

Modules make it easy to write DDlog programs that span multiple files.  DDlog supports a simple
module system, where each file is a separate module, and the module hierarchy matches the directory
structure.

Let's create a file called `mod1.dl` that declares a single relation `R1`:

```
input relation R1(f1: bool)
```

Next, create a directory called `lib` with a single file `lib/mod2.dl`:

```
input relation R2(f2: string)
```

We have just built a simple module hierarchy:

```
|-mod1
|-lib
   |-mod2
```

Finally, create the main test file `test.dl` in the same directory as `mod1.dl`:

```
// declarations from mod1 are added to the local name space
import mod1
// declarations from mod2 are available via the "m2." prefix
import lib::mod2 as m2

output relation Main(f1: bool, f2: string)

Main(f1, f2) :- R1(f1), m2::R2(f2).
```

Note the two forms of the `import` directive.  The first form (`import <module_path>`) makes all
type, function, relation, and constructor declarations from the module available in the local
namespace of the module importing it.  The second form (`import <module_path> as <alias>`) makes all
declarations available to the importing module using the `<alias>::<name>` notation.

We create some data to feed to the test program in a `test.dat` file.

```
start;

insert mod1::R1(true),
insert lib::mod2::R2("hello");

commit;
dump;
```

Note that we refer to relations and constructors via their fully qualified names.
Compile and run the test:

```
ddlog -i test.dl
cd test
cargo build --release
target/release/test_cli < ../test.dat
```

Modules do not have to be in the same directory with the main program.  Assuming that our module
hierarchy is located, e.g., in `../modules`,  the first command above must be changed as follows:

```
ddlog -i test.dl -L../modules
```

Multiple `-L` options are allowed to access modules scattered across multiple directories.


## Implementing extern functions and types in Rust

We have encountered many examples of extern functions and types throughout the
tutorial.  Here we summarize the rules for integrating Rust code into your DDlog
program.

Since the DDlog compiler generates Rust, external Rust code integrates with
DDlog seamlessly and efficiently.  Consider a DDlog program `prog.dl` that
imports modules `mod1` and `mod2`:

```
prog.dl  // imports `mod1` and `mod2`.
mod1.dl
mod2/
 |
 +--+submod1.dl
 +--+submod2.dl
```


The compiler generates several Rust crates for this program:

```
prog_ddlog
    |                           +-+
    +----+types                   |
    |       +--+Cargo.toml        |  types crate:
    |       +--+std_ddlog.rs      |  types, functions, external Rust code
    |       +--+mod1.dl           |
    |       +--+mod2              |
    |       |    +--+lib.rs       |
    |       |    +--+submod1.rs   |
    |       |    +--+submod2.rs   |
    |       +--+lib.rs            |
    |                           +-+
    |                           +-+
    +----+Cargo.toml              |
    +----+src                     |  main crate:
           +--+lib.rs             |  Rust encoding of DDlog rules and relations.
           +--+main.rs            |
           +--+api.rs             |
                                +-+
```

The `types` crate is the one relevant for the purposes of this section.  It
contains all function and type declarations, including extern functions and
types.  Its internal module structure mirrors the structure of the DDlog
program, with a separate Rust module for each DDlog module.  For example,
declarations from a DDlog module `mod2/submod1.dl` are placed in
`prog_ddlog/types/mod1/submod1.rs`.  Types and functions declared in the main
module of the program are placed in `types/lib.rs`.  If `submod1.dl` contains
extern function or type declarations, corresponding Rust declarations must be
placed in `mod2/submod1.rs`.  This file is picked up by the DDlog compiler
and its contents are appended verbatim to the generated
`prog_ddlog/types/mod1/submod1.rs` module.

Extern type and function declarations must follow these rules:

- Extern type and function names must match their DDlog declarations.

- Extern types must implement a number of traits expected by DDlog.
  See the [section on extern types](#extern-types) for details.

- Extern function signatures must match DDlog declarations.  Arguments are
  passed by reference, functions return results by value, unless labeled
  with [`return_by_ref` attribute](#return_by_ref).  DDlog generates
  commented out function prototypes in Rust to help the user come up with
  correct signatures.
  See the [section on extern functions](#extern-functions) for details.

- Rust code can access function and type declarations in other program
  modules and libraries through the `crate::` namespace, e.g., to import
  the standard DDlog library:

  ```
  use crate::ddlog_std;
  ```

- Extern functions and types often contain dependencies on third-party crates.
  Such dependencies must be added to the generated `types/Cargo.toml` file.
  To this end, create a file with the same name and location as the DDlog module
  and `.toml` extension, containing dependency clauses in `Cargo.toml` format.
  As an example, the [`lib/regex.dl`](../../lib/regex.dl) library that
  implements DDlog bindings to the regular expressions crate `regex` has an
  accompanying `lib/regex.toml` file with the following contents:

  ```
  [dependencies.regex]
  version = "1.1"
  ```

  There is an important caveat: only one module in your program can import any
  given external crate. As a workaround, we recommend creating a separate DDlog
  library for each extern crate and only list this crate as a dependency for this
  library, e.g., `lib/regex.dl for the `regex` crate, `lib/url.dl` for the url
  crate, etc. Other libraries import the corresponding DDlog library and can then
  use it either via the DDlog bindings or by calling the Rust API in the external
  crate directly.

## The standard library

The module system enables the creation of reusable DDlog libraries.  Some of
these libraries are distributed with DDlog in the `lib` directory.  A
particularly important one is the standard library
[`ddlog_std.dl`](../../lib/ddlog_std.dl), that defines types like `Vec`,
`Set`, `Map`, `Option`, `Result`, `Ref`, and others.  This library is imported
automatically into every DDlog program.

## Indexes

DDlog normally operates on **changes** to relations: it accepts changes to
`input` relations as inputs and produces changes to `output` relations as
outputs.  In some applications, however, it is necessary to know the current
state of a relation, i.e., the set of all records in the relation, or the set of
records with specific values in a subset of columns.  For example, given a
relation that stores edges of a digraph

```
relation Edge(from: node_t, to: node_t)
```

one may need to query the set of edges with a given `from` or `to` node.  To do
so, the developer must create an **index** on the `Edge` relation in the DDlog
program for each subset of columns used as a key in a query:

```
// Index the `Edge` relation by `from` node.
index Edge_by_from(from: node_t) on Edge(from, _)
```

This declaration creates an index that can be used to lookup all records in the
`Edge` relation with the given `from` node.  It consists of a unique name
(`Edge_by_from`), a set of typed arguments that form the **key** (`from:
node_t`), and a pattern, which defines the set of values associated with each
key (`Edge(from, _)`).

Once an index has been created, it can be queried at runtime, e.g., using the
`query_index` [CLI](../command_reference/command_reference.md) command or an
equivalent method in your favorite language API:

```
# Output all edges where `from=100`.
query_index Edge_by_from(100);
```

It is also possible to dump the entire content of an index:

```
# Output all records in the `Edge` relation.
dump_index Edge_by_from;
```

One can create multiple indexes on the same relation:

```
// Index the `Edge` relation by `to` node.
index Edge_by_to(to: node_t) on Edge(_, to)
```

One can also define an index over multiple fields of the relation,
for example, we can index `Edge` by both `from` and `to` nodes:

```
// Index `Edge` by both source and destination nodes.
index Edge_by_to(from: node_t, to: node_t) on Edge(from, to)
```

The pattern expression simultaneously constraints the set of values and projects
it on the subset of columns that form the key.

```
// Type with multiple constructors.
typedef Many = A{x: string}
             | B{b: bool}
             | D{t: tuple}

relation VI(a: bool, b: Many)

// Index over a subset of records with `a` field equal to `true`,
// `b` field having type constructor `D`, and using the second
// element of the `b.t` tuple as key.
index VI_by_t1(x: bit<8>) on VI(.a=true,.b=D{(_, x, _)})
```

DDlog indexes can be associated with any input, output, or internal relation.

Although similar in spirit to indexes in SQL, DDlog indexes serve a different
purpose.  They do not affect the performance of the DDlog program and cannot be
used as an optimization technique (DDlog does use indexes for performance, but
these indexes are constructed automatically and are not visible to the user).

## Compiler flags

flags to the `ddlog` compiler.

### `nested-ts-32`

This option is useful in recursive programs where the recursive fragment of the
program may require more than `2^16` iterations.  Since in practice most
recursive computations perform fewer iterations, DDlog uses a 16-bit iteration
counter (or timestamp in the Differential Dataflow terminology) by default.
When this is not sufficient, the `nested-ts-32` can be used to tell DDlog to use
32-bit iteration counter instead.

Consider the following example that enumerates all numbers from `0` to `65599`
and stores the last 70 numbers in an output relation:

```
relation Rb(x: u32)
output relation Rque(x: u32)

Rb(0).
Rb(x+1) :- Rb(x), x < 65599.
Rque(x) :- Rb(x), x > 65530.
```

The recursive fragment of this program performs `65600` iterations; hence a
16-bit counter is not sufficient.

If the DDlog project was compiled without the `nested-ts-32` flag, 32-bit
timestamps can be enabled when compiling the generated Rust project by enabling
its using the `nested_ts_32` feature:

```
cargo build --release --features "nested_ts_32"
```

**TODO: Document other flags**

## Meta-attributes

Meta-attributes are annotations that can be attached to various DDlog program
declarations (types, constructors, fields, etc.).  They affect compilation
process and program's external behavior in ways that are outside of the language
semantics.  We describe currently supported meta-attributes in the following
sections.

### Size attribute: `#[size=N]`

This attribute is applicable to `extern type` declarations.  It specifies the
size of the corresponding Rust data type in bytes and serves as a hint to compiler
to optimize data structures layout:

```
#[size=4]
extern type IObj<'A>
```

### `#[original="name"]`

This attribute is applicable to relation declarations.  Some tools
generate DDlog code from other programming languages, and they may
need to synthesize relation names.  This annotation contains a string
holding the original name of the entity which caused the relation to
be generated.

### `#[dyn_alloc]`

This attribute is applicable to `extern type` declarations.  It tells the
compiler that the type allocates its storage dynamically on the heap.  Rust
knows the size of the stack-allocated portion of such types statically and
allows using them as part of recursive typedefs.

```
#[dyn_alloc]
extern type Set<'A>
```

### `#[custom_serde]`

Tells DDlog not to generate `Serialize` and `Deserialize` implementations for a type.
The user must write their own implementations in Rust.

```
#[custom_serde]
typedef JsonWrapper<'T> = JsonWrapper{x: 'T}
```

### `#[has_side_effects]`

Labels functions that have side effects, e.g., perform I/O.  The compiler will
not perform certain optimizations such as static evaluation for such functions.
This annotation is only required for extern functions with side effects. The
compiler automatically derives this annotation for functions that invoke
functions labeled with `#[has_side_effects]`.

```
#[has_side_effects]
extern function log(module: module_t, level: log_level_t, msg: string): bool
```

### `#[return_by_ref]`

Labels functions that return values by reference.  DDlog functions are compiled
into Rust functions that take arguments by reference and return results by value.
DDlog assumes the same calling convention for extern functions.  However it
also supports extern functions that return references, which is often more efficient.
The `#[return_by_ref]` annotation tells the compiler that the annotated extern
function returns a reference in Rust:

```
#[return_by_ref]
extern function deref(x: Ref<'A>): 'A
```

### `#[iterate_by_ref]` and `#[iterate_by_val]`

These attributes apply to extern types only and tell the compiler that the given
type is **iterable**.  Iterable types can be iterated over in a for-loop and
flattened by `FlatMap`.  The syntax for these attributes is:

```
#[iterate_by_ref=iter:<type>]
```

```
#[iterate_by_val=iter:<type>]
```

Both variants tell the DDlog compiler that the Rust type that the extern type
declaration binds to implements the `iter()` method, which returns a type that
implements the Rust `Iterator` trait, and that the `next()` method of the
resulting iterator returns values of type `<type>`.  The first form indicates
that the iterator returns the contents of the collection by reference, while
the second form indicates that the iterator returns elements in the collection
by value.

Here are two examples from `ddlog_std.dl`:

```
#[iterate_by_ref=iter:'A]
extern type Vec<'A>

#[iterate_by_val=iter:('K,'V)]
extern type Map<'K,'V>
```

### `#[deserialize_from_array=func()]`

This attribute is used in conjunction with `json.dl` library (and potentially
other serialization libraries).  When working with JSON, it is common to have
to deserialize an array of entities that encodes a map, with each entity containing
a unique key.  Deserializing directly to a map rather than a vector (which would
be the default representation) can be beneficial for performance if
frequent map lookups are required.

The `deserialize_from_array` attribute can be associated with a field of a struct
of type `Map<'K,'V>`. The annotation takes a function name as its value.  The function,
whose signature must be: `function f(V): K` is used to project each
value `V` to key `K`:

```
// We will be deserializing a JSON array of these structures.
typedef StructWithKey = StructWithKey {
    key: u64,
    payload: string
}

// Key function.
function key_structWithKey(x: StructWithKey): u64 {
    x.key
}

// This struct's serialized representaion is an array of
// `StructWithKey`; however it is deserialized into a map rather than
// vector.
typedef StructWithMap = StructWithMap {
    #[deserialize_from_array=key_structWithKey()]
    f: Map<u64, StructWithKey>
}

Example JSON representation of `StructWithMap`
{"f": [{"key": 100, "payload": "foo"}]}
```

### `#[rust="..."]`

This attribute is applicable to type definitions, constructors, and individual
fields of a constructor.  Its value is copied directly to the generated Rust
type definition.  It is particularly useful in controlling
serialization/deserialization behavior of the type.  DDlog generates
`serde::Serialize` and `serde::Deserialize` implementations for all types in
the program.  Meta-attributes defined in the `serde` Rust crate can be used to
control the behavior of these traits.  For example, the following declaration:

```
#[rust="serde(tag = \"@type\")"]
typedef TaggedEnum = #[rust="serde(rename = \"t.V1\")"]
                     TVariant1 { b: bool }
                   | #[rust="serde(rename = \"t.V2\")"]
                     TVariant2 { u: u32 }
```

creates a type whose JSON representation is:

```
{"@type": "t.V1", "b": true}
```

Other useful serde attributes are `default` and `flatten`.  See [serde
documentation](https://serde.rs/attributes.html) for details.

## Input/output to DDlog

DDlog offers several ways to feed data to a program:

1. Statically, by listing ground facts as part of the program.

1. Via a text-based command-line interface.

1. From a Rust program.

1. From a C or C++ program.

1. From a Java program.

1. From a Go program.

In the following sections, we expand on each method.

### Specifying ground facts statically in the program source code

This method is useful for specifying ground facts that are guaranteed to hold in every instantiation
of the program.  Such facts can be specified statically to save the hassle of adding them manually
on every program instantiation.  A ground fact is just a rule without a body.  Add the following
declarations to the program to pre-populate `Word1` and `Word2` relations:

```
Word1("Hello,", CategoryOther).
Word2("world!", CategoryOther).
```

### Using DDlog programs as libraries

The text-based interface to DDlog, described so far, is primarily intended for
testing and debugging purposes.  In production use, a DDlog program is typically
invoked as a library from a program written in a different language (e.g., C or Java).

Compile your program as explained [above](#compiling-the-hello-world-program).
Let's assume your program is `playpen.dl`.
When compilation completes, the `playpen_ddlog` directory will be created,
containing generated Rust crate for the DDlog program.  You can invoke this
crate from programs written in Rust, C/C++, Java, or any other language that
is able to invoke DDlog's C API through a foreign function interface.

1. **Rust**

See [Rust API test](../../test/datalog_tests/rust_api_test) for a by-example
description of the DDlog Rust API.

1. **C/C++**
The compiled program will contain a static library `playpen_ddlog/target/release/libplaypen_ddlog.a`
that can be linked against your C or C++ application.  The C API to DDlog is defined in
`playpen_ddlog/ddlog.h`.  To generate a dynamic library instead, pass the
`--dynlib` flag to `ddlog` to generate a `.so` (or `.dylib` on a Mac) file,
along with `--no-staticlib` to disable generation of the static library.

See [C API tutorial](../c_tutorial/c_tutorial.rst) for a detailed description of
the C API.

1. **Java**
If you plan to use the library from a Java program, make sure to use
`--dynlib` and `--no-staticlib` flags to generate a dynamically linked library.
    See [Java API documentation](../java_api.md) for a detailed description of
    the Java API.

1. **Go**

See [Go API documentation](../../go/README.md) for a detailed description of the
Go API.

1. The text-based interface is implemented by an
auto-generated executable `./playpen_ddlog/target/release/playpen_cli`.  This interface is
primarily meant for testing and debugging purposes, as it does not offer the same performance and
flexibility as the API-based interfaces.
    See [CLI documentation](../command_reference/command_reference.md) for a complete list of commands
supported by the CLI tool.

## Profiling

DDlog's profiling features are designed to help the programmer to understand
what parts of the DDlog program use the most CPU and memory.  DDlog supports two
commands related to profiling (also available through Rust, C, and Java APIs):

1. `profile cpu on/off;` - enables/disables recording of CPU usage info in
addition to memory profiling.  CPU profiling is not enabled by default, as
it can slow down the program somewhat, especially for large programs
that handle many small updates.

1. `profile;` - returns information about program's CPU and memory usage.  CPU
usage is expressed as the total amount of time DDlog spent evaluating each operator,
assuming CPU profiling was enabled.  For example the following CPU profile
record:
```
CPU profile
    ...
       0s005281us (        112calls)     Join: DdlogDependency(.parent=parent, .child=child), LabeledNode(.node=parent, .scc=parentscc), LabeledNode(.node=child, .scc=childscc) 165
    ...
```
indicates that the program spent `5,281` microseconds in 112 activations of the
join operator that joins the prefix of the rule (`DdlogDependency(.parent=parent, .child=child), LabeledNode(.node=parent, .scc=parentscc)`)
with the `LabeledNode(.node=child, .scc=childscc)` literal.

  Memory profile reports current (at the time when the profile is being generated)
  and peak (since the start of the program) number of records in each DDlog
  *arrangement*.  An arrangement is similar to an indexed representation of a
  relation in databases.  Arrangements are responsible for the majority of memory
  consumption of a DDlog program.  For example, the following memory profile
  fragment:
  ```
  Arrangement peak sizes
  ...
  451529      Arrange: LabeledNode{.node=_, .scc=_0} 136
  372446      Arrange: LabeledNode{.node=_0, .scc=_} 132
  ```
  indicates that the program contains two different arrangements of the `LabeledNode`
  relation, indexed by the second and first fields, whose peak size is
  451,529 and 372,446 records respectively (the numbered variables, e.g., `_0`)
  indicate one or more fields used to index the relation by.

## Replay debugging

When using DDlog as a library, it may be difficult to isolate bugs in the DDlog
program from those in the application using DDlog.  Replay debugging is a DDlog
feature that intercepts all DDlog invocations made by a program and dumps them
in a DDlog command file that can later be replayed against the DDlog program
running standalone, via the CLI interface.  This has several advantages.  First,
it allows the user to inspect DDlog inputs in human-readable form.  Second, one
can easily modify the recorded command file, e.g., to dump relations of interest
or to print profiling information in various points throughout the execution, or
to simplify inputs in order to narrow down search for an error.  Third, one can
replay the log against a modified DDlog program, e.g., to check that the program
behaves correctly after a bug fix. As another example, it may be useful to dump
the contents of intermediate relations by labeling them as `output relation` and
adding a `dump <relation_name>;` command to the command file.

Finally, by replaying recorded commands, one can obtain a relatively accurate
estimate of the amount of CPU time and memory spent in the DDlog computation
in isolation from the host program.  For example, here we use the UNIX `time`
program (note: this is not the same as the `time` command in `bash`)
to measure time and memory footprint of a DDlog computation:
```
/usr/bin/time playpen_ddlog/target/release/playpen_cli -w 2 --no-store < replay.dat
```
where `-w 2` runs DDlog with two worker threads,
`--no-store` tells DDlog not to cache the content of output relations (which takes
time and memory), and
`replay.dat` is the name of the file that contains recorded DDlog commands.

To enable replay debugging, call the `ddlog_record_commands()` function in C (see
`ddlog.h`), `DDlogAPI.record_commands()` method in Java (`DDlogAPI.java`) or
the `HDDlog.record_commands()` method in Rust right after starting the
DDlog program, and before pushing any data to it.

**TODO: checkpointing feature**

## Logging

** TODO **
