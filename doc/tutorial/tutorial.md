# A Differential Datalog (DDlog) tutorial

**Note:**: All examples from this tutorial can be found in
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

### Capitalization rules

1. Identifiers start with letters or underscore `_`, and may contain digits.

1. Relation names must be capitalized (start with an uppercase symbol).

1. Variable names must start with a lowercase symbol or `_`.

1. Type names have no restrictions.

### Declaration order

The order of declarations in a DDlog program is unimportant; you
should use the order that makes programs easiest to read.

### Compiling the "Hello, world!" program

Compiling a DDlog program consists of two steps.
First, run the DDlog compiler to generate the Rust program:
```
ddlog -i playpen.dl -L <ddlog/lib>
```
where `<ddlog/lib>` is the path to the `lib` directory inside the
DDlog installation directory (when using a binary release) or
inside the DDlog repository (when building DDlog from source).

Second, run the Rust compiler to generate a binary executable:
```
(cd playpen_ddlog && cargo build --release)
```

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

And finally we complete the transaction with `commit`:

```
>> commit;
insert 30 Word1(Word1{"Hello,",Category::CategoryOther{}})
insert 31 Word2(Word2{"world!",Category::CategoryOther{}})
insert 21 Phrases(Phrases{"Hello, world!"})
```

The CLI has executed all the operations that we have requested.  It
has inserted records in relations `Word1` and `Word2`, and the rule
for `Phrases` has inferred that a new record should be added to
this relation.  When commiting a transaction DDlog reports all
*changes* to all (input and derived) relations.

We can now again inspect the contents of `Phrases`:

```
>> dump Phrases;
Phrases{"Hello, world!"}
>>
```

### Relations are sets

All DDlog relations are sets, i.e., a relation cannot contain multiple
identical records.  Try adding an existing record to `Word1` and
check that it only appears once.

### Incremental evaluation

We can delete records from input relations:

```
start;
delete Word2("world!", CategoryOther);
commit;

dump Prases;
```

`Phrases` should be empty again.  DDlog only computes the change of `Phrases`
incrementally, without recomputing all records.

See [this document](../command_reference/command_reference.md) for a complete list of commands
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
Phrases{"Goodbye, Ruby Tuesday"}
Phrases{"Goodbye, World"}
Phrases{"Hello, Ruby Tuesday"}
Phrases{"Hello, World"}
Phrases{"Help me, Obi-Wan Kenobi"}
Phrases{"Help me, father"}
Phrases{"I am your Obi-Wan Kenobi"}
Phrases{"I am your father"}
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
> ddlog -i playpen.dl -L <ddlog/lib>
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

Built-in types have built-in conversions to strings.  To convert a
user-defined type (such as `Category` above) the user can implement a
function named `category2string` that returns a `string` given a category
(functions are described [below](#functions)).

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
function ip_addr_t2string(ip: ip_addr_t): string {
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

function mac_addr_t2string(mac: mac_addr_t): string {
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

function nethost_t2string(h: nethost_t): string {
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
NetHostString{1,"Host: IP=170.187.204.221, MAC=11:22:33:44:55:66"}
NetHostString{2,"Host: IP=160.176.192.208, MAC=10:20:30:40:50:60"}
```

### Bit vectors and integers

The type `bigint` describes arbitrary-precision (unbounded) integers.
`var x: bigint = 125` declares a variable `x` with type integer and
initial value 125.

Signed integers are written as `signed<N>`, where `N` is the width of
the integer in bits.  Currently DDlog only supports integer with
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

### Control flow

DDlog functions are written using an *expression-oriented language*,
where all statements are expressions.
Evaluation order can be controlled using several constructs:

1. Semicolon is used to separate expressions that are evaluated in sequence, from left to right.

1. *`if (cond) e1 [else e2]`* evaluates one of two subexpressions depending on the value of `cond`.
   If the `else` is missing, the value `()` (emtpy tuple) is used for `e2`

1. *Matching* is a generalization of C/Java `switch` statements.

1. A new block (scope) can be created with curly braces `{ }`

1. A for loop

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
evaluating the function.  If the `else` is missing the value `()` (empty tuple)
is used for the `else` branch.  In `match` expressions the patterns must cover
all possible cases (for instance, the match expression above would not be
correct without the last "catch-all" (`_`) case).

DDlog variables must always be initialized when declared.  In this
example, the `port` variable is assigned the result of the `match`
expression.  A variable can be assigned multiple times, overwriting
previous values.

DDlog assignments cannot be chained like in C; although an assignment
is an expression, it produces the value `()` (an empty tuple,
described [below](#tuples)).

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

### Functions

DDlog functions are pure (side-effect-free) computations.  A function
may not modify its arguments.  The body of a function is an expression
whose type must match the function's return type.  A function call can
be inserted anywhere an expression of the function's return type can
be used.  DDlog currently does not allow recursive functions.

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


### Extern functions

Functions that cannot be easily expressed in DDlog can be implemented as
*extern* functions.  Currently these must be written in Rust; the Rust
implementation may in turn invoke implementations in C or any other language.

For instance, DDlog does not provide a substring function.  We can
declare such a function as `extern`:

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

### Advanced rules

#### Negations and antijoins

Let's assume we want to compute a relation `SanitizedEndpoint`
containing the endpoints that do not appear in a `Blacklisted`
relation:

```
input relation Blacklisted(ep: string)
output relation SanitizedEndpoint(ep: string)

SanitizedEndpoint(endpoint) :-
    EndpointString(endpoint),
    not Blacklisted(endpoint).
```

The `not` operator in the last line eliminates all endpoint values
that appear in `Blacklisted`.  In database terminology this is known
as *antijoin*.

#### Assignments in rules

We can directly use assignments in rules:

```
SanitizedEndpoint(endpoint) :-
    Endpoint(ip, proto, preferred_port),
    var endpoint = addr_port(ip, proto, preferred_port),
    not Blacklisted(endpoint).
```

#### Binding variables to rows

It is sometimes convenient to refer to a table row as a whole.
The following binds the `ep` variable to a row of the `Endpoint` relation.

```
SanitizedEndpoint(endpoint) :-
    ep in Endpoint,
    var endpoint = addr_port(ep.ip, ep.proto, ep.preferred_port),
    not Blacklisted(endpoint).
```

Here we filter the `Endpoint` relation to select HTTP endpoints only
and bind resulting rows to `ep`:

```
SanitizedHTTPEndpoint(endpoint) :-
    ep in Endpoint(.proto = "HTTP"),
    var endpoint = addr_port(ep.ip, ep.proto, ep.preferred_port),
    not Blacklisted(endpoint).
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

#### Container types, FlatMap, and for-loops

DDlog supports three built-in container data types: `Vec`, `Set`, and `Map`.  These
are *generic* types that can be parameterized by any other
DDlog types, e.g., `Vec<string>` is a vector of strings, `Map<string,bool>` is
a map from strings to Booleans.

Let us assume that we have an extern function that splits a string
into a list of substrings according to a separator:

```
extern function split(s: string, sep: string): Vec<string>
```

The Rust implementation can be as follows:

```
pub fn split_ip_list(s: &String, sep: &String) -> Vec<String> {
    s.as_str().split(sep).map(|x| x.to_string()).collect()
}
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
to split `addrs` into individul addresses.

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

#### Aggregation

The `Aggregate` operator groups records that have the same values of a subset of
variables (group-by variables) and applies an aggregation function to each group.
The following program groups the `Price` relation by `item` and selects the minimal
price for each item:

```
input relation Price(item: string, vendor: string, price: bit<64>)
output relation BestPrice(item: string, price: bit<64>)

BestPrice(item, best_price) :-
    Price(.item = item, .price = price),
    var best_price = Aggregate((item), group_min(price)).
```

Here `group_min()` is one of several aggregation functions defined in
`lib/std.rs`.

It is possible to group a relation by multiple fields.  For
example, the following rule computes the lowest price that each vendor
charged for each item:

```
BestPricePerVendor(item, vendor, best_price) :-
    Price(.item = item, .vendor = vendor, .price = price),
    var best_price = Aggregate((item, vendor), group_min(price)).
```

What if we wanted to return the name of the vendor along with the
lowest price for each item?  The following naive attempt will not work:

```
output relation BestVendor(item: string, vendor: string, price: bit<64>)

BestVendor(item, vendor, best_price) :-
    Price(.item = item, .vendor = vendor, .price = price),
    var best_price = Aggregate((item), group_min(price)).
```

DDlog will complain that `vendor` is unknown in the head of the rule. What
happens here is that we first group records with the same `item` value
and then reduce each group to a single value, `best_price`.  All other
variables (except `item` and `best_price`) disappear from the scope and cannot
be used in the body of the rule following the `Aggregate` operator or in
its head.

A correct solution requires a different aggregation function that takes a
group of `(vendor, price)` tuples and picks one with the smallest price.  To
enable such use cases, DDlog allows users to implement their own custom
aggregation functions:

```
/* User-defined aggregate that picks a tuple with the smallest price */
function best_vendor(g: Group<'K, (string, bit<64>)>): (string, bit<64>)
{
    var min_vendor = "";
    var min_price: bit<64> = 'hffffffffffffffff;
    for (vendor_price in g) {
        if (vendor_price.1 < min_price) {
            min_vendor = vendor_price.0;
            min_price = vendor_price.1
        }
    };
    (min_vendor, min_price)
}

BestVendor(item, best_vendor, best_price) :-
    Price(item, vendor, price),
    var best_vendor_price = Aggregate((item), best_vendor((vendor, price))),
    (var best_vendor, var best_price) = best_vendor_price.
```

The aggregation function takes an argument of type `Group`, parameterized by
group key and group value types.  The group key is a tuple consisting of all
group-by variables.  The value type is the type of records in the group and.

The aggregation function can access the group key using the `group_key()`
function.  The following custom aggregation function computes the cheapest
vendor for each item and returns a string containing item name, vendor,
and price:

```
function best_vendor_string(g: Group<string, (string, bit<64>)>): string
{
    var min_vendor = "";
    var min_price: bit<64> = 'hffffffffffffffff;
    for (vendor_price in g) {
        if (vendor_price.1 < min_price) {
            min_vendor = vendor_price.0;
            min_price = vendor_price.1
        }
    };
    "Best deal for ${group_key(g)}: ${min_vendor}, $${min_price}"
}

output relation BestDeal(best: string)
BestDeal(best) :-
    Price(item, vendor, price),
    var best = Aggregate((item), best_vendor_string((vendor, price))).
```

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

### Extern types

**TODO**

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
in `Ref<>`'s.  The ampersant (`&`) in these declaration is a shortcut notation for:

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

## A more imperative syntax

Some people dislike Datalog because the evaluation order of rules is not always obvious.  DDlog
provides an alternative syntax for writing rules which resembles more traditional imperative
languages.  For example, the previous program can be written as follows:

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

for (lsp in Logical_Switch_Port) {
    Some{var lrport_name} = map_get(lsp.options, "router-port") in
    ...
}

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
in arbitrary ways.  When using this syntax semicolons must be used as separators between statements.
The DDlog compiler translates such programs into regular DDlog programs.

## Modules

Modules make it easy to write DDlog programs that span multiple files.  DDlog supports a simple
module system, where each file is a separate module, and the module hierachy matches the directory
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
import lib.mod2 as m2

output relation Main(f1: bool, f2: string)

Main(f1, f2) :- R1(f1), m2.R2(f2).
```

Note the two forms of the `import` directive.  The first form (`import <module_path>`) makes all
type, function, relation, and constructor declarations from the module available in the local
namespace of the module importing it.  The second form (`import <module_path> as <alias>`) makes all
declarations available to the importing module using the `<alias>.<name>` notation.

We create some data to feed to the test program in a `test.dat` file.

```
start;

insert mod1.R1(true),
insert lib.mod2.R2("hello");

commit;
dump;
```

Note that we refer to relations and constructors via their fully qualified names.
Compile and run the test:

```
ddlog -i test.dl -L../../lib
cd test
cargo build --release
target/release/test_cli < ../test.dat
```

Modules do not have to be in the same directory with the main program.  Assuming that our module
hierarchy is located, e.g., in `../modules`,  the first command above must be changed as follows:

```
ddlog -i test.dl -L../modules -L../../lib
```

Multiple `-L` options are allowed to access modules scattered across multiple directories.

## The standard library

The module system enables the creation of reusable DDlog libraries.  Some of these libraries
are distributed with DDlog in the `lib` directory.  A particularly important one is the standard
library [`std.dl`](../../lib/std.dl), which defines types like `Vec`, `Set`, `Map`, `Option`, `Ref`, and others.
This library is imported automatically into every DDlog program; therefore the path to the
`lib` directory must always be specified using the `-L` switch.

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
type definition.  It is particularly useful in controling
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
In order to use the generated crate directly from another Rust program,
add it as a dependency to your `Cargo.toml`, e.g.,
 ```
 [dependencies]
 playpen = {path = "../playpen_ddlog"}
 ```
 And invoke it through the API defined in the `./playpen_ddlog/api.rs` file.

 **TODO: link to a separate API document**

1. **C/C++**
The compiled program will contain a static library `playpen_ddlog/target/release/libplaypen_ddlog.a`
that can be linked against your C or C++ application.  The C API to DDlog is defined in
`playpen_ddlog/ddlog.h`.  To generare a dynamic library instead, pass the
`--dynlib` flag to `ddlog` to generate a `.so` (or `.dylib` on a Mac) file,
along with `--no-staticlib` to disable generation of the static library.

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
additition to memory profiling.  CPU profiling is not enabled by default, as
it can slow down the program somewhat, especially for large programs
that handle many small updates.

1. `profile;` - returns information about program's CPU and memory usage.  CPU
usage is expessed as the total amount of time DDlog spent evaluating each operator,
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
to measure time and memory footpint of a DDlog computation:
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
