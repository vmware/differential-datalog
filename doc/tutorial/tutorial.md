# A Differential Datalog (DDlog) tutorial

**Note:**: All examples from this tutorial can be found in
[`test/datalog_tests/tutorial.dl`](../../test/datalog_tests/tutorial.dl).
The examples can be executed using test inputs from
[`test/datalog_tests/tutorial.dat`](../../test/datalog_tests/tutorial.dat)
and the expected outputs are in
[`test/datalog_tests/tutorial.dump.expected`](../../test/datalog_tests/tutorial.dump.expected).

## Introduction

Differential Datalog or **DDlog** is an implementation of an enhanced
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

Installation instructions are found in the [README](../../README.md).

## "Hello, World!" in DDlog

Files storing DDlog programs have the `.dl` suffix.
[dl.vim](../../tools/dl.vim) is a file that offers syntax highlighting
support for the vim editor.

If you add a file called `playpen.dl`, in the `test/datalog_tests`
directory, it will be executed automatically when you run the DDlog tests, by
typing `stack test`.  You can execute just this file by typing `stack
test --ta '-p playpen'`.

**TODO: replace these instructions once issue #64 has been fixed**

You should create the following `playpen.dl` file:

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

DDlog comments are like C++ and Java comments (`//` and `/* */`).

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
can be queried (see below).

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

Run `stack test --ta '-p playpen'` to compile this program.
(Unfortunately the Rust compiler is quite slow, so this may take a few
minutes).

Alternatively, run the DDlog compiler directly to produce the Rust program
```
ddlog -i playpen.dl --action=compile
```
and call the Rust toolchain to compile the program:
```
cd playpen; cargo build --release
```

### Running the "Hello, world!" program

The text-based interface offers a convenient way to interact with the
DDlog program during development and debugging.  In your favorite Unix
shell start the program that was generated by the compiler `./playpen/target/release/playpen_cli`.
The CLI shows you a prompt `>>`.  Now you can type some commands that are executed by the datalog engine.

CLI commands can be used to show the contents of relations, or to
insert or delete records from input relations.

We can display the contents of the ``Phrases` relation:

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
describing `Phrases` has inferred that a new record should be added to
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
delete Word2("World", CategoryOther);
commit;

dump Prases;
```

`Phrases` should be empty again.  DDlog only computes the change of `Phrases`
incrementally, without recomputing all records.


See [this document](../testing/testing.md#command-reference) for a complete list of commands
supported by the CLI tool.

### Running CLI in batch mode

You can also run the CLI in batch mode, feeding commands via a UNIX pipe from a file or another
program.  Create a file called `playpen.dat` with the following content:

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
./playpen/target/release/playpen_cli < playpen.dat
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
Interpolated strings start with the `$` character, embedded expressions
are wrapped in `${}`.  The following program defines relation
`Pow2` to contain strings that enumerate the squares of all numbers in relation `Number`.

```
input relation Number(n: bigint)
output relation Pow2(p: string)
Pow2($"The square of ${x} is ${x*x}") :- Number(x).
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
`$"IP address: ${format_bit32_as_ip(addr)}"`, but
this is error-prone, since it is easy to forget to call the
formatting function when printing IP addresses.  An alternative is to declare a new type for IP addresses:

```
typedef ip_addr_t = IPAddr{addr: bit<32>}
```

This declares a new type named `ip_addr_t`, having a single *type constructor* called `IPAddr`.
Think of this declaration as a C struct with
a single field of type `bit<32>`.  We can write a user-defined formatting method:

```
function ip_addr_t2string(ip: ip_addr_t): string = {
    $"${ip.addr[31:24]}.${ip.addr[23:16]}.${ip.addr[15:8]}.${ip.addr[7:0]}"
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

function mac_addr_t2string(mac: mac_addr_t): string = {
    $"${hex(mac.addr[47:40])}:${hex(mac.addr[39:32])}:${hex(mac.addr[31:24])}:\
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

function nethost_t2string(h: nethost_t): string = {
    $"Host: IP=${h.ip}, MAC=${h.mac}"
}
```

DDlog will automatically invoke the user-defined string conversion
functions to format the `ip` and `mac` fields when it needs to produce strings.

Here is a program that computes a derived relation storing string
representation of hosts:

```
input relation NetHost(id: bigint, h: nethost_t)
output relation NetHostString(id: bigint, s: string)

NetHostString(id, $"${h}") :- NetHost(id, h).
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

Bit-vector types are described by `bit<N>`, where `N` is the
bit-vector width.  Bit-vectors are unsigned integers when performing
arithmetic.  DDlog supports all standard arithmetic and bitwise
operators over bit-vectors.  Concatenation of bit-vectors is
expressed with the `++` operator (same notation as string
concatenation).

```
// Form IP address from bytes using bit vector concatenation
function ip_from_bytes(b3: bit<8>, b2: bit<8>, b1: bit<8>, b0: bit<8>)
    : ip_addr_t =
{
    IPAddr{.addr = b3 ++ b2 ++ b1 ++ b0}
}

// Check for multicast IP address using bit slicing
function is_multicast_addr(ip: ip_addr_t): bool = ip.addr[31:28] == 14

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

### Control flow

DDlog functions are written using an *expression-oriented language*,
where all statements are expressions.  DDlog does not support loops.
Evaluation order can be controlled using several constructs:

1. Semicolon is used to separate expressions that are evaluated in sequence, from left to right.

1. *`if (cond) e1 else e2`* evaluates one of two subexpressions depending on the value of `cond`.

1. *Matching* is a generalization of C/Java `switch` statements.

1. A new block (scope) can be created with curly braces `{ }`

The following example illustrates all these constructs:

```
function addr_port(ip: ip_addr_t, proto: string, preferred_port: bit<16>): string =
{
    var port: bit<16> = match (proto) {  // match protocol string
        "FTP"   -> 20,  // default FTP port
        "HTTPS" -> 443, // default HTTP port
        _       -> {    // other protocol
            if (preferred_port != 0)
                preferred_port // return preferred_port if specified
            else
                16'd80         // assume HTTP otherwise
        }
    };  // semicolon required for sequencing
    // Return the address:port string
    $"${ip}:${port}"
}
```

The result computed by a function is the result of the last expression
evaluated.  There is no `return` statement.  The `else` clause is
mandatory for an `if`.  In `match` expressions the patterns must cover
all possible cases (for instance, the match expression above would not
be correct without the last "catch-all" (`_`) case).

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

### Extern functions

DDlog is not a Turing-complete programming language.  (In particular,
it lacks loops, recursion, and inductive data types.)  If needed, such
computations must be implemented as *extern* functions.  Currently
these must be written in Rust; the Rust implementation may in turn
invoke implementations in C or any other language.

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
in `playpen/lib.rs`.  Do not modify this file; instead, create a new
file called `playpen.rs` in the same directory:

```
pub fn string_slice(x: &String, from: &u64, to: &u64) -> String {
    x.as_str()[(*from as usize)..(*to as usize)].to_string()
}
```

`stack test` will automatically pick up this file. Alternatively, if calling
the DDlog compiler directly, modify the command line as follows:

```
ddlog -i tutorial.dl --action=compile --inline-rust-file=playpen.rs
```

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

### Assignments in rules

We can directly use assignments in rules:

```
SanitizedEndpoint(endpoint) :-
    Endpoint(ip, proto, preferred_port),
    var endpoint = addr_port(ip, proto, preferred_port),
    not Blacklisted(endpoint).
```

#### Sets and FlatMap

DDlog supports two built-in container data types: `Vec`, `Set`.  `Vec`
and `Set` are *generic* types that can be parameterized by any other
DDlog type, e.g., `Vec<string>` is a vector of strings.  These types
do not have constructors in DDlog, so objects with these types can
only be created by external functions.

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
function split_ip_list(x: string): Vec<string> =
   split(x, " ")
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
                      FlatMap(addr=split_ip_list(addrs)).
```

You can read this rule as follows:

1. For every `(host, addrs)` pair in the `HostAddress` relation, use `split_ip_list()`
to split `addrs` into individul addresses.

1. Bind each address in the resulting set to the `addr` variable, producing a new set of `(host,
addr)` records.

1. Store the resulting records in the `HostIP` relation.

#### Rules with multiple heads

The following program computes the sums and products of pairs of values from `X`:

```
input relation X(x: bit<16>)

output relation Sum(x: bit<16>, y: bit<16>, sum: bit<16>)
output relation Product(x: bit<16>, y: bit<16>, prod: bit<16>)

Sum(x,y,x*y) :- X(x), X(y).
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

**TODO**

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
                     srcport: bit<16>, dstport: bit<16>): eth_pkt_t =
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
function pkt_ip4(pkt: eth_pkt_t): ip4_pkt_t = {
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
function pkt_udp_port(pkt: eth_pkt_t): bit<16> = {
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
function addr_to_tuple(addr: bit<32>): (bit<8>, bit<8>, bit<8>, bit<8>) =
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
output relation IntranetHost(addr: ip4_addr_t)

IntranetHost(addr) :- KnownHost(addr),
                      (192, 168, _, _) = addr_to_tuple(addr).
```

### Generic types

Let's revise the `pkt_ip4()` function defined above.  The function extracts IPv4 header from a
packet.  If the packet does not have an IPv4 header, it returns a default value with all fields set
to 0:

```
function pkt_ip4(pkt: eth_pkt_t): ip4_pkt_t = {
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
instantiated for any concrete type.  This types is defined in the DDlog standard library as follows:

```
typedef Option<'A> = None
                   | Some {value : 'A}
```

Here `'A` is a *type argument* that must be replaced with a concrete type to create a concrete
instantiation of `Option`.  We can now rewrite the `pkt_ip4()` function using `Option`:

```
function pkt_ip4(pkt: eth_pkt_t): Option<ip4_pkt_t> = {
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
function is_target_audience(person: Person): bool = {
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

One can also define variables:

```
for (person in Person)
    var is_audience: bool = is_target_audience(person);
    if (is_audience)
        TargetAudience(person)
```

and one can also use `match` statements:

```
for (person in Person)
   var is_audience = is_target_audience(person);
   match (is_audience) {
       true -> TargetAudience(person)
       _    -> skip
   }
```

These 5 kinds of statements (`for`, `if`, `switch`, `var`, blocks enclosed in braces) can be nested
in arbitrary ways.  When using this syntax semicolons must be used as separators between statements.
Also note that the `if` statement is not an expression, and thus the `else` clause is not required
when using this syntax.  The DDlog compiler translates such programs in regular DDlog programs.

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

## Advanced topics

**TODO** probably these should be moved out of the tutorial into a more
 detailed reference document.

### Using DDlog programs as libraries

Place your program in the `test/datalog_tests` folder.  Let's assume your
program is `playpen.dl`.
Run `stack test --ta '-p playpen'` to compile the `playpen` program.
When compilation completes the following artifacts are
produced in the same directory with the input file:

1. Three Rust packages (or "crates") in separate directories:
    * `./differential_dataflow/`
    * `./cmd_parser/`
    * `./playpen/` (this is the main crate, which imports the other two)

1. If you plan to use this library directly from a Rust program, have a look at the
`./playpen/lib.rs` file, which contains the Rust API to DDlog.

    **TODO: link to a separate document explaining the structure and API of the Rust project**

1. If you plan to use the library from a C/C++ program, your program must link against the
`./playpen/target/release/libplaypen.so` library, which wraps the DDlog program into a C API.  This
API is declared in the auto-generated `./playpen/playpen.h` header file.

    **TODO: link to a separate document explaining the use of the C FFI**

1. The text-based interface is implemented by an
auto-generated executable `./playpen/target/release/playpen_cli`.  This interface is
primarily meant for testing and debugging purposes, as it does not offer the same performance and
flexibility as the API-based interfaces.

### Input/output to DDlog

DDlog offers several ways to feed data to a program:

1. Statically, by listing ground facts as part of the program.

1. Via a text-based command-line interface.

1. From a Rust program.

1. From a C or C++ program.

In the following sections, we expand on each method.

#### Specifying ground facts statically in the program source code

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
