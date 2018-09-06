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
ground facts and the derived facts computed by DDlog being written back to the database.

At the moment, DDlog can only operate on databases that completely fit the memory of a single
machine. (This may change in the future, as DDlog builds on the differential dataflow library that
supports distributed computation over partitioned data).

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

1. A powerful type system, including Booleans, unlimited precision integers, bitvectors, strings,
tuples, and tagged unions.

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

Datalog relations are similar to database tables in that they store sets of records.  Relation names
in DDlog always start with an uppercase letter.  `Word1` and `Word2` are two *input* relations in
our example.  Input relations (aka *extensional* relations in the Datalog world) are populated by
user-provided *ground* facts.  The other kind of relations are *derived* (or *intensional*)
relations that are computed based on rules.  The two kinds do not overlap: a relation must be
populated exclusively by either ground or derived facts.  The `input` qualifier is used to declare
an input relation; relations declared without this qualifier are derived relations.

The records in `Word1` and `Word2` have the same type, including a `word` field of type `string` and
a `cat` field of type `Category`.  We would like to combine strings from `Word1` and `Word2`
relations into phrases and store them in the `Phrases` relation.  However, we only want to combine
words that belong to the same category.  This is captured by the DDlog rule in the end of the
listing.  The rule can be read as follows: *for every `w1`, `w2`, and `cat`, such that `(w1, cat)`
is in `Word1` and `(w2, cat)` is in `Word2`, there is a record `w1 ++ " " ++ w2` in Phrases*.  `++`
is a string concatenation operator.

The fact to the left of `:-` is called the *head* of the rule; facts to the right of `:-` form the
*body* of the rule.  Commas in the body of the rule should be read as "and", i.e., if all facts in
the body are true, then the head of the rule holds.  Finally, note the mandatory dot in the end of
the rule.

Using database terminology, the above rule computes an inner join of `Word1` and `Word2` relations
on the `cat` field.

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

We use the "Hello, world!" example to demonstrate the incremental aspect of DDlog.  Add the
following commands to the `.dat` file to delete one of the input records.  All phrases ending with
"World" should disappear from `Phrases`.  DDlog performs this computation incrementally, without
recomputing the entire relation from scratch.

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
typedef IP4     = bit<32>
typedef NetMask = bit<32>

//IP host specified by its name and address.
input relation Host(id: UUID, name: string, ip: IP4)

//IP subnet specified by its IP prefix and mask
input relation Subnet(id: UUID, prefix: IP4, mask: NetMask)

// HostInSubnet relation maps hosts to known subnets
relation HostInSubnet(host: UUID, subnet: UUID)

HostInSubnet(host_id, subnet_id) :- Host(host_id, _, host_ip),
                                    Subnet(subnet_id, subnet_prefix, subnet_mask),
                                    ((host_ip & subnet_mask) == subnet_prefix). // filter condition
```

First, note the three *type alias* declarations in the first lines of the example.  A type alias is
a shortcut that can be used interchangeably with the type it aliases.  Type aliases improve code
readability and simplify refactoring.  For example, while in this example `IP4` and `bit<32>`
refer to the exact same type (a 32-bit unsigned integer), the former makes it explicit that the
corresponding variable or field stores an IP address value.

Next we declare the two input relations that store known IP hosts and subnets respectively, followed
by the derived `HostInSubnet` relation.  `HostInSubnet` is computed by filtering all host-subnet
pairs where host address matches subnet prefix and mask.  This is captured by the rule in the end of
the program.  The first two lines use already familiar syntax to *bind* `host_id`, `host_ip`,
`subnet_id`, `subnet_prefix`, `subnet_mask` variables to records from `Host` and `Subnet` relations.

The first two lines of the rule iterate over all host-subnet pairs.  We would like to narrow this
set down to only those pairs where the host's IP address belongs to the subnet.  This is captured by
the expression in the third line: `(host_ip & subnet_mask) == subnet_prefix`, where `&` is the
bitwise "and" operaror, and `==` is the equality operator.  This expression acts as a *filter* that
eliminates all tuples that do not satisfy the condition.

Note that this rule does not use the `name` field of the `Host` relation.  We do not want to pollute
the name space with an unused variable and instead use the *"don't care"* symbol (`_`) to indicate
that the value of the field is immaterial:

```
Host(host_id, _, host_ip)
```

DDlog supports an alternative syntax for facts, identifying relation arguments by name instead of by
position:

```
Host(.id=host_id, .ip=host_ip)
```

While this syntax is more verbose, it improves code clarity and prevents errors due to incorrect
ordering of arguments.  In addition, it only requires the programmer to list arguments that are used
in the rule.  Omitted arguments are automaticaly wildcarded.

### String literals

DDlog supports a small set of operations on strings, allowing many string-manipulating programs to
be expressed completely within the language, without relying on external functions.  DDlog's
primitive `string` type contains UTF-8 strings.  Two forms of string literals are supported:

1. *Quoted strings* are Unicode strings enclosed in quotes, with the same single-character
escapes as in C, e.g.,
  ```
  "bar", "\tbar\n", "\"buzz", "\\buzz", "barΔ"
  ```
  A quoted string may not contain an unescaped new-line character, backslash, or quote, e.g.,
  ```
  "foo
  bar"
  ```
  is illegal.  Long strings can be broken into multiple lines using backslash:
  ```
  "foo\
     \bar"
  ```
  is equivalent to `"foobar"`

1. *Raw strings*, enclosed in `[| |]`, can contain arbitrary Unicode characters, except the `|]`
sequence, including newlines and backslashes, e.g.,
  ```
  [|
  foo\n
  buzz
  bar|]
  ```
  is the same string as `"foo\\n\nbuzz\nbar"`.

Adjacent string literals (of both kinds) are automatically concatenated, e.g., `"foo" "bar"` is
converted to `"foobar"`.

See [`strings.dl`](../../test/datalog_tests/strings.dl) for more examples of string literals.

### String concatenation and interpolation

We have already encountered the string concatenation operator `++`.  Another way to construct
strings in DDlog is using *string interpolation*, which allows embedding arbitrary DDlog expressions
inside a string literal.  At runtime, expressions are evaluated and converted to strings.
Interpolated strings are preceded by the `$` character, and embedded expession are wrapped in `${}`.
The following program will insert in relation `Pow2` the squares of all numbers in relation
`Number`.

```
input relation Number(n: bigint)
relation Pow2(p: string)
Pow2($"The square of ${x} is ${x*x}") :- Number(x).
```

Values of primitive types (`string`, `bigint`, `bit<N>`, and `bool`) are converted to strings using
builtin methods.  For user-defined types, conversion is performed by calling a user-provided
function (see [below](#functions)) whose name is formed from the name of the type by changing the
first letter of the type name to lower case (if it is in upper case) and adding the `"2string"`
suffix.  The function must take exactly one argument of the given type and return a string.

In the following example, we would like to print information about network hosts, including their IP
and Ethernet addresses, in the standard human-readable format, e.g., `"192.168.0.1"` for IP
addresses and `"aa:b6:d0:11:ae:c1"` for Ethernet addresses.  Earlier in this tutorial, we declared
the `IP4` type as an alias to 32-bit numbers.  When embedded in an interpolated string, such a
number is automatically printed as a decimal integer.  One way to obtain the desired formatting
would be to implement a custom formatting function for IP addresses and call it every time we would
like to convert an IP address to a string, e.g., `$"IP address: ${format_bit32_as_ip(addr)}"`.  This
is somewhat inelegant and error-prone (since forgetting to call the formatting function causes DDlog
to apply the standard conversion function for numbers).  A better option is to declare a new type for
IP addresses rather than a type alias:

```
typedef ip_addr_t = IPAddr{addr: bit<32>}
```

This declares the `ip_addr_t` type with a single *type constructor* called `IPAddr`.  We discuss the
type system in more detail below.  For the time being, think of this declaration as a C struct with
a single field of type `bit<32>`.  DDlog does not have a default way to print this type, giving us
an opportunity to provide a user-defined formatting method:

```
function ip_addr_t2string(ip: ip_addr_t): string = {
    $"${ip.addr[31:24]}.${ip.addr[23:16]}.${ip.addr[15:8]}.${ip.addr[7:0]}"
}
```

This function introduces two new constructs.  First, it shows the C-style notation for accessing
struct fields, e.g., `ip.addr`.  Second, it demonstrates the bit slicing syntax (`ip.addr[31:24]`),
which selects a range of bits from a bit vector and returns it as a new bit vector of type `bit<N>`,
where `N` is the width of the selected range.

One might feel that our new type for IP addresses is more cumbersome to use than the simple
`bit<32>` alias.  Both implementations are valid and choosing one over the other is a matter of
taste.  We do point out that the new declaration offers additional type safety, as it prevents one
from accidentally mixing IP addresses and numbers.

Next, we declare a data type for Ethernet addresses (aka MAC addresses) and associate a string
conversion function with it in a similar way.  One little twist here is that individual bytes of the
Ethernet address must be printed in hexadecimal format.  Since DDlog's default formatter uses
decimal format, we call the built-in `hex()` function to perform the conversion:

```
typedef mac_addr_t = MACAddr{addr: bit<48>}

function mac_addr_t2string(mac: mac_addr_t): string = {
    $"${hex(mac.addr[47:40])}:${hex(mac.addr[39:32])}:${hex(mac.addr[31:24])}:\
     \${hex(mac.addr[23:16])}:${hex(mac.addr[15:8])}:${hex(mac.addr[7:0])}"
}
```

We can now declare the `NHost` type that describes a network host and contains the host's IP and MAC
addresses, and define a string conversion function for it.

```
typedef nethost_t = NHost {
    ip:  ip_addr_t,
    mac: mac_addr_t
}

function nethost_t2string(h: nethost_t): string = {
    $"Host: IP=${h.ip}, MAC=${h.mac}"
}
```

DDlog will automatically invoke the user-defined string conversion functions for `ip_addr_t` and
`mac_addr_t` types to format `ip` and `mac` fields respectively.

The following DDlog program tests the above types and functions.  It maps values of type `nethost_t`
in the input relation to string representation and stores them in the derived relation:

```
input relation NetHost(id: bigint, h: nethost_t)
relation NetHostString(id: bigint, s: string)

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

### Bit vectors

The design of DDlog was strongly influenced by applications from the domain of networking, where
programmers frequently deal with bit vectors of non-standard sizes. We have encountered several
instances of the bitvector type, e.g., `typedef UUID=bit<128>`.  In addition to standard arithmetic
and bitwise operators over bit vectors (see [language
reference](../language_reference/language_reference.md#expressions)), DDlog supports bit vector
slicing and concatenation.  Slicing is expressed using the `x[N:M]` syntax examplified in the
previous section.  Bit vector concatenation is implemented by the `++` operator (which is also
overloaded for strings).  Concatenation can be applied to bit vectors of arbitrary width, producing
a bit vector whose width is the sum of the widths of its arguments.  The following example
illustrates both operators:

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
relation Address(addr: ip_addr_t)
Address(ip_from_bytes(b3,b2,b1,b0)) :- Bytes(b3,b2,b1,b0).

// filter multicast IP addresses
relation MCastAddress(addr: ip_addr_t)
MCastAddress(a) :- Address(a), is_multicast_addr(a).
```

DDlog offers two ways to write bit vector literals. First, they can be written as decimal numbers.
DDlog performs *type inference* to determine whether the number should be interpreted as an unbounded
mathematical integer or a bit vector.  In the latter case it also infers its bit width.  For
example, the literal `125` is interpreted as a mathematical integer in `var x: int = 125` and as an
8-bit bit-vector in `var x: bit<8> = 125`.

Second, bit vector literals can be written with explicit width and radix specifiers using the
`[<width>]<radix_specifier><value>` syntax, where `<radix>` is one of `'d`, `'h`, `'o`, `'b` for
decimal, hexadecimal, octal and binary numbers respectively:

```
32'd15              // 32-bit decimal
48'haabbccddeeff    // 48-bit hexadecimal
3'b101              // 3-bit binary
'd15                // bit vector whose value is 15 and whose width is determined from the context
```

### Control flow

Complex computations can be expressed in DDlog using control-flow constructs:

1. *Sequencing* evaluates semicolumn-separated expressions in order.

1. *`if (cond) e1 else e2`* evaluates one of its subexpressions depending on the value of `cond`.

1. Matching evaluates one of several expressions based on the an argument and a series of patterns.

Note that DDlog does not support loops.

The following example illustrates these constructs:

```
function addr_port(ip: ip_addr_t, proto: string, preferred_port: bit<16>): string =
{
    var port: bit<16> = match (proto) {
        "FTP"   -> 20,  // default FTP port
        "HTTPS" -> 443, // default HTTP port
        _       -> {    // other protocol
            if (preferred_port != 0)
                preferred_port // return preferred_port if specified
            else
                16'd80         // assume HTTP otherwise
        }
    };
    // Return the address:port string
    $"${ip}:${port}"
}
```

This example highlights several important aspects of DDlog's control flow constructs.  First, note
that the function does not have a `return` statement (in fact, there is currently no `return`
statemnt in DDlog).  DDlog is an *expression-oriented language*, which does not differentiate
between expressions and statements.  All control flow constructs are expressions that have a
statically assigned type and a dynamically assigned value and can be used as terms in other
expressions.  The function's return value is simply the value produced by the expression in its
body.  In particular, a sequential block returns the value of the last evaluated expression in the
sequence; `if-else` and `match` expressions return the value of the taken branch.  As a consequence,
the `else` clause is not optional in an `if-else` expression, and match clauses must be exhaustive
(for instance, the match expression above would not be valid without the last "catch-all" (`_`)
clause).

Second, this example illustrates the use of local *variables* in DDlog. Similar to other languages,
variables store intermediate values.  DDlog enforces that a variable is assigned when it is
declared, thus ruling out uninitialized variables.  In this example, the `port` variable is assigned
the result of the `match` expression.  A variable can be assigned multiple times, overwriting
previous values.  Like other expressions, assignments produce values; however, unlike C assignments,
DDlog assignments return an empty [tuple](#tuples) (roughly, DDlog's equivalent of null), as opposed
to the assigned value, hence DDlog assignments cannot be chained.

Finally, this example shows DDlog's `match` syntax, which matches the value of its argument against
a series of *patterns*.  The simplest pattern is a constant value, e.g., `"FTP"`.  Another pattern
used in this example is the wildcard pattern (`_`) that matches any value.  We discuss more complex
patterns [below](#tagged-unions).

Let's put the `addr_port()` function to work.  The following program converts its input relation
that stores a set of network endpoints represented by their IP addresses, protocols, and preferred
port numbers and converts these endpoints to string representation using `addr_port()`.  Note the
use of the function in the head of the rule:

```
input relation Endpoint(ip: ip_addr_t,
                        proto: string,
                        preferred_port: bit<16>)

relation EndpointString(s: string)

EndpointString(addr_port(ip, proto, preferred_port)) :-
    Endpoint(ip, proto, preferred_port).
```

## Functions

We have already encountered several functions in this tutorial.  Functions facilitate modularity and
code reuse.  DDlog functions are pure (side-effect-free) computations.  A function may not modify
its arguments.  The body of a function is an expression whose type must match the function's return
type.  A function call can be inserted anywhere an expression of the function's return type can be
used.  DDlog currently does not allow recursive functions.

### Extern functions

Despite its rich expression syntax and type system, DDlog does not strive to be a general-purpose
programming language.  In particular, the absence of loops, recursion, and inductive data types make
it impossible to express many computations in DDlog.  Such computations must be implemented in Rust
and invoked from DDlog programs as *extern* functions.  The Rust implementation may in turn invoke a
function implemented in C or any other language.

For instance, DDlog does not provide native means to extract a substring of a string.  To achieve
this functionality, we must implement it as an extern function:

```
extern function string_slice(x: string, from: bit<64>, to: bit<64>): string
```

We can invoke `string_slice()` just like a normal DDlog function, e.g.,

```
relation First5(str: string)
First5(string_slice(p, 0, 5)) :- Phrases(p).
```

For this code to compile, we must provide an implementation of `string_slice()` in Rust.  To help
with this step, DDlog generates a commented out function prototype that can be found in
`playpen/lib.rs`.  Do not modify this autogenerated file.  Instead, create a new file called
`playpen.rs` in the same directory as `playpen.dl` with the following content:

```
fn string_slice(x: &String, from: &u64, to: &u64) -> String {
    x.as_str()[(*from as usize)..(*to as usize)].to_string()
}
```

This file will be automatically inlined in `lib.rs` next time you run `stack test`.  If your program
uses many extern functions, you may choose to partition them into multiple modules and import them
from `playpen.rs`.

**TODO: the above method is good for writing tests only.  Describe a proper way to do this once it
is implemented**

## Advanced rule syntax

### Antijoins

It is sometimes necessary to restrict the set of records produced by a rule to those that do *not*
appear in some relation.  Consider, for example, the `EndpointString` relation from
[before](#control-flow).  Imagine that we would like to compute a subset of endpoints that do not
appear in the `Blacklisted` relation and store this subset in a new relation called
`SanitizedEndpoint`:

```
input relation Blacklisted(ep: string)
relation SanitizedEndpoint(ep: string)

SanitizedEndpoint(endpoint) :-
    EndpointString(endpoint),
    not Blacklisted(endpoint).
```

Note the `not` prefix in the last line of the rule, which has the effect of eliminating all endpoint
values that appear in `Blacklisted`. This operation is known as *antijoin* in the database world.

### Assignments

To make things more interesting, let us assume that the `EndpointString` relation did not exist, so
we need to convert endpoints to strings before matching them against the `Blacklisted` relation.  An
obvious solution would be to introduce `EndpointString` first; however DDlog allows a more concise
and space-efficient implementation that performs string conversion and antijoin within the same
rule:

```
SanitizedEndpoint(endpoint) :-
    Endpoint(ip, proto, preferred_port),
    var endpoint = addr_port(ip, proto, preferred_port),
    not Blacklisted(endpoint).
```

Note the `endpoint` variable declaration embedded in the rule.  It is functionally equivalent to
substituting the righ-hand side of the assignment everywhere the `endpoint` variable is used, but is
more compact and efficient.

### FlatMap

The `FlatMap` operator maps all elements of a relation to sets of values and yields a union of the
resulting sets.  Consider a `HostAddress` relation that associates each network host with a string
that lists all IP addresses assigned to this host separated by whitespaces.  We would like
to break these strings into individual IP addresses and compute a `HostIP` relation that consists of
(host, ip) pairs:

```
input relation HostAddress(host: bit<64>, addrs: string)
relation HostIP(host: bit<64>, addr: string)
```

To this end, we will use a helper function that splits a string into components:

```
extern function split_ip_list(x: string): set<string>
```

The function returns the builtin `set` data type.  `set` is a *generic* type ([see
below](#generic-types)) that can be parameterized by any other DDlog type, e.g., `set<string>` is a
set of strings.

We can now use this function in conjunction with the `FlatMap` operator to compute `HostIP`:

```
HostIP(host, addr) :- HostAddress(host, addrs),
                      FlatMap(addr=split_ip_list(addrs)).
```

This rule can be read as follows:

1. For every `(host, addrs)` pair in the `HostAddress` relation, use `split_ip_list()`
to split `addrs` into individul addresses.

1. Bind each address in the resulting set to the `addr` variable, producing a new set of `(host,
addr)` tuples.

1. Store the resulting tuples int the `HostIP` relation.

Let us look at the implementation of `split_ip_list`.  It is an extern function defined in Rust as
follows:

```
fn split_ip_list(x: &String) -> Vec<String> {
    x.as_str().split(" ").map(|x| x.to_string()).collect()
}
```

Note that the function returns a vector, not a set.  In fact, any Rust type that implements the
`IntoIterator` trait can be used to represent sets for `FlatMap` purposes.

### Rules with multiple heads

Consider a DDlog program that computes pairwise sums and products of values from `X`.

```
input relation X(x: bit<16>)

relation Sum(x: bit<16>, y: bit<16>, sum: bit<16>)
relation Product(x: bit<16>, y: bit<16>, prod: bit<16>)

Sum(x,y,x*y) :- X(x), X(y).
Product(x,y,x*y) :- X(x), X(y).
```

Note that both rules in this program have identical bodies.  Such rules can be merged into one rule
with multiple comma-separated heads:

```
Sum(x,y,x+y), Product(x,y,x*y) :- X(x), X(y).
```

### Recursive rules

DDlog allows rules that refer to the same relation in the body and in the head.  For example, the
following program computes pairs of connected nodes in a graph given the set of edges:

```
input relation Edge(s: node, t: node)

relation Path(s1: node, s2: node)

Path(x, y) :- Edge(x,y).
Path(x, z) :- Path(x, w), Edge(w, z).
```

This is an example of a *recursive* program, where a relation depends on itself, either directly or
via a chain of rules.  See the [language
reference](../language_reference/language_reference.md#constraints-on-dependency-graph) for a more
precise specification of valid recursive programs that DDlog accepts.

### Aggregation

**TODO**

## The type system

### Tagged unions

### Filtering relations with structural matching

### match expressions

### Tuples

*Can be used to return multiple values*

### Generic types

### Extern types

## Explicit relation types

## Flow Template Language
