# Differential Datalog (DDlog) Language Reference

## Identifiers

DDlog is case-sensitive.  Relation, constructor, and type variable
names must start with upper-case ASCII letters; variable, function,
and argument names must start with lower-case ASCII letters or
underscore.  A type variable name must be prefixed with a tick (').
A type name can start with either an upper-case or a lower-case letter
or underscore.

DDlog programs refer to types, functions, relations, and constructors using *scoped* names, consisting of
identifier prefixed by path to the *module* that declares the identifier, which an be empty if the identifier
is defined in the local scope).

```
    uc_identifier ::= [A..Z][a..zA..Z0..9_]*
    lc_identifier ::= [a..z_][a..zA..Z0..9_]*
    identifier ::= uc_identifier | lc_identifier

    typevar_name ::= 'uc_identifier

    var_name     ::= lc_identifier
    field_name   ::= lc_identifier
    arg_name     ::= lc_identifier

    scope ::= [identifier "."]*
    uc_scoped_identifier ::= scope uc_identifier
    lc_scoped_identifier ::= scope lc_identifier
    scoped_identifier ::= uc_scoped_identifier | lc_scoped_identifier

    rel_name     ::= uc_scoped_identifier
    cons_name    ::= uc_scoped_identifier
    func_name    ::= lc_scoped_identifier
    type_name    ::= scoped_identifier
```

## Top-level declarations

A Datalog program is a list of type definitions, functions, relations, and rules.
The ordering of declarations does not matter, e.g., a type can be used
before being defined.  Declarations can be optionally annotated by
meta-attributes.

```EBNF
datalog ::= annotated_decl*

annotated_decl ::= [attributes] decl

decl ::= import
       | typedef
       | function
       | relation
       | rule

attributes ::= "#[" attribute ["," attribute]* "]"

attribute ::= name "=" expr

```

### Constraints on top-level declarations
1. Type names must be globally unique
1. Function names must be globally unique
1. Relation names must be globally unique

## Imports

The `import` statement makes type, function, relation, and constructor declarations from the
imported module available in the importing module.

```EBNF
module_path ::= identifier ["." identifier]*
import ::= "import" module_path ["as" module_alias]
module_alias ::- identifier
```

`import` statement without the `as` clause adds all imported declarations to the local namespaces of
the importing module.  The `as` clause is used to prevent name clashes.  It creates a local alias
for the imported module, making its declarations accessible via dot notation: `alias.name`.

The DDlog compiler resolves imports by searching all known *library directories* for the imported
module.  By default, the directory of the main module (specified via the `-i` command-line switch)
is searched.  The user can specify additional library paths via the `-L` switch to the compiler.
The compiler converts each import path to a file path by replacing `.` with `/` and adding the `.dl`
extension and checks for a file with the given path under each library directory.  For example,
`import lib_name.mod_name` is converted to `lib_name/mod_name.dl`.

## Types

Type definition introduces a new user-defined type, optionally
parameterized by one or more type arguments.

```EBNF
typedef ::= "typedef" type_name (* unique type name *)
            ["<" typevar_name [("," typevar_name)*] ">"] (* optional type arguments *)
            "=" type_spec (* type definition *)
          | "extern type" type_name ["<" typevar_name [("," typevar_name)*] ">"]
```

The second form above declares an externally defined type.  Variables
of such types can be used just like any normal variables.  The only builtin operators
defined for extern types are `==` and `!=`.  All other operations must
be implemented as extern functions.

```EBNF
(* A full form of typespec. Used in typedef's only. *)
type_spec ::= bigint_type
            | bool_type
            | string_type
            | bitvector_type
            | integer_type
            | double_type
            | float_type
            | tuple_type
            | union_type     (* tagged union *)
            | type_alias     (* reference to user-defined type *)
            | typevar_name   (* type variable *)

(* A restricted form of typespec that does not declare new tagged
    unions (and hence does not introduce new constructor names to
    the namespace.  Used in argument, field, variable declarations. *)
simple_type_spec ::= bigint_type
                   | bool_type
                   | string_type
                   | bitvector_type
                   | double_type
                   | float_type
                   | tuple_type
                   | type_alias
                   | typevar_name
```

```EBNF
bigint_type      ::= "bigint" (* unbounded mathematical integer *)
bool_type        ::= "bool"
string_type      ::= "string" (* UTF-8 string *)
bitvector_type   ::= "bit" "<" decimal ">"
integer_type     ::= "signed" "<" decimal ">"
double_type      ::= "double"
float_type       ::= "float"
tuple_type       ::= "(" simple_type_spec* ")"
union_type       ::= (constructor "|")* constructor
type_alias       ::= type_name           (* type name declared using typedef*)
                     ["<" type_spec [("," type_spec)*] ">"] (* type arguments *)

constructor      ::= [attributes] cons_name (* constructor without fields *)
                   | [attributes] cons_name "{" [field ("," field)*] "}"
field            ::= [attributes] field_name ":" simple_type_spec
```

### Constraints on types
1. Type argument names must be unique within a typedef, e.g.,
`typedef t1<'A,'A,'B>` is invalid.
1. All type arguments of a `typedef` must be used in the type definition:
    ```
    // error: type argument 'D not used in type definition
    typedef Parameterized<'A,'B,'C,'D> = Option1{x: 'A}
                                       | Option2{y: 'B, z: 'C}
    ```
1. The number of bits in a bitvector type must be greater than 0, e.g.,
`bit<0>` is invalid.
1. Type constructor names must be globally unique.
1. If multiple type constructors for the same type have arguments with
identical names, their types must be identical, e.g., the following is invalid:
    ```
    typedef type1 = Constr1{field1: string, field2: bool} | Constr2{field1: bigint}
    ```
1. A type must be instantiated with the number of type
arguments matching its declaration:
    ```
    typedef type1<'A,'B>
    function f(): bool {
        var x: type1<bigint> // error: not enough type arguments
    }
    ```
1. Recursive type definitions are not allowed.
1. A type variable must be declared in the syntactic scope where it is
   used.  There are two types of syntactic scopes that can contain
   type variables: `typedef`'s and functions.  In a typedef, type
   variables are declared using angle brackets following type name.
   These variables can be referred in the type expression to the right
   of "=":
   ```
   typedef type2<'A,'B> = Cons1{field1: 'A, field2: type2<'B>}
                  |                      |                 |
                  |                       \---used here---/
                  \- type variables declared here
   ```

   In a function declaration, type variables are declared implicitly
   by referring to them in function arguments.  They are used in the
   return type of the function and in its body:
   ```
   function f(arg1: 'A, arg2: type2<'A,'B>): 'A {
       var x: 'A = arg1;
       x
   }
   ```
   Examples of invalid use of type variables in functions:
   ```
   function f(arg: 'A): 'B /*error: type variable 'B is not defined here*/
   {
       var x: 'C; /* error: type variable 'C is not defined here */
   }
   ```
1. Rules and relations are defined over concrete types and cannot
   refer to type variables.

## Functions

Functions are pure (side-effect-free computations).  A function
declared with `extern` keyword refers to an external function
defined outside of Datalog.  Such functions are declared without a
body.

```EBNF
function ::= "function" func_name "(" [arg(,arg)*]")"
              ":" simple_type_spec (* return type *)
              "{" expr "}          (* body of the function *)
           | "extern function" func_name "(" [arg(,arg)*]")"
              ":" simple_type_spec
```

```EBNF
arg ::= arg_name ":" simple_type_spec
```


### Constraints on functions

1. The body of the function must be a valid expression whose type
   matches the return type of the function.
1. Recursive functions are not allowed.
1. Just like regular functions, `extern` functions are expected to be
   side-effect-free.  While there is nothing preventing the user from
   defining functions with side effects (e.g., for tracing purposes),
   the language does not give any guarantees on the number, order, or
   timing of calls to these functions.

## Relations

```EBNF
relation ::= ["input" | "output"] "relation" rel_name "(" [arg ","] arg ")" [primary_key]
           | ["input" | "output"] "relation" rel_name "[" simple_type_spec "]" [primary_key]
primary_key ::= "primary" "key" "(" var_name ")" expr
```

The first form declares relation by listing its arguments.  The second
form explicitly specifies relation's element type.  The
second form is more general:

```
relation R(f1: bigint, f2: bool)
```
is equivalent to:
```
typedef R = R{f1: bigint, f2:bool}
relation R[R]
```

The optional primary key clause is only allowed in `input` relations and defines a mapping from a record to
its unique key. For example, the following declares a relation with primary key `f1`:

```
relation R(f1: bigint, f2: bool)
primary key (r) r.f1
```

### Constraints of relations
1. Column names must be unique within a relation
1. Column types cannot use type variables
1. Only `input` relations can have primary keys

## Expressions

```EBNF
expr ::= term
       | expr "["decimal "," decimal"]"  (*bit slice (e[h:l])*)
       | expr ":" simple_type_spec       (*explicit type signature*)
       | expr "." identifier             (*struct field*)
       | expr "." decimal                (*tuple field access*)
       | "-" expr                        (*unary arithmetic negation*)
       | "~" expr                        (*bitwise negation*)
       | "not" expr                      (*boolean negation*)
       | "(" expr ")"                    (*grouping*)
       | "{" expr "}"                    (*grouping (alternative syntax)*)
       | expr "*" expr                   (*multiplication*)
       | expr "/" expr                   (*division*)
       | expr "%" expr                   (*remainder*)
       | expr "+" expr
       | expr "-" expr
       | expr ">>" expr                  (*right shift*)
       | expr "<<" expr                  (*left shift*)
       | expr "++" expr                  (*concatenation (applies to bitvectors or strings)*)
       | expr "==" expr
       | expr "!=" expr
       | expr ">" expr
       | expr ">=" expr
       | expr "<" expr
       | expr "<=" expr
       | expr "&" expr                   (*bitwise and*)
       | expr "|" expr                   (*bitwise or*)
       | expr "and" expr                 (*logical and*)
       | expr "or" expr                  (*logical or*)
       | expr "=>" expr                  (*implication*)
       | expr "=" expr                   (*assignment*)
       | expr ";" expr                   (*sequential composition*)
       | expr "as" simple_type_spec      (*cast*)
```

The following table lists operators order by decreasing priority.

|**priority** | **operators**       |
| ------ |:-----------------------:|
| Highest| e[h:l], x:t, x.f        |
|        | ~                       |
|        | not                     |
|        | %  / *                  |
|        | +, -                    |
|        | <<, >>                  |
|        | ++                      |
|        | ==, !=, < , <=, >, >=   |
|        | &                       |
|        | &#124;                  |
|        | and                     |
|        | or                      |
|        | =>                      |
|        | =                       |
| Lowest | ;                       |

```EBNF
term ::= "_"                 (* wildcard *)
       | int_literal         (* integer literal *)
       | bool_literal        (* Boolean literal *)
       | fp_literal          (* floating point literal *)
       | string_literal      (* string literal *)
       | cons_term           (* type constructor invocation *)
       | apply_term          (* function application *)
       | var_term            (* variable reference *)
       | match_term          (* match term *)
       | ite_term            (* if-then-else term *)
       | for_term            (* for-loop *)
       | "continue"          (* abort current loop iteration *)
       | "break"             (* break out of a loop *)
       | return_term         (* return from a function *)
       | vardecl_term        (* local variable declaration *)
```

**Integer literal syntax is currently arcane and may be changed.**

```EBNF
int_literal  ::= decimal
               | [width] "'d" decimal
               | [width] "'h" hexadecimal
               | [width] "'o" octal
               | [width] "'b" binary
               | [width] "'sd" decimal
               | [width] "'sh" hexadecimal
               | [width] "'so" octal
               | [width] "'sb" binary
width ::= decimal

fp_literal ::= fp_value
             | "32'f" fp_value
             | "64'f" fp_value

fp_value ::= decimal "." decimal exponent

exponent ::= (* empty *)
           | ("e"|"E") ["+"|"-"] decimal
```

The "s" in a literal indicates a "signed" literal.

We support two types of UTF-8 string literals: quoted strings with escaping,
e.g., `"foo\nbar"`
(**We rely on parsec's standard parser for strings, which
supports unicode and escaping. TODO: check and document its exact
functionality.**) and raw strings where all characters, including backslash and
line breaks are interpreted as is:

```EBNF
string_literal   ::= '"' utf8_character* '"'
                     | "[|" utf8_character* "|]"
```

Multiple string literals are automatically concatenated, e.g.,
`"foo" [|bar|]` is equivalent to `"foobar"`.

Interpolated strings are string literals, that contain
expressions inside curly brackets preceeded by a dollar sign (`${}`), whose values are substituted at runtime.
Quoted strings are interpolate by default, e.g.,
`"x: ${x}, y: ${y}, f(x): ${f(x)}"` is equivalent to
`"x: " ++ x ++ ", y: " ++ y ++ ", f(x): " ++ f(x)`.

Raw interpolated strings must be preceed by a dollar sign:

```EBNF
raw_interpolated_string ::= ("$[|" utf8_character* "|]")+
```

Expressions in curly brackets can be arbitrarily complex, as long as
they produce results of type `string`, e.g.:
`$[|foo{var x = "bar"; x}|]` will evaluate to "foobar" at runtime.

Other terms:

```EBNF
bool_literal ::= "true" | "false"
cons_term    ::= (* positional arguments *)
                 cons_name ["{" [expr (,expr)*] "}"]
                 (* named arguments *)
               | cons_name ["{" "." field_name "=" expr
                            ("," "." field_name "=" expr)* "}"]
apply_term   ::= func_name "(" [expr (,expr)*] ")"
var_term     ::= var_name
ite_term     ::= "if" term term [ "else" term ]
for_term     ::= "for" "(" var_name "in" expr ")" term
return_term  ::= "return" [expr]
vardecl_term ::= "var" var_name

match_term   ::= "match" "(" expr ")" "{" match_clause (,match_clause)*"}"
match_clause ::= pattern "->" expr
```

```EBNF
(* match pattern *)
pattern ::= (* tuple pattern *)
            "(" [pattern (,pattern)* ")"
            (* constructor pattern with positional arguments *)
          | cons_name ["{" [pattern (,pattern)*] "}"]
            (* constructor pattern with named arguments *)
          | cons_name "{" ["." field_name "=" pattern
                           ("," "." field_name "=" pattern)*] "}"
          | vardecl_term    (* binds variable to a field inside the matched value *)
          | var_term        (* binds variable to a field inside the matched value
                               (shorthand for vardecl_term) *)
          | bool_literal    (* matches specified bool value *)
          | string_literal  (* matches specified string value *)
          | int_literal     (* matches specified integer or bitvector value *)
          | "_"             (* wildcard, matches any value *)
```

### Automatic string conversion

Values of arbitrary types that occur inside interpolated strings or as
a second argument to the string concatenation operator (`++`) are
automatically converted to strings.
Values of primitive types (`string`, `bigint`, `bit`, and `bool`) are converted using
builtin methods.

For user-defined types, conversion is performed by calling a user-defined function
whose name is formed from the type name by changing the first letter of the type name
to lower case (if it is in upper case) and adding the `"2string"` suffix.  The function
must take exactly one argument of the given type and return a string.
Compilation fails if a function with this name and signature is not found.

For example, the last statement in
```
typedef udf_t = Cons1 | Cons2{f: bigint}
function udf_t2string(x: udf_t): string { ... }
x: udf_t;

y = "x:{x}";
```
is equivalent to:
```
y = "x:{udf_t2string(x)}";
```

### Constraints on expressions

1. Number and types of arguments to a function must match function
   declaration.
1. Comparisons for equality and ordering are supported for all base types
   and derived types, but both arguments of a comparison must have the same type.
   Strings and tuples are compared lexicographically.
   Types with multiple constructors also compare lexicographically,
   first by constructor, using the order of constructor declaration.

   The following comparisons all return `true`:
   ```
   false <= true
   "a" <= "b"
   "A" <= "a"
   (0, 1) <= (0, 2)
   (1, 0) <= (2, 0)
   (0, 1) <= (0, 1)
   C{"a"} <= C{"b"}
   TwoFields{"a", "b"} <= TwoFields{"a", "c"}
   None <= Some{0}
   Some{0} <= Some{1}
   ```

1. Variable declarations can occur in the left-hand side of an
   assignment or as a separate statement followed by another
   statement.  In the latter case variable type must be explicitly
   specified:
   ```
   typedef C = C{x: string}
   typedef TwoFields = TwoFields{f1: string, f2: string}
   ...

   // ok: type of x specified explicitly
   var x: bigint;

   // error: variable declared without a type
   var x;

   // ok: the type of y is derived from the right-hand side
   // of the assignment
   var y = C{.x = "foo"};

   // ok: no harm in specifying type explicitly even if it could
   // be inferred automatically.
   var z: C = C{.x = "bar"};

   // ok: assigning multiple variables simultaneously
   (var a, var b) = (x+5, x-5);
   C{var c} = y;

   // ok
   var t = TwoFields{.f1 = "foo", .f2 = "bar"};

   // ok: field f2 omitted in the left-hand side of an assignment
   TwoFields{.f1 = var g} = t;

   // error: field f2 omitted in the right-hand side of an assignment
   var h = TwoFields{.f1 = "foo"}
   ```
1. A variable declaration cannot shadow existing variables visible
   in the local scope.  Variables visible inside the body of a function
   include: function arguments, local variables declared using `var`, and
   variables introduced through `match` patterns:
   ```
   var i: bit<32> = match (a) {
       C0{.x = v} -> v, // variable v bound inside match pattern
       C1{v} -> v       // variable v bound inside match pattern
   };

   function shadow(a: string): () {
       var v: string;
       var v = "foo"; // error: variable re-definition
       var a = "bar"  // error: variable shadows argument name
   }
   ```
   Variables in rules are discussed below.
1. *Guarded fields* of tagged unions cannot be accessed using '.field'
   syntax.  A guarded field is a field that is present in some but not
   all of the type constructors, e.g., `value` in the `option_t` type
   ```
   typedef option_t<'A> = None
                        | Some {value : 'A}
   ```
   Accessing such a field has undefined outcome when the field is not
   present:
   ```
   var a: Some<string>;
   var b = a.value // error: access to guarded field value
   ```
   Use pattern matching instead, e.g.:
   ```
   var b = match (a) {
       Some{v} -> v,
       None    -> ""
   }
   ```
1. Patterns in a `match` expression must be exhaustive.
1. A type constructor can only be used in the left-hand side of an
   assignment if the given type only has one constructor:
   ```
   var v: option_t<string>;
   Some{x} = v; // error type option_t has multiple constructors
   ```

## Rules

A Datalog rule consists of the *head* comprised of one or more *atoms* (multiple
atoms abbreviate rules with identical right-hand sides) and zero or more *body* clauses.

```EBNF
rule ::= atom (,atom)* ":-" rhs_clause (,rhs_clause)*

atom ::= [var_name "in"] rel_name "(" expr (,expr)* ")"
       | [var_name "in"] rel_name "(" "." arg_name "=" expr [("," "." arg_name "=" expr)*] ")"
       | rel_name "[" expr "]"

rhs_clause ::= atom                                      (* 1.atom *)
             | "not" atom                                (* 2.negated atom *)
             | expr                                      (* 3.condition *)
             | expr "=" expr                             (* 4.assignment *)
             | "var" var_name "=" "FlatMap" "(" expr ")" (* 5.flat map *)
             | "var" var_name = "Aggregate" "("          (* 6.aggregation *)
                "(" [var_name ("," var_name)*] ")" ","
                    func_name "(" expr ")" ")"
```

An atom is a predicate that holds when a given value belongs to a relation.
It can be specified by listing its fields or by giving its value explicitly.
The former is a special case of the latter:

```
Rel(val1, val2)
```
is expanded into
```
Rel[Rel1(val1, val2)]
```

A body clause can have several forms.  The
first two forms (`atom` and `"not" atom`) represent a *literal*, i.e.,
an atom or its negation:

```
Cousins(x,y) :- Parent(z,x),
                Parent(q,y),
                Siblings(z,q),
                not Siblings(x,y)
```

We say that the atom appears with *positive polarity* (in the
non-negated case) or *negative polarity* in the rule.

The third form is a Boolean expression over variables introduced in the
body of the rule. It filters the result of the query.

```
Siblings(x,y) :- Parent(z,x), Parent(z,y), x != y
```

The fourth form is an assignment expression that may introduce new
variables as well as filters the query by pattern matching, e.g.,:

```
DHCP_Options_server_ip(opts, ipv6_string_mapped(in6_generate_lla(mac))) :-
    DHCP_Options_options(opts, "server_id", val),
    NoIP4Addr = ip_parse(val),
    SomeMAC{var mac} = eth_addr_from_string(val)
```

Here we first filter the relation, only keeping records where
`ip_parse(val) == NoIP4Addr`. Next we extract Ethernet address from
`val` and bind the result to new variable `mac`, while filtering out
those values that do not parse to a valid Ethernet address.

The fifth form is a flat-map operation, which expands each record in
the relation computed so far to a set of records, computes a union of
all sets and binds a record in the resulting relation to a fresh
variable, e.g.:

```
Logical_Switch_Port_ips(lsp, mac, ip) :-
    Logical_Switch_Port_addresses(lsp, addrs),
    (mac, ips) = extract_mac(addrs),
    var ip = FlatMap(extract_ips(ips))
```

Here, `extract_ips` must return a *set* of IP addresses:
```
extern function extract_ips(addrs: string): Set<ip_addr_t>
```

The sixth form groups records computed so far by a subset of fields,
computes an aggreagate for each group using specified aggregate function,
and binds the result to a new variable:

```
ShortestPath(x,y, c) :- Path(x,y,cost), var c = Aggregate((x,y), min(cost))
```

The aggregate function (e.g., `min` in this example) must have the following
signature:

```
function min(Group<'A>): 'B
```

where `'A` is the type of expression passed as argument to the function and
`'B` is the aggregate value computed by the function.  `Group` is a DDlog
built-in type.  It can only currently be manipulated from Rust, meaning that
aggregate functions can only be defined in Rust and imported to a DDlog
program as `extern function`.

### Variables and patterns

A clause in the body of a rule can introduce variables that are
visible in clauses following it and in the head of the rule. Variables
are introduced *explicitly* in the left-hand side of an assignment
clause, a flat-map or an aggregate clause or *implicitly*,
by referring to them in a positive atom.  An implicit declaration must
appear in a *pattern expression*, i.e., an expression built
recursively out of variable names, wildcards, tuples, type
constructors, and constants (i.e., string, integer, booleand or
bit-vector literals).

For example, in the following rule, the first occurrence of variable
`x` in pattern `Some{(_, x)}` introduces the variable, whereas the
second occurrence (in `T(x,y)`) refers to it:
```
R(x,y) :- S(Some{(_, x)}), T(x, y).
```

It is illegal to introduce a new variable in an expression that is not
a pattern:
```
R(x,y) :- S(f(x)), T(x, y). // illegal, as f(x) is not a pattern.
R(x,y) :- S(x), T(f(x), y). // ok, x is introduced before being used in f(x)
```

### Constraints on rules

1. Negative atoms and condition clauses may not introduce new variables.
1. Variables introduced in a clause are visible in clauses following it.  An aggregate
   clause has the effect of concealing all variables except for the
   group-by variables and the aggregate variable.
   ```
   R(x,y) :- S(x), T(x, y), var z = Aggregate((x), min(y)). // error:
        // y cannot be used in the head, as it is concealed by aggregation
   ```
1. *Safety*: Negative literals may not introduce new variables or use wildcards.
   ```
   R(x) :- S(x), T(x,y).     // ok
   R(x) :- S(x), not T(x,y). // error: variable y introduced in a negative literal
   ```
1. A variable cannot be declared and used in the same literal.
   ```
   R(x) :- S(x,_), T(x). // ok
   R(x) :- S(x,x).       // error variable x is introduced and used in the same literal
   ```
1. All variables occurring in the head of a rule must be declared in
   its body.
   ```
   R(f(x)) :- S(x). //ok
   R(f(y)) :- S(x). //error: y is not declared
   ```
1. A flat-map expression must have type `Set<'X>` or `Vec<'X>` or `Map<'K,'V>` for some type `'X`,
   `'K`, `'V`.

### Constraints on dependency graph

A *dependency graph* is a labeled directed graph whose vertices represent
relations.  For each rule that contains relation 'R1' in its head and
relation 'R2' with polarity 'p' in the body, there is an edge in the
graph from 'R2' to 'R1' labeled 'p'.

1. *Stratified negation*: No cycle in a graph can contain an edge
   labeled with negative polarity.
1. A body of a rule with an aggregate clause cannot contain an atom mutually
   recursive with its head.


# FTL syntax

FTL is an imperative language for expressing datalog relations, with a syntax based on FLWOR
https://en.wikipedia.org/wiki/FLWOR.

```
rule ::= forStatement

forStatement ::= "for" "(" expr "in" atom ["if" expression] ")" statement

statement ::= forStatement
          | ifStatement
          | matchStatement
          | varStatement
          | insertStatement
          | blockStatement
          | emptyStatement

ifStatement ::= "if" "(" expression ")" statement
            |   "if" "(" expression ")" statement "else" statement

matchStatement ::= "match" "(" expression ")" "{" expression "->" statement (, expression "->" statement )* "}"

varStatement ::= expr "=" expr "in" statement

insertStatement ::= rel_name "(" expression ( "," expression )* ")"

blockStatement ::= "{" statement ( ";" statement )* "}"

emptyStatement ::= "skip"
```
