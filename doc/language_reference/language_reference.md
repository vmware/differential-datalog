# Datalog Language Reference

## Identifiers

Datalog is case-sensitive.  Relation, constructor, and type variable
names must start with upper-case ASCII letters; variable, function,
and argument names must start with lower-case ASCII letters or
underscore.  A type variable name must be prefixed with a tick (').
A type name can start with either an upper-case or a lower-case letter
or underscore.

```
    uc_identifier ::= [A..Z][a..zA..Z0..9_]*
    lc_identifier ::= [a..z_][a..zA..Z0..9_]*

    rel_name     ::= uc_identifier
    cons_name    ::= uc_identifier
    typevar_name ::= 'uc_identifier

    var_name     ::= lc_identifier
    field_name   ::= lc_identifier
    arg_name     ::= lc_identifier
    func_name    ::= lc_identifier

    type_name    ::= lc_identifier | uc_identifier
```

## Top-level declarations

A Datalog program is a list of type definitions, functions, relations, and rules.
The ordering of declarations does not matter, e.g., a type can be used
before being defined.

```EBNF
datalog ::= decl*

decl ::= typedef
       | function
       | relation
       | rule
```

### Constraints on top-level declarations
1. Type names must be globally unique
1. Function names must be globally unique
1. Relation names must be globally unique

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
                   | tuple_type
                   | type_alias
                   | typevar_name
```

```EBNF
bigint_type      ::= "bigint" (* unbounded mathematical integer *)
bool_type        ::= "bool"
string_type      ::= "string" (* UTF-8 string *)
bitvector_type   ::= "bit" "<" decimal ">"
tuple_type       ::= "(" simple_type_spec* ")"
union_type       ::= (constructor "|")* constructor
type_alias       ::= type_name           (* type name declared using typedef*)
                     ["<" type_spec [("," type_spec)*] ">"] (* type arguments *)

constructor      ::= cons_name (* constructor without fields *)
                   | cons_name "{" [field ("," field)*] "}"
field            ::= field_name ":" simple_type_spec
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
    function f(): bool = {
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
   function f(arg1: 'A, arg2: type2<'A,'B>): 'A = {
       var x: 'A = arg1;
       x
   }
   ```
   Examples of invalid use of type variables in functions:
   ```
   function f(arg: 'A): 'B = /*error: type variable 'B is not defined here*/
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
              ["=" expr]
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

## Relations

```EBNF
relation ::= ["input"] "relation" rel_name "(" [arg ","] arg ")"
           | ["input"] "relation" rel_name "[" simple_type_spec "]"
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

### Constraints of relations
1. Column names must be unique within a relation
1. Column types cannot use type variables

## Expressions

```EBNF
expr ::= term
       | expr "["decimal "," decimal"]"  (*bit slice (e[h:l])*)
       | expr ":" simple_type_spec       (*explicit type signature*)
       | expr "." identifier             (*struct field*)
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
       | string_literal      (* string literal *)
       | interpolated_string (* string literal *)
       | cons_term           (* type constructor invocation *)
       | apply_term          (* function application *)
       | var_term            (* variable reference *)
       | match_term          (* match term *)
       | ite_term            (* if-then-else term *)
       | vardecl_term        (* local variable declaration *)
```

**Integer literal syntax is currently arcane and will be changed to
C-style syntax.**

```EBNF
int_literal  ::= decimal
               | [width] "'d" decimal
               | [width] "'h" hexadecimal
               | [width] "'o" octal
               | [width] "'b" binary
width ::= decimal
```

We support two types of UTF-8 string literals: quoted strings with escaping,
e.g., `"foo\nbar"`
(**We rely on parsec's standard parser for strings, which
supports unicode and escaping. TODO: check and document its exact
functionality.**) and raw strings where all characters, including backslash and 
line breaks are interpreted as is:

```EBNF
string_literal   ::= ( '"' utf8_character* '"' 
                     | "[|" utf8_character* "|]")+
```

Multiple string literals are automatically concatenated, e.g.,
`"foo" [|bar|]` is equivalent to `"foobar"`.

Interpolated strings are string literals, that can contain
expressions inside curly brackets, whose values are substituted at runtime. 
Interpolated strings are preceded by the `$` character (syntax
borrowed from C#).

```EBNF
interpolated_string ::= ( '$"' utf8_character* '"' 
                        | "$[|" utf8_character* "|]")+
```

For example,
`$"x: {x}, y: {y}, f(x): {f(x)}"` is equivalent to 
`"x: " ++ x ++ ", y: " ++ y ++ ", f(x): " ++ f(x)`.

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
ite_term     ::= "if" term term "else" term
vardecl_term ::= "var" var_name

match_term   ::= "match" "(" expr ")" "{" match_clause (,match_clause)*"}"
match_clause ::= pattern "-" expr
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
function udf_t2string(x: udf_t): string = ...
x: udf_t;

y = $"x:{x}";
```
is equivalent to:
```
y = $"x:{udf_t2string(x)}";
```

### Constraints on expressions

1. Number and types of arguments to a function must match function
   declaration.  
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

   function shadow(a: string): () = {
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

atom ::= rel_name "(" expr (,expr)* ")"
       | rel_name "(" "." arg_name "=" expr [("," "." arg_name "=" expr)*] ")"
       | rel_name "[" expr "]"

rhs_clause ::= atom                                      (* 1.atom *)
             | "not" atom                                (* 2.negated atom *)
             | expr                                      (* 3.condition *)
             | expr "=" expr                             (* 4.assignment *)
             | "FlatMap" "(" var_name "=" expr ")"       (* 5.flat map *)
             | "Aggregate" "("                           (* 6.aggregation *)
                "(" [var_name ("," var_name)*] ")" "," 
                var_name "=" expr ")"
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
    FlatMap(ip = extract_ips(ips))
```

Here, `extract_ips` must return a *set* of IP addresses:
```
extern function extract_ips(addrs: string): set<ip_addr_t>
```

The sixth form groups records computed so far by a subset of fields,
computes an aggreagate for each group using specified aggregate function 
(*requirements on aggregate function signature to be specified*), and
binds the result to a new variable:

```
ShortestPath(x,y, c) :- Path(x,y,cost), Aggregate((x,y), c = min(cost))
```

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
   R(x,y) :- S(x), T(x, y), Aggregate((x), z = min(y)). // error:
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
1. A flat-map expression must have type `set<x>` for some type `x`. 

### Constraints on dependency graph

A *dependency graph* is a labeled directed graph whose vertices represent
relations.  For each rule that contains relation 'R1' in its head and
relation 'R2' with polarity 'p' in the body, there is an edge in the
graph from 'R2' to 'R1' labeled 'p'.

1. *Linearity*: For each atom 'R(...)' in the head of a rule, at most one 
   relation in the body of the rule can be mutually recursive with 'R'.
1. *Stratified negation*: No cycle in a graph can contain an edge
   labeled with negative polarity.
1. A body of a rule with an aggregate clause cannot contain an atom mutually 
   recursive with its head.


# FTL syntax

FTL is an imperative language for expressing datalog relations, with a syntax based on FLWOR
https://en.wikipedia.org/wiki/FLWOR.

```
rule ::= forStatement

forStatement ::= "for" "(" expr "in" rel_name ")" statement
             | "for" "(" expr "in" rel_name "if" expression ")" statement           

statement ::= forStatement
          | ifStatement
          | matchStatement
          | letStatement
          | insertStatement
          | blockStatement
          | emptyStatement
          
ifStatement ::= "if" "(" expression ")" statement
            |   "if" "(" expression ")" statement "else" statement
             
matchStatement ::= "match" "(" expression ")" "{" expression "->" statement (, expression "->" statement )* "}"                                               
             
letStatement ::= "let" identifier "=" expression (, identifier "=" expression )* in" statement

insertStatement ::= rel_name "(" expression ( "," expression )* ")"

blockStatement ::= "{" statement ( ";" statement )* "}"

emptyStatement ::= "skip"            
```
