# Datalog Language Reference

## Identifiers

Datalog is case-sensitive.  Relation, constructor, and type variable
names must start with upper-case ASCII letters; variable, function,
and argument names must start with lower-case ASCII letters or
underscore.  A type name can start with either an upper-case or a lower-case letter
or underscore.

```
    uc_identifier ::= [A..Z][a..zA..Z0..9_]*
    lc_identifier ::= [a..z_][a..zA..Z0..9_]*

    rel_name     ::= uc_identifier
    cons_name    ::= uc_identifier
    typevar_name ::= uc_identifier

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

### Constraints
1. Type names must be globally unique
1. Function names must be globally unique
1. Relation names must be globally unique

## Types

Type definition introduces a new user-defined type, optionally
parameterized by one or more type arguments.  Type given without
definition is an *opaque* type implemented outside of the language.

```EBNF
typedef ::= "typedef" type_name (* unique type name *)
            ["<" typevar_name [("," typevar_name)*] ">"] (* optional type arguments *)
            ["=" type_spec] (* optional type definition *)
```

```EBNF
(* A full form of typespec. Used in typedef's only. *)
type_spec ::= int_type
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
simple_type_spec ::= int_type
                   | bool_type
                   | string_type
                   | bitvector_type
                   | tuple_type
                   | type_alias
                   | typevar_name
```

```EBNF
int_type         ::= "int" (* unbounded mathematical integer *)
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

### Constraints
1. Type argument names must be unique within a typedef, e.g.,
`typedef t1<A,A,B>` is invalid.
1. The number of bits in a bitvector type must be greater than 0, e.g.,
`bit<0>` is invalid.
1. Type constructor names must be globally unique.
1. If multiple type constructors for the same type have arguments with
identical names, their types must be identical, e.g., the following is invalid:
    ```
    typedef type1 = Constr1{field1: string, field2: bool} | Constr2{field1: int}
    ```
1. A type must be instantiated with the number of type
arguments matching its declaration:
    ```
    typedef type1<A,B>
    function f(): bool = {
        var x: type1<int> // error: not enough type arguments
    }
    ```

## Functions

Functions are pure (side-effect-free computations).  A function can have 
optional definition.  A function without definition refers to a
foreign function implemented outside of Datalog.

```EBNF
function ::= "function" func_name "(" [arg(,arg)*]")"
              ":" simple_type_spec (* return type *)
              ["=" expr]    (* optional function definition *)
```

```EBNF
arg ::= arg_name ":" simple_type_spec
```

## Relations

```EBNF
relation ::= ["ground"] "relation" rel_name "(" [arg ","] arg ")"
```

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
       | expr "*" expr                   (* multiplication *)
       | expr "/" expr                   (* division *)
       | expr "%" expr                   (*remainder*)
       | expr "+" expr
       | expr "-" expr
       | expr ">>" expr                  (*right shift*)
       | expr "<<" expr                  (*left shift*)
       | expr "++" expr                  (*bitvector concatenation*)
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
term ::= "_"               (* wildcard *)
       | int_literal       (* integer literal *)
       | bool_literal      (* Boolean literal *)
       | string_literal    (* string literal *)
       | cons_term         (* type constructor invocation *)
       | apply_term        (* function application *)
       | var_term          (* variable reference *)
       | match_term        (* match term *)
       | ite_term          (* if-then-else term *)
       | vardecl_term      (* local variable declaration *)
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

**We rely on parsec's standard parser for strings, which
supports unicode and escaping. TODO: check and document its exact
functionality.**

```EBNF
string_literal   ::= '"' UTF-8 string with escaping '"' 
```

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

match_term   ::= "match" "(" expr ")" "{" match_clause [(,match_clause)*]"}"
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
          | var_decl_term (* variable declaration inside pattern *)
          | "_"             (* wildcard, matches any value *)
```

## Rules

A Datalog rule consists of one or more lefthand-side atoms (multiple 
LHS atoms are used to abbreviate rules with identical right-hand 
sides), zero or more . 

```EBNF
rule ::= atom [(,atom)*] ":-" atom' [(,atom')*]

atom ::= rel_name "(" expr [(,expr)*] ")"
       | rel_name "(" "." arg_name "=" expr [("," "." arg_name "=" expr)*] ")"

atom' ::= atom                                      (* 1.atom *)
        | "not" atom                                (* 2.negated atom *)
        | expr                                      (* 3.condition *)
        | expr "=" expr                             (* 4.condition *)
        | "FlatMap" "(" var_name "=" expr ")"       (* 5.flat map *)
        | "Aggregate" "("                           (* 6.aggregation *)
          "(" [var_name [("," var_name)*]] ")" "," 
          var_name "=" expr ")"
```

We briefly discuss each form of right-hand-side clause (`atom'`). The
first two forms (`atom` and `"not" atom`) represent a *literal*, i.e.,
an atomic predicate or its negation:

```
Cousins(x,y) :- Parent(z,x),
                Parent(q,y),
                Siblings(z,q),
                not Siblings(x,y)
```

The third form is a Boolean expression over variables introduced in the 
RHS of the rule. It filters the result of the query.

```
Siblings(x,y) :- Parent(z,x), Parent(z,y), x != y
```

The fourth form is an assignment expression that may introduce new
variables as well as filter the query by pattern matching, e.g.,:

```
DHCP_Options_server_ip(opts, ipv6_string_mapped(in6_generate_lla(mac))) :-
    DHCP_Options_options(opts, "server_id", val),
    NoIP4Addr = ip_parse(val),
    SomeMAC{mac} = eth_addr_from_string(val)
```

Here we first filter the relation, only keeping records where
`ip_parse(val) == NoIP4Addr`. Next we extract Ethernet address from 
`val` and bind the result to new variable `mac`, while filtering out 
those values that do not parse to a valid Ethernet address.

The fifth form is a flat map operation, which expands each record in
the relation computed so far to a set of records, computes a union of 
all sets and binds a record in the resulting relation to a fresh
variable, e.g.:

```
Logical_Switch_Port_ips(lsp, mac, ip) :-
    Logical_Switch_Port_addresses(lsp, addrs),
    (mac, ips) = extract_mac(addrs),
    FlatMap(ip = extract_ips(ips))
```

Here, `extract_ips` must return a *set* of IP addresses.

The sixth form groups records computed so far by a subset of fields,
computes an aggreagate for each group using specified aggregate function 
(*requirements on aggregate function signature to be specified*), and
binds the result to a new variable:

```
ShortestPath(x,y, c) :- Path(x,y,cost), Aggregate((x,y), c = min(cost))
```
