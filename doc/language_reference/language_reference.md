#Datalog Language Reference

##Identifiers

Datalog is case-sensitive.  Relation and constructor
names must start with upper-case ASCII letters; and variable, function, 
and argument names must start with lower-case ASCII letters or
underscore.  A type name can start with either an upper-case or a lower-case letter.

```
    <uc_identifier> := [A..Z][a..zA..Z0..9_]*
    <lc_identifier> := [a..z_][a..zA..Z0..9_]*

    <rel_name> := <uc_identifier>
    <cons_name> := <uc_identifier>

    <var_name> := <lc_identifier>
    <field_name> := <lc_identifier>
    <arg_name> := <lc_identifier>
    <func_name> := <lc_identifier>

    <type_name> := <lc_identifier> | <uc_identifier>
```

##Top-level declarations

A Datalog program is a list of type definitions, functions, relations, and rules.
The ordering of declarations does not matter, e.g., a type can be used
before being defined.

```EBNF
<datalog> := <decl>*

<decl> := <typedef>
        | <function>
        | <relation>
        | <rule>
```

##Types

Type definition introduces a new user-defined type.

```EBNF
<typedef> := "typedef" <type_name> = <type_spec>
```

```EBNF
/* A full form of typespec. Used in typedef's only. */
<type_spec> := <int_type>
             | <bool_type>
             | <string_type>
             | <bitvector_type>
             | <tuple_type>
             | <union_type>     /* tagged union */
             | <type_name>      /* type alias */

/* A restricted form of typespec that does not declare new tagged
    unions (and hence does not introduce new contructor named to
    the namespace.  Used in argument, field, variable declarations. */
<simple_type_spec> := <int_type>
                    | <bool_type>
                    | <string_type>
                    | <bitvector_type>
                    | <tuple_type>
                    | <type_name>

```

```EBNF
<int_type>         := "int" /* unbounded mathematical integer */
<bool_type>        := "bool"
<string_type>      := "string" /* UTF-8 string */
<bitvector_type>   := "bit" "<" <decimal> ">"
<tuple_type>       := "(" <simple_type_spec>* ")"
<union_type>      := (<constructor> "|")* <constructor>

<constructor>      := <cons_name> /* constructor without fields */
                    | <cons_name> "{" [<field> ("," <field>)*] "}"
<field> := <field_name> ":" <simple_type_spec> 
```

##Functions

Functions are pure (side-effect-free computations).  A function can have 
optional definition.  A function without definition refers to a
foreign function implemented outside of Datalog.

```EBNF
<function> := "function" <func_name> "(" [<arg>(,<arg>)*]")"
              ":" <simple_type_spec> /* return type */
              ["=" <expr>]    /* optional function definition */
```

```EBNF
<arg> := <arg_name> ":" <simple_type_spec>
```

##Relations

```EBNF
<relation> := ["ground"] "relation" <rel_name> "(" [<arg> ","] <arg> ")"
```

##Expressions


```EBNF
<expr> := <term>
        | <expr> "["<decimal> "," <decimal>"]"  /*bit slice (e[h:l])*/
        | <expr> ":" <simple_type_spec>         /*explicit type signature*/
        | <expr> "." <identifier>               /*struct field*/
        | "~" <expr>                            /*bitwise negation*/
        | "not" <expr>                          /*boolean negation*/
        | "(" <expr> ")"                        /*grouping*/
        | "{" <expr> "}"                        /*grouping (alternative syntax)*/
        | <expr> "%" <expr>                     /*remainder*/
        | <expr> "+" <expr>
        | <expr> "-" <expr>
        | <expr> ">>" <expr>                    /*right shift*/
        | <expr> "<<" <expr>                    /*left shift*/
        | <expr> "++" <expr>                    /*bitvector concatenation*/
        | <expr> "==" <expr>
        | <expr> "!=" <expr>
        | <expr> "<" <expr>
        | <expr> "<=" <expr>
        | <expr> ">" <expr>
        | <expr> ">=" <expr>
        | <expr> "&" <expr>                     /*bitwise and*/
        | <expr> "|" <expr>                     /*bitwise or*/
        | <expr> "and" <expr>                   /*logical and*/
        | <expr> "or" <expr>                    /*logical or*/
        | <expr> "=>" <expr>                    /*implication*/
        | <expr> "=" <expr>                     /*assignment*/
        | <expr> ";" <expr>                     /*sequential composition*/
```

The following table lists operators order by decreasing priority.

\begin{tabular}{|l|l|}
    \hline
    \textbf{priority} & \textbf{operators} \\
    \hline\hline
    Highest & \src{e[h:l], x:t, x.f} \\
            & \src{\~} \\
            & \src{not} \\
            & \src{\%} \\
            & \src{+, -} \\
            & \src{>>, <<} \\
            & \src{++} \\
            & \src{==, !=, <, <=, >, >=} \\
            & \src{\&} \\
            & \src{|} \\
            & \src{and} \\
            & \src{or} \\
            & \src{=>} \\
            & \src{=} \\
    Lowest  & \src{;} \\
    \hline
\end{tabular}

```EBNF
<term> := "_"                 /* wildcard */
        | <int_literal>       /* integer literal */
        | <bool_literal>      /* Boolean literal */
        | <string_literal>    /* string literal */
        | <cons_term>         /* type constructor invocation */
        | <apply_term>        /* function application */
        | <var_term>          /* variable reference */
        | <match_term>        /* match term */
        | <ite_term>          /* if-then-else term */
        | <vardecl_term>      /* local variable declaration */
```

{\color{red}Integer literal syntax is currently arcane and will be changed to
C-style syntax.}

```EBNF
<int_literal>  := <decimal>
                | [<width>] "'d" <decimal>
                | [<width>] "'h" <hexadecimal>
                | [<width>] "'o" <octal>
                | [<width>] "'b" <binary>
<width> := <decimal>
```

{\color{red}We rely on parsec's standard parser for strings, which
supports unicode and escaping. TODO: check and document its exact
functionality.}

```EBNF
<string_literal>   := '"' <UTF-8 string with escaping> '"' 
```

Other terms:

```EBNF
<bool_literal> := "true" | "false"
<cons_term>    := /* positional arguments */
                  <cons_name> ["{" [<expr> (,<expr>)*] "}"]
                  /* named arguments */
                | <cons_name> ["{" <field_name>":"<expr> (,<field_name>":"<expr>)* "}"]
<apply_term>   := <func_name> "(" [<expr> (,<expr>)*] ")"
<var_term>     := <var_name>
<ite_term>     := "if" <term> <term> "else" <term>
<vardecl_term> := "var" <var_name>

<match_term>   := "match" "(" <expr> ")" "{" <match_clause> [(,<match_clause>)*]"}"
<match_clause> := <pattern> "->" <expr>
```

```EBNF
/* match pattern */
<pattern> := /* tuple pattern */
             "(" [<pattern> (,<pattern>)* ")"                      
             /* constructor pattern with positional arguments */
           | <cons_name> ["{" [<pattern> (,<pattern>)*] "}"]   
             /* constructor pattern with named arguments */
           | <cons_name> "{" [<field_name>":"<pattern> 
                             (,<field_name>":"<pattern>)*] "}"
           | <var_decl_term> /* variable declaration inside pattern */
           | "_"             /* wildcard, matches any value */
```

##Rules

A Datalog rule consists of one or more lefthand-side atoms (multiple 
LHS atoms are used to abbreviate rules with identical right-hand 
sides), zero or more . 

```EBNF
<rule> := <atom> [(,<atom>)*] ":-" <atom'> [(,<atom'>)*]

<atom> := <rel_name> "(" <expr> [(,<expr>)*] ")"
        | <rel_name> "(" <arg_name>":"<expr> [(,<arg_name>":"<expr>)*] ")"

<atom'> := <atom>        /* atom */
         | "not" <atom>  /* negated atom */
         | <expr>        /* condition */

```
