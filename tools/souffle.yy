/*
 * Souffle - A Datalog Compiler
 * Copyright (c) 2013, 2015, Oracle and/or its affiliates. All rights reserved
 * Licensed under the Universal Permissive License v 1.0 as shown at:
 * - https://opensource.org/licenses/UPL
 * - <souffle root>/licenses/SOUFFLE-UPL.txt
 */

/************************************************************************
 *
 * @file parser.yy
 *
 * @brief Parser for Datalog
 *
 ***********************************************************************/
%skeleton "lalr1.cc"
%require "3.0.2"
%defines
%define parser_class_name {parser}
%define api.token.constructor
%define api.value.type variant
%define parse.assert
%define api.location.type {SrcLocation}

%code requires {
    #include <config.h>
    #include <cassert>
    #include <cstdarg>
    #include <cstdio>
    #include <memory.h>
    #include <cstdlib>
    #include <stack>
    #include <string>
    #include <unistd.h>

    #include "AstArgument.h"
    #include "AstClause.h"
    #include "AstComponent.h"
    #include "AstFunctorDeclaration.h"
    #include "AstIO.h"
    #include "AstNode.h"
    #include "AstParserUtils.h"
    #include "AstProgram.h"
    #include "AstRelation.h"
    #include "AstTypes.h"
    #include "BinaryConstraintOps.h"
    #include "FunctorOps.h"
    #include "SrcLocation.h"
    #include "Util.h"

    using namespace souffle;

    namespace souffle {
        class ParserDriver;
    }

    using yyscan_t = void*;

    #define YY_NULLPTR nullptr

    /* Macro to update locations as parsing proceeds */
    # define YYLLOC_DEFAULT(Cur, Rhs, N)                       \
    do {                                                       \
        if (N) {                                               \
            (Cur).start        = YYRHSLOC(Rhs, 1).start;       \
            (Cur).end          = YYRHSLOC(Rhs, N).end;         \
            (Cur).filename     = YYRHSLOC(Rhs, N).filename;    \
        } else {                                               \
            (Cur).start    = YYRHSLOC(Rhs, 0).end;             \
            (Cur).end      = YYRHSLOC(Rhs, 0).end;             \
            (Cur).filename = YYRHSLOC(Rhs, 0).filename;        \
        }                                                      \
    } while (0)
}

%param { ParserDriver &driver }
%param { yyscan_t yyscanner }

%locations

%define parse.trace
%define parse.error verbose

%code {
    #include "ParserDriver.h"
}

%token <std::string> RESERVED    "reserved keyword"
%token END 0                     "end of file"
%token <std::string> STRING      "symbol"
%token <std::string> IDENT       "identifier"
%token <AstDomain> NUMBER        "number"
%token <std::string> RELOP       "relational operator"
%token PRAGMA                    "pragma directive"
%token OUTPUT_QUALIFIER          "relation qualifier output"
%token INPUT_QUALIFIER           "relation qualifier input"
%token PRINTSIZE_QUALIFIER       "relation qualifier printsize"
%token BRIE_QUALIFIER            "BRIE datastructure qualifier"
%token BTREE_QUALIFIER           "BTREE datastructure qualifier"
%token EQREL_QUALIFIER           "equivalence relation qualifier"
%token OVERRIDABLE_QUALIFIER     "relation qualifier overidable"
%token INLINE_QUALIFIER          "relation qualifier inline"
%token TMATCH                    "match predicate"
%token TCONTAINS                 "checks whether substring is contained in a string"
%token CAT                       "concatenation of two strings"
%token ORD                       "ordinal number of a string"
%token STRLEN                    "length of a string"
%token SUBSTR                    "sub-string of a string"
%token MIN                       "min aggregator"
%token MAX                       "max aggregator"
%token COUNT                     "count aggregator"
%token SUM                       "sum aggregator"
%token TRUE                      "true literal constraint"
%token FALSE                     "false literal constraint"
%token STRICT                    "strict marker"
%token PLAN                      "plan keyword"
%token IF                        ":-"
%token DECL                      "relation declaration"
%token FUNCTOR                   "functor declaration"
%token INPUT_DECL                "input directives declaration"
%token OUTPUT_DECL               "output directives declaration"
%token PRINTSIZE_DECL            "printsize directives declaration"
%token OVERRIDE                  "override rules of super-component"
%token TYPE                      "type declaration"
%token COMPONENT                 "component declaration"
%token INSTANTIATE               "component instantiation"
%token NUMBER_TYPE               "numeric type declaration"
%token SYMBOL_TYPE               "symbolic type declaration"
%token TONUMBER                  "convert string to number"
%token TOSTRING                  "convert number to string"
%token AS                        "type cast"
%token NIL                       "nil reference"
%token PIPE                      "|"
%token LBRACKET                  "["
%token RBRACKET                  "]"
%token UNDERSCORE                "_"
%token DOLLAR                    "$"
%token PLUS                      "+"
%token MINUS                     "-"
%token EXCLAMATION               "!"
%token LPAREN                    "("
%token RPAREN                    ")"
%token COMMA                     ","
%token COLON                     ":"
%token SEMICOLON                 ";"
%token DOT                       "."
%token EQUALS                    "="
%token STAR                      "*"
%token AT                        "@"
%token SLASH                     "/"
%token CARET                     "^"
%token PERCENT                   "%"
%token LBRACE                    "{"
%token RBRACE                    "}"
%token LT                        "<"
%token GT                        ">"
%token BW_AND                    "band"
%token BW_OR                     "bor"
%token BW_XOR                    "bxor"
%token BW_NOT                    "bnot"
%token L_AND                     "land"
%token L_OR                      "lor"
%token L_NOT                     "lnot"

%type <uint32_t>                         qualifiers
%type <AstTypeIdentifier *>              type_id
%type <AstRelationIdentifier *>          rel_id
%type <AstType *>                        type
%type <AstComponent *>                   component component_head component_body
%type <AstComponentType *>               comp_type
%type <AstComponentInit *>               comp_init
%type <AstFunctorDeclaration *>          functor_decl
%type <std::string>                      functor_type
%type <std::string>                      functor_typeargs
%type <AstRelation *>                    attributes non_empty_attributes relation_body
%type <std::vector<AstRelation *>>       relation_list relation_decl
%type <AstArgument *>                    arg
%type <AstAtom *>                        arg_list non_empty_arg_list atom
%type <std::vector<AstAtom*>>            head
%type <RuleBody *>                       literal term disjunction conjunction body
%type <AstClause *>                      fact
%type <AstPragma *>                      pragma
%type <std::vector<AstClause*>>          rule rule_def
%type <AstExecutionOrder *>              exec_order exec_order_list
%type <AstExecutionPlan *>               exec_plan exec_plan_list
%type <AstRecordInit *>                  recordlist
%type <AstUserDefinedFunctor *>          functor_list functor_args
%type <AstRecordType *>                  recordtype
%type <AstUnionType *>                   uniontype
%type <std::vector<AstTypeIdentifier>>   type_params type_param_list
%type <std::string>                      comp_override
%type <AstIO *>                          key_value_pairs non_empty_key_value_pairs iodirective_body
%type <std::vector<AstIO *>>             iodirective_list
%type <std::vector<AstLoad *>>           load_head
%type <std::vector<AstStore *>>          store_head

%destructor { for (auto* cur : $$) { delete cur; } } head rule rule_def iodirective_list load_head store_head relation_list relation_decl
%destructor { delete $$; } body conjunction disjunction literal term
%destructor { delete $$; } rel_id
%destructor { delete $$; } attributes non_empty_attributes

%printer { yyoutput << $$; } <*>;

%precedence AS
%left L_OR
%left L_AND
%left BW_OR
%left BW_XOR
%left BW_AND
%left PLUS MINUS
%left STAR SLASH PERCENT
%precedence BW_NOT L_NOT
%precedence NEG
%right CARET

%%
%start program;

/* Program */
program
  : unit

/* Top-level statement */
unit
  : unit type {
        driver.addType(std::unique_ptr<AstType>($2));
    }
  | unit functor_decl {
        driver.addFunctorDeclaration(std::unique_ptr<AstFunctorDeclaration>($2));
    }
  | unit relation_decl {
        for (auto* cur : $2) {
            driver.addRelation(std::unique_ptr<AstRelation>(cur));
        }
        $2.clear();
    }
  | unit load_head {
        for (auto* cur : $2) {
            driver.addLoad(std::unique_ptr<AstLoad>(cur));
        }
        $2.clear();
    }
  | unit store_head {
        for (auto* cur : $2) {
            driver.addStore(std::unique_ptr<AstStore>(cur));
        }
        $2.clear();
    }
  | unit fact {
        driver.addClause(std::unique_ptr<AstClause>($2));
    }
  | unit rule {
        for (auto* cur : $2) {
            driver.addClause(std::unique_ptr<AstClause>(cur));
        }
        $2.clear();
    }
  | unit component {
        driver.addComponent(std::unique_ptr<AstComponent>($2));
    }
  | unit comp_init {
        driver.addInstantiation(std::unique_ptr<AstComponentInit>($2));
    }
  | unit pragma {
        driver.addPragma(std::unique_ptr<AstPragma>($2));
    }
  | %empty {
    }

/* Pragma directives */
pragma
  : PRAGMA STRING STRING {
        $$ = new AstPragma($2,$3);
        $$->setSrcLoc(@$);
    }

  | PRAGMA STRING {
        $$ = new AstPragma($2, "");
        $$->setSrcLoc(@$);
    }



/* Type Identifier */
type_id
  : IDENT {
        $$ = new AstTypeIdentifier($1);
    }
  | type_id DOT IDENT {
        $$ = $1;
        $$->append($3);
    }

/* Type Declaration */
type
  : NUMBER_TYPE IDENT {
        $$ = new AstPrimitiveType($2, true);
        $$->setSrcLoc(@$);
    }
  | SYMBOL_TYPE IDENT {
        $$ = new AstPrimitiveType($2, false);
        $$->setSrcLoc(@$);
    }
  | TYPE IDENT {
        $$ = new AstPrimitiveType($2);
        $$->setSrcLoc(@$);
    }
  | TYPE IDENT EQUALS uniontype {
        $$ = $4;
        $$->setName($2);
        $$->setSrcLoc(@$);
    }
  | TYPE IDENT EQUALS LBRACKET recordtype RBRACKET {
        $$ = $5;
        $$->setName($2);
        $$->setSrcLoc(@$);
    }
  | TYPE IDENT EQUALS LBRACKET RBRACKET {
        $$ = new AstRecordType();
        $$->setName($2);
        $$->setSrcLoc(@$);
    }

recordtype
  : IDENT COLON type_id {
        $$ = new AstRecordType();
        $$->add($1, *$3); delete $3;
    }
  | recordtype COMMA IDENT COLON type_id {
        $$ = $1;
        $1->add($3, *$5); delete $5;
    }

uniontype
  : type_id {
        $$ = new AstUnionType();
        $$->add(*$1); delete $1;
    }
  | uniontype PIPE type_id {
        $$ = $1;
        $1->add(*$3); delete $3;
    }


/* Relation Identifier */

rel_id
  : IDENT {
        $$ = new AstRelationIdentifier($1);
    }
  | rel_id DOT IDENT {
        $$ = $1;
        $1 = nullptr;
        $$->append($3);
    }


/* Relations */
non_empty_attributes
  : IDENT COLON type_id {
        $$ = new AstRelation();
        auto a = std::make_unique<AstAttribute>($1, *$3);
        a->setSrcLoc(@3);
        $$->addAttribute(std::move(a));
        delete $3;
    }
  | attributes COMMA IDENT COLON type_id {
        $$ = $1;
        $1 = nullptr;
        auto a = std::make_unique<AstAttribute>($3, *$5);
        a->setSrcLoc(@5);
        $$->addAttribute(std::move(a));
        delete $5;
    }

attributes
  : non_empty_attributes {
        $$ = $1;
        $1 = nullptr;
    }
  | %empty {
        $$ = new AstRelation();
    }

qualifiers
  : qualifiers OUTPUT_QUALIFIER {
        if($1 & OUTPUT_RELATION) driver.error(@2, "output qualifier already set");
        $$ = $1 | OUTPUT_RELATION;
    }
  | qualifiers INPUT_QUALIFIER {
        if($1 & INPUT_RELATION) driver.error(@2, "input qualifier already set");
        $$ = $1 | INPUT_RELATION;
    }
  | qualifiers PRINTSIZE_QUALIFIER {
        if($1 & PRINTSIZE_RELATION) driver.error(@2, "printsize qualifier already set");
        $$ = $1 | PRINTSIZE_RELATION;
    }
  | qualifiers OVERRIDABLE_QUALIFIER {
        if($1 & OVERRIDABLE_RELATION) driver.error(@2, "overridable qualifier already set");
        $$ = $1 | OVERRIDABLE_RELATION;
    }
  | qualifiers INLINE_QUALIFIER {
        if($1 & INLINE_RELATION) driver.error(@2, "inline qualifier already set");
        $$ = $1 | INLINE_RELATION;
    }
  | qualifiers BRIE_QUALIFIER {
        if($1 & (BRIE_RELATION|BTREE_RELATION|EQREL_RELATION)) driver.error(@2, "btree/brie/eqrel qualifier already set");
        $$ = $1 | BRIE_RELATION;
    }
  | qualifiers BTREE_QUALIFIER {
        if($1 & (BRIE_RELATION|BTREE_RELATION|EQREL_RELATION)) driver.error(@2, "btree/brie/eqrel qualifier already set");
        $$ = $1 | BTREE_RELATION;
    }
  | qualifiers EQREL_QUALIFIER {
        if($1 & (BRIE_RELATION|BTREE_RELATION|EQREL_RELATION)) driver.error(@2, "btree/brie/eqrel qualifier already set");
        $$ = $1 | EQREL_RELATION;
    }
  | %empty {
        $$ = 0;
    }

functor_decl
  : FUNCTOR IDENT LPAREN functor_typeargs RPAREN COLON functor_type {
        $$ = new AstFunctorDeclaration($2, $4+$7);
        $$->setSrcLoc(@$);
    }
  | FUNCTOR IDENT LPAREN RPAREN COLON functor_type {
        $$ = new AstFunctorDeclaration($2, $6);
        $$->setSrcLoc(@$);
    }
  ;

functor_type
  : IDENT {
      if ($1 == "number") {
          $$ = "N";
      } else if ($1 == "symbol") {
          $$ = "S";
      } else driver.error(@1, "number or symbol identifier expected");
    }
  ;

functor_typeargs
  : functor_type COMMA functor_typeargs { $$ = $1 + $3; }
  | functor_type { $$ = $1;  }
  ;

relation_decl
  : DECL relation_list {
      $$.swap($2);
    }

relation_list
  : relation_body {
      $$.push_back($1);
    }
  | IDENT COMMA relation_list {
      $$.swap($3);
      auto tmp = $$.back()->clone();
      tmp->setName($1);
      tmp->setSrcLoc(@$);
      $$.push_back(tmp);
    }

relation_body
  : IDENT LPAREN attributes RPAREN qualifiers {
        $$ = $3;
        $3 = nullptr;
        $$->setName($1);
        $$->setQualifier($5);
        $$->setSrcLoc(@$);
    }

non_empty_key_value_pairs
  : IDENT EQUALS STRING {
        $$ = new AstIO();
        $$->addKVP($1, $3);
    }
  | key_value_pairs COMMA IDENT EQUALS STRING {
        $$ = $1;
        $$->addKVP($3, $5);
    }
  | IDENT EQUALS IDENT {
        $$ = new AstIO();
        $$->addKVP($1, $3);
    }
  | key_value_pairs COMMA IDENT EQUALS IDENT {
        $$ = $1;
        $$->addKVP($3, $5);
    }
  | IDENT EQUALS TRUE {
        $$ = new AstIO();
        $$->addKVP($1, "true");
    }
  | key_value_pairs COMMA IDENT EQUALS TRUE {
        $$ = $1;
        $$->addKVP($3, "true");
    }
  | IDENT EQUALS FALSE {
        $$ = new AstIO();
        $$->addKVP($1, "false");
    }
  | key_value_pairs COMMA IDENT EQUALS FALSE {
        $$ = $1;
        $$->addKVP($3, "false");
    }

key_value_pairs
  : non_empty_key_value_pairs {
        $$ = $1;
    }
  | %empty {
        $$ = new AstIO();
        $$->setSrcLoc(@$);
    }

load_head
  : INPUT_DECL iodirective_list {
      for (auto* cur : $2) {
          $$.push_back(new AstLoad(*cur));
          delete cur;
      }
      $2.clear();
    }
store_head
  : OUTPUT_DECL iodirective_list {
      for (auto* cur : $2) {
          $$.push_back(new AstStore(*cur));
          delete cur;
      }
      $2.clear();
    }
  | PRINTSIZE_DECL iodirective_list {
      for (auto* cur : $2) {
          $$.push_back(new AstPrintSize(*cur));
          delete cur;
      }
      $2.clear();
    }

iodirective_list
  : iodirective_body {
      $$.push_back($1);
    }
  | IDENT COMMA iodirective_list {
      $$.swap($3);
      auto tmp = $$.back()->clone();
      tmp->setName($1);
      tmp->setSrcLoc(@1);
      $$.push_back(tmp);
    }

iodirective_body
  : rel_id LPAREN key_value_pairs RPAREN {
        $$ = $3;
        $3 = nullptr;
        $$->addName(*$1);
        $$->setSrcLoc(@1);
    }
  | rel_id {
        $$ = new AstIO();
        $$->setName(*$1);
        $$->setSrcLoc(@1);
    }

arg
  : STRING {
        $$ = new AstStringConstant(driver.getSymbolTable(), $1);
        $$->setSrcLoc(@$);
    }
  | UNDERSCORE {
        $$ = new AstUnnamedVariable();
        $$->setSrcLoc(@$);
    }
  | DOLLAR {
        $$ = new AstCounter();
        $$->setSrcLoc(@$);
    }
  | AT IDENT functor_list {
        $$ = $3;
        $3->setName($2);
        $$->setSrcLoc(@$);
    }
  | IDENT {
        $$ = new AstVariable($1);
        $$->setSrcLoc(@$);
    }
  | NUMBER {
        $$ = new AstNumberConstant($1);
        $$->setSrcLoc(@$);
    }
  | LPAREN arg RPAREN {
        $$ = $2;
    }
  | arg BW_OR arg {
        $$ = new AstIntrinsicFunctor(FunctorOp::BOR, std::unique_ptr<AstArgument>($1), std::unique_ptr<AstArgument>($3));
        $$->setSrcLoc(@$);
    }
  | arg BW_XOR arg {
        $$ = new AstIntrinsicFunctor(FunctorOp::BXOR, std::unique_ptr<AstArgument>($1), std::unique_ptr<AstArgument>($3));
        $$->setSrcLoc(@$);
    }
  | arg BW_AND arg {
        $$ = new AstIntrinsicFunctor(FunctorOp::BAND, std::unique_ptr<AstArgument>($1), std::unique_ptr<AstArgument>($3));
        $$->setSrcLoc(@$);
    }
  | arg L_OR arg {
        $$ = new AstIntrinsicFunctor(FunctorOp::LOR, std::unique_ptr<AstArgument>($1), std::unique_ptr<AstArgument>($3));
        $$->setSrcLoc(@$);
    }
  | arg L_AND arg {
        $$ = new AstIntrinsicFunctor(FunctorOp::LAND, std::unique_ptr<AstArgument>($1), std::unique_ptr<AstArgument>($3));
        $$->setSrcLoc(@$);
    }
  | arg PLUS arg {
        $$ = new AstIntrinsicFunctor(FunctorOp::ADD, std::unique_ptr<AstArgument>($1), std::unique_ptr<AstArgument>($3));
        $$->setSrcLoc(@$);
    }
  | arg MINUS arg {
        $$ = new AstIntrinsicFunctor(FunctorOp::SUB, std::unique_ptr<AstArgument>($1), std::unique_ptr<AstArgument>($3));
        $$->setSrcLoc(@$);
    }
  | arg STAR arg {
        $$ = new AstIntrinsicFunctor(FunctorOp::MUL, std::unique_ptr<AstArgument>($1), std::unique_ptr<AstArgument>($3));
        $$->setSrcLoc(@$);
    }
  | arg SLASH arg {
        $$ = new AstIntrinsicFunctor(FunctorOp::DIV, std::unique_ptr<AstArgument>($1), std::unique_ptr<AstArgument>($3));
        $$->setSrcLoc(@$);
    }
  | arg PERCENT arg {
        $$ = new AstIntrinsicFunctor(FunctorOp::MOD, std::unique_ptr<AstArgument>($1), std::unique_ptr<AstArgument>($3));
        $$->setSrcLoc(@$);
    }
  | arg CARET arg {
        $$ = new AstIntrinsicFunctor(FunctorOp::EXP, std::unique_ptr<AstArgument>($1), std::unique_ptr<AstArgument>($3));
        $$->setSrcLoc(@$);
    }
  | MAX LPAREN arg COMMA non_empty_arg_list RPAREN {
        std::vector<std::unique_ptr<AstArgument>> args;
        args.emplace_back($3);
        for (auto* arg : $5->getArguments()) {
            args.emplace_back(arg->clone());
        }
        delete $5;
        $$ = new AstIntrinsicFunctor(FunctorOp::MAX, std::move(args));
        $$->setSrcLoc(@$);
    }
  | MIN LPAREN arg COMMA non_empty_arg_list RPAREN {
        std::vector<std::unique_ptr<AstArgument>> args;
        args.emplace_back($3);
        for (auto* arg : $5->getArguments()) {
            args.emplace_back(arg->clone());
        }
        delete $5;
        $$ = new AstIntrinsicFunctor(FunctorOp::MIN, std::move(args));
        $$->setSrcLoc(@$);
    }
  | CAT LPAREN arg COMMA non_empty_arg_list RPAREN {
        std::vector<std::unique_ptr<AstArgument>> args;
        args.emplace_back($3);
        for (auto* arg : $5->getArguments()) {
            args.emplace_back(arg->clone());
        }
        delete $5;
        $$ = new AstIntrinsicFunctor(FunctorOp::CAT, std::move(args));
        $$->setSrcLoc(@$);
    }
  | ORD LPAREN arg RPAREN {
        $$ = new AstIntrinsicFunctor(FunctorOp::ORD, std::unique_ptr<AstArgument>($3));
        $$->setSrcLoc(@$);
    }
  | STRLEN LPAREN arg RPAREN {
        $$ = new AstIntrinsicFunctor(FunctorOp::STRLEN, std::unique_ptr<AstArgument>($3));
        $$->setSrcLoc(@$);
    }
  | TONUMBER LPAREN arg RPAREN {
        $$ = new AstIntrinsicFunctor(FunctorOp::TONUMBER, std::unique_ptr<AstArgument>($3));
        $$->setSrcLoc(@$);
    }
  | TOSTRING LPAREN arg RPAREN {
        $$ = new AstIntrinsicFunctor(FunctorOp::TOSTRING, std::unique_ptr<AstArgument>($3));
        $$->setSrcLoc(@$);
    }
  | SUBSTR LPAREN arg COMMA arg COMMA arg RPAREN {
        $$ = new AstIntrinsicFunctor(FunctorOp::SUBSTR,
                std::unique_ptr<AstArgument>($3),
                std::unique_ptr<AstArgument>($5),
                std::unique_ptr<AstArgument>($7));
        $$->setSrcLoc(@$);
    }
  | arg AS IDENT {
        $$ = new AstTypeCast(std::unique_ptr<AstArgument>($1), $3);
        $$->setSrcLoc(@$);
    }
  | MINUS arg %prec NEG {
        std::unique_ptr<AstArgument> arg;
        if (const AstNumberConstant* original = dynamic_cast<const AstNumberConstant*>($2)) {
            $$ = new AstNumberConstant(-1*original->getIndex());
            $$->setSrcLoc($2->getSrcLoc());
            delete $2;
        } else {
            $$ = new AstIntrinsicFunctor(FunctorOp::NEG, std::unique_ptr<AstArgument>($2));
            $$->setSrcLoc(@$);
        }
    }
  | BW_NOT arg {
        $$ = new AstIntrinsicFunctor(FunctorOp::BNOT, std::unique_ptr<AstArgument>($2));
        $$->setSrcLoc(@$);
    }
  | L_NOT arg {
        $$ = new AstIntrinsicFunctor(FunctorOp::LNOT, std::unique_ptr<AstArgument>($2));
        $$->setSrcLoc(@$);
    }
  | LBRACKET RBRACKET {
        $$ = new AstRecordInit();
        $$->setSrcLoc(@$);
    }
  | LBRACKET recordlist RBRACKET {
        $$ = $2;
        $$->setSrcLoc(@$);
    }
  | NIL {
        $$ = new AstNullConstant();
        $$->setSrcLoc(@$);
    }
  | COUNT COLON atom {
        auto res = new AstAggregator(AstAggregator::count);
        res->addBodyLiteral(std::unique_ptr<AstLiteral>($3));
        $$ = res;
        $$->setSrcLoc(@$);
    }
  | COUNT COLON LBRACE body RBRACE {
        auto res = new AstAggregator(AstAggregator::count);
        auto bodies = $4->toClauseBodies();
        if (bodies.size() != 1) {
            std::cerr << "ERROR: currently not supporting non-conjunctive aggregation clauses!";
            exit(1);
        }
        for (const auto& cur : bodies[0]->getBodyLiterals()) {
            res->addBodyLiteral(std::unique_ptr<AstLiteral>(cur->clone()));
        }
        delete bodies[0];
        $$ = res;
        $$->setSrcLoc(@$);
    }
  | SUM arg COLON atom {
        auto res = new AstAggregator(AstAggregator::sum);
        res->setTargetExpression(std::unique_ptr<AstArgument>($2));
        res->addBodyLiteral(std::unique_ptr<AstLiteral>($4));
        $$ = res;
        $$->setSrcLoc(@$);
    }
  | SUM arg COLON LBRACE body RBRACE {
        auto res = new AstAggregator(AstAggregator::sum);
        res->setTargetExpression(std::unique_ptr<AstArgument>($2));
        auto bodies = $5->toClauseBodies();
        if (bodies.size() != 1) {
            std::cerr << "ERROR: currently not supporting non-conjunctive aggregation clauses!";
            exit(1);
        }
        for (const auto& cur : bodies[0]->getBodyLiterals()) {
	    res->addBodyLiteral(std::unique_ptr<AstLiteral>(cur->clone()));
        }
        delete bodies[0];
        $$ = res;
        $$->setSrcLoc(@$);
    }
  | MIN arg COLON atom {
        auto res = new AstAggregator(AstAggregator::min);
        res->setTargetExpression(std::unique_ptr<AstArgument>($2));
        res->addBodyLiteral(std::unique_ptr<AstLiteral>($4));
        $$ = res;
        $$->setSrcLoc(@$);
    }
  | MIN arg COLON LBRACE body RBRACE {
        auto res = new AstAggregator(AstAggregator::min);
        res->setTargetExpression(std::unique_ptr<AstArgument>($2));
        auto bodies = $5->toClauseBodies();
        if (bodies.size() != 1) {
            std::cerr << "ERROR: currently not supporting non-conjunctive aggregation clauses!";
            exit(1);
        }
        for (const auto& cur : bodies[0]->getBodyLiterals()) {
            res->addBodyLiteral(std::unique_ptr<AstLiteral>(cur->clone()));
        }
        delete bodies[0];
        $$ = res;
        $$->setSrcLoc(@$);
    }
  | MAX arg COLON atom {
        auto res = new AstAggregator(AstAggregator::max);
        res->setTargetExpression(std::unique_ptr<AstArgument>($2));
        res->addBodyLiteral(std::unique_ptr<AstLiteral>($4));
        $$ = res;
        $$->setSrcLoc(@$);
    }
  | MAX arg COLON LBRACE body RBRACE {
        auto res = new AstAggregator(AstAggregator::max);
        res->setTargetExpression(std::unique_ptr<AstArgument>($2));
        auto bodies = $5->toClauseBodies();
        if (bodies.size() != 1) {
            std::cerr << "ERROR: currently not supporting non-conjunctive aggregation clauses!";
            exit(1);
        }
        for (const auto& cur : bodies[0]->getBodyLiterals()) {
            res->addBodyLiteral(std::unique_ptr<AstLiteral>(cur->clone()));
        }
        delete bodies[0];
        $$ = res;
        $$->setSrcLoc(@$);
    }
  | RESERVED LPAREN arg RPAREN {
        std::cerr << "ERROR: '" << $1 << "' is a keyword reserved for future implementation!" << std::endl;
        exit(1);
    }

functor_list
  : LPAREN RPAREN {
        $$ = new AstUserDefinedFunctor();
    }
  | LPAREN functor_args RPAREN {
        $$ = $2;
    }
  ;

functor_args
  : arg {
        $$ = new AstUserDefinedFunctor();
        $$->add(std::unique_ptr<AstArgument>($1));
    }
  | functor_args COMMA arg {
        $$ = $1;
        $$->add(std::unique_ptr<AstArgument>($3));
    }
  ;

recordlist
  : arg {
        $$ = new AstRecordInit();
        $$->add(std::unique_ptr<AstArgument>($1));
    }
  | recordlist COMMA arg {
        $$ = $1;
        $$->add(std::unique_ptr<AstArgument>($3));
    }

non_empty_arg_list
  : arg {
        $$ = new AstAtom();
        $$->addArgument(std::unique_ptr<AstArgument>($1));
    }
  | arg_list COMMA arg {
        $$ = $1;
        $$->addArgument(std::unique_ptr<AstArgument>($3));
    }

arg_list
  : non_empty_arg_list {
        $$ = $1;
    }
  | %empty {
        $$ = new AstAtom();
    }

atom
  : rel_id LPAREN arg_list RPAREN {
        $$ = $3;
        $3 = nullptr;
        $$->setName(*$1);
        $$->setSrcLoc(@$);
    }

/* Literal */
literal
  : arg RELOP arg {
        auto* res = new AstBinaryConstraint($2, std::unique_ptr<AstArgument>($1), std::unique_ptr<AstArgument>($3));
        res->setSrcLoc(@$);
        $$ = new RuleBody(RuleBody::constraint(res));
    }
  | arg LT arg {
        auto* res = new AstBinaryConstraint(BinaryConstraintOp::LT, std::unique_ptr<AstArgument>($1), std::unique_ptr<AstArgument>($3));
        res->setSrcLoc(@$);
        $$ = new RuleBody(RuleBody::constraint(res));
    }
  | arg GT arg {
        auto* res = new AstBinaryConstraint(BinaryConstraintOp::GT, std::unique_ptr<AstArgument>($1), std::unique_ptr<AstArgument>($3));
        res->setSrcLoc(@$);
        $$ = new RuleBody(RuleBody::constraint(res));
    }
  | arg EQUALS arg {
        auto* res = new AstBinaryConstraint(BinaryConstraintOp::EQ, std::unique_ptr<AstArgument>($1), std::unique_ptr<AstArgument>($3));
        res->setSrcLoc(@$);
        $$ = new RuleBody(RuleBody::constraint(res));
    }
  | atom {
        $1->setSrcLoc(@$);
        $$ = new RuleBody(RuleBody::atom($1));
    }
  | TMATCH LPAREN arg COMMA arg RPAREN {
        auto* res = new AstBinaryConstraint(BinaryConstraintOp::MATCH, std::unique_ptr<AstArgument>($3), std::unique_ptr<AstArgument>($5));
        res->setSrcLoc(@$);
        $$ = new RuleBody(RuleBody::constraint(res));
    }
  | TCONTAINS LPAREN arg COMMA arg RPAREN {
        auto* res = new AstBinaryConstraint(BinaryConstraintOp::CONTAINS, std::unique_ptr<AstArgument>($3), std::unique_ptr<AstArgument>($5));
        res->setSrcLoc(@$);
        $$ = new RuleBody(RuleBody::constraint(res));
    }
  | TRUE {
        auto* res = new AstBooleanConstraint(true);
        res->setSrcLoc(@$);
        $$ = new RuleBody(RuleBody::constraint(res));
    }
  | FALSE {
        auto* res = new AstBooleanConstraint(false);
        res->setSrcLoc(@$);
        $$ = new RuleBody(RuleBody::constraint(res));
    }

/* Fact */
fact
  : atom DOT {
        $$ = new AstClause();
        $$->setHead(std::unique_ptr<AstAtom>($1));
        $$->setSrcLoc(@$);
    }

/* Head */
head
  : atom {
        $$.push_back($1);
    }
  | head COMMA atom {
        $$.swap($1);
        $$.push_back($3);
    }

/* Term */
term
  : literal {
        $$ = $1;
        $1 = nullptr;
    }
  | EXCLAMATION term {
        $$ = $2;
        $2 = nullptr;
        $$->negate();
    }
  | LPAREN disjunction RPAREN {
        $$ = $2;
        $2 = nullptr;
    }

/* Conjunction */
conjunction
  : term {
        $$ = $1;
        $1 = nullptr;
    }
  | conjunction COMMA term {
        $$ = $1;
        $1 = nullptr;
        $$->conjunct(std::move(*$3));
    }

/* Disjunction */
disjunction
  : conjunction {
        $$ = $1;
        $1 = nullptr;
    }
  | disjunction SEMICOLON conjunction {
        $$ = $1;
        $1 = nullptr;
        $$->disjunct(std::move(*$3));
    }

/* Body */
body
  : disjunction {
        $$ = $1;
        $1 = nullptr;
    }

/* execution order list */
exec_order_list
  : NUMBER {
        $$ = new AstExecutionOrder();
        $$->appendAtomIndex($1);
    }
  | exec_order_list COMMA NUMBER {
        $$ = $1;
        $$->appendAtomIndex($3);
    }

/* execution order */
exec_order
  : LPAREN exec_order_list RPAREN {
        $$ = $2;
        $$->setSrcLoc(@$);
    }

/* execution plan list */
exec_plan_list
  : NUMBER COLON exec_order {
        $$ = new AstExecutionPlan();
        $$->setOrderFor($1, std::unique_ptr<AstExecutionOrder>($3));
    }
  | exec_plan_list COMMA NUMBER COLON exec_order {
        $$ = $1;
        $$->setOrderFor($3, std::unique_ptr<AstExecutionOrder>($5));
    }

/* execution plan */
exec_plan
  : PLAN exec_plan_list {
        $$ = $2;
        $$->setSrcLoc(@$);
    }

/* Rule Definition */
rule_def
  : head IF body DOT {
        auto bodies = $3->toClauseBodies();
        for (const auto* head : $1) {
            for (const auto* body : bodies) {
                AstClause* cur = body->clone();
                cur->setHead(std::unique_ptr<AstAtom>(head->clone()));
                cur->setSrcLoc(@$);
                cur->setGenerated($1.size() != 1 || bodies.size() != 1);
                $$.push_back(cur);
            }
        }
        for (auto* head : $1) {
            delete head;
        }
        $1.clear();

        for (auto* body : bodies) {
            delete body;
        }
    }

/* Rule */
rule
  : rule_def {
        std::swap($$, $1);
    }
  | rule STRICT {
        std::swap($$, $1);
        for (auto* cur : $$) cur->setFixedExecutionPlan();
    }
  | rule exec_plan {
        std::swap($$, $1);
        for (auto* cur : $$) cur->setExecutionPlan(std::unique_ptr<AstExecutionPlan>($2->clone()));
        delete $2;
    }

/* Type Parameters */

type_param_list
  : IDENT {
        $$.push_back($1);
    }
  | type_param_list COMMA type_id {
        $$ = $1;
        $$.push_back(*$3);
        delete $3;
    }

type_params
  : %empty {
    }
  | LT type_param_list GT {
        $$ = $2;
    }

/* Component type */

comp_type
  : IDENT type_params {
        $$ = new AstComponentType($1,$2);
    }

/* Component */

component_head
  : COMPONENT comp_type {
        $$ = new AstComponent();
        $$->setComponentType(std::unique_ptr<AstComponentType>($2));
    }
  | component_head COLON comp_type {
        $$ = $1;
        $$->addBaseComponent(std::unique_ptr<AstComponentType>($3));
    }
  | component_head COMMA comp_type {
        $$ = $1;
        $$->addBaseComponent(std::unique_ptr<AstComponentType>($3));
    }

component_body
  : component_body type {
        $$ = $1;
        $$->addType(std::unique_ptr<AstType>($2));
    }
  | component_body relation_decl {
        $$ = $1;
        for (auto* cur : $2) {
            $$->addRelation(std::unique_ptr<AstRelation>(cur));
        }
        $2.clear();
    }
  | component_body load_head {
        $$ = $1;
        for (auto* cur : $2) {
            $$->addLoad(std::unique_ptr<AstLoad>(cur));
        }
        $2.clear();
    }
  | component_body store_head {
        $$ = $1;
        for (auto* cur : $2) {
            $$->addStore(std::unique_ptr<AstStore>(cur));
        }
        $2.clear();
    }
  | component_body fact {
        $$ = $1;
        $$->addClause(std::unique_ptr<AstClause>($2));
    }
  | component_body rule {
        $$ = $1;
        for (auto* cur : $2) {
            $$->addClause(std::unique_ptr<AstClause>(cur));
        }
        $2.clear();
    }
  | component_body comp_override {
        $$ = $1;
        $$->addOverride($2);
    }
  | component_body component {
        $$ = $1;
        $$->addComponent(std::unique_ptr<AstComponent>($2));
    }
  | component_body comp_init {
        $$ = $1;
        $$->addInstantiation(std::unique_ptr<AstComponentInit>($2));
    }
  | %empty {
        $$ = new AstComponent();
    }

component
  : component_head LBRACE component_body RBRACE {
        $$ = $3;
        $$->setComponentType(std::unique_ptr<AstComponentType>($1->getComponentType()->clone()));
        $$->copyBaseComponents($1);
        delete $1;
        $$->setSrcLoc(@$);
    }

/* Component Instantition */
comp_init
  : INSTANTIATE IDENT EQUALS comp_type {
        $$ = new AstComponentInit();
        $$->setInstanceName($2);
        $$->setComponentType(std::unique_ptr<AstComponentType>($4));
        $$->setSrcLoc(@$);
    }

/* Override rules of a relation */
comp_override
  : OVERRIDE IDENT {
        $$ = $2;
}

%%
void yy::parser::error(const location_type &l, const std::string &m) {
    driver.error(l, m);
}