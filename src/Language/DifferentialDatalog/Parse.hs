{-
Copyright (c) 2018-2020 VMware, Inc.
SPDX-License-Identifier: MIT

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
-}

{-# LANGUAGE FlexibleContexts, RecordWildCards, TupleSections, LambdaCase, ScopedTypeVariables #-}
{-# OPTIONS_GHC -fno-warn-missing-signatures #-}

module Language.DifferentialDatalog.Parse (
    parseDatalogString,
    datalogGrammar,
    exprGrammar,
    reservedNames) where

import Control.Applicative hiding (many,optional,Const)
import Control.Monad.Except
import qualified Control.Monad.State as ST
import Text.Parsec hiding ((<|>))
import Text.Parsec.Expr
import Text.Parsec.Language
import qualified Text.Parsec.Token as T
import qualified Data.Map as M
import Data.Maybe
import Data.List
import Data.List.Extra (groupSort)
import Data.Functor.Identity
import Data.Char
import Numeric
import GHC.Float
--import Debug.Trace

import Language.DifferentialDatalog.Syntax
import Language.DifferentialDatalog.Statement
import Language.DifferentialDatalog.Pos
import Language.DifferentialDatalog.Util
import Language.DifferentialDatalog.Name
import Language.DifferentialDatalog.Ops
import Language.DifferentialDatalog.Error
import {-# SOURCE #-} Language.DifferentialDatalog.Expr

-- parse a string containing a datalog program and produce the intermediate representation
parseDatalogString :: String -> String -> IO DatalogProgram
parseDatalogString program file = do
  case parse datalogGrammar file program of
       Left  e    -> errorWithoutStackTrace $ "Failed to parse input file: " ++ show e
       Right prog -> return prog { progSources = M.singleton file program }

-- The following Rust keywords are declared as Datalog keywords to
-- prevent users from declaring variables with the same names.
rustKeywords =
    [ "abstract", "async", "await", "become", "box"
    , "const", "crate", "do", "dyn", "final", "fn"
    , "impl", "let", "loop", "macro", "match", "mod"
    , "move", "override", "priv", "pub", "ref", "self"
    , "Self", "static", "struct", "super", "trait"
    , "try", "type", "typeof", "unsafe", "unsized"
    , "use", "virtual", "where", "while", "yield"
    ]

-- Datalog keywords
ddlogKeywords =
    [ "_", "Aggregate", "and", "apply", "as", "bigint"
    , "bit", "bool", "break", "continue", "double"
    , "else", "extern", "false", "FlatMap", "float"
    , "for", "function", "if", "import", "in", "input"
    , "Inspect", "multiset", "mut", "not", "or", "output"
    , "relation", "return", "signed", "skip", "stream"
    , "string", "transformer", "true", "typedef", "var"
    ]

reservedOpNames =
    [ ":", "::", "|", "&", "==", "=", ":-", "%", "*"
    , "/", "+", "-", ".", "->", "=>", "<=", "<=>"
    , ">=", "<", ">", "!=", ">>", "<<", "~", "@", "#"
    ]

reservedNames = ddlogKeywords ++ rustKeywords

ccnDef = emptyDef { T.commentStart      = "/*"
                  , T.commentEnd        = "*/"
                  , T.commentLine       = "//"
                  , T.nestedComments    = True
                  , T.identStart        = letter <|> char '_'
                  , T.identLetter       = alphaNum <|> char '_'
                  , T.reservedOpNames   = reservedOpNames
                  , T.reservedNames     = reservedNames
                  , T.opLetter          = oneOf "!:%*-+./=|<>"
                  , T.caseSensitive     = True}

ccnLCDef = ccnDef{T.identStart = lower <|> char '_'}
ccnUCDef = ccnDef{T.identStart = upper}

lexer   = T.makeTokenParser ccnDef
lcLexer = T.makeTokenParser ccnLCDef
ucLexer = T.makeTokenParser ccnUCDef

reservedOp   = T.reservedOp lexer
reserved     = T.reserved lexer
identifier   = T.identifier lexer
lcIdentifier = T.identifier lcLexer
ucIdentifier = T.identifier ucLexer
semiSep      = T.semiSep lexer
--semiSep1     = T.semiSep1 lexer
colon        = T.colon lexer
-- commaSep     = T.commaSep lexer
-- commaSep1    = T.commaSep1 lexer
symbol       = try . T.symbol lexer
semi         = T.semi lexer
comma        = T.comma lexer
braces       = T.braces lexer
parens       = T.parens lexer
angles       = T.angles lexer
brackets     = T.brackets lexer
natural      = T.natural lexer
decimal      = T.decimal lexer
double       = T.float lexer
--integer    = T.integer lexer
whiteSpace   = T.whiteSpace lexer
lexeme       = T.lexeme lexer
dot          = T.dot lexer
stringLit    = T.stringLiteral lexer
--charLit    = T.charLiteral lexer
commaSepEnd p = p `sepEndBy` comma
commaSepEnd1 p = p `sepEndBy1` comma

varIdent     = lcIdentifier <?> "variable name"
targIdent    = identifier   <?> "transformer argument name"
typevarIdent = ucIdentifier <?> "type variable name"
modIdent     = identifier   <?> "module name"
attrIdent    = identifier   <?> "attribute name"

consIdent       = ucScopedIdentifier <?> "constructor name"
relIdent        = ucScopedIdentifier <?> "relation name"
indexIdent      = scopedIdentifier   <?> "index name"
funcIdent       = lcScopedIdentifier <?> "function name"
globalFuncIdent = lcGlobalIdentifier <?> "qualified function name"
transIdent      = ucScopedIdentifier <?> "transformer name"
typeIdent       = scopedIdentifier   <?> "type name"

scopedIdentifier = do
    (intercalate "::") <$> identifier `sepBy1` reservedOp "::"

ucScopedIdentifier = do
    path <- try $ lookAhead $ identifier `sepBy1` reservedOp "::"
    if isUpper $ head $ last path
       then scopedIdentifier
       else unexpected (last path)

lcScopedIdentifier = do
    path <- try $ lookAhead $ identifier `sepBy1` reservedOp "::"
    if isLower (head $ last path) || head (last path) == '_'
       then scopedIdentifier
       else unexpected (last path)

-- Like 'lcScopedIdentifier', but requires explicit module name in the path.
lcGlobalIdentifier = do
    path <- try $ lookAhead $ identifier `sepBy1` reservedOp "::"
    if (isLower (head $ last path) || head (last path) == '_') && length path > 1
       then scopedIdentifier
       else unexpected (last path)

removeTabs = do s <- getInput
                let s' = map (\c -> if c == '\t' then ' ' else c ) s
                setInput s'

data SpecItem = SpImport      Import
              | SpType        TypeDef
              | SpRelation    Relation
              | SpIndex       Index
              | SpRule        Rule
              | SpApply       Apply
              | SpFunc        Function
              | SpTransformer Transformer

instance WithPos SpecItem where
    pos   (SpType         t)   = pos t
    pos   (SpRelation     r)   = pos r
    pos   (SpIndex        i)   = pos i
    pos   (SpRule         r)   = pos r
    pos   (SpApply        a)   = pos a
    pos   (SpFunc         f)   = pos f
    pos   (SpImport       i)   = pos i
    pos   (SpTransformer  t)   = pos t
    atPos (SpType         t) p = SpType        $ atPos t p
    atPos (SpRelation     r) p = SpRelation    $ atPos r p
    atPos (SpIndex        i) p = SpIndex       $ atPos i p
    atPos (SpRule         r) p = SpRule        $ atPos r p
    atPos (SpApply        a) p = SpApply       $ atPos a p
    atPos (SpFunc         f) p = SpFunc        $ atPos f p
    atPos (SpImport       i) p = SpImport      $ atPos i p
    atPos (SpTransformer  t) p = SpTransformer $ atPos t p


datalogGrammar = removeTabs *> ((optional whiteSpace) *> spec <* eof)
exprGrammar = removeTabs *> ((optional whiteSpace) *> expr <* eof)

spec = do
    items <- concat <$> many decl
    let imports = mapMaybe (\case
                             SpImport i -> Just i
                             _          -> Nothing) items
    let relations = mapMaybe (\case
                               SpRelation r -> Just (name r, r)
                               _            -> Nothing) items
    let indexes = mapMaybe (\case
                             SpIndex i -> Just (name i, i)
                             _         -> Nothing) items
    let types = mapMaybe (\case
                           SpType t -> Just (name t, t)
                           _        -> Nothing) items
    let funcs = mapMaybe (\case
                           SpFunc f -> Just (name f, f)
                           _        -> Nothing) items
    let transformers = mapMaybe (\case
                                 SpTransformer t -> Just (name t, t)
                                 _               -> Nothing) items
    let rules = mapMaybe (\case
                           SpRule r -> Just r
                           _        -> Nothing) items
    let applys = mapMaybe (\case
                           SpApply a -> Just a
                           _         -> Nothing) items
    let program = DatalogProgram { progImports      = imports
                                 , progTypedefs     = M.fromList types
                                 , progFunctions    = M.fromList $ groupSort funcs
                                 , progTransformers = M.fromList transformers
                                 , progRelations    = M.fromList relations
                                 , progIndexes      = M.fromList indexes
                                 , progRules        = rules
                                 , progApplys       = applys
                                 , progSources      = M.empty }
    let res = do uniqNames (Just program) ("Multiple definitions of type " ++) $ map snd types
                 --uniqNames (Just program) ("Multiple definitions of function " ++) $ map snd funcs
                 uniqNames (Just program) ("Multiple definitions of relation " ++) $ map snd relations
                 uniqNames (Just program) ("Multiple definitions of index " ++) $ map snd indexes
                 uniqNames (Just program) ("Multiple definitions of transformer " ++) $ map snd transformers
                 --uniq importAlias (\imp -> "Alias " ++ show (importAlias imp) ++ " used multiple times ") imports
                 uniq (Just program) importModule (\imp -> "Module " ++ show (importModule imp) ++ " is imported multiple times ") imports
                 return program
    case res of
         Left e     -> errorWithoutStackTrace e
         Right prog -> return prog

attributes = many $ reservedOp "#" *> (brackets attribute)
attribute = withPos $ Attribute nopos <$> attrIdent <*> (option eTrue $ reservedOp "=" *>  expr)

decl =  do attrs <- attributes
           items <- (withPosMany $
                         (return . SpImport)         <$> imprt
                     <|> (return . SpType)           <$> typeDef
                     <|> relation
                     <|> (return . SpIndex)          <$> index
                     <|> (return . SpFunc)           <$> func
                     <|> (return . SpTransformer)    <$> transformer
                     <|> (return . SpRule)           <$> rule
                     <|> (return . SpApply)          <$> apply)
                   <|> (map SpRule . convertStatement) <$> parseForStatement
           case items of
                [SpType t] -> return [SpType t{tdefAttrs = attrs}]
                [SpFunc f] -> return [SpFunc f{funcAttrs = attrs}]
                _          -> do
                    when (not $ null attrs) $ fail "#-attributes are currently only supported for type and function declarations"
                    return items

imprt = Import nopos <$ reserved "import" <*> modname <*> (option (ModuleName []) $ reserved "as" *> modname)

modname = ModuleName <$> modIdent `sepBy1` reservedOp "::"

typeDef = (TypeDef nopos []) <$ reserved "typedef" <*> typeIdent <*>
                                (option [] (symbol "<" *> (commaSepEnd $ symbol "'" *> typevarIdent) <* symbol ">")) <*>
                                (Just <$ reservedOp "=" <*> typeSpec)
       <|>
          (TypeDef nopos []) <$ (try $ reserved "extern" *> reserved "type") <*> typeIdent <*>
                                (option [] (symbol "<" *> (commaSepEnd $ symbol "'" *> typevarIdent) <* symbol ">")) <*>
                                (return Nothing)

func = (Function nopos [] <$  (try $ reserved "extern" *> reserved "function")
                         <*> funcIdent
                         <*> (parens $ commaSepEnd farg)
                         <*> (option (TTuple nopos []) (colon *> typeSpecSimple))
                         <*> (return Nothing))
       <|>
       (Function nopos [] <$  reserved "function"
                         <*> funcIdent
                         <*> (parens $ commaSepEnd farg)
                         <*> (option (TTuple nopos []) (colon *> typeSpecSimple))
                         <*> (Just <$> ((reservedOp "=" *> expr) <|> (braces eseq))))

farg = withPos $ (FuncArg nopos) <$> varIdent <*> (colon *> atype)
atype = withPos $ ArgType nopos <$> (option False (True <$ reserved "mut")) <*> typeSpecSimple

transformer = (Transformer nopos True <$  (reserved "extern" *> reserved "transformer")
                                      <*> transIdent
                                      <*> (parens $ commaSepEnd targ)
                                      <*> (reservedOp "->" *> (parens $ commaSepEnd targ)))
              <|>
              (reserved "transformer" *> fail "Only extern trasformers are currently supported")

targ = withPos $ HOField nopos <$> targIdent <*> (colon *> hotypeSpec)

hotypeSpec = withPos $ (HOTypeRelation nopos <$ reserved "relation" <*> (brackets typeSpecSimple))
                       <|>
                       (HOTypeFunction nopos <$ reserved "function" <*> (parens $ commaSepEnd farg) <*> (colon *> typeSpecSimple))

index = withPos $ Index nopos <$ symbol "index" <*> indexIdent <*> parens (commaSepEnd arg) <*>
                  (symbol "on" *> atom False)

rel_semantics =  RelSet      <$ reserved "relation"
             <|> RelStream   <$ reserved "stream"
             <|> RelMultiset <$ reserved "multiset"

relation = do
    (role, mult) <-  (RelInput,)    <$ reserved "input" <*> rel_semantics
                 <|> (RelOutput,)   <$ reserved "output" <*> rel_semantics
                 <|> (RelInternal,) <$> rel_semantics
    p1 <- getPosition
    isref <- option False $ (\_ -> True) <$> reservedOp "&"
    relName <- relIdent
    ((do start <- getPosition
         fields <- parens $ commaSepEnd arg
         pkey <- optionMaybe $ symbol "primary" *> symbol "key" *> key_expr
         end <- getPosition
         let p = (start, end)
         let tspec = TStruct p [Constructor p [] relName fields]
         let tdef = TypeDef nopos [] relName [] $ Just tspec
         let t = if isref
                    then TUser p "Ref" [TUser p relName []]
                    else TUser p relName []
         let rel = Relation nopos role mult relName t pkey
         return [SpType tdef, SpRelation rel])
      <|>
       (do rel <- (\tspec p2 -> let t = if isref then TUser (p1,p2) "Ref" [tspec] else tspec
                                in Relation nopos role mult relName t)
                  <$> (brackets typeSpecSimple) <*> getPosition <*>
                      (optionMaybe $ symbol "primary" *> symbol "key" *> key_expr)
           return [SpRelation rel]))

key_expr = withPos $ KeyExpr nopos <$> (parens varIdent) <*> expr

arg = withPos $ (Field nopos) <$> attributes <*> varIdent <*> (colon *> typeSpecSimple)

parseForStatement = withPos $
                    ForStatement nopos <$ reserved "for"
                                       <*> (symbol "(" *> atom False)
                                       <*> (optionMaybe (reserved "if" *> expr))
                                       <*> (symbol ")" *> statement)

statement = (withPos $
                 parseForStatement
             <|> parseEmptyStatement
             <|> parseIfStatement
             <|> parseMatchStatement
             <|> parseAssignStatement
             <|> parseInsertStatement
             <|> parseBlockStatement)
            <?> "statement"

parseEmptyStatement = EmptyStatement nopos <$ reserved "skip"

parseBlockStatement = BlockStatement nopos <$> (braces $ semiSep statement)

parseIfStatement = IfStatement nopos <$ reserved "if"
                                    <*> (parens expr) <*> statement
                                    <*> (optionMaybe (reserved "else" *> statement))

parseMatchStatement = MatchStatement nopos <$ reserved "match" <*> parens expr
                                          <*> (braces $ (commaSepEnd1 $ (,) <$> pattern <* reservedOp "->" <*> statement))

parseAssignStatement = do e <- try $ do e <- expr
                                        reserved "in"
                                        return e
                          s <- statement
                          return $ AssignStatement nopos e s

parseInsertStatement = InsertStatement nopos (ModuleName []) <$> try (atom True)

apply = Apply nopos (ModuleName [])
              <$  reserved "apply" <*> transIdent
              <*> (parens $ commaSepEnd (relIdent <|> funcIdent))
              <*> (reservedOp "->" *> (parens $ commaSepEnd relIdent))

rule = Rule nopos (ModuleName []) <$>
       (commaSepEnd1 $ atom True) <*>
       (option [] (reservedOp ":-" *> (concat <$> commaSepEnd rulerhs))) <* dot

rulerhs :: ParsecT String () Identity [RuleRHS]
rulerhs =  (do _ <- try $ lookAhead $ (optional $ reserved "not") *> (optional $ try $ varIdent <* reserved "in") *> (optional $ reservedOp "&") *> relIdent *> (symbol "(" <|> symbol "[")
               (\x -> [x]) <$> (RHSLiteral <$> (option True (False <$ reserved "not")) <*> atom False))
          <|> aggregate
          <|> do _ <- try $ lookAhead $ reserved "var" *> varIdent *> reservedOp "=" *> reserved "FlatMap"
                 (\x -> [x]) <$> (RHSFlatMap <$> (reserved "var" *> varIdent) <*>
                                                 (reservedOp "=" *> reserved "FlatMap" *> parens expr))
          <|> (((\x -> [x]) . RHSInspect) <$ reserved "Inspect" <*> expr)
          <|> rhsCondition

-- If expression contains any group-by subexpressions, transform them into
-- separate RHSGroupBy clauses and replace them with fresh variables, e.g.:
-- 'var c = (x,y).group_by(z).count()'
-- becomes
-- 'var __group = (x,y).group_by(z),
--  var c = __group.count()'
rhsCondition = do
    e <- expr
    case ST.runState (runExceptT $ exprFoldM extractGroupBy e) Nothing of
         (Left emsg, _)       -> errorWithoutStackTrace emsg
         (Right e', group_by) ->
               case e' of
                    E (ESet _ (E (EVarDecl _ gvar1)) (E (EVar _ gvar2))) | gvar1 == "__group" && gvar1 == gvar2
                                                                         -> return $ maybeToList group_by
                    _ -> return $ (maybeToList group_by) ++ [RHSCondition e']

extractGroupBy (EApply p (E (EFunc _ [f])) args) | f == "group_by" = do
    checkNoProg (length args == 2) p
                $ "The 'group_by' operator must be invoked with two arguments, e.g., 'expr1.group_by(expr2)' or 'group_by(expr1, expr2)'," ++
                  " but it is invoked with " ++ show (length args)
    let [project, group_by] = args
    previous_groupby_clause <- ST.get
    checkNoProg (isNothing previous_groupby_clause) p
                $ "'group_by' operator can occur at most once in an expression. Previous occurrence: " ++
                  show (pos $ rhsGroupBy $ fromJust previous_groupby_clause)
    ST.put $ Just $ RHSGroupBy "__group" project group_by
    return $ E $ EVar p "__group"
extractGroupBy e  = return $ E e

-- Deprecated Aggregate syntax.
-- TODO: generate warning.
aggregate = do
    _ <- try $ lookAhead $ reserved "var" *> varIdent *> reservedOp "=" *> reserved "Aggregate"
    before_var <- getPosition
    var <- reserved "var" *> varIdent
    after_var <- getPosition
    _ <- reservedOp "=" *> reserved "Aggregate" *> symbol "("
    grp_by <- group_by_expr
    before_func <- getPosition
    f <- comma *> funcIdent
    after_func <- getPosition
    project <- parens expr
    after_project <- getPosition
    _ <- symbol ")"
    end <- getPosition
    return [ RHSGroupBy "__group" project grp_by
           , RHSCondition $ E $ ESet (before_var, end)
                                     (E $ EVarDecl (before_var, after_var) var)
                                     (E $ EApply (before_func, end) (E $ EFunc (before_func, after_func) [f]) [E $ EVar (after_func, after_project) "__group"])]

{- group-by expression: variable or a tuple of variables -}
group_by_expr = withPos
   (     evar
     <|> (eTuple <$> (parens $ commaSepEnd $ withPos evar))
     <?> "variable or tuple")

atom is_head = withPos $ do
       p1 <- getPosition
       binding <- optionMaybe $ try $ varIdent <* reserved "in"
       p2 <- getPosition
       isref <- option False $ (\_ -> True) <$> reservedOp "&"
       p2' <- getPosition
       rname <- relIdent
       val <- brackets expr
              <|>
              (withPos $ (E . EStruct nopos rname) <$> (option [] $ parens $ commaSepEnd (namedarg <|> anonarg)))
       p3 <- getPosition
       let val' = if isref && is_head
                     then E (EApply (p2,p3) (E $ EFunc (p2,p2') ["ref_new"]) [val])
                     else if isref
                          then E (ERef (p2,p3) val)
                          else val
       return $ Atom nopos rname $ maybe val' (\b -> E $ EBinding (p1, p2) b val') binding

anonarg = ((IdentifierWithPos nopos ""),) <$> expr
namedarg = (,) <$> (dot *> posVarIdent) <*> (reservedOp "=" *> expr)

-- Identifier with position
posVarIdent :: ParsecT String () Identity IdentifierWithPos
posVarIdent = withPos $ IdentifierWithPos nopos <$> varIdent

typeSpec = withPos $
            bitType
        <|> signedType
        <|> intType
        <|> doubleType
        <|> floatType
        <|> stringType
        <|> boolType
        <|> structType
        <|> userType
        <|> typeVar
        <|> functionType
        <|> tupleType

typeSpecSimple = withPos $
                  bitType
              <|> signedType
              <|> intType
              <|> doubleType
              <|> floatType
              <|> stringType
              <|> boolType
              <|> tupleType
              <|> userType
              <|> functionType
              <|> typeVar

bitType    = TBit    nopos <$ reserved "bit" <*> (fromIntegral <$> angles decimal)
signedType = TSigned nopos <$ reserved "signed" <*> (fromIntegral <$> angles decimal)
intType    = TInt    nopos <$ reserved "bigint"
doubleType = TDouble nopos <$ reserved "double"
floatType  = TFloat  nopos <$ reserved "float"
stringType = TString nopos <$ reserved "string"
boolType   = TBool   nopos <$ reserved "bool"
userType   = TUser   nopos <$> typeIdent <*> (option [] $ symbol "<" *> commaSepEnd typeSpec <* symbol ">")
typeVar    = TVar    nopos <$ symbol "'" <*> typevarIdent
structType = TStruct nopos <$ isstruct <*> sepBy1 constructor (reservedOp "|")
    where isstruct = try $ lookAhead $ attributes *> consIdent *> (symbol "{" <|> symbol "|")
tupleType  = (\fs -> case fs of
                          [f] -> f
                          _   -> TTuple nopos fs)
             <$> (parens $ commaSepEnd typeSpecSimple)
functionType =  (TFunction nopos <$ reserved "function" <*> (parens $ commaSepEnd atype) <*> (option (TTuple nopos []) $ colon *> typeSpecSimple))
            <|> (TFunction nopos <$> (symbol "|" *> commaSepEnd atype <* symbol "|") <*> (option (TTuple nopos []) $ colon *> typeSpecSimple))


constructor = withPos $ Constructor nopos <$> attributes
                                          <*> consIdent
                                          <*> (option [] $ braces $ commaSepEnd arg)

expr = buildExpressionParser etable term
    <?> "expression"

term  =  elhs
     <|> (withPos $ eTuple <$> (parens $ commaSepEnd expr))
     <|> braces eseq
     <|> term'
     <?> "expression term"
term' = withPos $
         ebinding
     <|> epholder
     <|> estruct
     <|> enumber
     <|> ebool
     <|> estring
     <|> einterned_string
     <|> efunc
     <|> evar
     <|> ematch
     <|> eite
     <|> efor
     <|> evardcl
     <|> econtinue
     <|> ebreak
     <|> ereturn
     <|> emap_literal
     <|> evec_literal
     <|> eclosure

eclosure =  (E <$> (EClosure nopos <$ reserved "function" <*> (parens $ commaSepEnd closure_arg) <*> (optionMaybe $ colon *> typeSpecSimple) <*> braces eseq))
        <|> (E <$> (EClosure nopos <$> (symbol "|" *> commaSepEnd closure_arg <* symbol "|") <*> (optionMaybe $ colon *> typeSpecSimple) <*> expr))

closure_arg = withPos $ ClosureExprArg nopos <$> varIdent <*> (optionMaybe $ colon *> atype)

-- Semicolon-separated expressions with an optional extra semicolon in the end.
eseq = do
    p1 <- getPosition
    me <- optionMaybe expr
    p2 <- getPosition
    case me of
       Nothing -> return $ E $ ETuple (p1, p2) []
       Just e -> do extra_semi <- optionMaybe semi
                    if isNothing extra_semi
                       then return e
                       else do suffix <- eseq
                               return $ E $ ESeq (fst $ pos e, snd $ pos suffix) e suffix

emap_literal = mkmap <$> (ismap *> (brackets $ commaSepEnd1 $ (,) <$> expr <* reservedOp "->" <*> expr))
    where ismap = try $ lookAhead $ do
                        _ <- symbol "["
                        _ <- expr
                        reservedOp "->"
          mkmap pairs = E $ ESeq p (E $ ESet p (E $ EVarDecl p "__map") (E $ EApply p (E $ EFunc p ["map_empty"]) [])) m
             where
             p = (fst $ pos $ fst $ head pairs, snd $ pos $ snd $ last pairs)
             m = foldr (\(k,v) e -> E $ ESeq (fst $ pos e, snd $ pos v) (E $ EApply (fst $ pos e, snd $ pos v) (E $ EFunc (pos v) ["insert"]) [E $ EVar p "__map", k, v]) e)
                        (E $ EVar p "__map")
                        pairs

evec_literal = mkvec <$> (isvec *> (brackets $ commaSepEnd1 expr))
    where isvec = try $ lookAhead $ do
                        _ <- symbol "["
                        _ <- expr
                        (comma <|> symbol "]")
          mkvec vs = E $ ESeq p (E $ ESet p (E $ EVarDecl p "__vec") (E $ EApply p (E $ EFunc p ["vec_with_capacity"]) [E $ EInt p $ fromIntegral $ length vs])) vec
             where
             p = (fst $ pos $ head vs, snd $ pos $ last vs)
             vec = foldr (\v e -> E $ ESeq (fst $ pos e, snd $ pos v) (E $ EApply (fst $ pos e, snd $ pos v) (E $ EFunc (pos v) ["push"]) [E $ EVar p "__vec", v]) e)
                         (E $ EVar p "__vec")
                         vs

econtinue = (E $ EContinue nopos) <$ reserved "continue"
ebreak    = (E $ EBreak nopos) <$ reserved "break"
ereturn   = (E . EReturn nopos) <$ reserved "return" <*> option (eTuple []) expr

ebinding = eBinding <$ isbinding <*> (varIdent <* reservedOp "@") <*> expr
    where isbinding = try $ lookAhead $ (\_ -> ()) <$> varIdent <* reservedOp "@"

ebool = eBool <$> ((True <$ reserved "true") <|> (False <$ reserved "false"))
evar = eVar <$> varIdent

-- Fully qualified function names ('module::func') are parsed as EFunc.  Other
-- function names are parsed as 'EVar' (as we don't have a way to tell them
-- apart from variables) and later converted to 'EFunc' when flattening the
-- module hierarchy in 'Module.hs'.
efunc = eFunc <$> globalFuncIdent

ematch = eMatch <$ reserved "match" <*> parens expr
               <*> (braces $ (commaSepEnd1 $ (,) <$> pattern <* reservedOp "->" <*> expr))

{- Match pattern -}
pattern = buildExpressionParser petable pterm
      <?> "match pattern"

petable = [[postf postType]]

pterm = (withPos $
           eTuple   <$> (parens $ commaSepEnd pattern)
       <|> E <$> ((EStruct nopos) <$> consIdent <*> (option [] $ braces $ commaSepEnd (namedpat <|> anonpat)))
       <|> (E . EVarDecl nopos) <$> varIdent
       <|> (E . EVarDecl nopos) <$ reserved "var" <*> varIdent
       <|> epholder
       <|> ebool
       <|> epattern_string
       <|> enumber)
      <?> "match term"

anonpat = (IdentifierWithPos nopos "",) <$> pattern
namedpat = (,) <$> (dot *> posVarIdent) <*> (reservedOp "=" *> pattern)

lhs = (withPos $
           eTuple <$> (parens $ commaSepEnd lhs)
       <|> E <$> ((EStruct nopos) <$> consIdent <*> (option [] $ braces $ commaSepEnd $ namedlhs <|> anonlhs))
       <|> fexpr
       <|> evardcl
       <|> epholder)
      <?> "l-expression"
elhs = islhs *> lhs
    where islhs = try $ lookAhead $ lhs *> reservedOp "="

anonlhs = (IdentifierWithPos nopos "",) <$> lhs
namedlhs = (,) <$> (dot *> posVarIdent) <*> (reservedOp "=" *> lhs)

enumber  = lexeme enumber'
estring =   equoted_string
        <|> eraw_string
        <|> einterpolated_raw_string

einterned_string = do
    (e, p) <- try $ do p1 <- getPosition
                       _ <- string "i"
                       p2 <- getPosition
                       e' <- estring
                       return (e', (p1, p2))
    return $ E $ EApply (pos e) (E $ EFunc p ["intern"]) [e]

eraw_string = eString <$> ((try $ string "[|") *> manyTill anyChar (try $ string "|]" *> whiteSpace))

-- A constant string that can be used to pattern match against
epattern_string = do
    estr <- equoted_string <|> eraw_string
    case estr of
         E EString{} -> return estr
         _ -> fail "Non-constant string in pattern expression"

-- Parse interpolated strings, converting them to string concatenation
-- expressions.
-- First, parse as normal string literal;
-- then apply a separate parser to the resulting string to extract
-- interpolated expressions.

equoted_string = do
    p <- try $ lookAhead $ do {_ <- string "\""; getPosition}
    str <- concat <$> many1 stringLit
    case parse (interpolate p) "" str of
         Left  er -> fail $ show er
         Right ex -> return ex

einterpolated_raw_string  = do
    _ <- try $ string "$[|"
    p <- getPosition
    str <- manyTill anyChar (try $ string "|]" *> whiteSpace)
    case parse (interpolate p) "" str of
         Left  er -> fail $ show er
         Right ex -> return ex

interpolate p = do
    setPosition p
    interpolate' Nothing

interpolate' mprefix = do
    str <- withPos $ (E . EString nopos) <$> manyTill anyChar (eof <|> do {_ <- try $ lookAhead $ string "${"; return ()})
    let prefix' = maybe str
                        (\prefix ->
                          if exprString (enode str) == ""
                             then prefix
                             else case prefix of
                                       E EString{} -> str
                                       _           -> E $ EBinOp (fst $ pos prefix, snd $ pos str) Concat prefix str)
                        mprefix
    (do eof
        return prefix'
     <|>
     do e <- string "${" *> whiteSpace *> expr <* char '}'
        p <- getPosition
        interpolate' $ (Just $ E $ EBinOp (fst $ pos prefix', p) Concat prefix' e))

estruct = E <$> ((EStruct nopos) <$> consIdent <*> (option [] $ braces $ commaSepEnd (namedarg <|> anonarg)))

enumber' = (lookAhead $ char '\'' <|> digit) *> (do w <- width
                                                    v <- sradval
                                                    mkNumberLit w v)

-- strip underscores
stripUnder :: String -> String
stripUnder = filter (/= '_')

digitPrefix :: Stream s m Char => ParsecT s u m Char -> ParsecT s u m String
digitPrefix dig = (:) <$> dig <*> (many (dig <|> char '_'))

-- Use 'term' instead of 'expr' for condition, if and then clauses to make
-- parsing unambiguous, e.g., consider the following expression:
-- 'if (x) { y } else { z } ++ if (q) { r } else { t }'.
-- We would like to parse if as:
-- '(if (x) { y } else { z }) ++ (if (q) { r } else { t })', but when parsing
-- the "else" clause as 'expr' instead of 'term', we end up with
-- 'if (x) { y } else ({ z } ++ if (q) { r } else { t })'.
eite = do reserved "if"
          cond <- term
          th   <- term
          p <- getPosition
          el <- option (E $ ETuple (p,p) []) $ (reserved "else" *> term)
          return $ eITE cond th el

efor    = eFor     <$ (reserved "for" *> symbol "(") <*> (varIdent <* reserved "in") <*> (expr <* symbol ")") <*> term
evardcl = (E . EVarDecl nopos) <$ reserved "var" <*> varIdent
epholder = ePHolder <$ reserved "_"

data NumberLiteral = UnsignedNumber Integer
                   | SignedNumber Integer
                   | FloatNumber Double

width = optionMaybe (try $ ((fmap fromIntegral parseDec) <* (lookAhead $ char '\'')))
sradval =  ((try $ string "'b") *> (UnsignedNumber <$> parseBin))
       <|> ((try $ string "'o") *> (UnsignedNumber <$> parseOct))
       <|> ((try $ string "'d") *> (UnsignedNumber <$> parseDec))
       <|> ((try $ string "'h") *> (UnsignedNumber <$> parseHex))
       <|> ((try $ string "'sb") *> (SignedNumber  <$> parseBin))
       <|> ((try $ string "'so") *> (SignedNumber  <$> parseOct))
       <|> ((try $ string "'sd") *> (SignedNumber  <$> parseDec))
       <|> ((try $ string "'sh") *> (SignedNumber  <$> parseHex))
       <|> ((try $ string "'f")  *> (FloatNumber   <$> double))
       <|> (FloatNumber <$> try double)
       <|> (SignedNumber <$> parseDec)
parseBin :: Stream s m Char => ParsecT s u m Integer
parseBin = readBin . stripUnder <$> (digitPrefix $ (char '0') <|> (char '1'))
parseOct :: Stream s m Char => ParsecT s u m Integer
parseOct = (fst . head . readOct . stripUnder) <$> digitPrefix octDigit
parseDec :: Stream s m Char => ParsecT s u m Integer
parseDec = (fst . head . readDec . stripUnder) <$> digitPrefix digit

--parseSDec = (\m v -> m * v)
--            <$> (option 1 ((-1) <$ reservedOp "-"))
--            <*> ((fst . head . readDec) <$> many1 digit)
parseHex :: Stream s m Char => ParsecT s u m Integer
parseHex = (fst . head . readHex . stripUnder) <$> digitPrefix hexDigit

mkNumberLit :: Maybe Int -> NumberLiteral -> ParsecT s u m Expr
mkNumberLit Nothing (SignedNumber v)          = return $ eInt v
mkNumberLit Nothing (UnsignedNumber v)        = return $ eInt v
mkNumberLit Nothing (FloatNumber v)           = return $ eDouble v
mkNumberLit (Just w) _ | w == 0               = fail "Width must be >0"
mkNumberLit (Just w) (SignedNumber v) | msb v < w = return $ eSigned w v
                                      | otherwise = fail "Value exceeds specified width"
mkNumberLit (Just w) (UnsignedNumber v) | msb v < w = return $ eBit w v
                                        | otherwise = fail "Value exceeds specified width"
mkNumberLit (Just w) (FloatNumber v) | w == 32 = return $ eFloat $ double2Float v
                                     | w == 64 = return $ eDouble v
                                     | otherwise = fail "Only 32- and 64-bit floating point values are supported"

etable = [[postf $ choice [postTry, postSlice, postDotApply, postApply, postField, postType, postAs, postTupField]]
         ,[pref  $ choice [preRef]]
         ,[pref  $ choice [prefixOp "-" UMinus]]
         ,[pref  $ choice [prefixOp "~" BNeg]]
         ,[pref  $ choice [prefixKeyWord "not" Not]]
         ,[binary "%" Mod AssocLeft,
           binary "*" Times AssocLeft,
           binary "/" Div AssocLeft]
         ,[binary "+" Plus AssocLeft,
           binary "-" Minus AssocLeft]
         ,[binary ">>" ShiftR AssocLeft,
           binary "<<" ShiftL AssocLeft]
         ,[binary "++" Concat AssocLeft]
         ,[binary "==" Eq  AssocLeft,
           binary "!=" Neq AssocLeft,
           binary "<"  Lt  AssocNone,
           binary "<=" Lte AssocNone,
           binary ">"  Gt  AssocNone,
           binary ">=" Gte AssocNone]
         ,[binary "&" BAnd AssocLeft]
         ,[binary "^" BXor AssocLeft]
         ,[binary "|" BOr AssocLeft]
         ,[binaryKeyWord "and" And AssocLeft]
         ,[binaryKeyWord "or" Or AssocLeft]
         ,[binary "=>" Impl AssocLeft]
         ,[assign AssocNone]
         --,[sbinary ";" ESeq AssocRight]
         ]

pref  p = Prefix  . chainl1 p $ return       (.)
postf p = Postfix . chainl1 p $ return (flip (.))
postField = (\f end e -> E $ EField (fst $ pos e, end) e f) <$> field <*> getPosition
postTupField = (\f end e -> E $ ETupField (fst $ pos e, end) e (fromIntegral f)) <$> tupField <*> getPosition
-- Object-oriented function call notation: 'x.f(y)'.
postDotApply = (\(f, args) end e -> E $ EApply (fst $ pos e, end) f (e:args)) <$> dotcall <*> getPosition
-- Traditional function application: 'f(x,y)'.
postApply = (\args end e -> E $ EApply (fst $ pos e, end) e args) <$> call <*> getPosition
postType = (\t end e -> E $ ETyped (fst $ pos e, end) e t) <$> etype <*> getPosition
postAs = (\t end e -> E $ EAs (fst $ pos e, end) e t) <$> eAsType <*> getPosition
postSlice  = try $ (\(h,l) end e -> E $ ESlice (fst $ pos e, end) e h l) <$> slice <*> getPosition
postTry = (\end e -> E $ ETry (fst $ pos e, end) e) <$ symbol "?" <*> getPosition
slice = brackets $ (\h l -> (fromInteger h, fromInteger l)) <$> natural <*> (colon *> natural)

field = isfield *> dot *> varIdent
    where isfield = try $ lookAhead $ do
                        _ <- dot
                        _ <- notFollowedBy relIdent
                        varIdent
tupField = isfield *> dot *> lexeme decimal
    where isfield = try $ lookAhead $ do
                        _ <- dot
                        _ <- notFollowedBy relIdent
                        lexeme decimal
dotcall = (,) <$ isapply <*> (withPos $ eFunc <$ dot <*> funcIdent) <*> (parens $ commaSepEnd expr)
    where isapply = try $ lookAhead $ do
                        _ <- dot
                        _ <- funcIdent
                        symbol "("

call = iscall *> (parens $ commaSepEnd expr)
    where iscall = try $ lookAhead $ do
                         symbol "("

etype = reservedOp ":" *> typeSpecSimple
eAsType = reserved "as" *> typeSpecSimple

preRef = (\start e -> E $ ERef (start, snd $ pos e) e) <$> getPosition <* reservedOp "&"
prefixOp n fun = (\start e -> E $ EUnOp (start, snd $ pos e) fun e) <$> getPosition <* reservedOp n
prefixKeyWord n fun = (\start e -> E $ EUnOp (start, snd $ pos e) fun e) <$> getPosition <* reserved n
binary n fun  = Infix $ (\le re -> E $ EBinOp (fst $ pos le, snd $ pos re) fun le re) <$ reservedOp n
binaryKeyWord n fun  = Infix $ (\le re -> E $ EBinOp (fst $ pos le, snd $ pos re) fun le re) <$ reserved n
--sbinary n fun = Infix $ (\l  r  -> E $ fun (fst $ pos l, snd $ pos r) l r) <$ reservedOp n
assign = Infix $ (\l r  -> E $ ESet (fst $ pos l, snd $ pos r) l r) <$ reservedOp "="

{- F-expression (variable or field name) -}
fexpr =  buildExpressionParser fetable fterm
    <?> "column or field name"

fterm  =  withPos $ evar

fetable = [[postf postField]]
