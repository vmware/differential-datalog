{-
Copyright (c) 2018 VMware, Inc.
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

{-# LANGUAGE FlexibleContexts, RecordWildCards, TupleSections, LambdaCase #-}

module Language.DifferentialDatalog.Parse (
    parseDatalogString,
    datalogGrammar,
    exprGrammar,
    reservedNames) where

import Control.Applicative hiding (many,optional,Const)
import qualified Control.Exception as E
import Control.Monad.Except
import Text.Parsec hiding ((<|>))
import Text.Parsec.Expr
import Text.Parsec.Language
import qualified Text.Parsec.Token as T
import qualified Data.Map as M
import Data.Maybe
import Data.Either
import Data.List
import Data.Char
import Numeric
import Debug.Trace

import Language.DifferentialDatalog.Syntax
import Language.DifferentialDatalog.Statement
import Language.DifferentialDatalog.Type
import Language.DifferentialDatalog.Pos
import Language.DifferentialDatalog.Util
import Language.DifferentialDatalog.Name
import Language.DifferentialDatalog.Ops

-- parse a string containing a datalog program and produce the intermediate representation
parseDatalogString :: String -> String -> IO DatalogProgram
parseDatalogString program file = do
  case parse datalogGrammar file program of
       Left  e    -> errorWithoutStackTrace $ "Failed to parse input file: " ++ show e
       Right prog -> return prog

-- The following Rust keywords are declared as Datalog keywords to
-- prevent users from declaring variables with the same names.
rustKeywords = ["type", "match"]

reservedOpNames = [":", "|", "&", "==", "=", ":-", "%", "*", "/", "+", "-", ".", "->", "=>", "<=",
                   "<=>", ">=", "<", ">", "!=", ">>", "<<", "~", "@", "#"]
reservedNames = ["as",
                 "apply",
                 "_",
                 "Aggregate",
                 "FlatMap",
                 "and",
                 "bit",
                 "bool",
                 "default",
                 "extern",
                 "else",
                 "false",
                 "for",
                 "function",
                 "if",
                 "import",
                 "in",
                 "mut",
                 "input",
                 "output",
                 "bigint",
                 "not",
                 "or",
                 "relation",
                 "signed",
                 "skip",
                 "string",
                 "transformer",
                 "true",
                 "typedef",
                 "var"] ++ rustKeywords


ccnDef = emptyDef { T.commentStart      = "/*"
                  , T.commentEnd        = "*/"
                  , T.commentLine       = "//"
                  , T.nestedComments    = True
                  , T.identStart        = letter <|> char '_'
                  , T.identLetter       = alphaNum <|> char '_'
                  , T.reservedOpNames   = reservedOpNames
                  , T.reservedNames     = reservedNames
                  , T.opLetter          = oneOf "!?:%*-+./=|<>"
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
--semiSep1   = T.semiSep1 lexer
colon        = T.colon lexer
commaSep     = T.commaSep lexer
commaSep1    = T.commaSep1 lexer
symbol       = T.symbol lexer
semi         = T.semi lexer
comma        = T.comma lexer
braces       = T.braces lexer
parens       = T.parens lexer
angles       = T.angles lexer
brackets     = T.brackets lexer
natural      = T.natural lexer
decimal      = T.decimal lexer
--integer    = T.integer lexer
whiteSpace   = T.whiteSpace lexer
lexeme       = T.lexeme lexer
dot          = T.dot lexer
stringLit    = T.stringLiteral lexer
--charLit    = T.charLiteral lexer

varIdent     = lcIdentifier <?> "variable name"
targIdent    = identifier   <?> "transformer argument name"
typevarIdent = ucIdentifier <?> "type variable name"
modIdent     = identifier   <?> "module name"
attrIdent    = identifier   <?> "attribute name"

consIdent    = ucScopedIdentifier <?> "constructor name"
relIdent     = ucScopedIdentifier <?> "relation name"
funcIdent    = lcScopedIdentifier <?> "function name"
transIdent   = ucScopedIdentifier <?> "transformer name"
typeIdent    = scopedIdentifier   <?> "type name"

scopedIdentifier = do
    (intercalate ".") <$> identifier `sepBy1` char '.'

ucScopedIdentifier = do
    path <- try $ lookAhead $ identifier `sepBy1` char '.'
    if isUpper $ head $ last path
       then scopedIdentifier
       else unexpected (last path)

lcScopedIdentifier = do
    path <- try $ lookAhead $ identifier `sepBy1` char '.'
    if isLower (head $ last path) || head (last path) == '_'
       then scopedIdentifier
       else unexpected (last path)

removeTabs = do s <- getInput
                let s' = map (\c -> if c == '\t' then ' ' else c ) s
                setInput s'

data SpecItem = SpImport      Import
              | SpType        TypeDef
              | SpRelation    Relation
              | SpRule        Rule
              | SpApply       Apply
              | SpFunc        Function
              | SpTransformer Transformer

instance WithPos SpecItem where
    pos   (SpType         t)   = pos t
    pos   (SpRelation     r)   = pos r
    pos   (SpRule         r)   = pos r
    pos   (SpApply        a)   = pos a
    pos   (SpFunc         f)   = pos f
    pos   (SpImport       i)   = pos i
    pos   (SpTransformer  t)   = pos t
    atPos (SpType         t) p = SpType        $ atPos t p
    atPos (SpRelation     r) p = SpRelation    $ atPos r p
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
    let res = do uniqNames ("Multiple definitions of type " ++) $ map snd types
                 uniqNames ("Multiple definitions of function " ++) $ map snd funcs
                 uniqNames ("Multiple definitions of relation " ++) $ map snd relations
                 uniqNames ("Multiple definitions of transformer " ++) $ map snd transformers
                 --uniq importAlias (\imp -> "Alias " ++ show (importAlias imp) ++ " used multiple times ") imports
                 uniq importModule (\imp -> "Module " ++ show (importModule imp) ++ " is imported multiple times ") imports
                 return $ DatalogProgram { progImports      = imports
                                         , progTypedefs     = M.fromList types
                                         , progFunctions    = M.fromList funcs
                                         , progTransformers = M.fromList transformers
                                         , progRelations    = M.fromList relations
                                         , progRules        = rules
                                         , progApplys       = applys}
    case res of
         Left err   -> errorWithoutStackTrace err
         Right prog -> return prog

attributes = reservedOp "#" *> (brackets $ many attribute)
attribute = withPos $ Attribute nopos <$> attrIdent <*> (reservedOp "=" *>  expr)

decl =  do attributes <- optionMaybe attributes
           items <- (withPosMany $
                         (return . SpImport)         <$> imprt
                     <|> (return . SpType)           <$> typeDef
                     <|> relation
                     <|> (return . SpFunc)           <$> func
                     <|> (return . SpTransformer)    <$> transformer
                     <|> (return . SpRule)           <$> rule
                     <|> (return . SpApply)          <$> apply)
                   <|> (map SpRule . convertStatement) <$> parseForStatement
           case items of
                [SpType t] -> return [SpType t{tdefAttrs = maybe [] id attributes}]
                _          -> do
                    when (isJust attributes) $ fail "#-attributes are currently only supported for type declarations"
                    return items

imprt = Import nopos <$ reserved "import" <*> modname <*> (option (ModuleName []) $ reserved "as" *> modname)

modname = ModuleName <$> modIdent `sepBy1` reservedOp "."

typeDef = (TypeDef nopos []) <$ reserved "typedef" <*> typeIdent <*>
                                (option [] (symbol "<" *> (commaSep $ symbol "'" *> typevarIdent) <* symbol ">")) <*>
                                (Just <$ reservedOp "=" <*> typeSpec)
       <|>
          (TypeDef nopos []) <$ (try $ reserved "extern" *> reserved "type") <*> typeIdent <*>
                                (option [] (symbol "<" *> (commaSep $ symbol "'" *> typevarIdent) <* symbol ">")) <*>
                                (return Nothing)

func = (Function nopos <$  (try $ reserved "extern" *> reserved "function")
                       <*> funcIdent
                       <*> (parens $ commaSep farg)
                       <*> (colon *> typeSpecSimple)
                       <*> (return Nothing))
       <|>
       (Function nopos <$  reserved "function"
                       <*> funcIdent
                       <*> (parens $ commaSep farg)
                       <*> (colon *> typeSpecSimple)
                       <*> (Just <$ reservedOp "=" <*> expr))

farg = withPos $ (FuncArg nopos) <$> varIdent <*> (colon *> option False (True <$ reserved "mut")) <*> typeSpecSimple

transformer = (Transformer nopos True <$  (reserved "extern" *> reserved "transformer")
                                      <*> transIdent
                                      <*> (parens $ commaSep targ)
                                      <*> (reservedOp "->" *> (parens $ commaSep targ)))
              <|>
              (reserved "transformer" *> fail "Only extern trasformers are currently supported")

targ = withPos $ HOField nopos <$> targIdent <*> (colon *> hotypeSpec)

hotypeSpec = withPos $ (HOTypeRelation nopos <$ reserved "relation" <*> (brackets typeSpecSimple))
                       <|>
                       (HOTypeFunction nopos <$ reserved "function" <*> (parens $ commaSep farg) <*> (colon *> typeSpecSimple))

relation = do
    role <-  RelInput    <$ reserved "input" <* reserved "relation"
         <|> RelOutput   <$ reserved "output" <* reserved "relation"
         <|> RelInternal <$ reserved "relation"
    p1 <- getPosition
    isref <- option False $ (\_ -> True) <$> reservedOp "&"
    relName <- relIdent
    ((do start <- getPosition
         fields <- parens $ commaSep arg
         pkey <- optionMaybe $ symbol "primary" *> symbol "key" *> key_expr
         end <- getPosition
         let p = (start, end)
         let tspec = TStruct p [Constructor p relName fields]
         let tdef = TypeDef nopos [] relName [] $ Just tspec
         let t = if isref
                    then TUser p "Ref" [TUser p relName []]
                    else TUser p relName []
         let rel = Relation nopos role relName t pkey
         return [SpType tdef, SpRelation rel])
      <|>
       (do rel <- (\tspec p2 -> let t = if isref then TUser (p1,p2) "Ref" [tspec] else tspec
                                in Relation nopos role relName t)
                  <$> (brackets typeSpecSimple) <*> getPosition <*>
                      (optionMaybe $ symbol "primary" *> symbol "key" *> key_expr)
           return [SpRelation rel]))

key_expr = withPos $ KeyExpr nopos <$> (parens varIdent) <*> expr

arg = withPos $ (Field nopos) <$> varIdent <*> (colon *> typeSpecSimple)

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
                                          <*> (braces $ (commaSep1 $ (,) <$> pattern <* reservedOp "->" <*> statement))

parseAssignStatement = do e <- try $ do e <- expr
                                        reserved "in"
                                        return e
                          s <- statement
                          return $ AssignStatement nopos e s

parseInsertStatement = InsertStatement nopos <$> try (atom True)

apply = Apply nopos <$  reserved "apply" <*> transIdent
                    <*> (parens $ commaSep (relIdent <|> funcIdent))
                    <*> (reservedOp "->" *> (parens $ commaSep relIdent))

rule = Rule nopos <$>
       (commaSep1 $ atom True) <*>
       (option [] (reservedOp ":-" *> commaSep rulerhs)) <* dot

rulerhs =  do _ <- try $ lookAhead $ (optional $ reserved "not") *> (optional $ try $ varIdent <* reservedOp "in") *> (optional $ reservedOp "&") *> relIdent *> (symbol "(" <|> symbol "[")
              RHSLiteral <$> (option True (False <$ reserved "not")) <*> atom False
       <|> do _ <- try $ lookAhead $ reserved "var" *> varIdent *> reservedOp "=" *> reserved "Aggregate"
              RHSAggregate <$> (reserved "var" *> varIdent) <*>
                               (reservedOp "=" *> reserved "Aggregate" *> symbol "(" *> (parens $ commaSep varIdent)) <*>
                               (comma *> funcIdent) <*>
                               (parens expr <* symbol ")")
       <|> do _ <- try $ lookAhead $ reserved "var" *> varIdent *> reservedOp "=" *> reserved "FlatMap"
              RHSFlatMap <$> (reserved "var" *> varIdent) <*>
                             (reservedOp "=" *> reserved "FlatMap" *> parens expr)
       <|> (RHSCondition <$> expr)

atom is_head = withPos $ do
       p1 <- getPosition
       binding <- optionMaybe $ try $ varIdent <* reservedOp "in"
       p2 <- getPosition
       isref <- option False $ (\_ -> True) <$> reservedOp "&"
       rname <- relIdent
       val <- brackets expr
              <|>
              (withPos $ eStruct rname <$> (option [] $ parens $ commaSep (namedarg <|> anonarg)))
       p3 <- getPosition
       let val' = if isref && is_head
                     then E (EApply (p2,p3) "ref_new" [val])
                     else if isref
                          then E (ERef (p2,p3) val)
                          else val
       return $ Atom nopos rname $ maybe val' (\b -> E $ EBinding (p1, p2) b val') binding

anonarg = ("",) <$> expr
namedarg = (,) <$> (dot *> varIdent) <*> (reservedOp "=" *> expr)

typeSpec = withPos $
            bitType
        <|> signedType
        <|> intType
        <|> stringType
        <|> boolType
        <|> structType
        <|> userType
        <|> typeVar
        <|> tupleType

typeSpecSimple = withPos $
                  bitType
              <|> signedType
              <|> intType
              <|> stringType
              <|> boolType
              <|> tupleType
              <|> userType
              <|> typeVar

bitType    = TBit    nopos <$ reserved "bit" <*> (fromIntegral <$> angles decimal)
signedType = TSigned nopos <$ reserved "signed" <*> (fromIntegral <$> angles decimal)
intType    = TInt    nopos <$ reserved "bigint"
stringType = TString nopos <$ reserved "string"
boolType   = TBool   nopos <$ reserved "bool"
userType   = TUser   nopos <$> typeIdent <*> (option [] $ symbol "<" *> commaSep typeSpec <* symbol ">")
typeVar    = TVar    nopos <$ symbol "'" <*> typevarIdent
structType = TStruct nopos <$ isstruct <*> sepBy1 constructor (reservedOp "|")
    where isstruct = try $ lookAhead $ consIdent *> (symbol "{" <|> symbol "|")
tupleType  = (\fs -> case fs of
                          [f] -> f
                          _   -> TTuple nopos fs)
             <$> (parens $ commaSep typeSpecSimple)

constructor = withPos $ Constructor nopos <$> consIdent <*> (option [] $ braces $ commaSep arg)

expr =  buildExpressionParser etable term
    <?> "expression"

term  =  elhs
     <|> (withPos $ eTuple <$> (parens $ commaSep expr))
     <|> braces expr
     <|> term'
     <?> "expression term"
term' = withPos $
         eapply
     <|> ebinding
     <|> epholder
     <|> estruct
     <|> eint
     <|> ebool
     <|> estring
     <|> evar
     <|> ematch
     <|> eite
     <|> efor
     <|> evardcl

eapply = eApply <$ isapply <*> funcIdent <*> (parens $ commaSep expr)
    where isapply = try $ lookAhead $ do
                        _ <- funcIdent
                        symbol "("

ebinding = eBinding <$ isbinding <*> (varIdent <* reservedOp "@") <*> expr
    where isbinding = try $ lookAhead $ (\_ -> ()) <$> varIdent <* reservedOp "@"

ebool = eBool <$> ((True <$ reserved "true") <|> (False <$ reserved "false"))
evar = eVar <$> varIdent

ematch = eMatch <$ reserved "match" <*> parens expr
               <*> (braces $ (commaSep1 $ (,) <$> pattern <* reservedOp "->" <*> expr))
pattern = (withPos $
           eTuple   <$> (parens $ commaSep pattern)
       <|> eStruct  <$> consIdent <*> (option [] $ braces $ commaSep (namedpat <|> anonpat))
       <|> eVarDecl <$> varIdent
       <|> eVarDecl <$ reserved "var" <*> varIdent
       <|> epholder
       <|> ebool
       <|> epattern_string
       <|> eint)
      <?> "match pattern"

anonpat = ("",) <$> pattern
namedpat = (,) <$> (dot *> varIdent) <*> (reservedOp "=" *> pattern)

lhs = (withPos $
           eTuple <$> (parens $ commaSep lhs)
       <|> eStruct <$> consIdent <*> (option [] $ braces $ commaSep $ namedlhs <|> anonlhs)
       <|> fexpr
       <|> evardcl
       <|> epholder)
      <?> "l-expression"
elhs = islhs *> lhs
    where islhs = try $ lookAhead $ lhs *> reservedOp "="

anonlhs = ("",) <$> lhs
namedlhs = (,) <$> (dot *> varIdent) <*> (reservedOp "=" *> lhs)


eint  = lexeme eint'
estring =   equoted_string
        <|> eraw_string
        <|> einterpolated_raw_string

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
    p <- try $ lookAhead $ do {string "\""; getPosition}
    str <- concat <$> many1 stringLit
    case parse (interpolate p) "" str of
         Left  er -> fail $ show er
         Right ex -> return ex

einterpolated_raw_string  = do
    try $ string "$[|"
    p <- getPosition
    str <- manyTill anyChar (try $ string "|]" *> whiteSpace)
    case parse (interpolate p) "" str of
         Left  er -> fail $ show er
         Right ex -> return ex

interpolate p = do
    setPosition p
    interpolate' Nothing

interpolate' mprefix = do
    str <- withPos $ (E . EString nopos) <$> manyTill anyChar (eof <|> do {try $ lookAhead $ string "${"; return ()})
    let prefix' = maybe str
                        (\prefix ->
                          if exprString (enode str) == ""
                             then prefix
                             else case prefix of
                                       E (EString _ s) -> str
                                       _               -> E $ EBinOp (fst $ pos prefix, snd $ pos str) Concat prefix str)
                        mprefix
    (do eof
        return prefix'
     <|>
     do e <- string "${" *> whiteSpace *> expr <* char '}'
        p <- getPosition
        interpolate' $ (Just $ E $ EBinOp (fst $ pos prefix', p) Concat prefix' e))

estruct = eStruct <$> consIdent <*> (option [] $ braces $ commaSep (namedarg <|> anonarg))

eint'   = (lookAhead $ char '\'' <|> digit) *> (do w <- width
                                                   (s, v) <- sradval
                                                   mkLit w s v)

-- strip underscores
stripUnder :: String -> String
stripUnder = filter (/= '_')

digitPrefix :: Stream s m Char => ParsecT s u m Char -> ParsecT s u m String
digitPrefix digit = (:) <$> digit <*> (many (digit <|> char '_'))

eite    = eITE     <$ reserved "if" <*> term <*> term <*> (reserved "else" *> term)
efor    = eFor     <$ (reserved "for" *> symbol "(") <*> (varIdent <* reserved "in") <*> (expr <* symbol ")") <*> term
evardcl = eVarDecl <$ reserved "var" <*> varIdent
epholder = ePHolder <$ reserved "_"

width = optionMaybe (try $ ((fmap fromIntegral parseDec) <* (lookAhead $ char '\'')))
sradval =  ((try $ string "'b") *> ((False, ) <$> parseBin))
       <|> ((try $ string "'o") *> ((False, ) <$> parseOct))
       <|> ((try $ string "'d") *> ((False, ) <$> parseDec))
       <|> ((try $ string "'h") *> ((False, ) <$> parseHex))
       <|> ((try $ string "'sb") *> ((True, ) <$> parseBin))
       <|> ((try $ string "'so") *> ((True, ) <$> parseOct))
       <|> ((try $ string "'sd") *> ((True, ) <$> parseDec))
       <|> ((try $ string "'sh") *> ((True, ) <$> parseHex))
       <|> ((True, ) <$> parseDec)
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

mkLit :: Maybe Int -> Bool -> Integer -> ParsecT s u m Expr
mkLit Nothing _ v                        = return $ eInt v
mkLit (Just w) s v | w == 0              = fail "Literals must have width >0"
                   | msb v < w           = return $ if s then eSigned w v else eBit w v
                   | otherwise           = fail "Value exceeds specified width"

etable = [[postf $ choice [postSlice, postApply, postField, postType]]
         ,[pref  $ choice [preRef]]
         ,[pref  $ choice [prefix "~" BNeg]]
         ,[pref  $ choice [prefix "not" Not]]
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
         ,[binary "and" And AssocLeft]
         ,[binary "or" Or AssocLeft]
         ,[binary "=>" Impl AssocLeft]
         ,[assign AssocNone]
         ,[sbinary ";" ESeq AssocRight]
         ]

pref  p = Prefix  . chainl1 p $ return       (.)
postf p = Postfix . chainl1 p $ return (flip (.))
postField = (\f end e -> E $ EField (fst $ pos e, end) e f) <$> field <*> getPosition
postApply = (\(f, args) end e -> E $ EApply (fst $ pos e, end) f (e:args)) <$> dotcall <*> getPosition
postType = (\t end e -> E $ ETyped (fst $ pos e, end) e t) <$> etype <*> getPosition
postSlice  = try $ (\(h,l) end e -> E $ ESlice (fst $ pos e, end) e h l) <$> slice <*> getPosition
slice = brackets $ (\h l -> (fromInteger h, fromInteger l)) <$> natural <*> (colon *> natural)

field = isfield *> dot *> varIdent
    where isfield = try $ lookAhead $ do
                        _ <- dot
                        _ <- notFollowedBy relIdent
                        varIdent
dotcall = (,) <$ isapply <*> (dot *> funcIdent) <*> (parens $ commaSep expr)
    where isapply = try $ lookAhead $ do
                        _ <- dot
                        _ <- funcIdent
                        symbol "("

etype = reservedOp ":" *> typeSpecSimple

preRef = (\start e -> E $ ERef (start, snd $ pos e) e) <$> getPosition <* reservedOp "&"
prefix n fun = (\start e -> E $ EUnOp (start, snd $ pos e) fun e) <$> getPosition <* reservedOp n
binary n fun  = Infix $ (\le re -> E $ EBinOp (fst $ pos le, snd $ pos re) fun le re) <$ reservedOp n
sbinary n fun = Infix $ (\l  r  -> E $ fun (fst $ pos l, snd $ pos r) l r) <$ reservedOp n

assign = Infix $ (\l r  -> E $ ESet (fst $ pos l, snd $ pos r) l r) <$ reservedOp "="

{- F-expression (variable of field name) -}
fexpr =  buildExpressionParser fetable fterm
    <?> "column or field name"

fterm  =  withPos $ evar

fetable = [[postf postField]]
