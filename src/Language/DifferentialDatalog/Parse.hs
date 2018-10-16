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
                   "<=>", ">=", "<", ">", "!=", ">>", "<<", "~"]
reservedNames = ["as",
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
                 "input",
                 "insert",
                 "bigint",
                 "not",
                 "or",
                 "relation",
                 "skip",
                 "string",
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
typevarIdent = ucIdentifier <?> "type variable name"
modIdent     = identifier   <?> "module name"

consIdent    = ucScopedIdentifier <?> "constructor name"
relIdent     = ucScopedIdentifier <?> "relation name"
funcIdent    = lcScopedIdentifier <?> "function name"
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

data SpecItem = SpImport    Import
              | SpType      TypeDef
              | SpRelation  Relation
              | SpRule      Rule
              | SpFunc      Function

instance WithPos SpecItem where
    pos   (SpType         t)   = pos t
    pos   (SpRelation     r)   = pos r
    pos   (SpRule         r)   = pos r
    pos   (SpFunc         f)   = pos f
    pos   (SpImport       i)   = pos i
    atPos (SpType         t) p = SpType      $ atPos t p
    atPos (SpRelation     r) p = SpRelation  $ atPos r p
    atPos (SpRule         r) p = SpRule      $ atPos r p
    atPos (SpFunc         f) p = SpFunc      $ atPos f p
    atPos (SpImport       i) p = SpImport    $ atPos i p


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
    let rules = mapMaybe (\case
                           SpRule r -> Just r
                           _        -> Nothing) items
    let res = do uniqNames ("Multiple definitions of type " ++) $ map snd $ types
                 uniqNames ("Multiple definitions of function " ++) $ map snd $ funcs
                 uniqNames ("Multiple definitions of relation " ++) $ map snd $ relations
                 --uniq importAlias (\imp -> "Alias " ++ show (importAlias imp) ++ " used multiple times ") imports
                 uniq importModule (\imp -> "Module " ++ show (importModule imp) ++ " is imported multiple times ") imports
                 return $ DatalogProgram { progImports    = imports
                                         , progTypedefs   = M.fromList types
                                         , progFunctions  = M.fromList funcs
                                         , progRelations  = M.fromList relations
                                         , progRules      = rules }
    case res of
         Left err   -> errorWithoutStackTrace err
         Right prog -> return prog

decl = (withPosMany $
             (return . SpImport)         <$> imprt
         <|> (return . SpType)           <$> typeDef
         <|> relation
         <|> (return . SpFunc)           <$> func
         <|> (return . SpRule)           <$> rule)
   <|> (map SpRule . convertStatement) <$> parseForStatement

imprt = Import nopos <$ reserved "import" <*> modname <*> (option (ModuleName []) $ reserved "as" *> modname)

modname = ModuleName <$> modIdent `sepBy1` reservedOp "."

typeDef = (TypeDef nopos) <$ reserved "typedef" <*> typeIdent <*>
                             (option [] (symbol "<" *> (commaSep $ symbol "'" *> typevarIdent) <* symbol ">")) <*>
                             (Just <$ reservedOp "=" <*> typeSpec)
       <|>
          (TypeDef nopos) <$ (try $ reserved "extern" *> reserved "type") <*> typeIdent <*>
                             (option [] (symbol "<" *> (commaSep $ symbol "'" *> typevarIdent) <* symbol ">")) <*>
                             (return Nothing)

func = (Function nopos <$  (try $ reserved "extern" *> reserved "function")
                       <*> funcIdent
                       <*> (parens $ commaSep arg)
                       <*> (colon *> typeSpecSimple)
                       <*> (return Nothing))
       <|>
       (Function nopos <$  reserved "function"
                       <*> funcIdent
                       <*> (parens $ commaSep arg)
                       <*> (colon *> typeSpecSimple)
                       <*> (Just <$ reservedOp "=" <*> expr))

relation = do
    ground <-  True <$ reserved "input" <* reserved "relation"
           <|> False <$ reserved "relation"
    relName <- relIdent
    ((do start <- getPosition
         fields <- parens $ commaSep arg
         pkey <- optionMaybe $ symbol "primary" *> symbol "key" *> key_expr
         end <- getPosition
         let p = (start, end)
         let tspec = TStruct p [Constructor p relName fields]
         let tdef = TypeDef nopos relName [] $ Just tspec
         let rel = Relation nopos ground relName (TUser p relName []) True pkey
         return [SpType tdef, SpRelation rel])
      <|>
       (do rel <- (\tspec -> Relation nopos ground relName tspec True) <$>
                     (brackets typeSpecSimple) <*>
                     (optionMaybe $ symbol "primary" *> symbol "key" *> key_expr)
           return [SpRelation rel]))

key_expr = withPos $ KeyExpr nopos <$> (parens varIdent) <*> expr

arg = withPos $ (Field nopos) <$> varIdent <*> (colon *> typeSpecSimple)

parseForStatement = withPos $
                    ForStatement nopos <$ reserved "for"
                                       <*> (symbol "(" *> expr)
                                       <*> (reserved "in" *> relIdent)
                                       <*> (optionMaybe (reserved "if" *> expr))
                                       <*> (symbol ")" *> statement)

statement = parseForStatement
        <|> parseEmptyStatement
        <|> parseIfStatement
        <|> parseMatchStatement
        <|> parseVarStatement
        <|> parseInsertStatement
        <|> parseBlockStatement

parseAssignment = withPos $ (Assignment nopos) <$> varIdent
                                               <*> (optionMaybe etype)
                                               <*> (reserved "=" *> expr)

parseEmptyStatement = withPos $ (EmptyStatement nopos) <$ reserved "skip"

parseBlockStatement = withPos $ (BlockStatement nopos) <$> (braces $ semiSep statement)

parseIfStatement = withPos $ (IfStatement nopos) <$ reserved "if"
                                                <*> (parens expr) <*> statement
                                                <*> (optionMaybe (reserved "else" *> statement))

parseMatchStatement = withPos $ (MatchStatement nopos) <$ reserved "match" <*> parens expr
                      <*> (braces $ (commaSep1 $ (,) <$> pattern <* reservedOp "->" <*> statement))

parseVarStatement = withPos $ (VarStatement nopos) <$ reserved "var"
                                                  <*> commaSep parseAssignment
                                                  <*> (reserved "in" *> statement)

parseInsertStatement = withPos $ (InsertStatement nopos) <$> atom

rule = Rule nopos <$>
       (commaSep1 atom) <*>
       (option [] (reservedOp ":-" *> commaSep rulerhs)) <* dot

rulerhs =  (do _ <- try $ lookAhead $ (optional $ reserved "not") *> relIdent *> (symbol "(" <|> symbol "[")
               RHSLiteral <$> (option True (False <$ reserved "not")) <*> atom)
       <|> (RHSAggregate <$ reserved "Aggregate" <*>
                            (symbol "(" *> (parens $ commaSep varIdent)) <*>
                            (comma *> varIdent) <*>
                            (reservedOp "=" *> funcIdent) <*>
                            parens expr <*
                            symbol ")")
       <|> do _ <- try $ lookAhead $ reserved "var" *> varIdent *> reservedOp "=" *> reserved "FlatMap"
              RHSFlatMap <$> (reserved "var" *> varIdent) <*>
                             (reservedOp "=" *> reserved "FlatMap" *> parens expr)
       <|> (RHSCondition <$> expr)

atom = withPos $ do
       rname <- relIdent
       val <- (withPos $ eStruct rname <$> (parens $ commaSep (namedarg <|> anonarg)))
              <|>
              brackets expr
       return $ Atom nopos rname val

anonarg = ("",) <$> expr
namedarg = (,) <$> (dot *> varIdent) <*> (reservedOp "=" *> expr)

typeSpec = withPos $
            bitType
        <|> intType
        <|> stringType
        <|> boolType
        <|> structType
        <|> userType
        <|> typeVar
        <|> tupleType

typeSpecSimple = withPos $
                  bitType
              <|> intType
              <|> stringType
              <|> boolType
              <|> tupleType
              <|> userType
              <|> typeVar

bitType    = TBit    nopos <$ reserved "bit" <*> (fromIntegral <$> angles decimal)
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
term' = withPos $
         eapply
     <|> epholder
     <|> estruct
     <|> eint
     <|> ebool
     <|> estring
     <|> einterpolated_string
     <|> evar
     <|> ematch
     <|> eite
     <|> evardcl

eapply = eApply <$ isapply <*> funcIdent <*> (parens $ commaSep expr)
    where isapply = try $ lookAhead $ do
                        _ <- funcIdent
                        symbol "("

ebool = eBool <$> ((True <$ reserved "true") <|> (False <$ reserved "false"))
evar = eVar <$> varIdent

ematch = eMatch <$ reserved "match" <*> parens expr
               <*> (braces $ (commaSep1 $ (,) <$> pattern <* reservedOp "->" <*> expr))
pattern = withPos $
          eTuple   <$> (parens $ commaSep pattern)
      <|> eStruct  <$> consIdent <*> (option [] $ braces $ commaSep (namedpat <|> anonpat))
      <|> eVarDecl <$> varIdent
      <|> eVarDecl <$ reserved "var" <*> varIdent
      <|> epholder
      <|> ebool
      <|> estring
      <|> eint

anonpat = ("",) <$> pattern
namedpat = (,) <$> (dot *> varIdent) <*> (reservedOp "=" *> pattern)

lhs = withPos $
          eTuple <$> (parens $ commaSep lhs)
      <|> eStruct <$> consIdent <*> (option [] $ braces $ commaSep $ namedlhs <|> anonlhs)
      <|> fexpr
      <|> evardcl
      <|> epholder
elhs = islhs *> lhs
    where islhs = try $ lookAhead $ lhs *> reservedOp "="

anonlhs = ("",) <$> lhs
namedlhs = (,) <$> (dot *> varIdent) <*> (reservedOp "=" *> lhs)


--eint  = Int <$> (fromIntegral <$> decimal)
eint  = lexeme eint'
estring = (eString . concat) <$>
          many1 (stringLit <|> ((try $ string "[|") *> manyTill anyChar (try $ string "|]" *> whiteSpace)))

-- Parse interpolated strings, converting them to string concatenation
-- expressions.
-- First, parse as normal string literal;
-- then apply a separate parser to the resulting string to extract
-- interpolated expressions.
einterpolated_string = einterpolated_quoted_string <|> einterpolated_raw_string

einterpolated_quoted_string = do
    p <- try $ lookAhead $ do {string "$\""; getPosition}
    str <- char '$' *> stringLit
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
                        (\prefix -> E $ EBinOp (fst $ pos prefix, snd $ pos str) Concat prefix str)
                        mprefix
    (do eof
        return prefix'
     <|>
     do e <- string "${" *> whiteSpace *> expr <* char '}'
        p <- getPosition
        interpolate' $ (Just $ E $ EBinOp (fst $ pos prefix', p) Concat prefix' e))

estruct = eStruct <$> consIdent <*> (option [] $ braces $ commaSep (namedarg <|> anonarg))

eint'   = (lookAhead $ char '\'' <|> digit) *> (do w <- width
                                                   v <- sradval
                                                   mkLit w v)

-- strip underscores
stripUnder :: String -> String
stripUnder = filter (/= '_')

digitPrefix :: Stream s m Char => ParsecT s u m Char -> ParsecT s u m String
digitPrefix digit = (:) <$> digit <*> (many (digit <|> char '_'))

eite    = eITE     <$ reserved "if" <*> term <*> term <*> (reserved "else" *> term)
evardcl = eVarDecl <$ reserved "var" <*> varIdent
epholder = ePHolder <$ reserved "_"

width = optionMaybe (try $ ((fmap fromIntegral parseDec) <* (lookAhead $ char '\'')))
sradval =  ((try $ string "'b") *> parseBin)
       <|> ((try $ string "'o") *> parseOct)
       <|> ((try $ string "'d") *> parseDec)
       <|> ((try $ string "'h") *> parseHex)
       <|> parseDec
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

mkLit :: Maybe Int -> Integer -> ParsecT s u m Expr
mkLit Nothing  v                       = return $ eInt v
mkLit (Just w) v | w == 0              = fail "Unsigned literals must have width >0"
                 | msb v < w           = return $ eBit w v
                 | otherwise           = fail "Value exceeds specified width"

etable = [[postf $ choice [postSlice, postApply, postField, postType]]
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
                        varIdent
dotcall = (,) <$ isapply <*> (dot *> funcIdent) <*> (parens $ commaSep expr)
    where isapply = try $ lookAhead $ do
                        _ <- dot
                        _ <- funcIdent
                        symbol "("

etype = reservedOp ":" *> typeSpecSimple

prefix n fun = (\start e -> E $ EUnOp (start, snd $ pos e) fun e) <$> getPosition <* reservedOp n
binary n fun  = Infix $ (\le re -> E $ EBinOp (fst $ pos le, snd $ pos re) fun le re) <$ reservedOp n
sbinary n fun = Infix $ (\l  r  -> E $ fun (fst $ pos l, snd $ pos r) l r) <$ reservedOp n

assign = Infix $ (\l r  -> E $ ESet (fst $ pos l, snd $ pos r) l r) <$ reservedOp "="

{- F-expression (variable of field name) -}
fexpr =  buildExpressionParser fetable fterm
    <?> "column or field name"

fterm  =  withPos $ evar

fetable = [[postf postField]]
