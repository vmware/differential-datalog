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

{-# LANGUAGE FlexibleContexts, RecordWildCards, TupleSections #-}

module Language.DifferentialDatalog.Parse (
    parseDatalogFile,
    parseDatalogString,
    datalogGrammar,
    exprGrammar) where

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
import Numeric
import Debug.Trace

import Language.DifferentialDatalog.Syntax
import Language.DifferentialDatalog.Pos
import Language.DifferentialDatalog.Util
import Language.DifferentialDatalog.Name
import Language.DifferentialDatalog.Ops
import Language.DifferentialDatalog.Preamble

-- | Parse a file containing a datalog program and produce the intermediate representation
-- if 'insert_preamble' is true, prepends the content of
-- 'datalotPreamble' to the file before parsing it.
parseDatalogFile :: Bool -> FilePath -> IO DatalogProgram
parseDatalogFile insert_preamble fname = do
    fdata <- readFile fname
    parseDatalogString insert_preamble fdata fname

-- parse a string containing a datalog program and produce the intermediate representation
parseDatalogString :: Bool -> String -> String -> IO DatalogProgram
parseDatalogString insert_preamble program file = do
  preamble <- if insert_preamble
                 then parseDatalogString False datalogPreamble "Preamble"
                 else return emptyDatalogProgram

  case parse (datalogGrammar preamble) file program of
         Left  e    -> errorWithoutStackTrace $ "Failed to parse input file: " ++ show e
         Right prog -> return prog

reservedOpNames = [":", "|", "&", "==", "=", ":-", "%", "*", "/", "+", "-", ".", "->", "=>", "<=",
                   "<=>", ">=", "<", ">", "!=", ">>", "<<", "~"]
reservedNames = ["_",
                 "Aggregate",
                 "FlatMap",
                 "and",
                 "bit",
                 "bool",
                 "default",
                 "else",
                 "false",
                 "for",
                 "function",
                 "if",
                 "in",
                 "insert",
                 "int",
                 "let",
                 "match",
                 "not",
                 "or",
                 "relation",
                 "skip",
                 "string",
                 "true",
                 "typedef",
                 "var"]


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

consIdent    = ucIdentifier
relIdent     = ucIdentifier
varIdent     = lcIdentifier
typevarIdent = ucIdentifier
funcIdent    = lcIdentifier

removeTabs = do s <- getInput
                let s' = map (\c -> if c == '\t' then ' ' else c ) s
                setInput s'

withPos x = (\ s a e -> atPos a (s,e)) <$> getPosition <*> x <*> getPosition

data SpecItem = SpType         TypeDef
              | SpRelation     Relation
              | SpRule         Rule
              | SpFunc         Function
              | SpStatement    Statement

datalogGrammar preamble = removeTabs *> ((optional whiteSpace) *> spec preamble <* eof)
exprGrammar = removeTabs *> ((optional whiteSpace) *> expr <* eof)

spec preamble = do
    items <- many decl
    let relations = (M.toList $ progRelations preamble) ++
                    mapMaybe (\i -> case i of
                                         SpRelation r -> Just (name r, r)
                                         _            -> Nothing) items
    let types = (M.toList $ progTypedefs preamble) ++
                mapMaybe (\i -> case i of
                                     SpType t -> Just (name t, t)
                                     _        -> Nothing) items
    let funcs = (M.toList $ progFunctions preamble) ++
                mapMaybe (\i -> case i of
                                     SpFunc f -> Just (name f, f)
                                     _        -> Nothing) items
    let rules = mapMaybe (\i -> case i of
                                     SpRule r -> Just r
                                     _        -> Nothing) items
    let statements = mapMaybe (\i -> case i of
                                     SpStatement r -> Just r
                                     _             -> Nothing) items
    let res = do uniqNames ("Multiple definitions of type " ++) $ map snd $ types
                 uniqNames ("Multiple definitions of function " ++) $ map snd $ funcs
                 uniqNames ("Multiple definitions of relation " ++) $ map snd $ relations
                 return $ DatalogProgram { progTypedefs   = M.fromList types
                                         , progFunctions  = M.fromList funcs
                                         , progRelations  = M.fromList relations
                                         , progRules      = progRules preamble ++ rules
                                         , progStatements = progStatements preamble ++ statements}
    case res of
         Left err   -> error err
         Right prog -> return prog

decl =  (SpType         <$> typeDef)
    <|> (SpRelation     <$> relation)
    <|> (SpFunc         <$> func)
    <|> (SpRule         <$> rule)
    <|> (SpStatement    <$> parseForStatement)

typeDef = withPos $ (TypeDef nopos) <$ reserved "typedef" <*> identifier <*>
                                       (option [] (symbol "<" *> (commaSep $ symbol "'" *> typevarIdent) <* symbol ">")) <*>
                                       (optionMaybe $ reservedOp "=" *> typeSpec)

func = withPos $ Function nopos <$  reserved "function"
                                <*> funcIdent
                                <*> (parens $ commaSep arg)
                                <*> (colon *> typeSpecSimple)
                                <*> (optionMaybe $ reservedOp "=" *> expr)

relation = withPos $ Relation nopos <$> ((True <$ reserved "ground" <* reserved "relation")
                                         <|>
                                         (False <$ reserved "relation"))
                                       <*> relIdent <*> (parens $ commaSep arg)

arg = withPos $ (Field nopos) <$> varIdent <*> (colon *> typeSpecSimple)

parseForStatement = withPos $ ForStatement nopos <$ reserved "for"
                                                 <*> (symbol "(" *> expr)
                                                 <*> (reserved "in" *> relIdent)
                                                 <*> (optionMaybe (reserved "if" *> expr))
                                                 <*> (symbol ")" *> statement)

statement = parseForStatement
        <|> parseEmptyStatement
        <|> parseIfStatement
        <|> parseMatchStatement
        <|> parseLetStatement
        <|> parseInsertStatement
        <|> parseBlockStatement

parseAssignment = withPos $ (Assignment nopos) <$> identifier
                                               <*> (reserved "=" *> expr)

parseEmptyStatement = withPos $ (EmptyStatement nopos) <$ reserved "skip"

parseBlockStatement = withPos $ (BlockStatement nopos) <$> (braces $ semiSep statement)

parseIfStatement = withPos $ (IfStatement nopos) <$ reserved "if"
                                                <*> (symbol "(" *> expr)
                                                <*> (symbol ")" *> statement)
                                                <*> (optionMaybe (reserved "else" *> statement))

parseMatchStatement = withPos $ (MatchStatement nopos) <$ reserved "match" <*> parens expr
                      <*> (braces $ (commaSep1 $ (,) <$> pattern <* reservedOp "->" <*> statement))

parseLetStatement = withPos $ (LetStatement nopos) <$ reserved "let"
                                                  <*> commaSep parseAssignment
                                                  <*> (reserved "in" *> statement)

parseInsertStatement = withPos $ (InsertStatement nopos) <$> relIdent
                                                         <*> (parens $ commaSep expr)

rule = withPos $ Rule nopos <$>
                 (commaSep1 atom) <*>
                 (option [] (reservedOp ":-" *> commaSep rulerhs)) <* dot

rulerhs =  (do _ <- try $ lookAhead $ (optional $ reserved "not") *> relIdent *> symbol "("
               RHSLiteral <$> (option True (False <$ reserved "not")) <*> atom)
       <|> (RHSCondition <$> expr)
       <|> (RHSAggregate <$ reserved "Aggregate" <*>
                            (symbol "(" *> (parens $ commaSep varIdent)) <*>
                            (comma *> varIdent) <*>
                            (reservedOp "=" *> expr)) <*
                            symbol ")"
       <|> (RHSFlatMap <$ reserved "FlatMap" <*> (symbol "(" *> varIdent) <*>
                          (reservedOp "=" *> expr <* symbol ")"))

atom = withPos $ Atom nopos <$> relIdent <*> (parens $ commaSep (namedarg <|> anonarg))

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
intType    = TInt    nopos <$ reserved "int"
stringType = TString nopos <$ reserved "string"
boolType   = TBool   nopos <$ reserved "bool"
userType   = TUser   nopos <$> identifier <*> (option [] $ symbol "<" *> commaSep typeSpec <* symbol ">")
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
      <|> eVarDecl <$> varIdent
      <|> eVarDecl <$ reserved "var" <*> varIdent
      <|> epholder
      <|> ebool
      <|> estring
      <|> eint
      <|> eStruct  <$> consIdent <*> (option [] $ braces $ commaSep (namedpat <|> anonpat))

anonpat = ("",) <$> pattern
namedpat = (,) <$> (dot *> varIdent) <*> (reservedOp "=" *> pattern)

lhs = withPos $
          eTuple <$> (parens $ commaSep lhs)
      <|> fexpr
      <|> evardcl
      <|> epholder
      <|> eStruct <$> consIdent <*> (option [] $ braces $ commaSep $ namedlhs <|> anonlhs)
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
parseBin = readBin <$> (many1 $ (char '0') <|> (char '1'))
parseOct :: Stream s m Char => ParsecT s u m Integer
parseOct = (fst . head . readOct) <$> many1 octDigit
parseDec :: Stream s m Char => ParsecT s u m Integer
parseDec = (fst . head . readDec) <$> many1 digit
--parseSDec = (\m v -> m * v)
--            <$> (option 1 ((-1) <$ reservedOp "-"))
--            <*> ((fst . head . readDec) <$> many1 digit)
parseHex :: Stream s m Char => ParsecT s u m Integer
parseHex = (fst . head . readHex) <$> many1 hexDigit

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
