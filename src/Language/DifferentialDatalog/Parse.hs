{-# LANGUAGE FlexibleContexts, RecordWildCards, TupleSections #-}

module Language.DifferentialDatalog.Parse (
    parseDatalogFile,
    datalogGrammar,
    exprGrammar) where

import Control.Applicative hiding (many,optional,Const)
import qualified Control.Exception as E
import Text.Parsec hiding ((<|>))
import Text.Parsec.Expr
import Text.Parsec.Language
import qualified Text.Parsec.Token as T
import qualified Data.Map as M
import Data.Maybe
import Data.Either
import Numeric

import Language.DifferentialDatalog.Syntax
import Language.DifferentialDatalog.Pos
import Language.DifferentialDatalog.Util
import Language.DifferentialDatalog.Name
import Language.DifferentialDatalog.Ops

parseDatalogFile :: FilePath -> IO DatalogProgram
parseDatalogFile fname = do
    fdata <- readFile fname
    case parse datalogGrammar fname fdata of
         Left  e    -> errorWithoutStackTrace $ "Failed to parse input file: " ++ show e
         Right prog -> return prog

reservedOpNames = [":", "|", "&", "==", "=", ":-", "%", "+", "-", ".", "->", "=>", "<=", "<=>", ">=", "<", ">", "!=", ">>", "<<", "~"]
reservedNames = ["_",
                 "and",
                 "bit",
                 "bool",
                 "default",
                 "else",
                 "false",
                 "function",
                 "if",
                 "int",
                 "match",
                 "not",
                 "or",
                 "relation",
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
--semiSep    = T.semiSep lexer
--semiSep1   = T.semiSep1 lexer
colon        = T.colon lexer
commaSep     = T.commaSep lexer
commaSep1    = T.commaSep1 lexer
symbol       = T.symbol lexer
--semi         = T.semi lexer
--comma        = T.comma lexer
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

consIdent   = ucIdentifier
relIdent    = ucIdentifier
varIdent    = lcIdentifier
funcIdent   = lcIdentifier

removeTabs = do s <- getInput
                let s' = map (\c -> if c == '\t' then ' ' else c ) s
                setInput s'

withPos x = (\s a e -> atPos a (s,e)) <$> getPosition <*> x <*> getPosition

data SpecItem = SpType         TypeDef
              | SpRelation     Relation
              | SpRule         Rule
              | SpFunc         Function

datalogGrammar = removeTabs *> ((optional whiteSpace) *> spec <* eof)
exprGrammar = removeTabs *> ((optional whiteSpace) *> expr <* eof)

spec = mkProgram <$> (many decl)

mkProgram :: [SpecItem] -> DatalogProgram
mkProgram items = DatalogProgram types funcs relations rules
    where relations = M.fromList $
                      mapMaybe (\i -> case i of
                                           SpRelation r -> Just (name r, r)
                                           _            -> Nothing) items
          types = M.fromList $
                  mapMaybe (\i -> case i of
                                       SpType t -> Just (name t, t)
                                       _        -> Nothing) items
                  -- ++
                  -- map (\Relation{..} -> (relName, TypeDef relPos relName $ Just $ TStruct relPos
                  --                                 $ [Constructor relPos relName relArgs])) relations
          funcs = M.fromList $
                  mapMaybe (\i -> case i of
                                       SpFunc f -> Just (name f, f)
                                       _        -> Nothing) items
          rules = mapMaybe (\i -> case i of
                                       SpRule r -> Just r
                                       _        -> Nothing) items


decl =  (SpType         <$> typeDef)
    <|> (SpRelation     <$> relation)
    <|> (SpFunc         <$> func)
    <|> (SpRule         <$> rule)

typeDef = withPos $ (TypeDef nopos) <$ reserved "typedef" <*> identifier <*> (reservedOp "=" *> typeSpec)

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

rule = withPos $ mkRule <$>
                 (commaSep1 atom <* reservedOp ":-") <*>
                 (commaSep $
                     ((Left <$> do _ <- try $ lookAhead $ (optional $ reserved "not") *> relIdent *> symbol "("
                                   (,) <$> (option True (False <$ reserved "not")) <*> atom)
                      <|>
                      (Right <$> expr)))

atom = withPos $ Atom nopos <$> relIdent <*> (parens $ commaSep (namedarg <|> anonarg))

anonarg = ("",) <$> expr
namedarg = (,) <$> (dot *> varIdent) <*> (reservedOp "=" *> expr)

mkRule :: [Atom] -> [Either (Bool, Atom) Expr] -> Rule
mkRule lhsAtoms rhs = Rule nopos lhsAtoms atoms exprs
    where (atoms, exprs) = partitionEithers rhs

typeSpec = withPos $
            bitType
        <|> intType
        <|> stringType
        <|> boolType
        <|> structType
        <|> userType
        <|> tupleType

typeSpecSimple = withPos $
                  bitType
              <|> intType
              <|> stringType
              <|> boolType
              <|> tupleType
              <|> userType

bitType    = TBit    nopos <$ reserved "bit" <*> (fromIntegral <$> angles decimal)
intType    = TInt    nopos <$ reserved "int"
stringType = TString nopos <$ reserved "string"
boolType   = TBool   nopos <$ reserved "bool"
userType   = TUser   nopos <$> identifier
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
      <|> epholder
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
estring = eString <$> stringLit
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
         ,[binary "%" Mod AssocLeft]
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

field = dot *> varIdent
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
