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

module Language.DifferentialDatalog.Debugger.DebugEventParser (
    eventsParser) where

import qualified Text.Parsec.Token as T
import Text.Parsec
import Text.Parsec.Language
import Data.Functor.Identity

data Operator = OpMap | OpAggregate | OpCondition | OpJoin
                | OpAntijoin | OpInspect | OpUndefined deriving (Show)

data OperatorId = OperatorId {opRelId:: Int, opRule::Int, opOperaror::Int} deriving (Show)

data Event = DebugEvent { evtOperatorId :: OperatorId
                        , evtWeight :: Int
                        , evtTimestamp :: Integer
                        , evtOperator :: Operator
                        , evtInput :: Record
                        , evtOutput :: Record
                        }
            | DebugJoinEvent { evtOperatorId :: OperatorId
                             , evtWeight :: Int
                             , evtTimestamp :: Integer
                             , evtOperator :: Operator
                             , evtInput1 :: Record
                             , evtInput2 :: Record
                             , evtOutput :: Record
                             }
            deriving (Show)

data Record = IntRecord {intVal :: Integer}
            | BoolRecord {boolVal :: Bool}
            | DoubleRecord {doubleVal :: Double}
            | StringRecord {stringVal :: String}
            | NamedStructRecord {name :: String, val :: [(String, Record)]}
            | TupleRecord {tupleVal :: [Record]}
            | ArrayRecord {arrayVal :: [Record]}
            deriving (Show)

debugDef :: GenLanguageDef String u Data.Functor.Identity.Identity
debugDef = emptyDef { T.identStart        = alphaNum
                    , T.identLetter       = alphaNum
                    , T.caseSensitive     = True}

identifier :: ParsecT String u Identity String
identifier   = T.identifier lexer

lexer :: T.GenTokenParser String u Data.Functor.Identity.Identity
lexer   = T.makeTokenParser debugDef

commaSep :: ParsecT String u Identity a -> ParsecT String u Identity [a]
commaSep     = T.commaSep lexer

symbol :: String -> ParsecT String u Identity String
symbol       = try . T.symbol lexer

comma :: ParsecT String u Identity String
comma        = T.comma lexer

braces :: ParsecT String u Identity a -> ParsecT String u Identity a
braces       = T.braces lexer

parens :: ParsecT String u Identity a -> ParsecT String u Identity a
parens       = T.parens lexer

brackets :: ParsecT String u Identity a -> ParsecT String u Identity a
brackets     = T.brackets lexer

decimal :: ParsecT String u Identity Integer
decimal      = T.decimal lexer

double :: ParsecT String u Identity Double
double       = T.float lexer

dot :: ParsecT String u Identity String
dot          = T.dot lexer

stringLit :: ParsecT String u Identity String
stringLit    = T.stringLiteral lexer

operatorParser :: ParsecT String u Identity Operator
operatorParser = do
                 op <- identifier
                 case op of
                        "Map" -> return OpMap
                        "Aggregate" -> return OpAggregate
                        "Condition" -> return OpCondition
                        "Join" -> return OpJoin
                        "Antijoin" -> return OpAntijoin
                        "Inspect" -> return OpInspect
                        _ -> return  OpUndefined

intParser :: ParsecT String u Identity Record
intParser = IntRecord <$> decimal

boolParser :: ParsecT String u Identity Record
boolParser = BoolRecord <$> ((symbol "true" >> return True)
                              <|> (symbol "false" >> return False))

doubleParser :: ParsecT String u Identity Record
doubleParser = DoubleRecord <$> double

stringParser :: ParsecT String u Identity Record
stringParser = StringRecord <$> stringLit

namedStructParser :: ParsecT String u Identity Record
namedStructParser = NamedStructRecord <$> identifier <*> (braces (commaSep ((,) <$> (dot *> identifier) <*> (symbol "=" *> recordParser))))

tupleParser :: ParsecT String u Identity Record
tupleParser = parens (TupleRecord <$> (commaSep recordParser))

arrayParser :: ParsecT String u Identity Record
arrayParser =  brackets (ArrayRecord <$> (commaSep recordParser))

recordParser :: ParsecT String u Identity Record
recordParser = try (doubleParser) <|>boolParser <|> intParser <|> stringParser
               <|> namedStructParser <|> tupleParser <|> arrayParser

debugEventParser :: ParsecT String u Identity Event
debugEventParser = do
    opid <- parens (OperatorId <$> (fromIntegral <$> (decimal <* comma))
            <*> (fromIntegral <$> (decimal <* comma)) <*> (fromIntegral <$> decimal))
    w <- fromIntegral <$> (comma *> decimal)
    ts <- comma *> decimal <* comma
    op <- operatorParser
    case op of
             OpJoin ->  do
                       input1 <- comma *> recordParser
                       input2 <- comma *> recordParser
                       output <- comma *> recordParser
                       return $ DebugJoinEvent opid w ts op input1 input2 output
             _ -> do
                  input <- comma *> recordParser
                  output <- comma *> recordParser
                  return $ DebugEvent opid w ts op input output

eventsParser :: ParsecT String u Identity [Event]
eventsParser = many debugEventParser
