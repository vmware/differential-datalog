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

{-# LANGUAGE RecordWildCards, ImplicitParams, LambdaCase, FlexibleContexts #-}

{- Parses a JSON schema for an OVS database.
   Schema specification is part of RFC 7047:
   https://tools.ietf.org/html/rfc7047
-}

import Control.Applicative hiding (many,optional,Const)
import System.Environment
import Text.Parsec hiding ((<|>))
import qualified Text.Parsec.Token as T
import Text.Parsec.Language
import Data.List

main = do
    args <- getArgs
    let file = args!!0
    content <- readFile file
    putStr $ show $ parseJson content file

parseJson :: String -> String -> String
parseJson jsonContent fileName =
  case parse jsonGrammar fileName jsonContent of
         Left  e    -> errorWithoutStackTrace $ "Failed to parse input file: " ++ show e
         Right prog -> show prog

jsonDef = emptyDef { T.commentStart      = "<!--"
                  , T.commentEnd        = "-->"
                  , T.identStart        = letter <|> char '_'
                  , T.identLetter       = alphaNum <|> char '_'
                  , T.caseSensitive     = True}

lexer   = T.makeTokenParser jsonDef

identifier   = T.identifier lexer
colon        = T.colon lexer
commaSep     = T.commaSep lexer
comma        = T.comma lexer
braces       = T.braces lexer
parens       = T.parens lexer
brackets     = T.brackets lexer
natural      = T.natural lexer
decimal      = T.decimal lexer
whiteSpace   = T.whiteSpace lexer
stringLit    = T.stringLiteral lexer
symbol       = T.symbol lexer
float        = T.float lexer

---------- utilities -------------

quoted :: String -> String
quoted s = "\"" ++ s ++ "\""

simpleOrQuoted :: String -> Parsec String u String
simpleOrQuoted val = try $ (symbol val) <|> (symbol $ quoted val)

booleanLit :: Parsec String u Bool
booleanLit = (True <$ symbol "true") <|> (False <$ symbol "false")

----------- schema grammar ---------

jsonGrammar :: Parsec String u [Table]
jsonGrammar = removeTabs *> ((optional whiteSpace) *> databaseSchema <* eof)

removeTabs = do s <- getInput
                let s' = map (\c -> if c == '\t' then ' ' else c ) s
                setInput s'

databaseSchema :: Parsec String u [Table]
databaseSchema = braces $ concat <$> commaSep databaseSchemaProperties

databaseSchemaProperties :: Parsec String u [Table]
databaseSchemaProperties = databaseCksum
                           <|> databaseName
                           <|> databaseVersion
                           <|> databaseTables

-- output is currently ignored
databaseName :: Parsec String u [Table]
databaseName = [] <$ ((simpleOrQuoted "name") *> colon *> stringLit)

-- output is currently ignored
databaseCksum :: Parsec String u [Table]
databaseCksum = [] <$ ((simpleOrQuoted "cksum") *> colon *> stringLit)

-- output is currently ignored
databaseVersion :: Parsec String u [Table]
databaseVersion = [] <$ ((simpleOrQuoted "version") *> colon *> stringLit)

databaseTables :: Parsec String u [Table]
databaseTables = (simpleOrQuoted "tables") *> colon *> (braces $ commaSep table)

table :: Parsec String u Table
table = Table <$> stringLit <*> (colon *> braces tableSchema)

tableSchema :: Parsec String u [TableProperty]
tableSchema = commaSep tableProperty

tableProperty :: Parsec String u TableProperty
tableProperty = tableRoot <|> tableIndexes <|> tableColumns <|> tableMaxRows

tableRoot :: Parsec String u TableProperty
tableRoot = RootProperty <$> ((simpleOrQuoted "isRoot") *> colon *> booleanLit)

-- currently ignored
tableIndexes :: Parsec String u TableProperty
tableIndexes = IgnoredProperty <$ ((simpleOrQuoted "indexes") *> colon *> stringArrayArray)

-- currently ignored
tableMaxRows :: Parsec String u TableProperty
tableMaxRows = IgnoredProperty <$ ((simpleOrQuoted "maxRows") *> colon *> decimal)

stringArrayArray :: Parsec String u [[String]]
stringArrayArray = brackets $ commaSep $ brackets $ commaSep stringLit

tableColumns :: Parsec String u TableProperty
tableColumns = ColumnsProperty <$> ((simpleOrQuoted "columns") *> colon *> (braces $ commaSep tableColumn))

tableColumn :: Parsec String u TableColumn
tableColumn = do name <- stringLit <* (colon *> (symbol "{"))
                 properties <- addColumnProperties (TCP name UndefinedType Nothing Nothing)
                 return $ TableColumn properties

addColumnProperties :: TableColumnProperties -> Parsec String u TableColumnProperties
addColumnProperties init = comma *> addColumnProperties init
                            <|> (symbol "}") *> (return init)
                            <|> do init' <- addColumnProperty init
                                   final <- addColumnProperties init'
                                   return final

addColumnProperty :: TableColumnProperties -> Parsec String u TableColumnProperties
addColumnProperty init =
  (\x -> init{columnType = x}) <$> ((simpleOrQuoted "type") *> colon *> parseColumnType) <|>
  (\x -> init{columnEphemeral = Just x}) <$> ((simpleOrQuoted "ephemeral") *> colon *> booleanLit) <|>
  (\x -> init{columnMutable = Just x}) <$> ((simpleOrQuoted "mutable") *> colon *> booleanLit)

parseColumnType :: Parsec String u ColumnType
parseColumnType = (AtomicType <$> atomicType) <|> complexType

atomicType :: Parsec String u AtomicType
atomicType = (IntegerType <$ try (symbol $ quoted "integer")) <|>
             (StringType <$ try (symbol $ quoted "string")) <|>
             (BooleanType <$ try (symbol $ quoted "boolean")) <|>
             (RealType <$ try (symbol $ quoted "real")) <|>
             (UUIDType <$ try (symbol $ quoted "uuid"))

complexType :: Parsec String u ColumnType
complexType = let init = (CTP UndefinedBaseType Nothing Nothing Nothing) in
                (symbol "{") *> (ComplexType <$> addComplexTypeProperties init)

addComplexTypeProperties :: ComplexTypeProperties -> Parsec String u ComplexTypeProperties
addComplexTypeProperties init = comma *> addComplexTypeProperties init
                             <|> (symbol "}") *> (return init)
                             <|> do init' <- addComplexTypeProperty init
                                    final <- addComplexTypeProperties init'
                                    return final

addComplexTypeProperty :: ComplexTypeProperties -> Parsec String u ComplexTypeProperties
addComplexTypeProperty init =
  (\x -> init{keyComplexType = x}) <$> ((simpleOrQuoted "key") *> colon *> baseType) <|>
  (\x -> init{valueComplexType = Just x}) <$> ((simpleOrQuoted "value") *> colon *> baseType) <|>
  (\x -> init{minComplexType = Just x}) <$> ((simpleOrQuoted "min") *> colon *> decimal) <|>
  (\x -> init{maxComplexType = Just x}) <$> ((simpleOrQuoted "max") *> colon *> decimalOrUnlimited)


baseType :: Parsec String u BaseType
baseType = (SimpleBaseType <$> atomicType) <|>
           let init = (ComplexBaseTypeInfo UndefinedAtomicType Nothing Nothing Nothing Nothing Nothing) in
             (symbol "{") *> (ComplexBaseType <$> addComplexBaseTypeProperties init)

decimalOrUnlimited :: Parsec String u IntegerOrUnlimited
decimalOrUnlimited = (Some <$> decimal) <|>
                     (Unlimited <$ (symbol $ quoted "unlimited"))

addComplexBaseTypeProperties :: ComplexBaseTypeInfo -> Parsec String u ComplexBaseTypeInfo
addComplexBaseTypeProperties init = comma *> addComplexBaseTypeProperties init
                             <|> (symbol "}") *> (return init)
                             <|> do init' <- addComplexBaseTypeProperty init
                                    final <- addComplexBaseTypeProperties init'
                                    return final

addComplexBaseTypeProperty :: ComplexBaseTypeInfo -> Parsec String u ComplexBaseTypeInfo
addComplexBaseTypeProperty init =
  (\x -> init{typeBaseType = x}) <$> ((simpleOrQuoted "type") *> colon *> atomicType) <|>
  (\x -> init{minBaseType = Just $ Integer x}) <$> ((simpleOrQuoted "minInteger") *> colon *> decimal) <|>
  (\x -> init{minBaseType = Just $ Real x}) <$> ((simpleOrQuoted "minReal") *> colon *> float) <|>
  (\x -> init{minBaseType = Just $ StringLength x}) <$> ((simpleOrQuoted "minLength") *> colon *> decimal) <|>
  (\x -> init{maxBaseType = Just $ Integer x}) <$> ((simpleOrQuoted "maxInteger") *> colon *> decimal) <|>
  (\x -> init{maxBaseType = Just $ Real x}) <$> ((simpleOrQuoted "maxReal") *> colon *> float) <|>
  (\x -> init{maxBaseType = Just $ StringLength x}) <$> ((simpleOrQuoted "maxLength") *> colon *> decimal) <|>
  (\x -> init{refTableBaseType = Just x}) <$> ((simpleOrQuoted "refTable") *> colon *> uuid) <|>
  (\x -> init{refTypeBaseType = Just x}) <$> ((simpleOrQuoted "refType") *> colon *> referenceType) <|>
  (\x -> init{typeEnum = Just x}) <$> ((simpleOrQuoted "enum") *> colon *> enumValue)

referenceType :: Parsec String u ReferenceType
referenceType = (ReferenceTypeStrong <$ (simpleOrQuoted "strong")) <|>
                (ReferenceTypeWeak   <$ (simpleOrQuoted "weak"))

enumValue :: Parsec String u Value
enumValue = (\x -> SetValue x) <$> setValue

atomValue :: Parsec String u Atom
atomValue = (\x -> StringAtom x) <$> stringLit <|>
            (\x -> NumberAtom x) <$> decimal <|>
            (\x -> BooleanAtom x) <$> booleanLit

setValue :: Parsec String u Set
setValue = brackets $ (simpleOrQuoted "set") *> comma *> (brackets $ commaSep atomValue)

uuid :: Parsec String u String
uuid = stringLit

-------------------------------------------------------------------------------------------

data Table = Table { tableName :: String
                   , tableProperties :: [TableProperty]
                   }

instance Show Table where
  show (Table n props) = "Table " ++ n ++ "\n" ++ (intercalate "\n\t" $ map show props)

data TableProperty = NameProperty String
                   | IgnoredProperty
                   | RootProperty Bool
                   | ColumnsProperty [TableColumn]

instance Show TableProperty where
  show (NameProperty n) = "name=" ++ n
  show IgnoredProperty = ""
  show (RootProperty b) = "isRoot=" ++ show b
  show (ColumnsProperty cols) = intercalate "," $ map show cols

data TableColumn = TableColumn TableColumnProperties

data TableColumnProperties = TCP { columnName      :: String
                                 , columnType      :: ColumnType
                                 , columnEphemeral :: Maybe Bool
                                 , columnMutable   :: Maybe Bool
                                 }

-- If the maybe value is not Nothing, show it like key=value
showMaybeWithKey :: Show a => String -> Maybe a -> String
showMaybeWithKey s Nothing = ""
showMaybeWithKey s (Just x) = " " ++ s ++ "=" ++ (show x)

instance Show TableColumn where
  show (TableColumn (TCP name t e m)) = "Column " ++ (quoted name) ++ ", " ++
    (show t) ++
    (showMaybeWithKey "ephemeral" e) ++
    (showMaybeWithKey "mutable" m)

data ColumnType = AtomicType AtomicType
                | ComplexType ComplexTypeProperties
                | UndefinedType

data ComplexTypeProperties = CTP { keyComplexType   :: BaseType
                                 , valueComplexType :: Maybe BaseType
                                 , minComplexType   :: Maybe Integer
                                 , maxComplexType   :: Maybe IntegerOrUnlimited
                                 }

data IntegerOrUnlimited = Some Integer
                        | Unlimited

instance Show IntegerOrUnlimited where
  show (Some x) = show x
  show Unlimited = "unlimited"

instance Show ColumnType where
  show (AtomicType a) = show a
  show (ComplexType c) = show c

instance Show ComplexTypeProperties where
  show (CTP k v min max) = "key=" ++ (show k) ++
                           (showMaybeWithKey "value" v) ++
                           (showMaybeWithKey "min" min) ++
                           (showMaybeWithKey "max" max)

data BaseType = SimpleBaseType AtomicType
              | ComplexBaseType ComplexBaseTypeInfo
              | UndefinedBaseType

instance Show BaseType where
  show (SimpleBaseType a) = show a
  show (ComplexBaseType c) = show c

data ComplexBaseTypeInfo = ComplexBaseTypeInfo { typeBaseType :: AtomicType
                                               , typeEnum :: Maybe Value
                                               , minBaseType :: Maybe Constant
                                               , maxBaseType :: Maybe Constant
                                               , refTableBaseType :: Maybe UUID
                                               , refTypeBaseType :: Maybe ReferenceType
                                               }

instance Show ComplexBaseTypeInfo where
  show (ComplexBaseTypeInfo t e min max refTable refType) = (show t) -- TODO

data Value = AtomValue Atom
           | SetValue Set

instance Show Value where
  show (AtomValue a) = show a
  show (SetValue s) = "[ \"set\", " ++ (intercalate ", " $ map show s) ++ " ]"

data Atom = StringAtom String
          | NumberAtom Integer
          | BooleanAtom Bool
          | UUIDAtom UUID
          | NamedUUIDAtom String
          | UndefinedAtom

instance Show Atom where
  show (StringAtom s) = s
  show (NumberAtom n) = show n
  show (BooleanAtom b) = show b
  show (UUIDAtom u) = "[ \"uuid\", " ++ show u ++ " ]"
  show (NamedUUIDAtom u) = "[ \"named-uuid\", " ++ show u ++ " ]"

type Set = [Atom]

type UUID = String

data ReferenceType = ReferenceTypeStrong
                   | ReferenceTypeWeak

instance Show ReferenceType where
  show ReferenceTypeWeak = "weak"
  show ReferenceTypeStrong = "strong"

data Constant = Integer Integer
              | Real Double
              | StringLength Integer

instance Show Constant where
  show (Integer i) = show i
  show (Real r)    = show r
  show (StringLength i) = show i

data AtomicType = IntegerType
                | RealType
                | BooleanType
                | StringType
                | UUIDType
                | UndefinedAtomicType

instance Show AtomicType where
  show IntegerType = "integer"
  show RealType = "real"
  show StringType = "string"
  show BooleanType = "boolean"
  show UUIDType = "uuid"
  show UndefinedAtomicType = "???"
