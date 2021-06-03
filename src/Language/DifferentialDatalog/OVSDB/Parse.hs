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
{-# OPTIONS_GHC -fno-warn-missing-signatures #-}

{- |
Module     : OVSDB.Parse
Description: Parses a JSON schema for an OVS database.
             Schema specification is part of RFC 7047:
             https://tools.ietf.org/html/rfc7047
-}

module Language.DifferentialDatalog.OVSDB.Parse(
    parseSchema,
    OVSDBSchema(..),
    Table(..),
    TableProperty(..),
    TableColumn(..),
    ColumnType(..),
    ComplexType(..),
    IntegerOrUnlimited(..),
    BaseType(..),
    ComplexBaseType(..),
    Value(..),
    Atom(..),
    Constant(..),
    AtomicType(..)
) where

import Control.Applicative hiding (many,optional,Const)
import Text.Parsec hiding ((<|>))
import qualified Text.Parsec.Token as T
import Text.Parsec.Language
import Data.List
import Data.Maybe

import Language.DifferentialDatalog.Pos
import Language.DifferentialDatalog.Name

parseSchema :: String -> String -> Either String OVSDBSchema
parseSchema jsonContent fileName =
  case parse jsonGrammar fileName jsonContent of
       Left  e    -> Left $ show e
       Right prog -> Right prog

jsonDef = emptyDef { T.commentStart      = "<!--"
                   , T.commentEnd        = "-->"
                   , T.identStart        = letter <|> char '_'
                   , T.identLetter       = alphaNum <|> char '_'
                   , T.caseSensitive     = True}

lexer   = T.makeTokenParser jsonDef

--identifier   = T.identifier lexer
colon        = T.colon lexer
commaSep     = T.commaSep lexer
comma        = T.comma lexer
braces       = T.braces lexer
--parens       = T.parens lexer
brackets     = T.brackets lexer
--natural      = T.natural lexer
decimal      = T.integer lexer
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

data SchemaProperty = PropertyName String
                    | PropertyTables [Table]
                    | PropertyIgnored

jsonGrammar :: Parsec String u OVSDBSchema
jsonGrammar = removeTabs *> ((optional whiteSpace) *> databaseSchema <* eof)

removeTabs = do s <- getInput
                let s' = map (\c -> if c == '\t' then ' ' else c ) s
                setInput s'

databaseSchema :: Parsec String u OVSDBSchema
databaseSchema = do
    properties <- braces $ commaSep databaseSchemaProperties
    nm <- case mapMaybe (\case
                          PropertyName n -> Just n
                          _              -> Nothing) properties of
                 [n] -> return n
                 []  -> fail "Schema name is missing"
                 _   -> fail "Multiple \"name\" fields"
    tables <- case mapMaybe (\case
                             PropertyTables ts -> Just ts
                             _                 -> Nothing) properties of
                   [ts] -> return ts
                   []   -> fail "Schema is empty"
                   _    -> fail "Multiple \"tables\" fields"
    return $ OVSDBSchema nm tables

databaseSchemaProperties :: Parsec String u SchemaProperty
databaseSchemaProperties = databaseCksum
                       <|> databaseName
                       <|> databaseVersion
                       <|> databaseTables

-- output is currently ignored
databaseName :: Parsec String u SchemaProperty
databaseName = PropertyName <$> (simpleOrQuoted "name" *> colon *> stringLit)

-- output is currently ignored
databaseCksum :: Parsec String u SchemaProperty
databaseCksum = PropertyIgnored <$ ((simpleOrQuoted "cksum") *> colon *> stringLit)

-- output is currently ignored
databaseVersion :: Parsec String u SchemaProperty
databaseVersion = PropertyIgnored <$ ((simpleOrQuoted "version") *> colon *> stringLit)

databaseTables :: Parsec String u SchemaProperty
databaseTables = PropertyTables <$> (simpleOrQuoted "tables" *> colon *> (braces $ commaSep table))

table :: Parsec String u Table
table = withPos $ Table nopos <$> stringLit <*> (colon *> braces tableSchema)

tableSchema :: Parsec String u [TableProperty]
tableSchema = commaSep tableProperty

tableProperty :: Parsec String u TableProperty
tableProperty = tableRoot <|> tableIndexes <|> tableColumns <|> tableMaxRows

tableRoot :: Parsec String u TableProperty
tableRoot = RootProperty <$> ((simpleOrQuoted "isRoot") *> colon *> booleanLit)

-- currently ignored
tableIndexes :: Parsec String u TableProperty
tableIndexes = IgnoredProperty <$ ((simpleOrQuoted "indexes") *> colon *> stringArrayArray)

tableMaxRows :: Parsec String u TableProperty
tableMaxRows = MaxRows <$> ((simpleOrQuoted "maxRows") *> colon *> (fromInteger <$> decimal))

stringArrayArray :: Parsec String u [[String]]
stringArrayArray = brackets $ commaSep $ brackets $ commaSep stringLit

tableColumns :: Parsec String u TableProperty
tableColumns = ColumnsProperty <$> ((simpleOrQuoted "columns") *> colon *> (braces $ commaSep tableColumn))

tableColumn :: Parsec String u TableColumn
tableColumn = do
    nm <- stringLit <* (colon *> (symbol "{"))
    properties <- withPos $ addColumnProperties (TableColumn nopos nm ColumnTypeUndefined Nothing Nothing)
    case columnType properties of
         ColumnTypeUndefined -> fail "Missing required field \"type\""
         _                   -> return properties

addColumnProperties :: TableColumn -> Parsec String u TableColumn
addColumnProperties col = comma *> addColumnProperties col
                            <|> (symbol "}") *> (return col)
                            <|> do col' <- addColumnProperty col
                                   final <- addColumnProperties col'
                                   return final

addColumnProperty :: TableColumn -> Parsec String u TableColumn
addColumnProperty col =
  (\x -> col{columnType = x}) <$> ((simpleOrQuoted "type") *> colon *> parseColumnType) <|>
  (\x -> col{columnEphemeral = Just x}) <$> ((simpleOrQuoted "ephemeral") *> colon *> booleanLit) <|>
  (\x -> col{columnMutable = Just x}) <$> ((simpleOrQuoted "mutable") *> colon *> booleanLit)

parseColumnType :: Parsec String u ColumnType
parseColumnType = (ColumnTypeAtomic <$> atomicType) <|> complexType

atomicType :: Parsec String u AtomicType
atomicType = withPos $
             (IntegerType nopos <$ try (symbol $ quoted "integer")) <|>
             (StringType nopos <$ try (symbol $ quoted "string"))   <|>
             (BooleanType nopos <$ try (symbol $ quoted "boolean")) <|>
             (RealType nopos <$ try (symbol $ quoted "real"))       <|>
             (UUIDType nopos <$ try (symbol $ quoted "uuid"))

complexType :: Parsec String u ColumnType
complexType = do
    let t = ComplexType nopos BaseTypeUndefined Nothing Nothing Nothing
    _ <- symbol "{"
    t' <- withPos $ addComplexTypeProperties t
    case keyComplexType t' of
         BaseTypeUndefined -> fail "Missing required field \"key\""
         _                 -> return $ ColumnTypeComplex t'

addComplexTypeProperties :: ComplexType -> Parsec String u ComplexType
addComplexTypeProperties t = comma *> addComplexTypeProperties t
                             <|> (symbol "}") *> (return t)
                             <|> do t' <- addComplexTypeProperty t
                                    final <- addComplexTypeProperties t'
                                    return final

addComplexTypeProperty :: ComplexType -> Parsec String u ComplexType
addComplexTypeProperty t =
  (\x -> t{keyComplexType = x}) <$> ((simpleOrQuoted "key") *> colon *> baseType) <|>
  (\x -> t{valueComplexType = Just x}) <$> ((simpleOrQuoted "value") *> colon *> baseType) <|>
  (\x -> t{minComplexType = Just x}) <$> ((simpleOrQuoted "min") *> colon *> decimal) <|>
  (\x -> t{maxComplexType = Just x}) <$> ((simpleOrQuoted "max") *> colon *> decimalOrUnlimited)


baseType :: Parsec String u BaseType
baseType =
    (BaseTypeSimple <$> atomicType) <|>
    do let t = ComplexBaseType (UndefinedAtomicType nopos) Nothing Nothing Nothing Nothing Nothing
       _ <- symbol "{"
       t' <- addComplexBaseTypeProperties t
       case typeBaseType t' of
            UndefinedAtomicType _ -> fail "Missing required field \"type\""
            _                     -> return $ BaseTypeComplex t'

decimalOrUnlimited :: Parsec String u IntegerOrUnlimited
decimalOrUnlimited = (Some <$> decimal) <|>
                     (Unlimited <$ (symbol $ quoted "unlimited"))

addComplexBaseTypeProperties :: ComplexBaseType -> Parsec String u ComplexBaseType
addComplexBaseTypeProperties t = comma *> addComplexBaseTypeProperties t
                             <|> (symbol "}") *> (return t)
                             <|> do t' <- addComplexBaseTypeProperty t
                                    final <- addComplexBaseTypeProperties t'
                                    return final

addComplexBaseTypeProperty :: ComplexBaseType -> Parsec String u ComplexBaseType
addComplexBaseTypeProperty t =
  (\x -> t{typeBaseType = x}) <$> ((simpleOrQuoted "type") *> colon *> atomicType) <|>
  (\x -> t{minBaseType = Just $ Integer x}) <$> ((simpleOrQuoted "minInteger") *> colon *> decimal) <|>
  (\x -> t{minBaseType = Just $ Real x}) <$> ((simpleOrQuoted "minReal") *> colon *> float) <|>
  (\x -> t{minBaseType = Just $ StringLength x}) <$> ((simpleOrQuoted "minLength") *> colon *> decimal) <|>
  (\x -> t{maxBaseType = Just $ Integer x}) <$> ((simpleOrQuoted "maxInteger") *> colon *> decimal) <|>
  (\x -> t{maxBaseType = Just $ Real x}) <$> ((simpleOrQuoted "maxReal") *> colon *> float) <|>
  (\x -> t{maxBaseType = Just $ StringLength x}) <$> ((simpleOrQuoted "maxLength") *> colon *> decimal) <|>
  (\x -> t{refTableBaseType = Just x}) <$> ((simpleOrQuoted "refTable") *> colon *> uuid) <|>
  (\x -> t{refTypeBaseType = Just x}) <$> ((simpleOrQuoted "refType") *> colon *> referenceType) <|>
  (\x -> t{typeEnum = Just x}) <$> ((simpleOrQuoted "enum") *> colon *> enumValue)

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

data OVSDBSchema = OVSDBSchema { schemaName   :: String
                               , schemaTables :: [Table]
                               }

data Table = Table { tablePos :: Pos
                   , tableName :: String
                   , tableProperties :: [TableProperty]
                   }

instance Show Table where
    show (Table _ n props) = "Table " ++ n ++ "\n" ++ (intercalate "\n\t" $ map show props)

instance WithName Table where
    name = tableName
    setName t n = t { tableName = n }

instance WithPos Table where
    pos = tablePos
    atPos t p = t{tablePos = p}

data TableProperty = IgnoredProperty
                   | RootProperty Bool
                   | ColumnsProperty [TableColumn]
                   | MaxRows Int

instance Show TableProperty where
  show IgnoredProperty = ""
  show (RootProperty b) = "isRoot: " ++ show b
  show (ColumnsProperty cols) = intercalate "," $ map show cols
  show (MaxRows i) = "maxRows: " ++ show i

data TableColumn = TableColumn { columnPos       :: Pos
                               , columnName      :: String
                               , columnType      :: ColumnType
                               , columnEphemeral :: Maybe Bool
                               , columnMutable   :: Maybe Bool
                               }

instance WithPos TableColumn where
    pos = columnPos
    atPos t p = t{columnPos = p}

instance WithName TableColumn where
    name = columnName
    setName c n = c { columnName = n }

-- If the maybe value is not Nothing, show it like key=value
showMaybeWithKey :: Show a => String -> Maybe a -> String
showMaybeWithKey _ Nothing = ""
showMaybeWithKey s (Just x) = " " ++ s ++ "=" ++ (show x)

instance Show TableColumn where
  show (TableColumn _ n t e m) = "Column " ++ (quoted n) ++ ", " ++
    (show t) ++
    (showMaybeWithKey "ephemeral" e) ++
    (showMaybeWithKey "mutable" m)

data ColumnType = ColumnTypeAtomic  AtomicType
                | ColumnTypeComplex ComplexType
                | ColumnTypeUndefined

data ComplexType = ComplexType { posComplexType   :: Pos
                               , keyComplexType   :: BaseType
                               , valueComplexType :: Maybe BaseType
                               , minComplexType   :: Maybe Integer
                               , maxComplexType   :: Maybe IntegerOrUnlimited
                               }

instance WithPos ComplexType where
    pos = posComplexType
    atPos t p = t{posComplexType = p}

data IntegerOrUnlimited = Some Integer
                        | Unlimited

instance Show IntegerOrUnlimited where
  show (Some x) = show x
  show Unlimited = "unlimited"

instance Show ColumnType where
  show (ColumnTypeAtomic a)  = show a
  show (ColumnTypeComplex c) = show c
  show ColumnTypeUndefined   = "???"

instance Show ComplexType where
  show (ComplexType _ k v vmin vmax) = "key=" ++ (show k) ++
                                       (showMaybeWithKey "value" v) ++
                                       (showMaybeWithKey "min" vmin) ++
                                       (showMaybeWithKey "max" vmax)

data BaseType = BaseTypeSimple  AtomicType
              | BaseTypeComplex ComplexBaseType
              | BaseTypeUndefined

instance Show BaseType where
  show (BaseTypeSimple a)  = show a
  show (BaseTypeComplex c) = show c
  show BaseTypeUndefined   = "???"

data ComplexBaseType = ComplexBaseType { typeBaseType :: AtomicType
                                       , typeEnum :: Maybe Value
                                       , minBaseType :: Maybe Constant
                                       , maxBaseType :: Maybe Constant
                                       , refTableBaseType :: Maybe UUID
                                       , refTypeBaseType :: Maybe ReferenceType
                                       }

instance Show ComplexBaseType where
  show (ComplexBaseType t _ _ _ _ _) = (show t) -- TODO

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

data AtomicType = IntegerType         {atypePos :: Pos}
                | RealType            {atypePos :: Pos}
                | BooleanType         {atypePos :: Pos}
                | StringType          {atypePos :: Pos}
                | UUIDType            {atypePos :: Pos}
                | UndefinedAtomicType {atypePos :: Pos}

instance Show AtomicType where
  show IntegerType{}         = "integer"
  show RealType{}            = "real"
  show StringType{}          = "string"
  show BooleanType{}         = "boolean"
  show UUIDType{}            = "uuid"
  show UndefinedAtomicType{} = "???"

instance WithPos AtomicType where
    pos = atypePos
    atPos t p = t{atypePos = p}
