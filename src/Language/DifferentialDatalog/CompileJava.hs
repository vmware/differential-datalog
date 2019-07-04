{-
Copyright (c) 2019 VMware, Inc.
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

{-# LANGUAGE RecordWildCards, FlexibleContexts, LambdaCase, TupleSections, OverloadedStrings, TemplateHaskell, QuasiQuotes #-}

{- |
Module     : CompileJava
Description: Compile 'DatalogProgram' to Java API.
-}

module Language.DifferentialDatalog.CompileJava (
    compileJava,
) where

import Prelude hiding((<>))
import Control.Monad.State
import Text.PrettyPrint
import Data.Tuple
import Data.Tuple.Select
import Data.Maybe
import Data.List
import Data.Int
import Data.Word
import Data.Bits hiding (isSigned)
import Data.List.Split
import Data.FileEmbed
import Data.String.Utils
import Data.Char
import System.FilePath
import System.Directory
import qualified Data.ByteString.Char8 as BS
import Numeric
import qualified Data.Set as S
import qualified Data.Map as M
import qualified Data.Graph.Inductive as G
import qualified Data.Graph.Inductive.Query.DFS as G
import Debug.Trace
import Text.RawString.QQ
import Data.WideWord
import Debug.Trace

import Language.DifferentialDatalog.PP
import Language.DifferentialDatalog.Name
import Language.DifferentialDatalog.Pos
import Language.DifferentialDatalog.Ops
import Language.DifferentialDatalog.Util
import Language.DifferentialDatalog.Syntax
import Language.DifferentialDatalog.Parse
import Language.DifferentialDatalog.NS
import Language.DifferentialDatalog.Expr
import Language.DifferentialDatalog.DatalogProgram
import Language.DifferentialDatalog.Relation
import Language.DifferentialDatalog.Optimize
import Language.DifferentialDatalog.Module
import Language.DifferentialDatalog.ECtx
import Language.DifferentialDatalog.Type
import Language.DifferentialDatalog.Rule

-- generate a Java program from a DDlog program
-- The Java program has some utility function to more easily create
-- records for the DDlog relations.  Note that Java does not support
-- all DDlog constructs
compileJava :: DatalogProgram -> String -> IO ()
compileJava d sourceName = do
    let content = generateJava d sourceName in
         writeFile (capitalize sourceName ++ ".java") (render content)

-- Generate a Java document from a program given the source filename
generateJava :: DatalogProgram -> String -> Doc
generateJava d sourceName =
    preamble sourceName $+$
    (braces' $ body d)

-- Generates the preamble of a Java file
preamble :: String -> Doc
preamble sourceName =
    "import java.util.*;"                       $+$
    "import ddlogapi.DDlogRecord;\n"            $+$
    "import ddlogapi.DDlogCommand;\n"           $+$
    "class" <+> pp (capitalize sourceName)

-- Generates the body of the Java class containing all utility methods
body :: DatalogProgram -> Doc
body d = genEnum d $+$
          (vcat $ punctuate "\n" $ map (genCreate d) (M.keys $ progRelations d)) $+$
          (vcat $ punctuate "\n" $ map (genClass d) (M.keys $ progRelations d))

-- Generate a Java class for an element of a specific relation
genClass :: DatalogProgram -> String -> Doc
genClass d@DatalogProgram{..} relationName =
    let rel = progRelations M.! relationName
        rtype = relType rel in
    "public static class" <+> makeIdentifier relationName $+$
    (braces' $ classBody d relationName rtype)

-- given a (relation) type generate the body of a Java class storing the columns of the relation
classBody :: DatalogProgram -> String -> Type -> Doc
classBody d className t =
  -- class fields
  (classFields d t) $+$
  -- constructor from DDlogRecord
  ("public" <+> pp className <> (parens "DDlogRecord r") $+$ (braces' $ constructorBody d t)) $+$
  -- constructor from fields
  ("public" <+> pp className <> (parens $ parameters d t) $+$ (braces' $ constructorFieldsBody d t)) $+$
  -- conversion to DDlogRecord
  "public DDlogRecord asRecord()" $+$
  (braces' $ ((pp ("return create_" :: String)) <> (pp className) <> (parens $ arguments d t)) <> semi) $+$
  -- toString method
  "@Override public String toString()" $+$
  (braces' $ "return" <+> (doubleQuotes $ pp className) <+> "+ \"{\" +" <+> (toStringBody d t) <+> "+ \"}\"" <> semi) $+$
  -- command to insert
  "public DDlogCommand createCommand(boolean insert)" $+$
  (braces' $ ("return new DDlogCommand(insert ? DDlogCommand.Kind.Insert : DDlogCommand.Kind.DeleteVal," $+$
              (pp $ "TableId_" ++ className ++ ",") $+$
              "this.asRecord());"))

-- generates the body of the constructor from values for all fields
constructorFieldsBody :: DatalogProgram -> Type -> Doc
constructorFieldsBody d t =
    case t of
        TBool{..}   -> "this.a0 = a0;"
        TInt{..}    -> "this.a0 = a0;"
        TString{..} -> "this.a0 = a0;"
        TSigned{..} -> "this.a0 = a0;"
        TBit{..}    -> "this.a0 = a0;"
        TTuple{..}  -> vcat $ mapIdx (\_ i -> "this.a" <> pp i <+> "= a" <> pp i <> semi) typeTupArgs
        -- we do not really do anything different for collections, so we can just use elementType
        TUser{..}   -> constructorFieldsBody d $ elementType $ resolveType d t
        TStruct{..} -> let c0:tail = typeCons in
                       if null tail then
                           let assignField f =
                                 "this." <> (pp $ fieldName f) <+> "=" <+> (pp $ fieldName f) <> semi
                           in vcat $ map assignField $ consArgs c0
                       else error $ "Unsupported type " ++ show t
        _           -> error $ "Unsupported type " ++ show t

-- given a relation type generate the body of the toString method
toStringBody :: DatalogProgram -> Type -> Doc
toStringBody d t =
    case t of
        TBool{..}   -> "this.a0"
        TInt{..}    -> "this.a0"
        TString{..} -> "this.a0"
        TSigned{..} -> "this.a0"
        TBit{..}    -> "this.a0"
        -- we don't do anything different for collections, so we can just use elementType
        TUser{..}   -> toStringBody d $ elementType $ resolveType d t
        TTuple{..}  -> (cat $ punctuate " + \",\" + " (mapIdx (\_ i -> "this.a" <> pp i) typeTupArgs))
        TStruct{..} -> let c0:tail = typeCons in
                       if tail == [] then
                           let toStringField f =
                                 ("this." <> (pp $ fieldName f))
                           in
                               (cat $ punctuate " + \",\" + " (map toStringField (consArgs c0)))
                       else error $ "Unsupported type " ++ show t
        _           -> error $ "Unsupported type " ++ show t


-- generate code to create a a collection-typed value from a DDlogRecord
collectionFromRecord :: String -> String -> Collection -> Doc
collectionFromRecord destination value valueType =
    let extractCollection kind =
          ((pp destination) <+> "= new ArrayList<" <> (classType $ elementType valueType) <> ">();") $+$
                     (pp $ "for (int index = 0; index < " ++ value ++ ".get" ++ kind ++ "Size(); index++)") $+$
                     (nest' $ (pp $ destination ++ ".add((" ++ (show $ simpleType $ elementType valueType) ++ ")" ++
                                    value ++ ".get" ++ kind ++ "Field(index).") <>
                                    (getRecord $ elementType valueType) <> ")" <> semi)
    in
    case valueType of
        CNone t     -> (pp destination) <+> "=" <+> (parens $ simpleType t) <> (pp value) <> "." <> (getRecord t) <> semi
        CVector{..} -> extractCollection "Vector"
        CSet{..}    -> extractCollection "Set"

-- given a relation type generate the body of the constructor that
-- initializes the corresponding Java class fields
constructorBody :: DatalogProgram -> Type -> Doc
constructorBody d t =
    case t of
        TBool{..}   -> "this.a0 = r." <> getRecord t <> semi
        TInt{..}    -> "this.a0 = (" <> (simpleType t) <> ")r." <> getRecord t <> semi
        TString{..} -> "this.a0 = r." <> getRecord t <> semi
        TSigned{..} -> "this.a0 = (" <> (simpleType t) <> ")r." <> getRecord t <> semi
        TBit{..}    -> "this.a0 = (" <> (simpleType t) <> ")r." <> getRecord t <> semi
        TUser{..}   -> let rt = resolveType d t in
                         case rt of
                         CNone{..} -> constructorBody d $ elementType
                         _         -> collectionFromRecord "this.a0" "r" rt
        TTuple{..}  -> vcat $ mapIdx (\ft i ->
                                        (pp $ "DDlogRecord t" ++ (show i) ++ " = r.getTupleField(" ++ (show i) ++ ");") $+$                                        collectionFromRecord ("this.a" ++ (show i)) ("t" ++ (show i)) (resolveType d ft)) typeTupArgs
        TStruct{..} -> let c0:tail = typeCons in
                       if null tail then
                           (vcat $ mapIdx (\f i ->
                                             (pp $ "DDlogRecord t" ++ (show i) ++ " = r.getStructField(" ++ (show i) ++ ");") $+$
                                             collectionFromRecord ("this." ++ (fieldName f)) ("t" ++ show i) (resolveType d $ fieldType f)) (consArgs c0))
                       else error $ "Unsupported type " ++ show t
        _           -> error $ "Unsupported type " ++ show t

-- given a relation type generate arguments for the call to the create_CLASS method
-- supplying fields as arguments
arguments :: DatalogProgram -> Type -> Doc
arguments d t =
    case t of
        TBool{..}   -> "this.a0"
        TInt{..}    -> "this.a0"
        TBit{..}    -> "this.a0"
        TString{..} -> "this.a0"
        TSigned{..} -> "this.a0"
        -- we do not do anything special for collections, so we just use elementType
        TUser{..}   -> arguments d $ elementType $ resolveType d t
        TTuple{..}  -> commaSep $ mapIdx (\_ i -> "this.a" <> pp i) typeTupArgs
        TStruct{..} -> let c0:tail = typeCons
                           printField p = "this." <> (pp $ fieldName p)
                       in
                       if null tail then
                           commaSep $ map printField (consArgs c0)
                       else error $ "Unsupported type " ++ show t
        _           -> error $ "Unsupported type " ++ show t


-- given a (relation) type generate the fields of a clas storing the columns of the relation
classFields :: DatalogProgram -> Type -> Doc
classFields d t =
    case t of
        TBool{..}   -> "public" <+> simpleType t <+> "a0" <> semi
        TInt{..}    -> "public" <+> simpleType t <+> "a0" <> semi
        TBit{..}    -> "public" <+> simpleType t <+> "a0" <> semi
        TString{..} -> "public" <+> simpleType t <+> "a0" <> semi
        TSigned{..} -> "public" <+> simpleType t <+> "a0" <> semi
        TUser{..}   -> let rt = resolveType d t in
                         case rt of
                         CNone{..}   -> classFields d $ elementType
                         _           -> "public" <+> (collectionType $ resolveType d t) <+> "a0" <> semi
        TTuple{..}  -> vcat $ mapIdx (\t i -> "public" <+> (collectionType $ resolveType d t) <+>
                                              "a" <> pp i <> ";") typeTupArgs
        TStruct{..} -> let c0:tail = typeCons in
                       if tail == [] then
                           let fieldField f = ("public" <+> (collectionType $ resolveType d $ fieldType f)) <+>
                                 (pp $ fieldName f) <> semi in
                               vcat $ map fieldField (consArgs c0)
                       else error $ "Unsupported type " ++ show t
        _           -> error $ "Unsupported type " ++ show t

-- generates an enum mapping table names to table ids
genEnum :: DatalogProgram -> Doc
genEnum d =
  let relNames = M.keys $ progRelations d
      addToIds r = "idToName.put(TableId_" <> pp r <> ", \"" <> pp r <> "\");"
      addToNames r = "nameToId.put(\"" <> pp r <> "\", TableId_" <> pp r <> ");"
      makeTableId (t,i) = "public static final int TableId_" <> makeIdentifier t <+> "=" <+> pp i <> ";"
      indexedTables = zip relNames [(0::Int)..]
  in
      (vcat $ map makeTableId indexedTables) $+$
      "public static HashMap<Integer, String> idToName = new HashMap<Integer, String>();" $+$
      "public static HashMap<String, Integer> nameToId = new HashMap<String, Integer>();" $+$
      "static" $+$ (braces' $ vcat $ (map addToIds relNames) ++ (map addToNames relNames))

-- given a relation name this creates a function to construct a DDlogRecord that can be inserted or
-- deleted in the relation
genCreate :: DatalogProgram -> String -> Doc
genCreate d relationName =
    let relations = progRelations d
        rel = fromJust $ M.lookup relationName relations
        rtype = relType rel in
    "public static DDlogRecord create_" <> makeIdentifier relationName <>
        (parens $ parameters d rtype) $+$
        (braces' $ genCreateBody d rtype $+$ "return r;")

-- convert a DDlog string into an identifier
makeIdentifier :: String -> Doc
makeIdentifier str = pp $ capitalize $ legalize str

-- capitalize the first letter of a string
capitalize :: String -> String
capitalize str = (toUpper (head str) : tail str)

-- convert a DDlog identifier (possibly including namespaces) into a Java identifier
legalize :: String -> String
legalize n = map legalizeChar n

-- convert characters that are illegal in Java identifiers into underscores
legalizeChar :: Char -> Char
legalizeChar c = if isAlphaNum c then c else '_'

-- generate parameters for the function that creates a DDlogRecord for a specific relation
-- t is the relation type
parameters :: DatalogProgram -> Type -> Doc
parameters d t =
    case t of
        TBool{..}   -> simpleType t <+> "a0"
        TInt{..}    -> simpleType t <+> "a0"
        TString{..} -> simpleType t <+> "a0"
        TSigned{..} -> simpleType t <+> "a0"
        TUser{..}   -> let rt = resolveType d t in
                         case rt of
                         CNone{..}   -> parameters d $ elementType
                         CVector{..} -> (collectionType rt) <> "a0"
        TTuple{..}  -> commaSep $ mapIdx (\t i -> (collectionType $ resolveType d t) <+> "a" <> pp i) typeTupArgs
        TStruct{..} -> let c0:tail = typeCons in
                       if null tail then
                           let fieldParam f = (collectionType $ resolveType d $ fieldType f) <+> (pp $ fieldName f) in
                               commaSep $ map fieldParam (consArgs c0)
                       else error $ "Unsupported type " ++ show t
        _           -> error $ "Unsupported type " ++ show t

-- convert a collection type to a Java type
collectionType :: Collection -> Doc
collectionType CVector{..} = "List<" <> (classType elementType) <> ">"
collectionType CSet{..} = "List<" <> (classType elementType) <> ">"
collectionType CNone{..} = simpleType elementType

-- classes corresponding to scalar Java types -- required for template arguments
classType :: Type -> Doc
classType TBool{..} = "Boolean"
classType t@TBit{..} = case show $ simpleType t of
  "int" ->   "Integer"
  "short" -> "Short"
  "long" ->  "Long"
classType t@TSigned{..} = case show $ simpleType t of
  "int" ->   "Integer"
  "short" -> "Short"
  "long" ->  "Long"
classType t = simpleType t

-- convert a simple type to a Java type
simpleType :: Type -> Doc
simpleType TBool{..} = "boolean"
simpleType TInt{..} = "BigInt"
simpleType TString{..} = "String"
simpleType TSigned{..} | typeWidth == 16 = "short"
simpleType TSigned{..} | typeWidth == 32 = "int"
simpleType TSigned{..} | typeWidth == 64 = "long"
-- the mapping of unsigned values is not really correct if the value overflows
-- but Java does not really support unsigned types, so any mapping will be a compromise
simpleType TBit{..}    | typeWidth == 16 = "short"
simpleType TBit{..}    | typeWidth == 32 = "int"
simpleType TBit{..}    | typeWidth == 64 = "long"
simpleType t = error $ "Unsupported type " ++ show t

-- given a simple type returns the accessor for a DDlogRecord to extract a field of this type
getRecord :: Type -> Doc
getRecord TBool{..}   = "getBoolean()"
getRecord TInt{..}    = "getU128()"
getRecord TString{..} = "getString()"
getRecord TSigned{..} = "getLong()"
getRecord TBit{..}    = "getLong()"
getRecord t = error $ "Unsupported type " ++ show t

-- This type describes collections of base types.
-- Currently collections of collections are not supported.
data Collection = CNone   { elementType:: Type }   -- not a collection at all
                | CVector { elementType:: Type }
                | CSet    { elementType:: Type }

instance Show Collection where
    show = render . pp

instance PP Collection where
    pp (CNone t) = pp t
    pp (CVector t) = "Vector<" <> (pp t) <> ">"
    pp (CSet t)    = "Set<" <> (pp t) <> ">"

-- resolves a TypeDef to its underlying type (which is a Collection description)
resolveType :: DatalogProgram -> Type -> Collection
resolveType d t =
    -- trace (show t) $
    case t of
        -- it is arguably wrong to hardwire these type names in the compiler, but we assume that
        -- they are more or less standard types.  Maybe the compiler should know about them?
        TUser{..} -> if typeName == "std.Ref"
                     then resolveType d $ head typeArgs
                     else if elem typeName ["std.Set", "tinyset.Set64"] then
                         let inner = resolveType d $ head typeArgs in
                         case inner of
                              CNone elem   -> CSet elem
                              _            -> error $ "Nested collections not supported: " ++ show t
                     else if typeName == "std.Vector" then
                         let inner = resolveType d $ head typeArgs in
                         case inner of
                              CNone elem   -> CVector elem
                              _            -> error $ "Nested collections not supported: " ++ show t
                     else resolveType d actual
                          where typeDef = getType d typeName
                                maybeActual = tdefType typeDef
                                actual = case maybeActual of
                                     Nothing        -> error $ "Extern type not supported: " ++ show t
                                     Just something -> something
        _         -> CNone t

-- generate code to create a DDlogRecord from a collection-typed value
recordFromCollection :: String -> String -> Collection -> Doc
recordFromCollection destination value valueType =
    let ds v = v ++ (capitalize destination)
        makeCollection creator =
          (pp $ (ds "DDlogRecord[] components") ++ " = new DDlogRecord[" ++ value ++ ".size()];") $+$
                       (pp $ "for (int i = 0; i < " ++ value ++ ".size(); i++)") $+$
                       (nest' $ pp $ (ds "components") ++ "[i] = new DDlogRecord(" ++ value ++ ".get(i));") $+$
                       (pp $ "DDlogRecord " ++ destination ++ " = DDlogRecord." ++ creator ++ "(" ++ (ds "components") ++ ");") in
    case valueType of
        CNone{..}   -> (pp $ "DDlogRecord " ++ destination ++ " = new DDlogRecord(" ++ value ++ ");")
        CVector{..} -> makeCollection "makeVector"
        CSet{..}    -> makeCollection "makeSet"

-- generates the body of a function which creates a DDlogRecord from an object
genCreateBody :: DatalogProgram -> Type -> Doc
genCreateBody d t =
    case t of
        TBool{..}   -> "DDlogRecord r = new DDlogRecord(a0);"
        TInt{..}    -> "DDlogRecord r = new DDlogRecord(a0);"
        TBit{..}    -> "DDlogRecord r = new DDlogRecord(a0);"
        TString{..} -> "DDlogRecord r = new DDlogRecord(a0);"
        TSigned{..} -> "DDlogRecord r = new DDlogRecord(a0);"
        TUser{..}   -> let rt = resolveType d t in
                         case rt of
                         CNone{..}   -> genCreateBody d $ elementType
                         _           -> recordFromCollection "r" "a0" rt
        TTuple{..}  -> (vcat $ mapIdx (\t i -> recordFromCollection ("r" ++ show i) ("a" ++ show i) (resolveType d t)) typeTupArgs) $+$
                       "DDlogRecord[] a = " <> (braces $ commaSep $ mapIdx (\_ i -> "r" <> pp i)
                                                                           typeTupArgs) <> semi $$
                       "DDlogRecord r = DDlogRecord.makeTuple(a);"
        TStruct{..} -> let c0:tail = typeCons in
                       if null tail then
                         let fieldRec f = recordFromCollection ("r" ++ (fieldName f)) (fieldName f) (resolveType d $ fieldType f) in
                           (vcat $ map fieldRec (consArgs c0)) $$
                         "DDlogRecord[] a = " <> (braces $ commaSep $ map (pp . ("r" ++) . fieldName) (consArgs c0)) <> semi $$
                         "DDlogRecord r = DDlogRecord.makeStruct" <> (parens $ commaSep [doubleQuotes $ pp $ consName c0, "a"]) <> semi
                         -- we do not support multiple constructors
                       else error $ "Unsupported type " ++ show t
        _           -> error $ "Unsupported type " ++ show t
