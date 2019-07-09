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
    compileJavaBindings
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
import Language.DifferentialDatalog.FlatBuffer

compileJavaBindings :: DatalogProgram -> String -> FilePath -> IO ()
compileJavaBindings prog specname dir = do
    let java_dir = dir </> "java"
    createDirectoryIfMissing True java_dir
    writeFile (java_dir </> specname <.> ".java") (render $ compileJava prog specname)
    writeFile (java_dir </> specname <.> ".fbs") (render $ compileFlatBufferSchema prog)


-- generate a Java program from a DDlog program
-- The Java program has some utility function to more easily create
-- records for the DDlog relations.  Note that Java does not support
-- all DDlog constructs
compileJava :: DatalogProgram -> String -> Doc
compileJava d sourceName = generateJava d sourceName 

-- Generate a Java document from a program given the source filename
generateJava :: DatalogProgram -> String -> Doc
generateJava d sourceName =
    preamble sourceName $+$
    (braces' $ body d)

-- Generates the preamble of a Java file
preamble :: String -> Doc
preamble sourceName =
    "import java.util.*;"             $+$
    "import ddlogapi.DDlogRecord;"    $+$
    "import ddlogapi.DDlogCommand;\n" $+$
    "class" <+> pp (capitalize sourceName)

data JavaState = JavaState {
    -- types that have been compiled
    typesCompiled  :: S.Set String,
    -- types that still have to be compiled
    typesToCompile :: M.Map String Type
}

type JavaMonad = State JavaState
emptyState :: JavaState
emptyState = JavaState {
    typesCompiled  = S.empty,
    typesToCompile = M.empty
}

data TypeKind = TypeRelation      -- Type used in a relation
              | TypeInputRelation -- Type used in an input relation
              | TypeOther         -- Another kind of type
              deriving(Eq)

-- Generates the body of the Java class containing all utility methods
body :: DatalogProgram -> Doc
body d =
    evalState (
        do relations <- mapM (genRelationCreate d) (M.keys $ progRelations d)
           objects <- generate d
           return $ genEnum d $+$
                 (vcat $ punctuate "\n" relations) $+$
                 (vcat $ punctuate "\n" $ map (genRelationClass d) (M.keys $ progRelations d)) $+$
                 (vcat $ punctuate "\n" objects)
    )
    emptyState

-- generates one class and a factory method for each type used
generate :: DatalogProgram -> JavaMonad [Doc]
generate d = do
    todo <- gets (\state -> M.toList $ typesToCompile state)
    if todo == [] then return []
    else do
        creates <- mapM (\(n,t) -> genCreate d n t) todo -- may add elements to typesToCompile
        let types = map (\(n,t) -> genClass d n t TypeOther) todo
        -- remove the types that we have generated from the list
        modify (\state -> state { typesToCompile = M.difference (typesToCompile state) (M.fromList todo) })
        -- recursive call for the remaining typesToCompile
        rest <- generate d
        return $ creates ++ types ++ rest

-- Generate a Java class for a specific relation
genRelationClass :: DatalogProgram -> String -> Doc
genRelationClass d@DatalogProgram{..} relationName =
    let relation = progRelations M.! relationName
        rtype = relType relation in
    genClass d relationName rtype $ if (relRole relation == RelInput) then TypeInputRelation else TypeRelation

-- Generate a Java class for a specific type
genClass :: DatalogProgram -> String -> Type -> TypeKind -> Doc
genClass d@DatalogProgram{} name rtype typeKind =
    "public static class" <+> makeIdentifier name $+$
    (braces' $ classBody d name rtype typeKind)

-- given a type generate the body of a Java class storing the columns of the relation
classBody :: DatalogProgram -> String -> Type -> TypeKind -> Doc
classBody d className t typeKind =
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
  if typeKind == TypeInputRelation then
      "public DDlogCommand createCommand(boolean insert)" $+$
      (braces' $ ("return new DDlogCommand(insert ? DDlogCommand.Kind.Insert : DDlogCommand.Kind.DeleteVal," $+$
              (pp $ "TableId_" ++ className ++ ",") $+$
              "this.asRecord());"))
  else empty

-- given a TStruct extract the first constructor if it is the only one;
-- report an error otherwise.  Thsi is because we don't support (yet) structs
-- with multiple constructors
getSingleConstructor :: Type -> Constructor
getSingleConstructor t@TStruct{..} =
    let c0:rest = typeCons in
       if null rest then c0
       else error $ "Types with multiple constructors not supported" ++ show t

-- generates the body of the constructor from values for all fields
constructorFieldsBody :: DatalogProgram -> Type -> Doc
constructorFieldsBody d t =
    case t of
        TBool{..}     -> "this.a0 = a0;"
        TInt{..}      -> "this.a0 = a0;"
        TString{..}   -> "this.a0 = a0;"
        TSigned{..}   -> "this.a0 = a0;"
        TBit{..}      -> "this.a0 = a0;"
        TTuple{..}    -> vcat $ mapIdx (\_ i -> "this.a" <> pp i <+> "= a" <> pp i <> semi) typeTupArgs
        -- we do not really do anything different for collections, so we can just use elementType
        TUser{..}     -> constructorFieldsBody d $ elementType $ resolveType d t
        t@TStruct{..} -> let c0 = getSingleConstructor t
                             assignField f = "this." <> (pp $ fieldName f) <+> "=" <+> (pp $ fieldName f) <> semi
                       in vcat $ map assignField $ consArgs c0
        _             -> error $ "Unsupported type " ++ show t

-- given a relation type generate the body of the toString method
toStringBody :: DatalogProgram -> Type -> Doc
toStringBody d t =
    case t of
        TBool{..}     -> "this.a0"
        TInt{..}      -> "this.a0"
        TString{..}   -> "this.a0"
        TSigned{..}   -> "this.a0"
        TBit{..}      -> "this.a0"
        -- we don't do anything different for collections, so we can just use elementType
        TUser{..}     -> toStringBody d $ elementType $ resolveType d t
        TTuple{..}    -> (cat $ punctuate " + \",\" + " (mapIdx (\_ i -> "this.a" <> pp i) typeTupArgs))
        t@TStruct{..} -> let c0 = getSingleConstructor t
                             toStringField f = ("this." <> (pp $ fieldName f))
                         in
                             (cat $ punctuate " + \",\" + " (map toStringField (consArgs c0)))
        _             -> error $ "Unsupported type " ++ show t


-- generate code to create a a collection-typed value from a DDlogRecord
-- (remember that CNone is a collection too)
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
        CNone t@TStruct{} -> (pp destination) <+> "= new" <+> (pp $ capitalize $ consName $ getSingleConstructor t) <> (parens $ pp value) <> semi
        CNone t           -> (pp destination) <+> "=" <+> (parens $ simpleType t) <> (pp value) <> "." <> (getRecord t) <> semi
        CVector{}         -> extractCollection "Vector"
        CSet{}            -> extractCollection "Set"

-- given a relation type generate the body of the constructor that
-- initializes the corresponding Java class fields
constructorBody :: DatalogProgram -> Type -> Doc
constructorBody d t =
    case t of
        TBool{..}     -> "this.a0 = r." <> getRecord t <> semi
        TInt{..}      -> "this.a0 = (" <> (simpleType t) <> ")r." <> getRecord t <> semi
        TString{..}   -> "this.a0 = r." <> getRecord t <> semi
        TSigned{..}   -> "this.a0 = (" <> (simpleType t) <> ")r." <> getRecord t <> semi
        TBit{..}      -> "this.a0 = (" <> (simpleType t) <> ")r." <> getRecord t <> semi
        TUser{..}     -> let rt = resolveType d t in
                         case rt of
                         CNone{..} -> constructorBody d $ elementType
                         _         -> collectionFromRecord "this.a0" "r" rt
        TTuple{..}    -> vcat $ mapIdx (\ft i ->
                                        (pp $ "DDlogRecord t" ++ (show i) ++ " = r.getTupleField(" ++ (show i) ++ ");") $+$                                        collectionFromRecord ("this.a" ++ (show i)) ("t" ++ (show i)) (resolveType d ft)) typeTupArgs
        t@TStruct{..} -> let c0 = getSingleConstructor t in
                           (vcat $ mapIdx (\f i ->
                                             (pp $ "DDlogRecord t" ++ (show i) ++ " = r.getStructField(" ++ (show i) ++ ");") $+$
                                             collectionFromRecord ("this." ++ (fieldName f)) ("t" ++ show i) (resolveType d $ fieldType f)) (consArgs c0))
        _             -> error $ "Unsupported type " ++ show t

-- given a relation type generate arguments for the call to the create_CLASS method
-- supplying fields as arguments
arguments :: DatalogProgram -> Type -> Doc
arguments d t =
    case t of
        TBool{..}     -> "this.a0"
        TInt{..}      -> "this.a0"
        TBit{..}      -> "this.a0"
        TString{..}   -> "this.a0"
        TSigned{..}   -> "this.a0"
        -- we do not do anything special for collections, so we just use elementType
        TUser{..}     -> arguments d $ elementType $ resolveType d t
        TTuple{..}    -> commaSep $ mapIdx (\_ i -> "this.a" <> pp i) typeTupArgs
        t@TStruct{..} -> let c0 = getSingleConstructor t
                             printField p = "this." <> (pp $ fieldName p)
                         in commaSep $ map printField (consArgs c0)
        _             -> error $ "Unsupported type " ++ show t


-- given a (relation) type generate the fields of a clas storing the columns of the relation
classFields :: DatalogProgram -> Type -> Doc
classFields d t =
    case t of
        TBool{..}     -> "public" <+> simpleType t <+> "a0" <> semi
        TInt{..}      -> "public" <+> simpleType t <+> "a0" <> semi
        TBit{..}      -> "public" <+> simpleType t <+> "a0" <> semi
        TString{..}   -> "public" <+> simpleType t <+> "a0" <> semi
        TSigned{..}   -> "public" <+> simpleType t <+> "a0" <> semi
        TUser{..}     -> let rt = resolveType d t in
                            case rt of
                            CNone{..}   -> classFields d $ elementType
                            _           -> "public" <+> (collectionType $ resolveType d t) <+> "a0" <> semi
        TTuple{..}    -> vcat $ mapIdx (\t i -> "public" <+> (collectionType $ resolveType d t) <+>
                                              "a" <> pp i <> ";") typeTupArgs
        t@TStruct{..} -> let c0 = getSingleConstructor t
                             fieldField f = ("public" <+> (collectionType $ resolveType d $ fieldType f)) <+>
                                 (pp $ fieldName f) <> semi
                         in vcat $ map fieldField (consArgs c0)
        _             -> error $ "Unsupported type " ++ show t

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
genRelationCreate :: DatalogProgram -> String -> JavaMonad Doc
genRelationCreate d relationName =
    let rtype = relType $ fromJust $ M.lookup relationName $ progRelations d in
    genCreate d relationName rtype

-- given a type name and a type this creates a function to construct a
-- DDlogRecord that can be inserted or deleted in the relation
genCreate :: DatalogProgram -> String -> Type -> JavaMonad Doc
genCreate d name rtype = do
    done <- gets (\state -> S.member name $ typesCompiled state)
    if done then return empty
    else do modify (\state -> state { typesCompiled = S.insert name $ typesCompiled state})
            body <- genCreateBody d rtype
            return $ "public static DDlogRecord create_" <> makeIdentifier name <>
                     (parens $ parameters d rtype) $+$
                     (braces' $ body $+$ "return r;")

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
        TBool{..}     -> simpleType t <+> "a0"
        TInt{..}      -> simpleType t <+> "a0"
        TString{..}   -> simpleType t <+> "a0"
        TSigned{..}   -> simpleType t <+> "a0"
        TUser{..}     -> let rt = resolveType d t in
                         case rt of
                         CNone{..}   -> parameters d $ elementType
                         CVector{..} -> (collectionType rt) <> "a0"
        TTuple{..}    -> commaSep $ mapIdx (\t i -> (collectionType $ resolveType d t) <+> "a" <> pp i) typeTupArgs
        t@TStruct{..} -> let c0 = getSingleConstructor t
                             fieldParam f = (collectionType $ resolveType d $ fieldType f) <+> (pp $ fieldName f)
                         in commaSep $ map fieldParam (consArgs c0)
        _             -> error $ "Unsupported type " ++ show t

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
simpleType TBool{} = "boolean"
simpleType TInt{} = "BigInt"
simpleType TString{} = "String"
simpleType TSigned{..} | typeWidth == 8  = "byte"
simpleType TSigned{..} | typeWidth == 16 = "short"
simpleType TSigned{..} | typeWidth == 32 = "int"
simpleType TSigned{..} | typeWidth == 64 = "long"
-- the mapping of unsigned values is not really correct if the value overflows
-- but Java does not really support unsigned types, so any mapping will be a compromise
simpleType TBit{..}    | typeWidth == 8  = "byte"
simpleType TBit{..}    | typeWidth == 16 = "short"
simpleType TBit{..}    | typeWidth == 32 = "int"
simpleType TBit{..}    | typeWidth == 64 = "long"
simpleType TStruct{..} | length typeCons == 1 = pp $ capitalize $ consName $ head typeCons
simpleType t = error $ "Unsupported type " ++ show t

-- given a simple type returns the accessor for a DDlogRecord to extract a field of this type
getRecord :: Type -> Doc
getRecord TBool{}   = "getBoolean()"
getRecord TInt{}    = "getU128()"
getRecord TString{} = "getString()"
getRecord TSigned{} = "getLong()"
getRecord TBit{}    = "getLong()"
getRecord t = error $ "Unsupported type " ++ show t

isSimpleType :: Type -> Bool
isSimpleType TBool{} = True
isSimpleType TInt{} = True
isSimpleType TSigned{} = True
isSimpleType TBit{} = True
isSimpleType _ = False

-- This type describes collections of base types.
-- Currently collections of collections are not supported.
data Collection = CNone   { elementType:: Type }   -- not a collection at all
                | CVector { elementType:: Type }
                | CSet    { elementType:: Type }

instance Show Collection where
    show = render . pp

instance PP Collection where
    pp (CNone t) = pp t
    pp (CVector t) = "Vec<" <> (pp t) <> ">"
    pp (CSet t)    = "Set<" <> (pp t) <> ">"

-- convert a collection type to a Java type
collectionType :: Collection -> Doc
collectionType CVector{..} = "List<" <> (classType elementType) <> ">"
collectionType CSet{..} = "List<" <> (classType elementType) <> ">"
collectionType CNone{..} = simpleType elementType

-- resolves a TypeDef to its underlying type (which is a Collection description)
resolveType :: DatalogProgram -> Type -> Collection
resolveType d t =
    case t of
        -- it is arguably wrong to hardwire these type names in the compiler, but we assume that
        -- they are more or less standard types.  Maybe the compiler should know about them?
        TUser{typeName = "std.Ref", ..} -> resolveType d $ head typeArgs
        TUser{typeName = "std.Vec", ..} ->
            let inner = resolveType d $ head typeArgs in
            case inner of
                CNone elem -> if isSimpleType elem then CVector elem
                              else error $ "Only vectors of base types supported: " ++ show t
                _          -> error $ "Nested collections not supported: " ++ show t
        TUser{..} | elem typeName ["std.Set", "tinyset.Set64"] ->
            let inner = resolveType d $ head typeArgs in
            case inner of
                CNone elem -> if isSimpleType elem then CSet elem
                              else error $ "Only sets of base types supported: " ++ show t
                _          -> error $ "Nested collections not supported: " ++ show t
        TUser{..} ->
            resolveType d actual
            where typeDef = getType d typeName
                  maybeActual = tdefType typeDef
                  actual = case maybeActual of
                      Nothing        -> error $ "Extern type not supported: " ++ show t
                      Just something -> something
        _         -> CNone t

-- generate code to create a DDlogRecord from a collection-typed value
recordFromCollection :: DatalogProgram -> String -> String -> Collection -> JavaMonad Doc
recordFromCollection d destination value valueType =
    let ds v = v ++ (capitalize destination)
        makeCollection creator =
          (pp $ (ds "DDlogRecord[] components") ++ " = new DDlogRecord[" ++ value ++ ".size()];") $+$
                       (pp $ "for (int i = 0; i < " ++ value ++ ".size(); i++)") $+$
                       (nest' $ pp $ (ds "components") ++ "[i] = new DDlogRecord(" ++ value ++ ".get(i));") $+$
                       (pp $ "DDlogRecord " ++ destination ++ " = DDlogRecord." ++ creator ++ "(" ++ (ds "components") ++ ");") in
    case valueType of
        CNone t@TStruct{} -> let c = consName $ getSingleConstructor t in do
                             modify (\state -> state { typesToCompile = M.insert c t $ typesToCompile state})
                             return $ pp $ "DDlogRecord " ++ destination ++ " = " ++ value ++ ".asRecord();"
        CNone{}           -> return $ (pp $ "DDlogRecord " ++ destination ++ " = new DDlogRecord(" ++ value ++ ");")
        CVector{}         -> return $ makeCollection "makeVector"
        CSet{}            -> return $ makeCollection "makeSet"

-- generates the body of a function which creates a DDlogRecord from an object
genCreateBody :: DatalogProgram -> Type -> JavaMonad Doc
genCreateBody d t =
    case t of
        TBool{}     -> return "DDlogRecord r = new DDlogRecord(a0);"
        TInt{}      -> return "DDlogRecord r = new DDlogRecord(a0);"
        TBit{}      -> return "DDlogRecord r = new DDlogRecord(a0);"
        TString{}   -> return "DDlogRecord r = new DDlogRecord(a0);"
        TSigned{}   -> return "DDlogRecord r = new DDlogRecord(a0);"
        TUser{..}   -> let rt = resolveType d t in
                       case rt of
                           CNone{..}   -> genCreateBody d $ elementType
                           _           -> recordFromCollection d "r" "a0" rt
        TTuple{..}  -> let convertField (t,i) = recordFromCollection d ("r" ++ show i) ("a" ++ show i) (resolveType d t) in
                       do fields <- mapM convertField $ zip typeTupArgs [0..]
                          return $ (vcat fields) $+$
                            "DDlogRecord[] a = " <> (braces $ commaSep $ mapIdx (\_ i -> "r" <> pp i) typeTupArgs) <> semi $$
                            "DDlogRecord r = DDlogRecord.makeTuple(a);"
        t@TStruct{} -> let c0 = getSingleConstructor t
                           fieldRec f = recordFromCollection d ("r" ++ (fieldName f)) (fieldName f) (resolveType d $ fieldType f)
                       in do fields <- mapM fieldRec (consArgs c0)
                             return $ (vcat fields) $+$
                                    "DDlogRecord[] a = " <> (braces $ commaSep $ map (pp . ("r" ++) . fieldName) (consArgs c0)) <> semi $$
                                    "DDlogRecord r = DDlogRecord.makeStruct" <> (parens $ commaSep [doubleQuotes $ pp $ consName c0, "a"]) <> semi
        _           -> error $ "Unsupported type " ++ show t
