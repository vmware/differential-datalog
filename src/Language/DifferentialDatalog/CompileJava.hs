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
    pp ("import java.util.*;" :: String) $+$
    pp ("import ddlogapi.DDlogRecord;\n" :: String) $+$
    pp ("import ddlogapi.DDlogCommand;\n" :: String) $+$
    pp ("class " ++ capitalize sourceName)

-- Generates the body of the Java class containing all utility methods
body :: DatalogProgram -> Doc
body d = genEnum d $+$
          (vcat $ punctuate "\n" $ map (genRecord d) (M.keys $ progRelations d)) $+$
          (vcat $ punctuate "\n" $ map (genClass d) (M.keys $ progRelations d))

-- Generate a Java class for an element of a specific relation
genClass :: DatalogProgram -> String -> Doc
genClass d relationName =
    let relations = progRelations d
        rel = fromJust $ M.lookup relationName relations
        rtype = relType rel in
      pp ("public static class " ++ makeIdentifier relationName) $+$
        (braces' $ classBody d relationName rtype)

-- given a (relation) type generate the body of a Java class storing the columns of the relation
classBody :: DatalogProgram -> String -> Type -> Doc
classBody d className t =
  -- class fields
  (classFields d t) $+$
  -- constructor from DDlogRecord
  ("public" <+> (pp className) <> (parens "DDlogRecord r") $+$ (braces' $ constructorBody d t)) $+$
  -- constructor from fields
  ("public" <+> (pp className) <> (parens $ parameters d t) $+$ (braces' $ constructorFieldsBody d t)) $+$
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
        TUser{..}   -> constructorFieldsBody d (resolveType d t)
        TTuple{..}  -> vcat $ map (\(i,t) ->
                                     pp ("this.a" ++ (show i) ++ " = a" ++ (show i) ++ "") <> semi) (zip [0..] typeTupArgs)
        TStruct{..} -> let c0:tail = typeCons in
                       if tail == [] then
                           let assignField f =
                                 ("this." <> (pp $ fieldName f)) <+> "=" <+> (pp $ fieldName f) <> semi
                           in
                               vcat $ map assignField $ consArgs c0
                       else errorWithoutStackTrace $ "Unsupported type " ++ show t
        _           -> errorWithoutStackTrace $ "Unsupported type " ++ show t

-- given a relation type generate the body of the toString method
toStringBody :: DatalogProgram -> Type -> Doc
toStringBody d t =
    case t of
        TBool{..}   -> "this.a0"
        TInt{..}    -> "this.a0"
        TString{..} -> "this.a0"
        TSigned{..} -> "this.a0"
        TBit{..}     -> "this.a0"
        TUser{..}   -> toStringBody d (resolveType d t)
        TTuple{..}  -> (cat $ punctuate " + \",\" + " (map (\i -> pp ("this.a" ++ (show i))) [0.. (length typeTupArgs)]))
        TStruct{..} -> let c0:tail = typeCons in
                       if tail == [] then
                           let toStringField f =
                                 ("this." <> (pp $ fieldName f)) <> ".toString()"
                           in
                               (cat $ punctuate " + \",\" + " (map toStringField (consArgs c0)))
                       else errorWithoutStackTrace $ "Unsupported type " ++ show t
        _           -> errorWithoutStackTrace $ "Unsupported type " ++ show t

-- given a relation type generate the body of the constructor that
-- initializes the corresponding Java class fields
constructorBody :: DatalogProgram -> Type -> Doc
constructorBody d t =
    case t of
        TBool{..}   -> "this.a0 = r." <> getRecord t <> semi
        TInt{..}    -> "this.a0 = r." <> getRecord t <> semi
        TString{..} -> "this.a0 = r." <> getRecord t <> semi
        TSigned{..} -> "this.a0 = r." <> getRecord t <> semi
        TBit{..}    -> "this.a0 = r." <> getRecord t <> semi
        TUser{..}   -> constructorBody d (resolveType d t)
        TTuple{..}  -> vcat $ map (\(i,t) ->
                                     pp ("this.a" ++ (show i) ++ " = r.getStructField(" ++ (show i) ++ ").") <+>
                                     (getRecord $ resolveType d t) <> semi) (zip [0..] typeTupArgs)
        TStruct{..} -> let c0:tail = typeCons in
                       if tail == [] then
                           let assignField (i,f) =
                                 ("this." <> (pp $ fieldName f)) <+> "= r.getStructField" <> (parens $ pp (i::Int)) <> "." <>
                                 (getRecord $ resolveType d $ fieldType f) <> semi
                           in
                               vcat $ map assignField (zip [0..] (consArgs c0))
                       else errorWithoutStackTrace $ "Unsupported type " ++ show t
        _           -> errorWithoutStackTrace $ "Unsupported type " ++ show t

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
        TUser{..}   -> arguments d (resolveType d t)
        TTuple{..}  -> commaSep $ map (\i -> pp ("this.a" ++ (show i))) [0 .. length typeTupArgs]
        TStruct{..} -> let c0:tail = typeCons
                           printField p = "this." <> (pp $ fieldName p)
                       in
                       if tail == [] then
                           commaSep $ map printField (consArgs c0)
                       else errorWithoutStackTrace $ "Unsupported type " ++ show t
        _           -> errorWithoutStackTrace $ "Unsupported type " ++ show t


-- given a (relation) type generate the fields of a clas storing the columns of the relation
classFields :: DatalogProgram -> Type -> Doc
classFields d t =
    case t of
        TBool{..}   -> "public" <+> simpleType t <+> "a0" <> semi
        TInt{..}    -> "public" <+> simpleType t <+> "a0" <> semi
        TBit{..}    -> "public" <+> simpleType t <+> "a0" <> semi
        TString{..} -> "public" <+> simpleType t <+> "a0" <> semi
        TSigned{..} -> "public" <+> simpleType t <+> "a0" <> semi
        TUser{..}   -> classFields d (resolveType d t)
        TTuple{..}  -> vcat $ map (\(i,t) -> ("public" <+> (simpleType $ resolveType d t)) <+>
                                    pp ("a" ++ (show i) ++ ";")) (zip [0..] typeTupArgs)
        TStruct{..} -> let c0:tail = typeCons in
                       if tail == [] then
                           let fieldField f = ("public" <+> (simpleType $ resolveType d $ fieldType f)) <+>
                                 (pp $ fieldName f) <> semi in
                               vcat $ map fieldField (consArgs c0)
                       else errorWithoutStackTrace $ "Unsupported type " ++ show t
        _           -> errorWithoutStackTrace $ "Unsupported type " ++ show t

-- generates an enum mapping table names to table ids
genEnum :: DatalogProgram -> Doc
genEnum d =
  let relNames = map fst (M.toList $ progRelations d)
      addToIds r = pp $ "idToName.put(TableId_" ++ r ++ ", \"" ++ r ++ "\");"
      addToNames r = pp $ "nameToId.put(\"" ++ r ++ "\", TableId_" ++ r ++ ");"
      makeTableId (t,i) = pp $ "public static final int TableId_" ++ (makeIdentifier t) ++ " = " ++ show(i) ++ ";"
      indexedTables = zip relNames [0..]
  in
      (vcat $ map makeTableId indexedTables) $+$
      "public static HashMap<Integer, String> idToName = new HashMap<Integer, String>();" $+$
      "public static HashMap<String, Integer> nameToId = new HashMap<String, Integer>();" $+$
      "static" $+$ (braces' $ vcat $ (map addToIds relNames) ++ (map addToNames relNames))

-- given a relation name this creates a function to construct a DDlogRecord that can be inserted or
-- deleted in the relation
genRecord :: DatalogProgram -> String -> Doc
genRecord d relationName =
    let relations = progRelations d
        rel = fromJust $ M.lookup relationName relations
        rtype = relType rel in
      pp ("public static DDlogRecord create_" ++ makeIdentifier relationName) <>
        (parens $ parameters d rtype) $+$
        (braces' $ genRecordBody d rtype $+$ "return r;")

-- convert a DDlog string into an identifier
makeIdentifier :: String -> String
makeIdentifier str = capitalize $ legalize str

-- capitalize the first letter of a string
capitalize :: String -> String
capitalize str = (toUpper (head str) : tail str)

-- convert a DDlog identifier (possibly including namespaces) into a Java identifier
legalize :: String -> String
legalize n = map legalizeChar n

-- convert characters that are illegal in Java identifiers into underscores
legalizeChar :: Char -> Char
legalizeChar c = if isAlpha c then c else '_'

-- generate parameters for the function that creates a DDlogRecord for a specific relation
-- t is the relation type
parameters :: DatalogProgram -> Type -> Doc
parameters d t =
    case t of
        TBool{..}   -> simpleType t <+> "a0"
        TInt{..}    -> simpleType t <+> "a0"
        TString{..} -> simpleType t <+> "a0"
        TSigned{..} -> simpleType t <+> "a0"
        TUser{..}   -> parameters d (resolveType d t)
        TTuple{..}  -> commaSep $ map (\(i,t) -> (simpleType $ resolveType d t) <+> pp ("a" ++ (show i))) (zip [0..] typeTupArgs)
        TStruct{..} -> let c0:tail = typeCons in
                       if tail == [] then
                           let fieldParam f = (simpleType $ resolveType d $ fieldType f) <+> (pp $ fieldName f) in
                               commaSep $ map fieldParam (consArgs c0)
                       else errorWithoutStackTrace $ "Unsupported type " ++ show t
        _           -> errorWithoutStackTrace $ "Unsupported type " ++ show t

-- convert a simple type to a Java type
simpleType :: Type -> Doc
simpleType TBool{..} = "boolean"
simpleType TInt{..} = "BigInt"
simpleType TString{..} = "String"
simpleType TSigned{..} | typeWidth == 16 = "short"
simpleType TSigned{..} | typeWidth == 32 = "int"
simpleType TSigned{..} | typeWidth == 64 = "long"
-- for unsigned values we use the next largest value.
-- that is not entirely correct, since truncation will behave differently
simpleType TBit{..}    | typeWidth == 16 = "int"
simpleType TBit{..}    | typeWidth == 32 = "long"
simpleType t = errorWithoutStackTrace $ "Unsupported type " ++ show t

-- given a simple type returns the accessor for a DDlogRecord to extract a field of this type
getRecord :: Type -> Doc
getRecord TBool{..}   = "getBoolean()"
getRecord TInt{..}    = "getU128()"
getRecord TString{..} = "getString()"
getRecord TSigned{..} = "getLong()"
getRecord TBit{..}    = "getLong()"
getRecord t = errorWithoutStackTrace $ "Unsupported type " ++ show t

-- resolves a TypeDef to its underlying type
resolveType :: DatalogProgram -> Type -> Type
resolveType d t =
    -- trace (show t) $
    case t of
        TUser{..} -> resolveType d actual
                     where typeDef = (getType d typeName)
                           maybeActual = tdefType typeDef
                           actual = case maybeActual of
                                Nothing -> errorWithoutStackTrace $ "Extern type not supported: " ++ show t
                                Just something -> something
        _         -> t

-- generates the body of a function which creates a DDlogRecord from an object
genRecordBody :: DatalogProgram -> Type -> Doc
genRecordBody d t =
    case t of
        TBool{..}   -> "DDlogRecord r = new DDlogRecord(a0);"
        TInt{..}    -> "DDlogRecord r = new DDlogRecord(a0);"
        TBit{..}    -> "DDlogRecord r = new DDlogRecord(a0);"
        TString{..} -> "DDlogRecord r = new DDlogRecord(a0);"
        TSigned{..} -> "DDlogRecord r = new DDlogRecord(a0);"
        TUser{..}   -> genRecordBody d (resolveType d t)
        TTuple{..}  -> (vcat $ map (\i -> pp $ "DDlogRecord r" ++ (show i) ++ " = new DDlogRecord(a" ++ (show i) ++ ")")
                                   [0..(length typeTupArgs)]) $+$
                       "DDlogRecord[] a = " <> (braces $ commaSep $ map (\i -> pp $ "r" ++ (show i))
                                                                        [0..(length typeTupArgs)]) <> semi $$
                       "DDlogRecord r = DDlogRecord.makeTuple(a);"
        TStruct{..} -> let c0:tail = typeCons in
                       if tail == [] then
                         let fieldRec f = "DDlogRecord r" <> (pp $ fieldName f) <+>
                               "= new DDlogRecord" <> (parens $ pp $ fieldName f) <> semi in
                           (vcat $ map fieldRec (consArgs c0)) $$
                         "DDlogRecord[] a = " <> (braces $ commaSep $ map (pp . ((++) "r") . fieldName) (consArgs c0)) <> semi $$
                         "DDlogRecord r = DDlogRecord.makeStruct" <> (parens $ commaSep [doubleQuotes $ pp $ consName c0, "a"]) <> semi
                         -- we do not support multiple constructors
                       else errorWithoutStackTrace $ "Unsupported type " ++ show t
        _           -> errorWithoutStackTrace $ "Unsupported type " ++ show t
