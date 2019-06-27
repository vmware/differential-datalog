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
-- all DDlog types
compileJava :: DatalogProgram -> String -> FilePath -> IO ()
compileJava d sourceName javaFile = do
    let content = generateJava d sourceName in
         writeFile javaFile (render content)

-- Generate a Java document from a program given the source filename
generateJava :: DatalogProgram -> String -> Doc
generateJava d sourceName =
    preamble sourceName $+$
    (braces' $ body d)

-- Generates the preamble of a Java file
preamble :: String -> Doc
preamble sourceName =
    pp ("package " ++ sourceName) $+$
    pp ("import java.util.*" :: String) $+$
    pp ("import DDlogRecord\n" :: String) $+$
    pp ("class " ++ capitalize sourceName)

-- Generates the body of the Java class containing all utility methods
body :: DatalogProgram -> Doc
body d = nest' $ genEnum d $+$
          (vcat $ punctuate "\n" $ map (genRecord d) (M.keys $ progRelations d))

-- generates an enum mapping table names to table ids
genEnum :: DatalogProgram -> Doc
genEnum d =
  let relNames = map fst (M.toList $ progRelations d)
      addToIds r = pp $ "idToName.put(TableIds." ++ r ++ ", \"" ++ r ++ "\");"
      addToNames r = pp $ "nameToId.put(\"" ++ r ++ "\", TableIds." ++ r ++ ");"
  in
  pp ("public static enum TableIds " :: String) $+$
  (braces' $ commaSep $ map (pp . makeIdentifier) relNames) $+$
  "public static HashMap<TableIds, String> idToName = new HashMap<TableIds, String>();" $+$
  "public static HashMap<String, TableIds> nameToId = new HashMap<String, TableIds>();" $+$
  "static" $+$ (braces' $ vcat $ (map addToIds relNames) ++ (map addToNames relNames))

-- given a relation name this creates a function to construct a DDlogRecord that can be inserted or
-- deleted in the relation
genRecord :: DatalogProgram -> String -> Doc
genRecord d relationName =
    let relations = progRelations d
        rel = fromJust $ M.lookup relationName relations
        rtype = relType rel in
      pp ("public static DDlogRecord create_" ++ makeIdentifier relationName) <>
        (parens $ arguments d rtype) $+$
        (braces' $ funcBody d rtype $+$ "return r;")

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

-- generate arguments
arguments :: DatalogProgram -> Type -> Doc
arguments d t =
    case t of
        TBool{..}   -> simpleType t <+> "a0"
        TInt{..}    -> simpleType t <+> "a0"
        TString{..} -> simpleType t <+> "a0"
        TSigned{..} -> simpleType t <+> "a0"
        TUser{..}   -> arguments d (resolveType d t)
        TTuple{..}  -> commaSep $ map (\(i,t) -> (simpleType $ resolveType d t) <+> pp ("a" ++ (show i))) (zip [0..] typeTupArgs)
        TStruct{..} -> let c0:tail = typeCons in
                       if tail == [] then
                           let fieldArg f = (simpleType $ resolveType d $ fieldType f) <+> (pp $ fieldName f) in
                               commaSep $ map fieldArg (consArgs c0)
                       -- we do not support multiple constructors
                       else errorWithoutStackTrace $ "Unsupported type " ++ show t
        -- Java has no support for unsigned, so we don't accept TBit
        _           -> errorWithoutStackTrace $ "Unsupported type " ++ show t

-- convert a simple type to a Java type
simpleType :: Type -> Doc
simpleType TBool{..} = "boolean"
simpleType TInt{..} = "BigInt"
simpleType TString{..} = "String"
simpleType TSigned{..} | typeWidth == 32 = "int"
simpleType TSigned{..} | typeWidth == 64 = "long"
simpleType t = errorWithoutStackTrace $ "Unsupported type " ++ show t

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

funcBody :: DatalogProgram -> Type -> Doc
funcBody d t =
    case t of
        TBool{..}   -> "DDlogRecord r = new DDlogRecord(a0);"
        TInt{..}    -> "DDlogRecord r = new DDlogRecord(a0);"
        TString{..} -> "DDlogRecord r = new DDlogRecord(a0);"
        TSigned{..} -> "DDlogRecord r = new DDlogRecord(a0);"
        TUser{..}   -> funcBody d (resolveType d t)
        TStruct{..} -> let c0:tail = typeCons in
                       if tail == [] then
                         let fieldRec f = "DDlogRecord r" <> (pp $ fieldName f) <+>
                               "= new DDlogRecord" <> (parens $ pp $ fieldName f) <> semi in
                           (vcat $ map fieldRec (consArgs c0)) $$
                         "DDlogRecord[] a = " <> (braces $ commaSep $ map (pp . fieldName) (consArgs c0)) <> semi $$
                         "DDlogRecord r = DDlogRecord.makeStruct" <> (parens $ commaSep [doubleQuotes $ pp $ consName c0, "a"]) <> semi
                         -- we do not support multiple constructors
                       else errorWithoutStackTrace $ "Unsupported type " ++ show t
        _           -> errorWithoutStackTrace $ "Unsupported type " ++ show t
