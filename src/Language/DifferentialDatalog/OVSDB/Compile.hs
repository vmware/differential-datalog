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

{-# LANGUAGE RecordWildCards, LambdaCase, FlexibleContexts, OverloadedStrings, QuasiQuotes #-}

{- |
Module     : OVSDB.Compile
Description: Compile 'OVSDB schema' to DDlog.
-}

module Language.DifferentialDatalog.OVSDB.Compile (compileSchema, compileSchemaFile) where

import qualified Data.Map as M
import Text.PrettyPrint
import Text.RawString.QQ
import Data.Either
import Data.Word
import Data.Maybe
import Data.Char
import Data.List

import Language.DifferentialDatalog.OVSDB.Parse
import Language.DifferentialDatalog.Syntax
import Language.DifferentialDatalog.Parse
import Language.DifferentialDatalog.Util
import Language.DifferentialDatalog.Name
import Language.DifferentialDatalog.Pos
import Language.DifferentialDatalog.PP
import Control.Monad.Except


data TableKind = TableInput
               | TableRealized
               | TableOutput
               | TableDeltaPlus
               | TableDeltaMinus
               | TableDeltaUpdate
               deriving(Eq)

builtins :: String
builtins = [r|import types
|]

compileSchemaFile :: FilePath -> [String] -> M.Map String [String] -> IO Doc
compileSchemaFile fname outputs keys = do
    content <- readFile fname
    schema <- case parseSchema content fname of
                   Left  e    -> errorWithoutStackTrace $ "Failed to parse input file: " ++ e
                   Right prog -> return prog
    case compileSchema schema outputs keys of
         Left e    -> errorWithoutStackTrace e
         Right doc -> return doc

compileSchema :: (MonadError String me) => OVSDBSchema -> [String] -> M.Map String [String] -> me Doc
compileSchema schema outputs keys = do
    let tables = schemaTables schema
    mapM_ (\o -> do let t = find ((==o) . name) tables
                    when (isNothing t) $ throwError $ "Table " ++ o ++ " not found") outputs
    uniqNames ("Multiple declarations of table " ++ ) tables
    rels <- vcat <$> mapM (\t -> mkTable schema (notElem (name t) outputs) t (M.lookup (name t) keys)) tables
    return $ pp builtins $+$ rels

mkTable :: (MonadError String me) => OVSDBSchema -> Bool -> Table -> Maybe [String] -> me Doc
mkTable schema isinput t@Table{..} keys = do
    ovscols <- tableGetCols t
    uniqNames (\col -> "Multiple declarations of column " ++ col ++ " in table " ++ tableName) ovscols
    if isinput
       then mkTable' schema TableInput t keys
       else do realized     <- mkTable' schema TableRealized    t keys
               delta_plus   <- mkTable' schema TableDeltaPlus   t keys
               delta_minus  <- mkTable' schema TableDeltaMinus  t keys
               delta_update <- mkTable' schema TableDeltaUpdate t keys
               output       <- mkTable' schema TableOutput      t keys
               return $ output       $+$
                        delta_plus   $+$
                        delta_minus  $+$
                        (if isJust keys then delta_update else empty) $+$
                        realized

mkTable' :: (MonadError String me) => OVSDBSchema -> TableKind -> Table -> Maybe [String] -> me Doc
mkTable' schema tkind t@Table{..} keys = do
    ovscols <- tableGetCols t
    -- uuid-name is only needed in an output if the table is referenced by another table in the schema
    referenced <- tableIsReferenced schema $ name t
    let uuidcol = case tkind of
                       TableInput       -> ["_uuid: uuid"]
                       TableRealized    -> ["_uuid: uuid"]
                       TableOutput      -> if referenced then ["uuid_name: string"] else []
                       TableDeltaPlus   -> ["uuid_name: string"]
                       TableDeltaMinus  -> ["_uuid: uuid"]
                       TableDeltaUpdate -> ["_uuid: uuid"]
    let prefix = case tkind of
                      TableInput    -> "input"
                      TableRealized -> "input"
                      _             -> empty
    let tname = case tkind of
                     TableInput       -> pp $ name t
                     TableRealized    -> "Realized_" <> (pp $ name t)
                     TableOutput      -> pp $ name t
                     TableDeltaPlus   -> "DeltaPlus_" <> (pp $ name t)
                     TableDeltaMinus  -> "DeltaMinus_" <> (pp $ name t)
                     TableDeltaUpdate -> "Update_" <> (pp $ name t)
    let cols = case tkind of
                    TableDeltaUpdate | isJust keys
                                     -> filter (\col -> notElem (name col) $ fromJust keys) ovscols
                                     | otherwise
                                     -> []
                    TableDeltaMinus  -> []
                    _                -> ovscols
    columns <- mapM (mkCol schema tkind tableName) cols
    let key = if elem tkind [TableInput, TableRealized]
                 then "primary key (x) x._uuid"
                 else empty
    return $ prefix <+> "relation" <+> pp tname <+> "("            $$
             (nest' $ vcat $ punctuate comma $ uuidcol ++ columns) $$
             ")"                                                   $$
             key

mkCol :: (MonadError String me) => OVSDBSchema -> TableKind -> String -> TableColumn -> me Doc
mkCol schema tkind tname c@TableColumn{..} = do
    check (columnName /= "_uuid") (pos c) $ "Reserved column name _uuid in table " ++ tname
    check (notElem columnName ["uuid-name", "uuid_name"]) (pos c) $ "Reserved column name " ++ columnName ++ " in table " ++ tname
    check (not $ elem columnName __reservedNames) (pos c) $ "Illegal column name " ++ columnName ++ " in table " ++ tname
    t <- case columnType of
              ColumnTypeAtomic at  -> mkAtomicType at
              ColumnTypeComplex ct -> mkComplexType schema tkind ct
    return $ mkColName columnName <> ":" <+>
             (if tkind == TableDeltaUpdate then ("option_t<" <> t <> ">") else t)

__reservedNames = map (("__" ++) . map toLower) $ reservedNames

mkColName :: String -> Doc
mkColName x =
    if elem x' reservedNames
       then pp $ "__" ++ x'
       else pp x'
    where x' = map toLower x

mkAtomicType :: (MonadError String me) => AtomicType -> me Doc
mkAtomicType IntegerType{} = return "integer"
mkAtomicType (RealType p)  = err p "\"real\" types are not supported"
mkAtomicType BooleanType{} = return "bool"
mkAtomicType StringType{}  = return "string"
mkAtomicType UUIDType{}    = return "uuid"

mkComplexType :: (MonadError String me) => OVSDBSchema -> TableKind -> ComplexType -> me Doc
mkComplexType schema tkind t@ComplexType{..} = do
    let min = maybe 1 id minComplexType
        max = maybe 1
                    (\case
                      Some x    -> x
                      Unlimited -> fromIntegral (maxBound::Word64))
                    maxComplexType
    check (max >= min) (pos t) $ "min bound exceeds max bound"
    check (min == 0 || min == 1) (pos t) $ "min bound must be 0 or 1"
    check (max > 0) (pos t) $ "max bound must be greater than 0"
    check (max /= 1 || isNothing valueComplexType) (pos t)
          $ "Cannot handle key-value pairs when max bound is 1"
    key <- mkBaseType tkind keyComplexType
    case (min, max) of
         (1,1) -> return key
         (0,1) -> return $ "option_t<" <> key <> ">"
         _     -> do
             case valueComplexType of
                  Nothing -> return $ "Set<" <> key <> ">"
                  Just v  -> do vt <- mkBaseType tkind v
                                return $ "Map<" <> key <> "," <> vt <> ">"

mkBaseType :: (MonadError String me) => TableKind -> BaseType -> me Doc
mkBaseType _     (BaseTypeSimple at)   = mkAtomicType at
mkBaseType tkind (BaseTypeComplex cbt) | isJust (refTableBaseType cbt) && tkind == TableOutput
                                       = return "string"
mkBaseType tkind (BaseTypeComplex cbt) | isJust (refTableBaseType cbt) && elem tkind [TableDeltaPlus, TableDeltaUpdate]
                                       = return "uuid_or_string_t"
mkBaseType _     (BaseTypeComplex cbt) = mkAtomicType $ typeBaseType cbt

tableGetCols :: (MonadError String me) => Table -> me [TableColumn]
tableGetCols t@Table{..} = do
    let tprops = filter (\case
                          ColumnsProperty{} -> True
                          _                 -> False) tableProperties
    check (not $ null tprops) (pos t) $ "Table " ++ tableName ++ " does not have a \"columns\" property"
    check (length tprops == 1) (pos t) $ "Table " ++ tableName ++ " has multiple \"columns\" properties"
    let (ColumnsProperty ovscols) : _ = tprops
    return ovscols

-- Is table referenced by at least one other table in the schema?
tableIsReferenced :: (MonadError String me) => OVSDBSchema -> String -> me Bool
tableIsReferenced schema tname =
    or <$> (mapM (\t' -> do cols <- tableGetCols t'
                            any ((\case
                                   ColumnTypeComplex ct -> isref (keyComplexType ct) ||
                                                           maybe False isref (valueComplexType ct)
                                   _ -> False) . columnType)
                                <$> tableGetCols t')
            $ schemaTables schema)
    where
    isref (BaseTypeComplex cbt) = refTableBaseType cbt == Just tname
    isref _ = False
