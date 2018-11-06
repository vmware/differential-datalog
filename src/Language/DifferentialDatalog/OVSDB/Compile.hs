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

{-# LANGUAGE RecordWildCards, LambdaCase, FlexibleContexts, OverloadedStrings, QuasiQuotes, ImplicitParams, TupleSections #-}

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

{-
The compiler generates 9 kinds of DDlog tables: Input, Output, Realized, Swizzled,
OutputProxy, Delta+, Delta-, DeltaUpdate, and UUIDMap:

Input: input tables that precisely replicate corresponding OVSDB tables

Output: output tables computed by the DDlog program.  They have the same
    structure as their OVSDB counterparts, except
    - all refTable columns have type 'string' instead of 'UUID'
    - tables that are not referenced by any other table do not have an id field
    - tables that are referenced by at least one other table have column
      'uuid_name' of type string

Realized: realized tables represent current state of output tables in OVSDB and
    precisely replicate the OVSDB table schema, including '_uuid' field

UUIDMap: these tables maintain mappings between 'uuid_name's in Output tables and
    actuall UUID's assigned by OVSDB. It is computed based on Output and Realized
    tables.

Swizzled: an output table with 'uuid_name's replaced with UUID's wherever the UUID
    is known (from the Realized table).  Uses 'uuid_or_string_t' types for all UUID
    fields. This table is computed based on Output and UUIDMap tables

OutputProxy: optional table used to perform additional computation on Swizzled and
    UUIDMap tables, e.g., to perform stable id allocation.  Has the same schema as
    Swizzled

Delta+: new records to be inserted to OVSDB.  Specifically, these are records
    from Swizzled such that a records with the same key does not exist in Realized.

Delta-: records to be deleted from OVSDB, i.e., records in Realized such that
    a record with the same key does not exist in Swizzled.  A row in the Delta- table
    only contains the '_uuid' field.

DeltaUpdate: records whose key exists in both Swizzled and Realized tables, but with
    different values.  The DeltaUpdate relation contans the '_uuid' field along with all
    fields that do not belong to the key.


Note that the user must only provide rules to compute Output relations based on Input
relations; the compiler generates rules to compute all other relations.

The following diagram illustrates dependencies among the various kinds of relations.


      (user-defined rules)                                  (user-defined rules)
Input--------------------> Output---------------->Swizzled------------------------>OutputProxy-------> Delta+ <---------|
                                                    ^                                   |                               |
                                                    |                                   |                               |
                                                    |                                   |                               |
Realized----------------> UUIDMap <------------------                                   |------------> Delta- <---------|
  |                                                                                     |                               |
  |                                                                                     |                               |
  |                                                                                     |------------> DeltaUpdate <-----
  |                                                                                                                     |
  |----------------------------------------------------------------------------------------------------------------------

-}

data TableKind = TableInput
               | TableOutput
               | TableOutputProxy
               | TableRealized
               | TableUUIDMap
               | TableOutputSwizzled
               | TableDeltaPlus
               | TableDeltaMinus
               | TableDeltaUpdate
               deriving(Eq)

builtins :: String
builtins = [r|import ovsdb
|]

mkTableName :: (?schema::OVSDBSchema, ?outputs::[(String, [String])], ?with_oproxies::[String]) => Table -> TableKind -> Doc
mkTableName t tkind =
    case tkind of
         TableInput       -> pp $ name t
         TableRealized    -> pp $ name t
         TableOutput      -> "Out_" <> (pp $ name t)
         TableOutputProxy    | tableNeedsOutputProxy t 
                          -> "OutProxy_" <> (pp $ name t)
                             | otherwise
                          -> mkTableName t TableOutputSwizzled
         TableOutputSwizzled | tableNeedsSwizzle t
                          -> "Swizzled_" <> (pp $ name t)
                             | otherwise
                          -> mkTableName t TableOutput
         TableDeltaPlus   -> "DeltaPlus_" <> (pp $ name t)
         TableDeltaMinus  -> "DeltaMinus_" <> (pp $ name t)
         TableDeltaUpdate -> "Update_" <> (pp $ name t)
         TableUUIDMap     -> "UUIDMap_" <> (pp $ name t)

tableNeedsSwizzle :: (?outputs::[(String, [String])]) => Table -> Bool
tableNeedsSwizzle t = any colIsRef $ tableGetNonROCols t

tableNeedsOutputProxy :: (?with_oproxies::[String]) => Table -> Bool
tableNeedsOutputProxy t = elem (name t) ?with_oproxies

-- Column is a key column
colIsKey :: (?outputs::[(String, [String])]) => Table -> TableColumn -> Maybe [String] -> Bool
colIsKey t c Nothing =
    elem (name c) $ map name $ tableGetNonROCols t
colIsKey t c (Just keys) =
    elem (name c) $ keys `intersect` (map name $ tableGetNonROCols t)

-- Column is a reference to an output table
-- (references to input tables are just regular values from DDlog perspective)
colIsRef :: (?outputs::[(String, [String])]) => TableColumn -> Bool
colIsRef c =
    case columnType c of
         ColumnTypeAtomic{}   -> False
         ColumnTypeComplex ct -> isref (keyComplexType ct) ||
                                 maybe False isref (valueComplexType ct)
    where
    isref (BaseTypeComplex cbt) =
        (isJust $ refTableBaseType cbt) &&
        isJust (lookup (fromJust $ refTableBaseType cbt) ?outputs)
    isref _ = False

-- column is of type set
colIsSet :: TableColumn -> Bool
colIsSet col =
    case columnType col of
         ColumnTypeAtomic{}   -> False
         ColumnTypeComplex ct -> let (min, max) = complexTypeBounds ct
                                 in max > min && isNothing (valueComplexType ct)

-- column is of type map
colIsMap :: TableColumn -> Bool
colIsMap col =
    case columnType col of
         ColumnTypeAtomic{}   -> False
         ColumnTypeComplex ct -> let (min, max) = complexTypeBounds ct
                                 in max > min && isJust (valueComplexType ct)

-- scalar column
colIsScalar :: TableColumn -> Bool
colIsScalar col =
    case columnType col of
         ColumnTypeAtomic{}   -> True
         ColumnTypeComplex ct -> let (min, max) = complexTypeBounds ct
                                 in max == min

-- table referenced by the column (or the key component of a set of map column)
colRefTable :: (?schema::OVSDBSchema, ?outputs::[(String, [String])]) => TableColumn -> Maybe String
colRefTable col =
    case columnType col of
         ColumnTypeAtomic{}   -> Nothing
         ColumnTypeComplex ct ->
             case keyComplexType ct of
                  BaseTypeComplex bt -> refTableBaseType bt
                  _ -> Nothing

-- table referenced by the value component of a map column
colRefTableVal :: (?schema::OVSDBSchema, ?outputs::[(String, [String])]) => TableColumn -> Maybe String
colRefTableVal col =
    case columnType col of
         ColumnTypeAtomic{}   -> Nothing
         ColumnTypeComplex ct ->
             case valueComplexType ct of
                  Just (BaseTypeComplex bt) -> refTableBaseType bt
                  _ -> Nothing

-- Convert uuid_or_string_t, Set<uuid_or_string_t> or Map<_, uuid_or_string_t>
-- to, respectively, uuid, Set<uuid>, Map<_,uuid>.
refCol2UUID :: TableColumn -> Doc -> Doc
refCol2UUID c n | colIsSet c  = "set_extract_uuids(" <> n <> ")"
                | colIsMap c  = "map_extract_val_uuids(" <> n <> ")"
                | otherwise   = "extract_uuid(" <> n <> ")"

compileSchemaFile :: FilePath -> [(String, [String])] -> [String] -> M.Map String [String] -> IO Doc
compileSchemaFile fname outputs with_oproxies keys = do
    content <- readFile fname
    schema <- case parseSchema content fname of
                   Left  e    -> errorWithoutStackTrace $ "Failed to parse input file: " ++ e
                   Right prog -> return prog
    case compileSchema schema outputs with_oproxies keys of
         Left e    -> errorWithoutStackTrace e
         Right doc -> return doc

compileSchema :: (MonadError String me) => OVSDBSchema -> [(String, [String])] -> [String] -> M.Map String [String] -> me Doc
compileSchema schema outputs with_oproxies keys = do
    let tables = schemaTables schema
    mapM_ (\(o, ro) -> do let t = find ((==o) . name) tables
                          when (isNothing t) $ throwError $ "Table " ++ o ++ " not found") outputs
    uniqNames ("Multiple declarations of table " ++ ) tables
    let ?schema = schema
    let ?outputs = outputs
    let ?with_oproxies = with_oproxies
    rels <- ((\(inp, outp, priv) -> vcat
                                    $ ["/* Input relations */\n"] ++ inp ++
                                      ["\n/* Output relations */\n"] ++ outp ++
                                      ["\n/* Delta tables definitions */\n"] ++ priv) . unzip3) <$>
            mapM (\t -> mkTable (isNothing $ lookup (name t) outputs) t (M.lookup (name t) keys)) tables
    return $ pp builtins $+$ "" $+$ rels

mkTable :: (?schema::OVSDBSchema, ?outputs::[(String, [String])], ?with_oproxies::[String], MonadError String me) => Bool -> Table -> Maybe [String] -> me (Doc, Doc, Doc)
mkTable isinput t@Table{..} keys = do
    ovscols <- tableCheckCols t
    maybe (return ())
          (\rocols -> mapM_ (\c -> when (isNothing $ find ((== c) . name) (tableGetCols t))
                                        $ throwError $ "Column " ++ c ++ " not found in table " ++ name t) rocols)
          $ lookup (name t) ?outputs
    uniqNames (\col -> "Multiple declarations of column " ++ col ++ " in table " ++ tableName) ovscols
    if isinput
       then (, empty, empty) <$> mkTable' TableInput t keys
       else do output           <- mkTable' TableOutput         t keys
               oproxy           <- mkTable' TableOutputProxy    t keys
               realized         <- mkTable' TableRealized       t keys
               uuid_map         <- mkTable' TableUUIDMap        t keys
               let uuid_map_rules   = mkUUIDMapRules            t keys
               delta_plus       <- mkTable' TableDeltaPlus      t keys
               let delta_plus_rules = mkDeltaPlusRules          t keys
               delta_minus      <- mkTable' TableDeltaMinus     t keys
               let delta_minus_rules = mkDeltaMinusRules        t keys
               delta_update     <- mkTable' TableDeltaUpdate    t keys
               let delta_update_rules = mkDeltaUpdateRules      t keys
               swizzled         <- mkTable' TableOutputSwizzled t keys
               swizzle_rules    <- mkSwizzleRules               t
               return $ (empty,
                         output $+$ oproxy,
                         uuid_map            $+$
                         uuid_map_rules      $+$
                         swizzled            $+$
                         swizzle_rules       $+$
                         delta_plus          $+$
                         delta_plus_rules    $+$
                         delta_minus         $+$
                         delta_minus_rules   $+$
                         (if isJust keys then delta_update $$ delta_update_rules else empty) $+$
                         realized)

mkTable' :: (?schema::OVSDBSchema, ?outputs::[(String, [String])], ?with_oproxies::[String], MonadError String me) => TableKind -> Table -> Maybe [String] -> me Doc
mkTable' tkind t@Table{..} keys = do
    let ovscols = tableGetCols t
    let writable_cols = tableGetNonROCols t
    -- uuid-name is only needed in an output if the table is referenced by another table in the schema
    let referenced = tableIsReferenced $ name t
    let uuidcol = case tkind of
                       TableInput          -> ["_uuid: uuid"]
                       TableRealized       -> ["_uuid: uuid"]
                       TableOutput         -> if referenced then ["uuid_name: string"] else []
                       TableOutputSwizzled -> if referenced then ["uuid_name: string"] else []
                       TableOutputProxy    -> if referenced then ["uuid_name: string"] else []
                       TableDeltaPlus      -> if referenced then ["uuid_name: string"] else []
                       TableDeltaMinus     -> ["_uuid: uuid"]
                       TableDeltaUpdate    -> ["_uuid: uuid"]
                       TableUUIDMap        -> ["uuid_name: string", "id: uuid_or_string_t"]
    let prefix = case tkind of
                      TableInput       -> "input"
                      TableRealized    -> "input"
                      TableDeltaPlus   -> "output"
                      TableDeltaMinus  -> "output"
                      TableDeltaUpdate -> "output"
                      _                -> empty
    let tname = mkTableName t tkind
    let cols = case tkind of
                    TableDeltaUpdate | isJust keys
                                     -> filter (\col -> notElem (name col) $ fromJust keys) writable_cols
                                     | otherwise
                                     -> []
                    TableDeltaMinus  -> []
                    TableUUIDMap     -> []
                    TableRealized    -> ovscols
                    _                -> writable_cols
    columns <- mapM (mkCol tkind tableName) cols
    let key = if elem tkind [TableInput, TableRealized]
                 then "primary key (x) x._uuid"
                 else empty
    return $ case tkind of
                  TableUUIDMap        | not referenced -> empty
                  TableOutputSwizzled | not (tableNeedsSwizzle t) -> empty
                  TableOutputProxy    | not (tableNeedsOutputProxy t) -> empty
                  _ -> prefix <+> "relation" <+> pp tname <+> "("    $$
                       (nest' $ vcommaSep $ uuidcol ++ columns)      $$
                       ")"                                           $$
                       key

mkDeltaPlusRules :: (?schema::OVSDBSchema, ?outputs::[(String, [String])], ?with_oproxies::[String]) => Table -> Maybe [String] -> Doc
mkDeltaPlusRules t@Table{..} mkeys =
    (mkTableName t TableDeltaPlus) <> "(" <> commaSep headcols <> ") :-"                $$
    (nest' $ mkTableName t TableOutputProxy <> "(" <> commaSep outcols <> "),")      $$
    (nest' $ "not" <+> mkTableName t TableRealized <> "(" <> commaSep realcols <> ").")
    where
    referenced = tableIsReferenced $ name t
    ovscols = tableGetCols t
    nonro_cols = tableGetNonROCols t
    -- replace columns that don't belong to primary key with "_"
    keycols = map (\c -> let col = if colIsRef c
                                      then refCol2UUID c (mkColName c)
                                      else mkColName c
                         in if colIsKey t c mkeys then col else "_")
                  ovscols
    headcols = (if referenced then ["uuid_name"] else []) ++ map mkColName nonro_cols
    outcols  = (if referenced then ["uuid_name"] else []) ++ map mkColName nonro_cols
    realcols = ["_"] ++ keycols

-- DeltaMinus(uuid) :- Realized(uuid, key, _), not Swizzled(_, key, _).
mkDeltaMinusRules :: (?schema::OVSDBSchema, ?outputs::[(String, [String])], ?with_oproxies::[String]) => Table -> Maybe [String] -> Doc
mkDeltaMinusRules t@Table{..} mkeys =
    (mkTableName t TableDeltaMinus) <> "(_uuid) :-"                                          $$
    (nest' $ mkTableName t TableRealized <> "(" <> commaSep realcols <> "),")                $$
    (nest' $ "not" <+> mkTableName t TableOutputProxy <> "(" <> commaSep outcols <> ").")
    where
    referenced = tableIsReferenced $ name t
    ovscols = tableGetCols t
    nonro_cols = tableGetNonROCols t
    -- replace columns that don't belong to primary key with "_"
    outcols  = (if referenced then ["_"] else []) ++
               map (\c -> let col = if colIsRef c
                                      then "Left{" <> mkColName c <> "}"
                                      else mkColName c
                          in maybe col (\ks -> if elem (name c) ks then col else "_") mkeys)
                    nonro_cols
    realcols = "_uuid": map (\c -> if colIsKey t c mkeys then mkColName c else "_") ovscols

-- DeltaUpdate(uuid, key, new) :- Swizzled(_, key, new), Realized(uuid, key, old), old != new.
mkDeltaUpdateRules :: (?schema::OVSDBSchema, ?outputs::[(String, [String])], ?with_oproxies::[String]) => Table -> Maybe [String] -> Doc
mkDeltaUpdateRules t@Table{..} (Just keys) =
    (mkTableName t TableDeltaUpdate) <> "(" <> commaSep headcols <> ") :-"                   $$
    (nest' $ mkTableName t TableOutputProxy <> "(" <> commaSep outcols <> "),")              $$
    (nest' $ mkTableName t TableRealized <> "(" <> commaSep realcols <> "),")                $$
    (nest' $ (parens $ commaSep old_vars) <+> "!=" <+> (parens $ commaSep new_vars) <> ".")
    where
    referenced = tableIsReferenced $ name t
    ovscols = tableGetCols t
    nonro_cols = tableGetNonROCols t
    headcols = "_uuid" :
               (map (\c -> "__new_" <> mkColName c)
                $ filter (\c -> notElem (name c) keys) nonro_cols)
    outcols  = (if referenced then ["_"] else []) ++
               map (\c -> if colIsKey t c (Just keys)
                             then if colIsRef c
                                     then "Left{" <> mkColName c <> "}"
                                     else mkColName c
                             else "__new_" <> mkColName c)
               nonro_cols
    realcols = "_uuid" :
               map (\c -> if colIsKey t c (Just keys)
                             then mkColName c
                             else ("__old_" <> mkColName c))
                   ovscols
    new_vars = map (\c -> if colIsRef c
                             then refCol2UUID c ("__new_" <> mkColName c)
                             else "__new_" <> mkColName c)
               $ filter (\c -> notElem (name c) keys) nonro_cols
    old_vars = map (\c -> "__old_" <> mkColName c)
               $ filter (\c -> notElem (name c) keys) nonro_cols

-- Generate rule to replace named-uuid table references with real uuid's when they are known
-- from the Realized table.
--
-- For a scalar column:
-- Swizzled(x,ref') :- Output(x,ref), UUIDMap(ref,ref').
--
-- For a set:
-- Swizzled(x,refs') :- Output(x,refs),
--                      var ref = FlatMap(refs),
--                      UUIDMap(ref, ref'),
--                      Aggregate((x),refs'=group2set(ref')).
--
-- For a map:
-- Swizzled(x,refs') :- Output(x,refs),
--                      var key_val = FlatMap(refs),
--                      (var key, var ref) = key_val,
--                      UUIDMap(ref, ref'),
--                      Aggregate((x),refs'=group2map((key, ref'))).
--
mkSwizzleRules :: (?schema::OVSDBSchema, ?outputs::[(String, [String])], ?with_oproxies::[String], MonadError String me) => Table -> me Doc
mkSwizzleRules t | not (tableNeedsSwizzle t) = return empty
mkSwizzleRules t@Table{..} = do
    let ovscols = tableGetNonROCols t
    let referenced = tableIsReferenced $ name t
    let swColName col | colIsRef col = "__id_" <> mkColName col
                      | otherwise = mkColName col
    let swizcols = (if referenced then ["uuid_name"] else []) ++ map swColName ovscols
    let outcols  = (if referenced then ["uuid_name"] else []) ++ map mkColName ovscols

    let colSwizzle :: (MonadError String me) => TableColumn -> me Doc
        colSwizzle col | colIsScalar col = do
            reftable <- getTable (columnPos col) $ fromJust $ colRefTable col
            return $ mkTableName reftable TableUUIDMap <> "(" <> mkColName col <> "," <+> swColName col <> ")"
        colSwizzle col | colIsSet col = do
            -- when computing the aggregate, use swizzled names for all columns before col and original
            -- names for remaining columns.
            let (cols_before, _: cols_after) = span ((/= name col) . name) ovscols
            reftable <- getTable (columnPos col) $ fromJust $ colRefTable col
            let group_by = (if referenced then ["uuid_name"] else []) ++
                           map swColName cols_before                  ++
                           map mkColName cols_after
            return $ "var __one =" <+> "FlatMap(if set_is_empty(" <> mkColName col <> ") set_singleton(\"\") else" <+> mkColName col <>"),"  $$
                     mkTableName reftable TableUUIDMap <> "(__one, __one_swizzled),"                                                         $$
                     "Aggregate((" <> commaSep group_by <> ")," <+> swColName col <+> "= group2set_remove_sentinel(__one_swizzled))"
        colSwizzle col | colIsMap col && isJust (colRefTableVal col) && isNothing (colRefTable col) = do
            let (cols_before, _: cols_after) = span ((/= name col) . name) ovscols
            reftable <- getTable (columnPos col) $ fromJust $ colRefTableVal col
            let group_by = (if referenced then ["uuid_name"] else []) ++
                           map swColName cols_before                  ++
                           map mkColName cols_after
            return $ "var __one =" <+> "FlatMap(if map_is_empty(" <> mkColName col <> ") map_singleton(\"\", \"\") else" <+> mkColName col <> "),"  $$
                     "(var __one_key, var __one_ref) = __one,"                                                                                      $$
                     mkTableName reftable TableUUIDMap <> "(__one_ref, __one_swizzled),"                                                            $$
                     "Aggregate((" <> commaSep group_by <> ")," <+> swColName col <+> "= group2map_remove_sentinel((__one_key, __one_swizzled)))"
        colSwizzle col = err (pos col) $ "mkSwizzleRules: reference swizzling not implemented for map keys"
    swizzles <- mapM colSwizzle $ filter colIsRef ovscols
    return $
        mkTableName t TableOutputSwizzled <> "(" <> commaSep swizcols <> ") :-"     $$
        (nest' $ mkTableName t TableOutput <> "(" <> commaSep outcols <> "),")      $$
        (nest' $ vcommaSep swizzles) <> "."

-- UUIDMap(name, Left(uuid)) :- Output(name, key), Realized(uuid, key).
-- UUIDMap(name, Right(name)) :- Output(name, key), not Realized(_, key).
mkUUIDMapRules :: (?schema::OVSDBSchema, ?outputs::[(String, [String])], ?with_oproxies::[String]) => Table -> Maybe [String] -> Doc
mkUUIDMapRules t@Table{..} mkeys =
    if referenced
       then -- add a sentinel value to ensure UUIDMap is not empty
            mkTableName t TableUUIDMap <> "(\"\", Right{\"\"})."                           $$
            mkTableName t TableUUIDMap <> "(__name, Left{__uuid}) :-"                      $$
            (nest' $ mkTableName t TableOutputSwizzled <> "(" <> commaSep outcols <> "),") $$
            (nest' $ mkTableName t TableRealized <> "(" <> commaSep realcols1 <> ").")     $$
            mkTableName t TableUUIDMap <> "(__name, Right{__name}) :-"                     $$
            (nest' $ mkTableName t TableOutputSwizzled <> "(" <> commaSep outcols <> "),") $$
            (nest' $ "not" <+> (nest' $ mkTableName t TableRealized <> "(" <> commaSep realcols2 <> ")."))
       else empty
    where
    referenced = tableIsReferenced $ name t
    ovscols = tableGetCols t
    nonro_cols = tableGetNonROCols t
    -- replace columns that don't belong to primary key with "_"
    keycols = map (\c -> if colIsKey t c mkeys then mkColName c else "_") nonro_cols
    -- additionally convert uuid_or_string_t to uuid in realized tables
    keycols' = map (\c -> let col = if colIsRef c
                                       then "extract_uuid(" <> mkColName c <> ")"
                                       else mkColName c
                          in if colIsKey t c mkeys then col else "_")
                   ovscols
    outcols   = ["__name"] ++ keycols
    realcols1 = ["__uuid"] ++ keycols'
    realcols2 = ["_"]      ++ keycols'

mkCol :: (?schema::OVSDBSchema, ?outputs::[(String, [String])], MonadError String me) => TableKind -> String -> TableColumn -> me Doc
mkCol tkind tname c@TableColumn{..} = do
    check (columnName /= "_uuid") (pos c) $ "Reserved column name _uuid in table " ++ tname
    check (notElem columnName ["uuid-name", "uuid_name"]) (pos c) $ "Reserved column name " ++ columnName ++ " in table " ++ tname
    check (not $ elem columnName __reservedNames) (pos c) $ "Illegal column name " ++ columnName ++ " in table " ++ tname
    t <- case columnType of
              ColumnTypeAtomic at  -> mkAtomicType at
              ColumnTypeComplex ct -> mkComplexType tkind ct
    return $ mkColName c <> ":" <+> t

__reservedNames = map (("__" ++) . map toLower) $ reservedNames

mkColName :: TableColumn -> Doc
mkColName c = mkColName' $ name c

mkColName' :: String -> Doc
mkColName' c =
    if elem x reservedNames
       then pp $ "__" ++ x
       else pp x
    where x = map toLower c


mkAtomicType :: (MonadError String me) => AtomicType -> me Doc
mkAtomicType IntegerType{} = return "integer"
mkAtomicType (RealType p)  = err p "\"real\" types are not supported"
mkAtomicType BooleanType{} = return "bool"
mkAtomicType StringType{}  = return "string"
mkAtomicType UUIDType{}    = return "uuid"

complexTypeBounds :: ComplexType -> (Integer, Integer)
complexTypeBounds ComplexType{..} = (min, max)
    where
    min = maybe 1 id minComplexType
    max = maybe 1
                (\case
                  Some x    -> x
                  Unlimited -> fromIntegral (maxBound::Word64))
                maxComplexType

mkComplexType :: (?schema::OVSDBSchema, ?outputs::[(String, [String])], MonadError String me) => TableKind -> ComplexType -> me Doc
mkComplexType tkind t@ComplexType{..} = do
    let (min, max) = complexTypeBounds t
    check (max >= min) (pos t) $ "min bound exceeds max bound"
    check (min == 0 || min == 1) (pos t) $ "min bound must be 0 or 1"
    check (max > 0) (pos t) $ "max bound must be greater than 0"
    check (max /= 1 || isNothing valueComplexType) (pos t)
          $ "Cannot handle key-value pairs when max bound is 1"
    key <- mkBaseType tkind keyComplexType
    case (min, max) of
         (1,1) -> return key
         --(0,1) -> return $ "option_t<" <> key <> ">"
         _     -> do
             case valueComplexType of
                  Nothing -> return $ "Set<" <> key <> ">"
                  Just v  -> do vt <- mkBaseType tkind v
                                return $ "Map<" <> key <> "," <> vt <> ">"

mkBaseType :: (?schema::OVSDBSchema, ?outputs::[(String, [String])], MonadError String me) =>  TableKind -> BaseType -> me Doc
mkBaseType _     (BaseTypeSimple at)   = mkAtomicType at
mkBaseType tkind (BaseTypeComplex cbt) | isJust (refTableBaseType cbt) && isNothing (lookup (fromJust $ refTableBaseType cbt) ?outputs)
                                       = return "uuid"
mkBaseType tkind (BaseTypeComplex cbt) | isJust (refTableBaseType cbt) && tkind == TableOutput
                                       = return "string"
mkBaseType tkind (BaseTypeComplex cbt) | isJust (refTableBaseType cbt) && elem tkind [TableDeltaPlus, TableDeltaUpdate, TableOutputSwizzled, TableOutputProxy]
                                       = return "uuid_or_string_t"
mkBaseType _     (BaseTypeComplex cbt) = mkAtomicType $ typeBaseType cbt

tableCheckCols :: (MonadError String me) => Table -> me [TableColumn]
tableCheckCols t@Table{..} = do
    let tprops = filter (\case
                          ColumnsProperty{} -> True
                          _                 -> False) tableProperties
    check (not $ null tprops) (pos t) $ "Table " ++ tableName ++ " does not have a \"columns\" property"
    check (length tprops == 1) (pos t) $ "Table " ++ tableName ++ " has multiple \"columns\" properties"
    let (ColumnsProperty ovscols) : _ = tprops
    return ovscols

tableGetCols :: Table -> [TableColumn]
tableGetCols t@Table{..} = ovscols
    where
    (ColumnsProperty ovscols) : _ = filter (\case
                                             ColumnsProperty{} -> True
                                             _                 -> False) tableProperties

tableGetNonROCols :: (?outputs::[(String, [String])]) => Table -> [TableColumn]
tableGetNonROCols t =
    case lookup (name t) ?outputs of
         Nothing -> ovscols
         Just ro -> filter (\col -> notElem (name col) ro) ovscols
    where
    ovscols = tableGetCols t

getTable :: (?schema::OVSDBSchema, ?outputs::[(String, [String])], MonadError String me) => Pos -> String -> me Table
getTable p tname =
    case find ((== tname) . name) $ schemaTables ?schema of
         Nothing -> err p $ "Unknown table " ++ tname
         Just t  -> return t

-- Is table referenced by at least one other table in the schema?
tableIsReferenced :: (?schema::OVSDBSchema) => String -> Bool
tableIsReferenced tname =
    or $ (map (\t' -> any ((\case
                             ColumnTypeComplex ct -> isref (keyComplexType ct) ||
                                                     maybe False isref (valueComplexType ct)
                             _ -> False) . columnType)
                          $ tableGetCols t')
         $ schemaTables ?schema)
    where
    isref (BaseTypeComplex cbt) = refTableBaseType cbt == Just tname
    isref _ = False
