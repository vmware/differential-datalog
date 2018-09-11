{-# LANGUAGE RecordWildCards, LambdaCase, FlexibleContexts, OverloadedStrings, QuasiQuotes #-}

module Language.DifferentialDatalog.OVSDB.Compile (compileSchemas, compileSchemaFiles) where

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
import Language.DifferentialDatalog.Util
import Language.DifferentialDatalog.Name
import Language.DifferentialDatalog.Pos
import Language.DifferentialDatalog.PP
import Control.Monad.Except

builtinTypes :: String
builtinTypes = [r|
typedef integer      = bit<64>
typedef uuid         = bit<128>
typedef option_t<'A> = Some{x: 'A}
                     | None
|]

compileSchemaFiles :: [FilePath] -> [String] -> IO Doc
compileSchemaFiles file_names outputs = do
    schemas <- mapM (\fname -> do 
                      content <- readFile fname
                      case parseSchema content fname of
                           Left  e    -> errorWithoutStackTrace $ "Failed to parse input file: " ++ e
                           Right prog -> return prog)
                    file_names
    case compileSchemas schemas outputs of
         Left e    -> errorWithoutStackTrace e
         Right doc -> return doc

compileSchemas :: (MonadError String me) => [OVSDBSchema] -> [String] -> me Doc
compileSchemas schemas outputs = do
    let tables = concatMap renameTables schemas
    mapM_ (\o -> do let t = find ((==o) . name) tables
                    when (isNothing t) $ throwError $ "Table " ++ o ++ " not found") outputs
    uniqNames ("Multiple declarations of table " ++ ) tables
    rels <- vcat <$> mapM (\t -> if elem (name t) outputs 
                                    then mkTable False t
                                    else mkTable True t) tables
    return $ pp builtinTypes $+$ rels

renameTables :: OVSDBSchema -> [Table]
renameTables OVSDBSchema{..} =
    map (\t -> t{tableName = schemaName ++ "_" ++ tableName t}) schemaTables

mkTable :: (MonadError String me) => Bool -> Table -> me Doc
mkTable isinput t@Table{..} = do
    let tprops = filter (\case
                          ColumnsProperty{} -> True
                          _                 -> False) tableProperties
    check (not $ null tprops) (pos t) $ "Table " ++ tableName ++ " does not have a \"columns\" property"
    check (length tprops == 1) (pos t) $ "Table " ++ tableName ++ " has multiple \"columns\" properties"
    let (ColumnsProperty ovscols) : _ = tprops
    uniqNames (\col -> "Multiple declarations of column " ++ col ++ " in table " ++ tableName) ovscols
    columns <- mapM (mkCol isinput tableName) ovscols
    let cols   = lefts  columns
        tables = rights columns
    let uuidcol = "_uuid: uuid"
    let prefix = if isinput then "input" else empty
    return $ prefix <+> "relation" <+> pp (name t) <> "("    $$
             (nest' $ vcat $ punctuate comma (uuidcol:cols)) $$
             ")"                                             $$
             vcat tables

mkCol :: (MonadError String me) => Bool -> String -> TableColumn -> me (Either Doc Doc)
mkCol isinput tname c@TableColumn{..} = do
    check (columnName /= "_uuid") (pos c) $ "Illegal column name _uuid in table " ++ tname
    case columnType of
         ColumnTypeAtomic at  -> (Left . (mkColName columnName <>) . (":" <+>)) <$> mkAtomicType at
         ColumnTypeComplex ct -> mkComplexType isinput tname c ct

mkColName :: String -> Doc
mkColName "type" = "_type"
mkColName "match" = "_match"
mkColName n      = pp $ map toLower n

mkAtomicType :: (MonadError String me) => AtomicType -> me Doc
mkAtomicType IntegerType{} = return "integer"
mkAtomicType (RealType p)  = err p "\"real\" types are not supported"
mkAtomicType BooleanType{} = return "bool"
mkAtomicType StringType{}  = return "string"
mkAtomicType UUIDType{}    = return "uuid"

mkComplexType :: (MonadError String me) => Bool -> String -> TableColumn -> ComplexType -> me (Either Doc Doc)
mkComplexType isinput tname TableColumn{..} t@ComplexType{..} = do
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
    key <- mkBaseType keyComplexType
    case (min, max) of
         (1,1) -> return $ Left $ mkColName columnName <> ":" <+> key
         (0,1) -> return $ Left $ mkColName columnName <> ": option_t<" <> key <> ">"
         _     -> Right <$> mkSubtable isinput tname columnName t

mkSubtable :: (MonadError String me) => Bool -> String -> String -> ComplexType -> me Doc
mkSubtable isinput tname cname t@ComplexType{..} = do
    keytype <- mkBaseType keyComplexType
    valtype <- maybe (return []) (\v -> (\vt -> ["value:" <+> vt]) <$> mkBaseType v) valueComplexType
    let cols = [ mkColName tname <> ": uuid"
               , "key:" <+> keytype] ++ valtype
    let prefix = if isinput then "input" else empty
    return $ prefix <+> "relation" <+> pp tname <> "_" <> pp cname <> "(" $$
             (nest' $ vcat $ punctuate comma cols)                        $$
             ")"

mkBaseType :: (MonadError String me) => BaseType -> me Doc
mkBaseType (BaseTypeSimple at)   = mkAtomicType at
mkBaseType (BaseTypeComplex cbt) = mkAtomicType $ typeBaseType cbt
