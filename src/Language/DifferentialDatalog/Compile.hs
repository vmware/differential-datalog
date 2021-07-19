{-
Copyright (c) 2018-2021 VMware, Inc.
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

{-# LANGUAGE RecordWildCards, FlexibleContexts, LambdaCase, TupleSections, OverloadedStrings, TemplateHaskell, QuasiQuotes, ImplicitParams, NamedFieldPuns #-}

{- |
Module     : Compile
Description: Compile 'DatalogProgram' to Rust.  See program.rs for corresponding Rust declarations.
-}

module Language.DifferentialDatalog.Compile (
    compile,
    unpackFixNewline,
    rustProjectDir,
    mkConstructorName,
    mkType,
    tupleStruct,
    rnameFlat,
    rnameScoped,
    recordAfterPrefix
) where

import Prelude hiding((<>), readFile, writeFile)
import Control.Monad.State
import Text.PrettyPrint
import Data.Tuple.Select
import Data.Maybe
import Data.List
import Data.Bits hiding (isSigned)
import Data.List.Split
import Data.FileEmbed
import System.FilePath
import qualified Data.ByteString.Char8 as BS
import Numeric
import qualified Data.Set as S
import qualified Data.Map as M
import qualified Data.Graph.Inductive as G
import Data.WideWord
import Text.Parsec (sourceName, sourceLine, sourceColumn)
--import Debug.Trace

import Language.DifferentialDatalog.Config
import Language.DifferentialDatalog.PP
import Language.DifferentialDatalog.Name
import Language.DifferentialDatalog.Crate
import Language.DifferentialDatalog.Module
import Language.DifferentialDatalog.Pos
import Language.DifferentialDatalog.Ops
import Language.DifferentialDatalog.Util
import Language.DifferentialDatalog.Syntax
import Language.DifferentialDatalog.Parse
import Language.DifferentialDatalog.NS
import Language.DifferentialDatalog.Expr
import Language.DifferentialDatalog.DatalogProgram
import Language.DifferentialDatalog.Relation
import Language.DifferentialDatalog.Index
import Language.DifferentialDatalog.Optimize
import Language.DifferentialDatalog.ECtx
import Language.DifferentialDatalog.Type
import Language.DifferentialDatalog.Rule
import Language.DifferentialDatalog.FlatBuffer
import Language.DifferentialDatalog.Attribute
import Language.DifferentialDatalog.Var
import Language.DifferentialDatalog.Rust
import Language.DifferentialDatalog.D3log

-- Some OSs think that they run on a typewriter and insert '\r'
-- at the end of each line.  We eliminate these characters as they confuse
-- `cargo`.
unpackFixNewline :: BS.ByteString -> String
unpackFixNewline = filter (/= '\r') . BS.unpack

-- Input argument name for Rust functions that take a datalog record.
vALUE_VAR :: Doc
vALUE_VAR = "__v"

-- Input arguments for Rust functions that take a key and one or two
-- values
kEY_VAR :: Doc
kEY_VAR = "__key"

vALUE_VAR1 :: Doc
vALUE_VAR1 = "__v1"

vALUE_VAR2 :: Doc
vALUE_VAR2 = "__v2"

-- Input argument to aggregation function
gROUP_VAR :: Doc
gROUP_VAR = "__group__"

-- Input argument to inspect function
tIMESTAMP_VAR :: Doc
tIMESTAMP_VAR = "__timestamp"

wEIGHT_VAR :: Doc
wEIGHT_VAR = "__weight"

-- Module that 'ctx' belongs to.
ctxModule :: (?crate_graph::CrateGraph) => ECtx -> ModuleName
ctxModule CtxTop                    = error "Compile.ctxModule CtxTop"
ctxModule CtxFunc{..}               = nameScope ctxFunc
ctxModule CtxRuleLAtom{..}          = ruleModule ctxRule
ctxModule CtxRuleLLocation{..}      = ruleModule ctxRule
ctxModule CtxRuleRAtom{..}          = ruleModule ctxRule
ctxModule CtxRuleRCond{..}          = ruleModule ctxRule
ctxModule CtxRuleRFlatMap{..}       = ruleModule ctxRule
ctxModule CtxRuleRFlatMapVars{..}   = ruleModule ctxRule
ctxModule CtxRuleRInspect{..}       = ruleModule ctxRule
ctxModule CtxRuleRProject{..}       = ruleModule ctxRule
ctxModule CtxRuleRGroupBy{..}       = ruleModule ctxRule
ctxModule CtxKey{..}                = nameScope ctxRelation
ctxModule CtxIndex{..}              = nameScope $ atomRelation $ idxAtom ctxIndex
ctxModule ctx                       = ctxModule $ ctxParent ctx

-- Crate that 'ctx' belongs to.
ctxCrate :: (?crate_graph::CrateGraph) => ECtx -> Crate
ctxCrate ctx = cgModuleCrate ?crate_graph $ ctxModule ctx

-- Extract static template header before the "/*- !!!!!!!!!!!!!!!!!!!! -*/"
-- separator from file; substitute "datalog_example" with 'specname' in
-- the header.
header :: (?specname::String) => String -> Doc
header template =
    let h = case splitOn "/*- !!!!!!!!!!!!!!!!!!!! -*/" template of
                []  -> error "Missing separator in lib.rs"
                x:_ -> x
    in pp $ replace "datalog_example" ?specname h

-- Header to prepend to each each generated module in the 'types' crate.
typesModuleHeader :: (?specname::String) => Doc
typesModuleHeader = header (unpackFixNewline $(embedFile "rust/template/types/lib.rs"))

-- Main crate containing compiled program rules.
mainHeader :: (?specname::String) => Doc
mainHeader = header (unpackFixNewline $(embedFile "rust/template/src/lib.rs"))

-- Top-level 'Cargo.toml'.
mainCargo :: (?cfg::Config, ?specname::String, ?crate_graph::CrateGraph) => [String] -> String -> Doc
mainCargo crate_types toml_footer =
    (pp $ replace "datalog_example" ?specname template)                                                             $$
    "crate-type = [" <> (hsep $ punctuate "," $ map (\t -> "\"" <> pp t <> "\"") $ "rlib" : crate_types) <> "]\n"   $$
    ""                                                                                                              $$
    deps                                                                                                            $$
    ""                                                                                                              $$
    --extra_toml_code                                                                                                 $$
    text toml_footer
    where
    deps = vcat $ map (\crate -> "[dependencies." <> pp (crateName crate) <> "]" $$
                                 "path = \"types/" <> pp (crateDirPath crate) <> "\"")
                $ cgCrates ?crate_graph
    template = replace "\"differential_datalog/c_api\"" "\"differential_datalog/c_api\", \"types/c_api\""
               $ replace "\"differential_datalog/flatbuf\"" "\"differential_datalog/flatbuf\", \"types/flatbuf\""
               $ (if confNestedTS32 ?cfg
                  then replace "[dependencies.differential_datalog]" "[dependencies.differential_datalog]\nfeatures=[\"nested_ts_32\"]"
                  else id)
               $ unpackFixNewline $ $(embedFile "rust/template/Cargo.toml")

rustProjectDir :: (?specname::String) => String
rustProjectDir = ?specname ++ "_ddlog"

-- IMPORTANT: KEEP THIS IN SYNC WITH FILE LIST IN 'build.rs'.
templateFiles :: (?specname::String) => [(String, String)]
templateFiles =
    map (mapSnd unpackFixNewline)
        [ ("src/build.rs"               , $(embedFile "rust/template/src/build.rs"))
        , ("src/main.rs"                , $(embedFile "rust/template/src/main.rs"))
        , ("src/inventory.rs"           , $(embedFile "rust/template/src/inventory.rs"))
        , ("src/ovsdb_api.rs"           , $(embedFile "rust/template/src/ovsdb_api.rs"))
        , ("ddlog.h"                    , $(embedFile "rust/template/ddlog.h"))
        , ("ddlog_ovsdb_test.c"         , $(embedFile "rust/template/ddlog_ovsdb_test.c"))
        ]

-- Rust differential_datalog library
rustLibFiles :: (?specname::String) => [(String, String)]
rustLibFiles =
    map (mapSnd unpackFixNewline)
        [ ("differential_datalog/Cargo.toml"                      , $(embedFile "rust/template/differential_datalog/Cargo.toml"))
        , ("differential_datalog/src/callback.rs"                 , $(embedFile "rust/template/differential_datalog/src/callback.rs"))
        , ("differential_datalog/src/ddlog.rs"                    , $(embedFile "rust/template/differential_datalog/src/ddlog.rs"))
        , ("differential_datalog/src/ddval/mod.rs"                , $(embedFile "rust/template/differential_datalog/src/ddval/mod.rs"))
        , ("differential_datalog/src/ddval/ddvalue.rs"            , $(embedFile "rust/template/differential_datalog/src/ddval/ddvalue.rs"))
        , ("differential_datalog/src/ddval/ddval_convert.rs"      , $(embedFile "rust/template/differential_datalog/src/ddval/ddval_convert.rs"))
        , ("differential_datalog/src/lib.rs"                      , $(embedFile "rust/template/differential_datalog/src/lib.rs"))
        , ("differential_datalog/src/profile.rs"                  , $(embedFile "rust/template/differential_datalog/src/profile.rs"))
        , ("differential_datalog/src/profile_statistics.rs"       , $(embedFile "rust/template/differential_datalog/src/profile_statistics.rs"))
        , ("differential_datalog/src/program/mod.rs"              , $(embedFile "rust/template/differential_datalog/src/program/mod.rs"))
        , ("differential_datalog/src/program/update.rs"           , $(embedFile "rust/template/differential_datalog/src/program/update.rs"))
        , ("differential_datalog/src/program/arrange.rs"          , $(embedFile "rust/template/differential_datalog/src/program/arrange.rs"))
        , ("differential_datalog/src/program/timestamp.rs"        , $(embedFile "rust/template/differential_datalog/src/program/timestamp.rs"))
        , ("differential_datalog/src/program/worker.rs"           , $(embedFile "rust/template/differential_datalog/src/program/worker.rs"))
        , ("differential_datalog/src/program/config.rs"           , $(embedFile "rust/template/differential_datalog/src/program/config.rs"))
        , ("differential_datalog/src/record/mod.rs"               , $(embedFile "rust/template/differential_datalog/src/record/mod.rs"))
        , ("differential_datalog/src/record/tuples.rs"            , $(embedFile "rust/template/differential_datalog/src/record/tuples.rs"))
        , ("differential_datalog/src/record/arrays.rs"            , $(embedFile "rust/template/differential_datalog/src/record/arrays.rs"))
        , ("differential_datalog/src/replay.rs"                   , $(embedFile "rust/template/differential_datalog/src/replay.rs"))
        , ("differential_datalog/src/test_record.rs"              , $(embedFile "rust/template/differential_datalog/src/test_record.rs"))
        , ("differential_datalog/src/valmap.rs"                   , $(embedFile "rust/template/differential_datalog/src/valmap.rs"))
        , ("differential_datalog/src/variable.rs"                 , $(embedFile "rust/template/differential_datalog/src/variable.rs"))
        , ("differential_datalog/src/render/mod.rs"               , $(embedFile "rust/template/differential_datalog/src/render/mod.rs"))
        , ("differential_datalog/src/render/arrange_by.rs"        , $(embedFile "rust/template/differential_datalog/src/render/arrange_by.rs"))
        , ("differential_datalog/src/dataflow/mod.rs"             , $(embedFile "rust/template/differential_datalog/src/dataflow/mod.rs"))
        , ("differential_datalog/src/dataflow/arrange.rs"         , $(embedFile "rust/template/differential_datalog/src/dataflow/arrange.rs"))
        , ("differential_datalog/src/dataflow/consolidate.rs"     , $(embedFile "rust/template/differential_datalog/src/dataflow/consolidate.rs"))
        , ("differential_datalog/src/dataflow/distinct.rs"        , $(embedFile "rust/template/differential_datalog/src/dataflow/distinct.rs"))
        , ("differential_datalog/src/dataflow/filter_map.rs"      , $(embedFile "rust/template/differential_datalog/src/dataflow/filter_map.rs"))
        , ("differential_datalog/src/api/mod.rs"                  , $(embedFile "rust/template/differential_datalog/src/api/mod.rs"))
        , ("differential_datalog/src/api/c_api.rs"                , $(embedFile "rust/template/differential_datalog/src/api/c_api.rs"))
        , ("differential_datalog/src/api/update_handler.rs"       , $(embedFile "rust/template/differential_datalog/src/api/update_handler.rs"))
        , ("differential_datalog/src/flatbuf/mod.rs"              , $(embedFile "rust/template/differential_datalog/src/flatbuf/mod.rs"))
        , ("differential_datalog/src/dataflow/map.rs"             , $(embedFile "rust/template/differential_datalog/src/dataflow/map.rs"))
        , ("differential_datalog_test/Cargo.toml"                 , $(embedFile "rust/template/differential_datalog_test/Cargo.toml"))
        , ("differential_datalog_test/lib.rs"                     , $(embedFile "rust/template/differential_datalog_test/lib.rs"))
        , ("differential_datalog_test/test_value.rs"              , $(embedFile "rust/template/differential_datalog_test/test_value.rs"))
        , ("ddlog_derive/Cargo.toml"                              , $(embedFile "rust/template/ddlog_derive/Cargo.toml"))
        , ("ddlog_derive/src/lib.rs"                              , $(embedFile "rust/template/ddlog_derive/src/lib.rs"))
        , ("ddlog_derive/src/from_record.rs"                      , $(embedFile "rust/template/ddlog_derive/src/from_record.rs"))
        , ("ddlog_derive/src/into_record.rs"                      , $(embedFile "rust/template/ddlog_derive/src/into_record.rs"))
        , ("ddlog_derive/src/mutator.rs"                          , $(embedFile "rust/template/ddlog_derive/src/mutator.rs"))
        , ("cmd_parser/Cargo.toml"                                , $(embedFile "rust/template/cmd_parser/Cargo.toml"))
        , ("cmd_parser/lib.rs"                                    , $(embedFile "rust/template/cmd_parser/lib.rs"))
        , ("cmd_parser/parse.rs"                                  , $(embedFile "rust/template/cmd_parser/parse.rs"))
        , ("ovsdb/Cargo.toml"                                     , $(embedFile "rust/template/ovsdb/Cargo.toml"))
        , ("ovsdb/lib.rs"                                         , $(embedFile "rust/template/ovsdb/lib.rs"))
        , ("ovsdb/test.rs"                                        , $(embedFile "rust/template/ovsdb/test.rs"))
        , (".cargo/config.toml"                                   , $(embedFile "rust/template/.cargo/config.toml"))
        ]

{- The following types model corresponding entities in program.rs -}

-- There are two kinds of arrangements:
--
-- + 'ArrangementMap' arranges the collection into key-value pairs of type
--   '(DDValue,DDValue)', where the second value in the pair is a record from the
--   original collection.  These arrangements are used in joins and semi-joins.
--
-- + 'ArrangementSet' arranges the collection in '(DDValue, ())' pairs, where the
--   value is the key extracted from relation.  These arrangements are used in
--   antijoins and semijoins.
--
-- A semijoin can use either 'ArrangementSet' or 'ArrangementMap'.  The latter is
-- more expensive and should only be used if the same arrangement is shared with a
-- join operator.
--
-- 'arngPattern' is a _normalized_ pattern that only contains variables involved
-- in the arrangement key, with normalized names (so that two patterns isomorphic
-- modulo variable names have the same normalized representation) and that only
-- expand constructors that either contain a key variable or are non-unique.
--
-- The 'distinct' flag in 'ArrangementSet' indicates that this arrangement is used
-- in an antijoin and therefore must contain distinct entries.
data Arrangement = ArrangementMap { arngPattern :: Expr, arngIndexes :: [Index] }
                 | ArrangementSet { arngPattern :: Expr, arngDistinct :: Bool}
                 deriving (Eq, Show)

arngUsedInIndexes :: Arrangement -> [Index]
arngUsedInIndexes ArrangementMap{arngIndexes} = arngIndexes
arngUsedInIndexes ArrangementSet{} = []

arngIsDistinct :: Arrangement -> Bool
arngIsDistinct ArrangementMap{} = False
arngIsDistinct ArrangementSet{arngDistinct} = arngDistinct

-- Compiled relation.  This struct is generated during the first pass, and is
-- used to populate Rust crates during the second pass of the compiler.
data ProgRel = ProgRel {
    prelName            :: String,
    -- Rust code to populate `struct Relation`.  Lives in the main crate.
    prelCode            :: Doc,
    -- Rust code for the key function if the relation has a primary key.  Lives
    -- in the module that declares the relation.
    prelKey             :: Maybe Doc,
    -- Rust code for relations rules.  Each rule lives in the module where the
    -- corresponding DDlog rule is declared.
    prelRules           :: [(ModuleName, Doc)],
    -- Rust code for facts (rules without the RHS).
    prelFacts           :: [(ModuleName, Doc, Doc)],
    -- Rust code for relation arrangements to be placed in the module that
    -- declares the relation.
    prelArrangements    :: [Doc]
}

-- Recursive relation contains additional flag that indicates whether the
-- `distinct` operator should be applied to the relation before each fixed
-- point iteration to ensure convergence.
data RecProgRel = RecProgRel {
    rprelRel      :: ProgRel,
    rprelDistinct :: Bool
}

-- Compiled program node: individual relation or a recursive fragment
data ProgNode = SCCNode   [RecProgRel]
              | ApplyNode ModuleName Int Doc
              | RelNode   ProgRel

nodeRels :: ProgNode -> [ProgRel]
nodeRels (SCCNode rels)     = map rprelRel rels
nodeRels (RelNode rel)      = [rel]
nodeRels (ApplyNode _ _ _)  = []

{- State accumulated by the compiler as it traverses the program -}
type CompilerMonad = State CompilerState

data CompilerState = CompilerState {
    cArrangements :: M.Map String [Arrangement],
    cDelayedRelIds :: M.Map (String, Delay) Int
}

emptyCompilerState :: CompilerState
emptyCompilerState = CompilerState {
    cArrangements = M.empty,
    cDelayedRelIds = M.empty
}

-- Relations, indexes, relation transformers and Value's are stored in the flat namespace.
-- Flatten a name by replacing "::" with "_"
rnameFlat :: String -> Doc
rnameFlat = pp . replace "::" "_"

-- Types, functions, rules, etc., are stored in Rust modules that mirror the DDlog
-- module hierarchy.  This function generates a qualified Rust identifier (e.g.,
-- 'crate_name::module_name::name') that refers to entity whose fully qualified
-- DDlog identifier is 'n'.
-- 'local_module' - module accessing the identifier or 'Nothing' if it's accessed
-- from an external crate.
-- 'n' - fully qualified DDlog identifier
rnameScoped :: (?crate_graph::CrateGraph, ?specname::String) => Maybe ModuleName -> String -> Doc
rnameScoped local_module n =
    ridentScoped local_module (nameScope n) (nameLocalStr n)

-- Generate a qualified Rust identifier that refers to variable 'n' declared in
-- 'target_module' from 'local_module'.
ridentScoped :: (?crate_graph::CrateGraph, ?specname::String) => Maybe ModuleName -> ModuleName -> String -> Doc
ridentScoped Nothing target_module n =
    pp $ intercalate "::" $ absolutePath target_module ++ [n]
ridentScoped (Just local_module) target_module n =
    pp $ intercalate "::" $ relativePath local_module target_module ++ [n]

mkRelEnum :: DatalogProgram -> Doc
mkRelEnum d =
    "#[derive(Copy,Clone,Debug,PartialEq,Eq,Hash)]"                                                                                        $$
    "pub enum Relations {"                                                                                                                 $$
    (nest' $ vcat $ punctuate comma $ map (\rel -> rnameFlat (name rel) <+> "=" <+> pp (relIdentifier d rel)) $ M.elems $ progRelations d) $$
    "}"

mkIdxEnum :: DatalogProgram -> Doc
mkIdxEnum d =
    "#[derive(Copy,Clone,Debug,PartialEq,Eq,Hash)]"                                                                                       $$
    "pub enum Indexes {"                                                                                                                  $$
    (nest' $ vcat $ punctuate comma $ map (\idx -> rnameFlat (name idx) <+> "=" <+> pp (idxIdentifier d idx)) $ M.elems $ progIndexes d)  $$
    "}"

relId :: DatalogProgram -> String -> Doc
-- It'd be nice to use symbolic names, but they are not available outside of the
-- main crate.  Consider fixing this in the future.
-- relId rel = "Relations::" <> rnameFlat rel <+> "as RelId"
relId d rel = pp $ M.findIndex rel $ progRelations d

atomRelId :: DatalogProgram -> Atom -> CompilerMonad Doc
atomRelId d a@Atom{..} | not (atomIsDelayed a) = return $ relId d atomRelation
                       | otherwise =
    gets $ \c -> pp $ cDelayedRelIds c M.! (atomRelation, atomDelay)

-- Create a new arrangement for use in a join operator:
-- * If the arrangement exists, do nothing
-- * If a semijoin arrangement with the same pattern exists,
--   promote it to a join arrangement
-- * Otherwise, add the new arrangement
--
-- NOTE: We assume that we are arranging a non-delayed relation.
addJoinArrangement :: DatalogProgram -> String -> Expr -> Maybe Index -> CompilerMonad ()
addJoinArrangement d relname pat midx | -- We use streaming joins for streams, which do not require an
                                            -- arrangement.
                                            relIsStream (getRelation d relname) = return ()
                                          | otherwise = do
    arrs <- gets $ (M.! relname) . cArrangements
    let existing_idx = findIndex (\a -> (arngPattern a == pat) && (arngIsDistinct a == False)) arrs
    let idxs = nub $ maybeToList midx ++ maybe [] (arngUsedInIndexes . (arrs !!)) existing_idx
    let join_arr = ArrangementMap pat idxs
    let arrs' = maybe (arrs ++ [join_arr])
                      (\idx -> take idx arrs ++ [join_arr] ++ drop (idx+1) arrs)
                      existing_idx
    modify $ \s -> s{cArrangements = M.insert relname arrs' $ cArrangements s}

-- Create a new arrangement for use in a semijoin operator:
-- * If a semijoin, antijoin or join arrangement with the same pattern exists, do nothing
-- * Otherwise, add the new arrangement
addSemijoinArrangement :: DatalogProgram -> String -> Expr -> CompilerMonad ()
addSemijoinArrangement d relname pat | relIsStream (getRelation d relname) = return ()
                                         | otherwise = do
    arrs <- gets $ (M.! relname) . cArrangements
    let arrs' = if isJust $ find ((== pat) . arngPattern) arrs
                   then arrs
                   else arrs ++ [ArrangementSet pat False]
    modify $ \s -> s{cArrangements = M.insert relname arrs' $ cArrangements s}

-- Create a new arrangement for use in a antijoin operator:
-- * If the arrangement exists, do nothing
-- * If a semijoin arrangement with the same pattern exists, promote it to
--   an antijoin by setting 'distinct' to true
-- * Otherwise, add the new arrangement
addAntijoinArrangement :: DatalogProgram -> String -> Expr -> CompilerMonad ()
addAntijoinArrangement d relname pat | relIsStream (getRelation d relname) = return ()
                                         | otherwise = do
    arrs <- gets $ (M.! relname) . cArrangements
    let antijoin_arr = ArrangementSet pat True
    let semijoin_idx = elemIndex (ArrangementSet pat False) arrs
    let arrs' = if elem antijoin_arr arrs
                   then arrs
                   else maybe (arrs ++ [antijoin_arr])
                              (\idx -> take idx arrs ++ [antijoin_arr] ++ drop (idx+1) arrs)
                              semijoin_idx
    modify $ \s -> s{cArrangements = M.insert relname arrs' $ cArrangements s}

-- Find an arrangement of the form 'ArrangementSet pattern _'
getSemijoinArrangement :: String -> Expr -> CompilerMonad (Maybe Int)
getSemijoinArrangement relname pat = do
    arrs <- gets $ (M.! relname) . cArrangements
    return $ findIndex (\case
                         ArrangementSet pattern' _ -> pattern' == pat
                         _                         -> False)
                       arrs

-- Find an arrangement of the form 'ArrangementMap pattern _'
getJoinArrangement :: String -> Expr -> CompilerMonad (Maybe Int)
getJoinArrangement relname pat = do
    arrs <- gets $ (M.! relname) . cArrangements
    return $ findIndex (\case
                         ArrangementMap{arngPattern} -> arngPattern == pat
                         _ -> False) arrs

-- Find an arrangement of the form 'ArrangementSet pattern True'
getAntijoinArrangement :: String -> Expr -> CompilerMonad (Maybe Int)
getAntijoinArrangement relname pat = do
    arrs <- gets $ (M.! relname) . cArrangements
    return $ elemIndex (ArrangementSet pat True) arrs

-- Rust does not like parenthesis around singleton tuples
tuple :: [Doc] -> Doc
tuple [x] = x
tuple xs = parens $ hsep $ punctuate comma xs

tupleTypeName :: (?crate_graph::CrateGraph, ?specname::String) => Maybe ModuleName -> [a] -> Doc
tupleTypeName local_module xs =
    rnameScoped local_module (mOD_STD ++ "::tuple" ++ show (length xs))

tupleStruct :: (?crate_graph::CrateGraph, ?specname::String) => Maybe ModuleName -> [Doc] -> Doc
tupleStruct _            [x]                  = x
tupleStruct local_module xs  | null xs = tuple xs
                             | otherwise      = tupleTypeName local_module xs <> tuple xs

-- Structs with a single constructor are compiled into Rust structs;
-- structs with multiple constructor are compiled into Rust enums.
isStructType :: Type -> Bool
isStructType TStruct{..} | length typeCons == 1 = True
isStructType TStruct{}                        = False
isStructType t                                  = error $ "Compile.isStructType " ++ show t

-- 'local' is true iff the constructor is being used in the same crate where it was
-- declared, i.e., the 'types' crate.
mkConstructorName :: (?crate_graph::CrateGraph, ?specname::String) => Maybe ModuleName -> String -> Type -> String -> Doc
mkConstructorName local_module tname t c =
    if isStructType t
       then rnameScoped local_module tname
       else rnameScoped local_module tname <> "::" <> nameLocal c

-- | Create a compilable Cargo crate.  If the crate already exists, only writes files
-- modified by the recompilation.
--
-- 'specname' - will be used as Cargo package and library names
--
-- 'modules' - list of all DDlog modules used in the original program. This function
-- will generate a Rust module for each DDlog module and partition these modules
-- into Rust crates.
--
-- 'rs_code' - additional Rust code to be added to the generated modules.
--
-- 'dir' - directory for the crate; will be created if does not exist
--
-- 'crate_types' - list of Cargo library crate types, e.g., [\"staticlib\"],
--                  [\"cdylib\"], [\"staticlib\", \"cdylib\"]
compile :: (?cfg :: Config) => DatalogProgram -> String -> [DatalogModule] -> M.Map ModuleName (Doc, Doc, Doc) -> FilePath -> [String] -> IO ()
compile d_unoptimized specname modules rs_code dir crate_types = do
    let ?modules = M.fromList $ map (\m -> (moduleName m, m)) modules
    let ?specname = specname
    let -- Partition modules into crates.
        ?crate_graph = partitionIntoCrates ?modules
    -- dump dependency graph to file
    let dot_file = (?specname <.> "dot", depGraphToDot $ progDependencyGraph d_unoptimized)
    -- Apply optimizations; transform the program to a form that this
    -- compiler can handle; make sure the program has at least one relation.
    let d_optimized = addDummyRel
                      $ simplifyDifferentiation
                      $ simplifyDelayedRels
                      $ optimize d_unoptimized
    -- Compile away D3log location annotations.
    let (d, d3log_rel_map) = processD3logAnnotations d_optimized
    when (confDumpDebug ?cfg) $
        writeFile (replaceExtension (confDatalogFile ?cfg) ".opt.ast") (show d)
    let (types, main) = compileLib d d3log_rel_map rs_code
    -- Produce flatbuffer bindings if either the java or rust bindings are enabled
    compileFlatBufferBindings d rs_code (dir </> rustProjectDir)
    -- Substitute specname in template files; write files if changed.
    let template_files = map (\(path, content) ->
                               (path, replace "datalog_example" ?specname content))
                         templateFiles
    -- Generate lib files if changed.
    let type_files = M.toList $ M.mapKeys ("types" </>) $ M.map render types
    let toml_footer =
            ( if confOmitProfile ?cfg
                then ""
                else
                "[profile.release]\n"
                    ++ "opt-level = 2\n"
                    ++ "debug = false\n"
                    ++ "rpath = false\n"
                    ++
                    -- false: Performs "thin local LTO" which performs "thin" LTO on the local crate
                    -- only across its codegen units. No LTO is performed if codegen units is 1 or
                    -- opt-level is 0.
                    "lto = false\n"
                    ++ "debug-assertions = false\n"
            )
            ++ ( if confOmitWorkspace ?cfg
                    then ""
                    else
                    (if confOmitProfile ?cfg then "" else "\n")
                        ++ "[workspace]\n"
                        ++ "members = [\n"
                        ++ "    \"cmd_parser\",\n"
                        ++ "    \"differential_datalog\",\n"
                        ++ "    \"ovsdb\",\n"
                        ++ "]\n"
                )
    let toml_file = ("Cargo.toml", render $ mainCargo crate_types toml_footer)
    let lib_file = ("src" </> "lib.rs", render main)
    updateDirectory (dir </> rustProjectDir)
                    ["flatbuf", "src" </> "flatbuf_generated.rs", "src" </> "flatbuf.rs", "target", "Cargo.lock"]
                    (dot_file : toml_file : lib_file : (type_files ++ rustLibFiles ++ template_files))
    return ()

-- For each delayed relation 'R-n' that occurs in the program,
-- add a synonym relation 'R_n' defined as 'R_n :- R-n' and replace
-- all occurrences of 'R-n' with 'R_n'.  This guarantees that
-- we don't need to arrange delayed relations.
simplifyDelayedRels :: DatalogProgram -> DatalogProgram
simplifyDelayedRels d =
    -- For each 'R-n', add a fresh relation called "__n_R".
    foldl' (\d_ (rname, delay) ->
             let rel = getRelation d rname
                 delayed_rel = Relation {
                     relPos          = nopos,
                     relAttrs        = relAttrs rel,
                     relRole         = RelInternal,
                     relSemantics    = relSemantics rel,
                     relName         = mk_delayed_rel_name (rname, delay),
                     relType         = typ rel,
                     relPrimaryKey   = Nothing
                 }
                 delayed_rule = Rule {
                    rulePos     = nopos,
                    ruleModule  = nameScope rel,
                    ruleLHS     = [RuleLHS {
                        lhsPos = nopos,
                        lhsAtom = Atom {
                            atomPos        = nopos,
                            atomRelation   = name delayed_rel,
                            atomDelay      = delayZero,
                            atomDiff       = False,
                            atomVal        = eTypedVar "x" $ typ rel
                        },
                        lhsLocation = Nothing
                     }],
                     ruleRHS    = [RHSLiteral {
                         rhsPolarity    = True,
                         rhsAtom        = Atom {
                             atomPos        = nopos,
                             atomRelation   = rname,
                             atomDelay      = delay,
                             atomDiff       = False,
                             atomVal        = eTypedVar "x" $ typ rel
                         }
                     }]
                 }
             in progAddRules [delayed_rule] $ progAddRel delayed_rel d_)
           delayed_rels d'
    where
    mk_delayed_rel_name :: (String, Delay) -> String
    mk_delayed_rel_name (rel, del) = scoped rel_scope $ "__" ++ show (delayDelay del) ++ "_" ++ rel_name
        where
        rel_scope = nameScope rel
        rel_name = nameLocalStr rel

    -- Find all delayed rels in the program, replacing each occurrence
    -- of 'R-n' with '__n_R'.
    (delayed_rels, d') = runState
        (progAtomMapM d
            $ \a@Atom{..} ->
               if atomIsDelayed a
               then do modify $ S.insert (atomRelation, atomDelay)
                       return a{ atomRelation = mk_delayed_rel_name (atomRelation, atomDelay)
                               , atomDelay = delayZero}
               else return a)
        S.empty

-- For each differentiated relation 'R'' that occurs in the program,
-- add a synonym relation '__diff_R' defined as '__diff_R :- R'' and replace
-- all occurrences of 'R'' with '__diff_R'.  This guarantees that
-- differentiation always applies to the entire body of a rule in the
-- transformed program.
-- NOTE: this transformation must be applied after simplifyDelayedRels,
-- as it doesn't handle differentiated delayed relations.
simplifyDifferentiation :: DatalogProgram -> DatalogProgram
simplifyDifferentiation d =
    -- For each 'R'', add a fresh relation called "__diff_R".
    foldl' (\d_ rname ->
             let rel = getRelation d rname
                 diff_rel = Relation {
                     relPos          = nopos,
                     relAttrs        = relAttrs rel,
                     relRole         = RelInternal,
                     relSemantics    = RelMultiset,
                     relName         = mk_diff_rel_name rname,
                     relType         = typ rel,
                     relPrimaryKey   = Nothing
                 }
                 diff_rule = Rule {
                    rulePos     = nopos,
                    ruleModule  = nameScope rel,
                    ruleLHS     = [RuleLHS {
                        lhsPos = nopos,
                        lhsAtom = Atom {
                            atomPos        = nopos,
                            atomRelation   = name diff_rel,
                            atomDelay      = delayZero,
                            atomDiff       = False,
                            atomVal        = eTypedVar "x" $ typ rel
                        },
                        lhsLocation = Nothing
                     }],
                     ruleRHS    = [RHSLiteral {
                         rhsPolarity    = True,
                         rhsAtom        = Atom {
                             atomPos        = nopos,
                             atomRelation   = rname,
                             atomDelay      = delayZero,
                             atomDiff       = True,
                             atomVal        = eTypedVar "x" $ typ rel
                         }
                     }]
                 }
             in progAddRules [diff_rule] $ progAddRel diff_rel d_)
           diff_rels d'
    where
    mk_diff_rel_name :: String -> String
    mk_diff_rel_name rname = scoped rel_scope $ "__diff_" ++ rel_name
        where
        rel_scope = nameScope rname
        rel_name = nameLocalStr rname
    -- Find all differentiated rels in the program, replacing each occurrence
    -- of 'R'' with '__diff_R'.
    (diff_rels, d') = runState
        (progAtomMapM d
            $ \a@Atom{..} ->
               if atomDiff
               then do modify $ S.insert atomRelation
                       return a{ atomRelation = mk_diff_rel_name atomRelation
                               , atomDiff = False}
               else return a)
        S.empty

compileLib :: (?cfg::Config, ?specname::String, ?crate_graph::CrateGraph, ?modules::M.Map ModuleName DatalogModule)
    => DatalogProgram -> M.Map String String -> M.Map ModuleName (Doc, Doc, Doc) -> (M.Map FilePath Doc, Doc)
compileLib d d3log_rel_map rs_code =
    (crates, main_lib)
    where
    statics = collectStatics d
    -- First pass: Compile DDlog rules, generate arrangements.
    (nodes, cstate) = compileRules d statics
    -- Second pass: Populate Rust crates with compiled code in `nodes`.
    crates = M.unions $ mapIdx (compileCrate d statics nodes rs_code) $ cgCrates ?crate_graph
    -- Generate the main library file.
    main_lib = compileMainLib d d3log_rel_map nodes cstate

compileRules :: (?cfg::Config, ?specname::String, ?modules::M.Map ModuleName DatalogModule, ?crate_graph::CrateGraph) => DatalogProgram -> Statics -> ([ProgNode], CompilerState)
compileRules d statics =
    runState (do -- First pass: compute arrangements.
                 createArrangements d
                 -- Second pass: Allocate delayed rel ids.
                 allocDelayedRelIds d
                 -- Third pass: compile relations.
                 mapIdxM (\scc i -> compileSCC d statics depgraph i scc) sccs)
             $ emptyCompilerState { cArrangements = arrs }
    where
    -- Compute ordered SCCs of the dependency graph.  These will define the
    -- structure of the program.
    depgraph = progDependencyGraph d
    sccs = G.topsort' $ G.condensation depgraph
    -- Initialize arrangements map
    arrs = M.fromList $ map (, []) $ M.keys $ progRelations d

compileCrate :: (?cfg::Config, ?specname::String, ?crate_graph::CrateGraph, ?modules::M.Map ModuleName DatalogModule)
    => DatalogProgram -> Statics -> [ProgNode] -> M.Map ModuleName (Doc, Doc, Doc) -> Crate -> Int -> M.Map FilePath Doc
compileCrate d statics nodes rs_code crate crate_id =
    M.fromList $ toml:modules
    where
    -- Generate 'Cargo.toml'.
    toml = mkCargoToml rs_code crate crate_id
    -- Generate individual modules.
    modules = map (compileModule d statics nodes rs_code crate) $ S.toList crate

mkCargoToml :: (?cfg::Config, ?specname::String, ?crate_graph::CrateGraph, ?modules::M.Map ModuleName DatalogModule)
    => M.Map ModuleName (Doc, Doc, Doc) -> Crate -> Int -> (FilePath, Doc)
mkCargoToml rs_code crate crate_id =
    (filepath, code)
    where
    filepath = crateDirPath crate </> "Cargo.toml"
    code = "[package]"                                                                     $$
           "name = \"" <> pp crate_name <> "\""                                            $$
           "version = \"0.1.0\""                                                           $$
           "edition = \"2018\""                                                            $$
           ""                                                                              $$
           "[features]"                                                                    $$
           "default = []"                                                                  $$
           "flatbuf = [\"differential_datalog/flatbuf\"," <> fb_features <> "]"            $$
           "c_api = [\"differential_datalog/c_api\"," <> capi_features <> "]"              $$
           ""                                                                              $$
           "[dependencies]"                                                                $$
           "differential_datalog = { path = \"" <> pp root <> "../differential_datalog\" }"$$
           "ddlog_derive = { path = \"" <> pp root <> "../ddlog_derive\" }"$$
           "abomonation = \"0.7\""                                                         $$
           "ordered-float = { version = \"2.0.0\", features = [\"serde\"] }"               $$
           "fnv = \"1.0.2\""                                                               $$
           "once_cell = \"1.4.1\""                                                         $$
           "libc = \"0.2\""                                                                $$
           "time = { version = \"0.2\", features = [\"serde\"] }"                          $$
           "serde_json = \"1.0\""                                                          $$
           "serde = { version = \"1.0\", features = [\"derive\"] }"                        $$
           "num = \"0.3\""                                                                 $$
           "erased-serde = \"0.3\""                                                        $$
           --"differential-dataflow = \"0.11.0\""                                            $$
           --"timely = \"0.11\""                                                             $$
           "differential-dataflow = { git = \"https://github.com/ddlog-dev/differential-dataflow\", branch = \"ddlog-4\" }" $$
           "timely = { git = \"https://github.com/ddlog-dev/timely-dataflow\", branch = \"ddlog-4\",  default-features = false }"  $$
           ""                                                                              $$
           dependencies                                                                    $$
           ""                                                                              $$
           "[lib]"                                                                         $$
           "name = \"" <> pp crate_name <> "\""                                            $$
           "path = \"" <> pp main_mod_name <> ".rs\""                                      $$
           extra_toml_code
    main_mod = crateMainModule crate
    -- Main module name without path.
    main_mod_name = moduleNameLocal main_mod
    crate_name = crateName crate
    -- Path to the root of the Rust project.
    root = rootDirPath crate
    -- Dependencies.  We include all transitive dependencies.  Although usually
    -- an overkill, this may be necessary when type annotations injected by the
    -- type inference engine refer to types defined in modules that are not
    -- direct dependencies of 'crate'.
    deps = map (cgCrates ?crate_graph !!) $ crateDependenciesRec crate_id
    dependencies = vcat
                   $ map (\dep -> pp (crateName dep) <+> "= { path = \"" <> pp (crateDirPathFrom crate dep) <> "\" }" ) deps
    -- Enable 'flatbuf' and 'capi' features in all dependencies.
    fb_features = commaSep $ map (\dep -> "\"" <> pp (crateName dep) <> "/flatbuf\"") deps
    capi_features = commaSep $ map (\dep -> "\"" <> pp (crateName dep) <> "/c_api\"") deps
    -- Add 'toml' code from 'rs_code'.
    extra_toml_code = vcat $ map (sel3 . snd) $ filter ((\mname -> S.member mname crate) . fst) $ M.toList rs_code

-- Recursive version of 'crateDependencies'.
crateDependenciesRec :: (?crate_graph::CrateGraph, ?modules::M.Map ModuleName DatalogModule) => Int -> [Int]
crateDependenciesRec crate_id =
    nub $ deps ++ concatMap crateDependenciesRec deps
    where
    deps = cgCrateDependencies ?crate_graph M.! crate_id

-- Relative path from the crate directory to the top-level of the Rust project.
rootDirPath :: Crate -> String
rootDirPath crate = concat $ replicate (length (modulePath $ crateMainModule crate)) "../"

-- Relative path from 'from' crate to 'to' crate.
crateDirPathFrom :: (?specname::String) => Crate -> Crate -> String
crateDirPathFrom from to = rootDirPath from ++ crateDirPath to

-- path from the root to 'crate'.
crateDirPath :: (?specname::String) => Crate -> String
crateDirPath crate = intercalate "/" $ modulePath $ crateMainModule crate

-- Module name without path.
moduleNameLocal :: (?specname::String) => ModuleName -> String
moduleNameLocal mod_name =
    case modulePath mod_name of
         []     -> ?specname
         path   -> last path

-- The name with which 'mod_other' is visible from 'mod_local'.
relativePath :: (?specname::String, ?crate_graph::CrateGraph) => ModuleName -> ModuleName -> [String]
relativePath mod_local mod_other | mod_local == mod_other
                                 = []
                                 | crate_local == crate_other
                                 = "crate" : path_other
                                 | otherwise
                                 = crateName crate_other : path_other
    where
    crate_local = cgModuleCrate ?crate_graph mod_local
    crate_other = cgModuleCrate ?crate_graph mod_other
    path_other = modulePathFromCrateRoot crate_other mod_other

absolutePath :: (?specname::String, ?crate_graph::CrateGraph) => ModuleName -> [String]
absolutePath mod_other = (crateName crate_other : path_other)
    where
    crate_other = cgModuleCrate ?crate_graph mod_other
    path_other = modulePathFromCrateRoot crate_other mod_other

moduleFilePathFromCrateRoot :: (?specname::String) => Crate -> ModuleName -> String
moduleFilePathFromCrateRoot crate mod_name | is_main   = moduleNameLocal mod_name ++ ".rs"
                                           | is_dir    = intercalate "/" $ relative_path ++ ["mod.rs"]
                                           | otherwise = (intercalate "/" relative_path) ++ ".rs"
    where
    -- Main module of the crate?
    is_main = (mod_name == crateMainModule crate)
    -- Real module or directory?
    is_dir = any (\mod_name' -> (mod_name /= mod_name') && (modulePath mod_name) `isPrefixOf` (modulePath mod_name')) crate
    prefix_len = length $ modulePath $ crateMainModule crate
    relative_path = drop prefix_len $ modulePath mod_name

modulePathFromCrateRoot :: Crate -> ModuleName -> [String]
modulePathFromCrateRoot crate mod_name | is_main   = []
                                       | otherwise = relative_path
    where
    -- Main module of the crate?
    is_main = (mod_name == crateMainModule crate)
    prefix_len = length $ modulePath $ crateMainModule crate
    relative_path = drop prefix_len $ modulePath mod_name

compileModule :: (?cfg::Config, ?specname::String, ?crate_graph::CrateGraph)
    => DatalogProgram -> Statics -> [ProgNode] -> M.Map ModuleName (Doc, Doc, Doc) -> Crate -> ModuleName -> (FilePath, Doc)
compileModule d statics nodes rs_code crate mod_name =
    (filepath, code)
    where
    crate_statics = fromMaybe M.empty $ M.lookup (crateName crate) statics
    -- Main module of the crate?
    is_main = (mod_name == crateMainModule crate)
    filepath = crateDirPath crate </> moduleFilePathFromCrateRoot crate mod_name
    -- Start with empty module, except the main module that contains static declarations.
    prelude = if is_main
              then typesModuleHeader $+$ mkStatics d (Just crate) crate_statics
              else typesModuleHeader
    -- Submodule lists.
    children = filter (\other_mod -> other_mod `moduleIsChildOf` mod_name)
                      $ S.toList crate
    submodules = vcat (map (\(ModuleName c) -> "pub mod" <+> pp (last c) <> ";") children)
    -- Rust code.
    user_rust_code = maybe empty sel1 $ M.lookup mod_name rs_code
    -- Typedefs.
    tdefs = vcat $ map (mkTypedef d)
                 $ filter ((== mod_name) . nameScope)
                 $ M.elems $ progTypedefs d
    -- Functions.
    (fdef, fextern) = partition (isJust . funcDef)
                                $ filter ((== mod_name) . nameScope)
                                $ concat $ M.elems $ progFunctions d
    externFuncs = let ?statics = crate_statics
                  in vcat $ map (mkFunc d) fextern
    ddlogFuncs = let ?statics = crate_statics
                 in vcat $ map (mkFunc d) fdef
    -- Primary keys for relations declared in this module.
    keys = vcat
           $ concatMap (mapMaybe prelKey . filter ((== mod_name) . nameScope . prelName) . nodeRels)
           $ nodes
    -- Arrangements for relations declared in this module.
    arrangements = vcat
                   $ concatMap ((concatMap prelArrangements) . filter  ((== mod_name) . nameScope . prelName) . nodeRels)
                   $ nodes
    -- Rules specified in this module.
    rules = vcat
            $ concatMap (map snd . filter ((== mod_name) . fst) . (concatMap prelRules) . nodeRels)
            $ nodes
    -- Facts specified in this module.
    facts = vcat
            $ concatMap (map sel3 . filter ((== mod_name) . sel1) . (concatMap prelFacts) . nodeRels)
            $ nodes
    -- Transformers.
    transformers = vcat
                   $ mapMaybe (\case
                       ApplyNode mname _ acode | mname == mod_name -> Just acode
                       _ -> Nothing)
                   $ nodes
    code = prelude          $+$
           submodules       $+$
           user_rust_code   $+$
           tdefs            $+$
           externFuncs      $+$
           ddlogFuncs       $+$
           keys             $+$
           arrangements     $+$
           rules            $+$
           facts            $+$
           transformers

-- | Compile DDlog rules into Rust.
compileMainLib :: (?cfg::Config, ?specname::String, ?modules::M.Map ModuleName DatalogModule, ?crate_graph::CrateGraph) => DatalogProgram -> M.Map String String -> [ProgNode] -> CompilerState -> Doc
compileMainLib d d3log_rel_map nodes cstate =
    mainHeader                             $+$
    reexports                              $+$
    mkUpdateDeserializer d                 $+$
    mkDDValueFromRecord d                  $+$ -- Function to convert cmd_parser::Record to Value
    mkIndexesIntoArrId d cstate            $+$
    mkRelEnum d                            $+$ -- 'enum Relations'
    mkIdxEnum d                            $+$ -- 'enum Indexes'
    mkD3logImpl d d3log_rel_map            $+$
    mkProg d cstate nodes
    where
    mod_type_reexports = foldl' (mrAddTypedef d) emptyModuleReexports
                                (M.elems $ progTypedefs d)
    mod_reexports = foldl' mrAddFunction mod_type_reexports
                           (concat $ M.elems $ progFunctions d)
    reexports = "pub mod typedefs" $+$
                (braces' $ ppModuleReexports d mod_reexports)

data ModuleReexports = ModuleReexports {
    mrLocalDefs     :: [TypeDef],
    mrLocalFuncs    :: [Function],
    mrSubmodules    :: ReexportTree
}

mkD3logImpl :: (?cfg::Config, ?crate_graph::CrateGraph, ?specname::String) => DatalogProgram -> M.Map String String -> Doc
mkD3logImpl d d3log_rel_map =
    "impl_trait_d3log!(" <> commaSep rels <> ");"
    where
    rels = map (\(rout_name, rin_name) ->
                 let rout = getRelation d rout_name
                     rin  = getRelation d rin_name
                     t = mkType d Nothing rin
                 in "(" <> pp (relIdentifier d rout) <> "," <> pp (relIdentifier d rin) <> "," <> t <> ")")
           $ M.toList d3log_rel_map

type ReexportTree = M.Map String ModuleReexports

emptyModuleReexports :: ModuleReexports
emptyModuleReexports = ModuleReexports [] [] M.empty

mrAddTypedef :: DatalogProgram -> ModuleReexports -> TypeDef -> ModuleReexports
mrAddTypedef d mr tdef | tdefGetAliasAttr d tdef = mr
                       | otherwise =
    mrAddTypedef' mr (modulePath $ nameScope tdef) tdef

mrAddTypedef' :: ModuleReexports -> [String] -> TypeDef -> ModuleReexports
mrAddTypedef' mr [] def = mr { mrLocalDefs = def:(mrLocalDefs mr) }

mrAddTypedef' mr (mname:path) def =
    mr { mrSubmodules = M.alter addtdef mname $ mrSubmodules mr}
    where
    addtdef Nothing    = Just $ mrAddTypedef' emptyModuleReexports path def
    addtdef (Just mr') = Just $ mrAddTypedef' mr' path def

mrAddFunction :: ModuleReexports -> Function -> ModuleReexports
mrAddFunction mr func = mrAddFunction' mr (modulePath $ nameScope func) func

mrAddFunction' :: ModuleReexports -> [String] -> Function -> ModuleReexports
mrAddFunction' mr [] func = mr { mrLocalFuncs = func:(mrLocalFuncs mr) }

mrAddFunction' mr (mname:path) func =
    mr { mrSubmodules = M.alter addfunc mname $ mrSubmodules mr}
    where
    addfunc Nothing    = Just $ mrAddFunction' emptyModuleReexports path func
    addfunc (Just mr') = Just $ mrAddFunction' mr' path func

ppModuleReexports  :: (?crate_graph::CrateGraph, ?specname::String) => DatalogProgram -> ModuleReexports -> Doc
ppModuleReexports d ModuleReexports{..} =
    (vcat $ map (\tdef -> "pub use ::" <> rnameScoped Nothing (name tdef) <> semi)
            mrLocalDefs)
    $+$
    (vcat $ map (\func -> "pub use ::" <> mkFuncName d Nothing func <> semi)
            mrLocalFuncs)
    $+$
    (vcat $ map (\(mname, mr') -> ("pub mod " <> pp mname) $+$
                                  (braces' $ ppModuleReexports d mr'))
          $ M.toList mrSubmodules)


-- Generate Deserialize implementation for UpdateSerializer wrapper.
mkUpdateDeserializer :: (?crate_graph::CrateGraph, ?specname::String) => DatalogProgram -> Doc
mkUpdateDeserializer d =
    "decl_update_deserializer!(UpdateSerializer," <> commaSep rels <> ");"
    where
    rels = map (\rel -> "(" <> pp (relIdentifier d rel) <> "," <+> mkType d Nothing rel <> ")" )
           $ filter (\rel -> elem (relRole rel) [RelInput, RelOutput])
           $ M.elems $ progRelations d


-- This type stores the set of statically evaluated constant sub-expressions
-- for each crate in the program.
-- We need to track the type of each expression, as generic functions like
-- 'set_empty()' can have different types in different contexts.
-- The value associated with each key is the unique index of the static
-- expression used to generate a name for it.
type CrateStatics = M.Map (Expr, Type) Int
type Statics = M.Map String CrateStatics

moduleStatics :: (?crate_graph::CrateGraph) => Statics -> ModuleName -> CrateStatics
moduleStatics statics modname =
    fromMaybe M.empty $ M.lookup (crateName $ cgModuleCrate ?crate_graph modname) statics

addStatic :: (?crate_graph::CrateGraph) => DatalogProgram -> Expr -> ECtx -> Statics -> Statics
addStatic d e ctx statics =
    M.alter (\case
              Nothing -> Just $ M.singleton (e, exprType'' d ctx e) 0
              Just mod_statics -> Just $ M.alter (maybe (Just $ M.size mod_statics) Just) (e, exprType'' d ctx e) mod_statics)
            (crateName $ ctxCrate ctx)
            statics

deleteStatic :: Expr -> Type -> CrateStatics -> CrateStatics
deleteStatic e t statics = M.delete (e,t) statics

lookupStatic :: DatalogProgram -> Expr -> ECtx -> CrateStatics -> Maybe Int
lookupStatic d e ctx statics = M.lookup (e, exprType'' d ctx e) statics

-- Extract all subexpressions of the program to be evaluated as lazy statics.
-- Rust lazy statics are computed once on the first access.  The current policy
-- is to evaluate function calls that take constant arguments and are side
-- effect-free statically.
collectStatics :: (?crate_graph::CrateGraph) => DatalogProgram -> Statics
collectStatics d =
    execState (progExprMapCtxM d checkStaticExpr) M.empty
    where
    checkStaticExpr :: ECtx -> ENode -> State Statics Expr
    checkStaticExpr ctx e = do
        when (exprIsStatic d ctx (E e)) $ modify (addStatic d (E e) ctx)
        return $ E e

-- Generate Rust lazy static declarations, one for each static expression
-- computed by 'collectStatics'.
mkStatics :: (?crate_graph::CrateGraph, ?specname::String) => DatalogProgram -> Maybe Crate -> CrateStatics -> Doc
mkStatics d crate statics =
    vcat
        $ map
            ( \((e, t), i) ->
                  let static_name = "__STATIC_" ++ show i
                   in let ?statics = deleteStatic e t statics
                      in let -- Manufacture a bogus function to make sure that the static expression
                             -- is evaluated in the context of the module where the static is declared.
                             bogus_func = Function nopos [] (scoped modname static_name) [] (tTuple []) (Just $ eTuple [])
                             e' = mkExpr d (CtxFunc bogus_func) (eTyped e t) EVal
                         in "pub static" <+> pp static_name <> ": ::once_cell::sync::Lazy<" <> mkType d scope t <> ">" <+> "="
                                <+> "::once_cell::sync::Lazy::new(||" <+> e' <> ");"
            )
        $ M.toList statics
    where
    -- Module where the static declaration should be places:
    -- Either the main module of 'crate', or the main module of the
    -- entire Rust project ("_"), if 'crate' is 'Nothing'.
    scope = crateMainModule <$> crate
    modname = fromMaybe (ModuleName ["_"]) scope

-- Add dummy relation to the spec if it does not contain any.
-- Otherwise, we have to tediously handle this corner case in various
-- parts of the compiler.
-- Likewise, add a dummy index if the spec does not contain any.
addDummyRel :: DatalogProgram -> DatalogProgram
addDummyRel d =
    d {progRelations = rels, progIndexes = idxs}
    where
    rels = if (M.null $ progRelations d) || (M.null $ progIndexes d)
              then M.insert "__Null" (Relation nopos [] RelInternal RelSet "__Null" (tTuple []) Nothing) (progRelations d)
              else progRelations d
    idxs = if M.null $ progIndexes d
              then M.singleton "__Null_by_none" $ Index nopos "__Null_by_none" [] $ Atom nopos "__Null" delayZero False $ eTuple []
              else progIndexes d

mkTypedef :: (?crate_graph::CrateGraph, ?specname::String) => DatalogProgram -> TypeDef -> Doc
-- Don't generate definitions of types annotated with the 'alias' attribute.
mkTypedef d tdef | tdefGetAliasAttr d tdef = empty
mkTypedef d tdef@TypeDef{..} =
    case tdefType of
         Just TStruct{..} | length typeCons == 1
                          -> let (fields, extras) = unzip $ map (mkField tdefName True) $ consArgs $ head typeCons in
                             derive_struct                                                             $$
                             rust_attrs                                                                $$
                             "#[ddlog(rename = \"" <> pp (name $ head typeCons) <> "\")]"              $$
                             "pub struct" <+> nameLocal tdefName <> targs <+> "{"                      $$
                             (nest' $ vcat $ punctuate comma fields)                                   $$
                             "}"                                                                       $$
                             impl_abomonate                                                            $$
                             display                                                                   $$
                             vcat extras
                          | otherwise
                          -> let (constructors, extras) = unzip $ map mkConstructor typeCons in
                             derive_enum                                                               $$
                             rust_attrs                                                                $$
                             "#[ddlog(rename = \"" <> pp tdefName <> "\")]"                            $$
                             "pub enum" <+> nameLocal tdefName <> targs <+> "{"                        $$
                             (nest' $ vcat $ punctuate comma constructors)                             $$
                             "}"                                                                       $$
                             impl_abomonate                                                            $$
                             display                                                                   $$
                             default_enum                                                              $$
                             vcat extras
         Just t           -> rust_attrs                                                                $$
                             "pub type" <+> nameLocal tdefName <+> targs <+> "=" <+> mkType d scope t <> ";"
         Nothing          -> empty -- The user must provide definitions of opaque types
    where
    rust_attrs = vcat $ map (\attr -> "#[" <> pp attr <> "]") $ getRustAttrs d tdefAttrs
    derive_serialize = if tdefGetCustomSerdeAttr d tdef then empty else ", Serialize, Deserialize"
    derive_fromrec = if tdefGetCustomFromRecord d tdef then empty else ", FromRecord"
    derive_struct = "#[derive(Eq, Ord, Clone, Hash, PartialEq, PartialOrd, IntoRecord, Mutator, Default" <> derive_serialize <> derive_fromrec <> ")]"
    derive_enum = "#[derive(Eq, Ord, Clone, Hash, PartialEq, PartialOrd, IntoRecord, Mutator" <> derive_serialize <> derive_fromrec <> ")]"
    targs = if null tdefArgs
               then empty
               else "<" <> (hsep $ punctuate comma $ map pp tdefArgs) <> ">"
    targs_traits = if null tdefArgs
                      then empty
                      else "<" <> (hsep $ punctuate comma $ map ((<> ": ::ddlog_rt::Val") . pp) tdefArgs) <> ">"
    targs_disp = if null tdefArgs
                    then empty
                    else "<" <> (hsep $ punctuate comma $ map ((<> ": ::std::fmt::Debug") . pp) tdefArgs) <> ">"
    targs_def = if null tdefArgs
                   then empty
                   else "<" <> (hsep $ punctuate comma $ map ((<> ": ::std::default::Default") . pp) tdefArgs) <> ">"
    scope = Just $ nameScope tdef

    -- Generate struct field, including #-annotations.
    -- If this field requires its own serde module (due to the deserialize_map_from_array attribute),
    -- this code is returned in the second component of the tuple.
    mkField :: String -> Bool -> Field -> (Doc, Doc)
    mkField cons pub f = ( from_arr_attr $$
                           vcat (map (\attr -> "#[" <> pp attr <> "]") rattrs) $$
                           ddlog_rename f $$
                           (if pub then "pub" else empty) <+> pp (name f) <> ":" <+> mkType d scope f
                        , from_array_module)
        where rattrs = getRustAttrs d $ fieldAttrs f
              TOpaque _ _ [ktype, vtype] = typ' d f
              from_arr_attr = maybe empty (\_ -> "#[serde(with=\"" <> from_array_module_name <> "\")]")
                              $ fieldGetDeserializeArrayAttr d f
              from_array_module = maybe empty (\fname -> let kfunc = fst $ getFunc d fname [vtype] ktype in
                                                         "ddlog_rt::deserialize_map_from_array!(" <>
                                                         from_array_module_name <> "," <>
                                                         mkType d scope ktype <> "," <> mkType d scope vtype <> "," <>
                                                         mkFuncName d scope kfunc <> ");")
                                  $ fieldGetDeserializeArrayAttr d f
              from_array_module_name = "__serde" <> rnameFlat cons <> "_" <> pp (name f)

    mkConstructor :: Constructor -> (Doc, Doc)
    mkConstructor c = (cons, vcat extras)
        where
        (fields, extras) = unzip $ map (mkField (name c) False) $ consArgs c
        args = vcat $ punctuate comma fields
        rattrs = getRustAttrs d $ consAttrs c
        cons = "#[ddlog(rename = \"" <> pp (name c) <> "\")]"      $$
               vcat (map (\attr -> "#[" <> pp attr <> "]") rattrs) $$
               if null $ consArgs c
                  then nameLocal c
                  else nameLocal c <+> "{" $$
                       nest' args $$
                       "}"

    impl_abomonate = "impl" <+> targs_traits <+> "abomonation::Abomonation for" <+> nameLocal tdefName <> targs <> "{}"

    display = "impl" <+> targs_disp <+> "::std::fmt::Display for" <+> nameLocal tdefName <> targs <+> "{" $$
              "    fn fmt(&self, __formatter: &mut ::std::fmt::Formatter) -> ::std::fmt::Result {"             $$
              "        match self {"                                                                           $$
              (nest' $ nest' $ nest' $ vcat $ punctuate comma $ map mkDispCons $ typeCons $ fromJust tdefType) $$
              "        }"                                                                                      $$
              "    }"                                                                                          $$
              "}"                                                                                              $$
              "impl" <+> targs_disp <+> "::std::fmt::Debug for" <+> nameLocal tdefName <> targs <+> "{"        $$
              "    fn fmt(&self, f: &mut ::std::fmt::Formatter) -> ::std::fmt::Result {"                       $$
              "        ::std::fmt::Display::fmt(&self, f)"                                                     $$
              "    }"                                                                                          $$
              "}"
    mkDispCons :: Constructor -> Doc
    mkDispCons c@Constructor{..} =
        cname <> "{" <> (hcat $ punctuate comma $ map (pp . name) consArgs) <> "} => {" $$
        (nest' $
            "__formatter.write_str(\"" <> (pp $ name c) <> "{\")?;" $$
            (vcat $
             mapIdx (\a i -> (if isString d a
                                 then "differential_datalog::record::format_ddlog_str(" <> (pp $ name a) <> ", __formatter)?;"
                                 else "::std::fmt::Debug::fmt(" <> (pp $ name a) <> ", __formatter)?;")
                             $$
                             (if i + 1 < length consArgs
                                 then "__formatter.write_str(\",\")?;"
                                 else empty))
                    consArgs) $$
            "__formatter.write_str(\"}\")")
        $$
        "}"
        where cname = mkConstructorName scope tdefName (fromJust tdefType) (name c)

    default_enum =
              "impl" <+> targs_def <+> "::std::default::Default for" <+> nameLocal tdefName <> targs <+> "{"   $$
              "    fn default() -> Self {"                                                                     $$
              "        " <> cname <> "{" <> def_args <> "}"                                                    $$
              "    }"                                                                                          $$
              "}"
        where c = head $ typeCons $ fromJust tdefType
              cname = mkConstructorName scope tdefName (fromJust tdefType) (name c)
              def_args = commaSep $ map (\a -> (pp $ name a) <+> ": ::std::default::Default::default()") $ consArgs c

-- Generate #[ddlog(rename=)] attribute to rename fields that clash with
-- reserved names.
ddlog_rename :: (WithName a) => a -> Doc
ddlog_rename x = if isPrefixOf "__" (name x) && elem short reservedNames
                 then "#[ddlog(rename = \"" <> pp short <> "\")]"
                 else empty
    where short = drop 2 $ name x

{-
 pub fn relValFromRecord(rel: Relations, rec: &Record) -> Result<Value, String> {
    match rel {
        Relations::Rel1 => {
            Ok(Value::Rel1Constr(<Rel1Type>::from_record(rec)?))
        },
        ..
        _ => Err(...)
    }
}
-}
mkDDValueFromRecord :: (?cfg::Config, ?crate_graph::CrateGraph, ?specname::String) => DatalogProgram -> Doc
mkDDValueFromRecord d@DatalogProgram{..} =
    mkRelationsTryFromStr d                                                                         $$
    mkIsOutputRels d                                                                                $$
    mkIsInputRels d                                                                                 $$
    makeRelationsTypeId d                                                                           $$
    mkRelationsTryFromRelId d                                                                       $$
    mkRelId2Name d                                                                                  $$
    mkRelId2NameC                                                                                   $$
    mkRelName2OrigName d                                                                            $$
    mkRelName2OrigNameC d                                                                           $$
    mkOrigRelName2Name d                                                                            $$
    mkRelIdMap d                                                                                    $$
    mkRelIdMapC d                                                                                   $$
    mkInputRelIdMap d                                                                               $$
    mkOutputRelIdMap d                                                                              $$
    mkIndexesTryFromStr d                                                                           $$
    mkIndexesTryFromIdxId d                                                                         $$
    mkIdxId2Name d                                                                                  $$
    mkIdxId2NameC                                                                                   $$
    mkIdxIdMap d                                                                                    $$
    mkIdxIdMapC d                                                                                   $$
    "pub fn relval_from_record(relation: Relations, record: &::differential_datalog::record::Record) -> ::std::result::Result<DDValue, ::std::string::String> {" $$
    "    match relation {"                                                                          $$
    (nest' $ nest' $ vcommaSep entries)                                                             $$
    "    }"                                                                                         $$
    "}"                                                                                             $$
    "pub fn relkey_from_record(relation: Relations, record: &::differential_datalog::record::Record) -> ::std::result::Result<DDValue, ::std::string::String> {" $$
    "    match relation {"                                                                          $$
    (nest' $ nest' $ vcommaSep key_entries)                                                         $$
    "        _ => Err(format!(\"relation {:?} does not have a primary key\", relation)),"           $$
    "    }"                                                                                         $$
    "}"                                                                                             $$
    "pub fn idxkey_from_record(idx: Indexes, record: &::differential_datalog::record::Record) -> ::std::result::Result<DDValue, ::std::string::String> {" $$
    "    match idx {"                                                                               $$
    (nest' $ nest' $ vcommaSep idx_entries)                                                         $$
    "    }"                                                                                         $$
    "}"
    where
    entries = map mkrelval $ M.elems progRelations

    mkrelval :: Relation ->  Doc
    mkrelval rel@Relation{..} =
        "Relations::" <> rnameFlat (name rel) <+> "=> {"
        $$ "    Ok(<" <> mkType d Nothing t <+> "as ::differential_datalog::record::FromRecord>::from_record(record)?.into_ddvalue())"
        $$ "}"
        where t = typeNormalize d relType

    key_entries = map mkrelkey $ filter (isJust . relPrimaryKey) $ M.elems progRelations

    mkrelkey :: Relation ->  Doc
    mkrelkey rel =
        "Relations::" <> rnameFlat (name rel) <+> "=> {"
        $$ "    Ok(<" <> mkType d Nothing t <+> "as ::differential_datalog::record::FromRecord>::from_record(record)?.into_ddvalue())"
        $$ "}"
        where t = typeNormalize d $ fromJust $ relKeyType d rel

    idx_entries = map mkidxkey $ M.elems progIndexes

    mkidxkey :: Index ->  Doc
    mkidxkey idx =
        "Indexes::" <> rnameFlat (name idx) <+> "=> {"
        $$ "    Ok(<" <> mkType d Nothing t <+> "as ::differential_datalog::record::FromRecord>::from_record(record)?.into_ddvalue())"
        $$ "}"
        where t = typeNormalize d $ idxKeyType idx

-- Convert string to `enum Relations`
mkRelationsTryFromStr :: DatalogProgram -> Doc
mkRelationsTryFromStr d =
    "impl TryFrom<&str> for Relations {"                                  $$
    "    type Error = ();"                                                $$
    "    fn try_from(rname: &str) -> ::std::result::Result<Self, ()> {"   $$
    "         match rname {"                                              $$
                  (nest' $ nest' $ vcat $ entries)                        $$
    "             _  => Err(()),"                                         $$
    "         }"                                                          $$
    "    }"                                                               $$
    "}"
    where
    entries = map mkrel $ M.elems $ progRelations d
    mkrel :: Relation -> Doc
    mkrel rel = "\"" <> pp (name rel) <> "\" => Ok(Relations::" <> rnameFlat (name rel) <> "),"

mkIsOutputRels :: DatalogProgram -> Doc
mkIsOutputRels d =
    "impl Relations {" $$
    "    pub fn is_output(&self) -> bool {"        $$
    "        match self {"                         $$
                 (nest' $ nest' $ vcat $ entries)  $$
    "            _  => false"                      $$
    "        }"                                    $$
    "    }"                                        $$
    "}"
    where
    entries = map mkrel $ filter ((== RelOutput) .relRole) $ M.elems $ progRelations d
    mkrel :: Relation -> Doc
    mkrel rel = "Relations::" <> rnameFlat (name rel) <> " => true,"

mkIsInputRels :: DatalogProgram -> Doc
mkIsInputRels d =
    "impl Relations {" $$
    "    pub fn is_input(&self) -> bool {"         $$
    "        match self {"                         $$
                 (nest' $ nest' $ vcat $ entries)  $$
    "            _  => false"                      $$
    "        }"                                    $$
    "    }"                                        $$
    "}"
    where
    entries = map mkrel $ filter ((== RelInput) .relRole) $ M.elems $ progRelations d
    mkrel :: Relation -> Doc
    mkrel rel = "Relations::" <> rnameFlat (name rel) <> " => true,"

makeRelationsTypeId :: (?crate_graph::CrateGraph, ?specname::String) => DatalogProgram -> Doc
makeRelationsTypeId datalog =
    "impl Relations {"
        $$ "    pub fn type_id(&self) -> ::std::any::TypeId {"
        $$ "        match self {"
        $$ (nest' $ nest' $ nest' $ vcat type_ids)
        $$ "        }"
        $$ "    }"
        $$ "}"
  where
    type_ids = map type_id_for $ M.elems $ progRelations datalog

    type_id_for :: Relation -> Doc
    type_id_for relation =
        "Relations::" <> rnameFlat (name relation)
            <+> "=> ::std::any::TypeId::of::<" <> (mkType datalog Nothing relation) <> ">(),"

-- Convert RelId to enum Relations
mkRelationsTryFromRelId :: DatalogProgram -> Doc
mkRelationsTryFromRelId d =
    "impl TryFrom<program::RelId> for Relations {"                        $$
    "    type Error = ();"                                                $$
    "    fn try_from(rid: program::RelId) -> ::std::result::Result<Self, ()> {"    $$
    "         match rid {"                                                $$
                  (nest' $ nest' $ vcat $ entries)                        $$
    "             _  => Err(())"                                          $$
    "         }"                                                          $$
    "    }"                                                               $$
    "}"
    where
    entries = map mkrel $ M.elems $ progRelations d
    mkrel :: Relation -> Doc
    mkrel rel = pp (relIdentifier d rel) <+> "=> Ok(Relations::" <> rnameFlat (name rel) <> "),"

mkRelId2Name :: DatalogProgram -> Doc
mkRelId2Name d =
    -- TODO: Documentation on the generated function
    "pub fn relid2name(rid: program::RelId) -> ::std::option::Option<&'static str> {" $$
    "   match rid {"                                          $$
    (nest' $ nest' $ vcat $ entries)                          $$
    "       _  => None"                                       $$
    "   }"                                                    $$
    "}"
    where
    entries = map mkrel $ M.elems $ progRelations d
    mkrel :: Relation -> Doc
    mkrel rel = pp (relIdentifier d rel) <+> "=> ::core::option::Option::Some(\"" <> pp (name rel) <> "\"),"

mkRelId2NameC :: Doc
mkRelId2NameC =
    -- TODO: Documentation on the generated function
    "#[cfg(feature = \"c_api\")]"
    $$ "pub fn relid2cname(rid: program::RelId) -> ::std::option::Option<&'static ::std::ffi::CStr> {"
    $$ "    RELIDMAPC.get(&rid).copied()"
    $$ "}"

-- The original relation name is retrieved from the 'original="name"' attribute
-- if present.  Validation ensures that at most one such attribute exists and
-- that is has a string argument.
-- If no attribute exists the relation name is returned
origRelName :: Relation -> Doc
origRelName rel = let attrs = map attrVal $ filter ((== "original") . name) $ relAttrs rel in
                  case attrs of
                      [E (EString _ str)] -> pp $ str
                      -- The only remaining case should be an empty list
                      _                   -> pp $ relName rel

-- Generates a function that converts relation names into the original relation names
-- (obtained via the 'original' attribute)
mkRelName2OrigName :: DatalogProgram -> Doc
mkRelName2OrigName d =
    "pub fn rel_name2orig_name(rname: &str) -> ::std::option::Option<&'static str> {" $$
    "   match rname {"                                        $$
    (nest' $ nest' $ vcat $ entries)                          $$
    "       _  => None"                                       $$
    "   }"                                                    $$
    "}"
    where
    entries = map mkrel $ M.elems $ progRelations d
    mkrel :: Relation -> Doc
    mkrel rel = (doubleQuotes $ pp $ name rel) <+> "=> ::core::option::Option::Some(\"" <> pp (origRelName rel) <> "\"),"

-- Generates a function that converts relation names into the original relation names
-- (obtained via the 'original' attribute)
mkRelName2OrigNameC :: DatalogProgram -> Doc
mkRelName2OrigNameC d =
    "#[cfg(feature = \"c_api\")]"                                                                   $$
    "pub fn rel_name2orig_cname(rname: &str) -> ::std::option::Option<&'static ::std::ffi::CStr> {" $$
    "   match rname {"                                        $$
    (nest' $ nest' $ vcat $ entries)                          $$
    "       _  => None"                                       $$
    "   }"                                                    $$
    "}"
    where
    entries = map mkrel $ M.elems $ progRelations d
    mkrel :: Relation -> Doc
    mkrel rel = (doubleQuotes $ pp $ origRelName rel) <+> "=>" <+>
            "Some(::std::ffi::CStr::from_bytes_with_nul(b\"" <> pp (name rel) <> "\\0\")" <+>
            ".expect(\"Unreachable: A null byte was specifically inserted\")),"

-- Generates a function that converts original relation names into relation names
-- Original names are obtained via the 'original' attribute
-- (The generated code is currently not exposed in any API)
mkOrigRelName2Name :: DatalogProgram -> Doc
mkOrigRelName2Name d =
    "pub fn orig_rel_name2name(rname: &str) -> ::std::option::Option<&'static str> {" $$
    "   match rname {"                                        $$
    (nest' $ nest' $ vcat $ entries)                          $$
    "       _  => None"                                       $$
    "   }"                                                    $$
    "}"
    where
    entries = map mkrel $ M.elems $ progRelations d
    mkrel :: Relation -> Doc
    mkrel rel = (doubleQuotes $ pp $ origRelName rel) <+> "=> ::core::option::Option::Some(\"" <> pp (name rel) <> "\"),"

mkRelIdMap :: DatalogProgram -> Doc
mkRelIdMap prog =
    createLazyStatic lazy_static
    where
        mapKey rel = "Relations::" <> rnameFlat (name rel)
        mapValue rel = "\"" <> pp (name rel) <> "\""
        entries = [(mapKey rel, mapValue rel) | rel <- M.elems (progRelations prog)]
        lazy_static =
            LazyStatic
                { staticName = "RELIDMAP",
                  staticDoc = Just "A map of `RelId`s to their name as an `&'static str`",
                  keyType = "Relations",
                  valueType = "&'static str",
                  staticEntries = entries,
                  c_api = False
                }

mkRelIdMapC :: DatalogProgram -> Doc
mkRelIdMapC prog =
    createLazyStatic lazy_static
    where
        mapKey rel = pp (relIdentifier prog rel)
        -- We make a the relation into a string, make it into a byte sequence with `b""`
        -- and then append a null terminator with `\0`
        mapValue rel =
            "::std::ffi::CStr::from_bytes_with_nul(b\"" <> pp (name rel) <> "\\0\")"
                <> ".expect(\"Unreachable: A null byte was specifically inserted\")"
        entries = [(mapKey rel, mapValue rel) | rel <- M.elems (progRelations prog)]
        lazy_static =
            LazyStatic
                { staticName = "RELIDMAPC",
                  staticDoc = Just "A map of `RelId`s to their name as an `&'static CStr`",
                  keyType = "program::RelId",
                  valueType = "&'static ::std::ffi::CStr",
                  staticEntries = entries,
                  c_api = True
                }

-- REFACTOR: `mkInputRelIdMap` and `mkOutputRelIdMap` can be combined pretty easily
mkInputRelIdMap :: DatalogProgram -> Doc
mkInputRelIdMap prog =
    createLazyStatic lazy_static
    where
        mapKey rel = "Relations::" <> (rnameFlat . name $ rel)
        mapValue rel = "\"" <> (pp . name $ rel) <> "\""
        entries = [(mapKey rel, mapValue rel) | rel <- filter ((== RelInput) . relRole) (M.elems (progRelations prog))]
        lazy_static =
            LazyStatic
                { staticName = "INPUT_RELIDMAP",
                  staticDoc = Just "A map of input `Relations`s to their name as an `&'static str`",
                  keyType = "Relations",
                  valueType = "&'static str",
                  staticEntries = entries,
                  c_api = False
                }

mkOutputRelIdMap :: DatalogProgram -> Doc
mkOutputRelIdMap prog =
    createLazyStatic lazy_static
    where
        mapKey rel = "Relations::" <> (rnameFlat . name $ rel)
        mapValue rel = "\"" <> (pp . name $ rel) <> "\""
        entries = [(mapKey rel, mapValue rel) | rel <- filter ((== RelOutput) . relRole) (M.elems (progRelations prog))]
        lazy_static =
            LazyStatic
                { staticName = "OUTPUT_RELIDMAP",
                  staticDoc = Just "A map of output `Relations`s to their name as an `&'static str`",
                  keyType = "Relations",
                  valueType = "&'static str",
                  staticEntries = entries,
                  c_api = False
                }

-- Convert string to `enum Indexes`
mkIndexesTryFromStr :: DatalogProgram -> Doc
mkIndexesTryFromStr d =
    "impl TryFrom<&str> for Indexes {"                                    $$
    "    type Error = ();"                                                $$
    "    fn try_from(iname: &str) -> ::std::result::Result<Self, ()> {"   $$
    "         match iname {"                                              $$
                  (nest' $ nest' $ vcat $ entries)                        $$
    "             _  => Err(())"                                          $$
    "         }"                                                          $$
    "    }"                                                               $$
    "}"
    where
    entries = map mkidx $ M.elems $ progIndexes d
    mkidx :: Index -> Doc
    mkidx idx = "\"" <> pp (name idx) <> "\" => Ok(Indexes::" <> rnameFlat (name idx) <> "),"

mkIndexesIntoArrId :: DatalogProgram -> CompilerState -> Doc
mkIndexesIntoArrId d CompilerState{..} =
    -- TODO: Documentation on the generated function
    "pub fn indexes2arrid(idx: Indexes) -> program::ArrId {"         $$
    "    match idx {"                                                $$
    (nest' $ nest' $ vcat $ entries)                                 $$
    "    }"                                                          $$
    "}"
    where
    entries = map mkidx $ M.elems $ progIndexes d
    mkidx :: Index -> Doc
    mkidx idx = "Indexes::" <> rnameFlat (name idx) <+> "=> (" <+> pp (relIdentifier d $ idxRelation d idx)
                                                    <> "," <+> pp (findidx idx) <> "),"
    findidx :: Index -> Int
    findidx idx@Index{..} =
        fromJust
        $ findIndex (elem idx . arngUsedInIndexes)
        $ cArrangements M.! atomRelation idxAtom

-- Convert IdxId to `enum Indexes`
mkIndexesTryFromIdxId :: DatalogProgram -> Doc
mkIndexesTryFromIdxId d =
    "impl TryFrom<program::IdxId> for Indexes {"                          $$
    "    type Error = ();"                                                $$
    "    fn try_from(iid: program::IdxId) -> ::core::result::Result<Self, ()> {" $$
    "         match iid {"                                                $$
                  (nest' $ nest' $ vcat $ entries)                        $$
    "             _  => Err(())"                                          $$
    "         }"                                                          $$
    "    }"                                                               $$
    "}"
    where
    entries = map mkidx $ M.elems $ progIndexes d
    mkidx :: Index -> Doc
    mkidx idx = pp (idxIdentifier d idx) <+> "=> Ok(Indexes::" <> rnameFlat (name idx) <> "),"

mkIdxId2Name :: DatalogProgram -> Doc
mkIdxId2Name d =
    -- TODO: Documentation on the generated function
    "pub fn indexid2name(iid: program::IdxId) -> ::std::option::Option<&'static str> {" $$
    "   match iid {"                                            $$
    (nest' $ nest' $ vcat $ entries)                            $$
    "       _  => None"                                         $$
    "   }"                                                      $$
    "}"
    where
    entries = map mkidx $ M.elems $ progIndexes d
    mkidx :: Index -> Doc
    mkidx idx = pp (idxIdentifier d idx) <+> "=> ::core::option::Option::Some(\"" <> pp (name idx) <> "\"),"

mkIdxId2NameC :: Doc
mkIdxId2NameC =
    -- TODO: Documentation on the generated function
    "#[cfg(feature = \"c_api\")]"
    $$ "pub fn indexid2cname(iid: program::IdxId) -> ::std::option::Option<&'static ::std::ffi::CStr> {"
    $$ "    IDXIDMAPC.get(&iid).copied()"
    $$ "}"

-- Generates a static HashMap named `IDXIDMAP` that associates `Indexes`s to their name
-- rendered as an `&'static str`
mkIdxIdMap :: DatalogProgram -> Doc
mkIdxIdMap prog =
    createLazyStatic lazy_static
    where
        mapKey idx = "Indexes::" <> (rnameFlat . name $ idx)
        mapValue idx = "\"" <> (pp . name $ idx) <> "\""
        entries = [(mapKey idx, mapValue idx) | idx <- M.elems (progIndexes prog)]
        lazy_static =
            LazyStatic
                { staticName = "IDXIDMAP",
                  -- TODO: Doc comment for the generated static
                  staticDoc = Just "A map of `Indexes` to their name as an `&'static str`",
                  keyType = "Indexes",
                  valueType = "&'static str",
                  staticEntries = entries,
                  c_api = False
                }

-- Generates a static HashMap named `IDXIDMAPC` that associates `IdxId`s to their name
-- rendered as an `&'static CStr`
mkIdxIdMapC :: DatalogProgram -> Doc
mkIdxIdMapC prog =
    createLazyStatic lazy_static
    where
        mapKey idx = pp (idxIdentifier prog idx)
        -- We make a the relation into a string, make it into a byte sequence with `b""`
        -- and then append a null terminator with `\0`
        mapValue idx =
            "::std::ffi::CStr::from_bytes_with_nul(b\"" <> pp (name idx) <> "\\0\")"
                <> nest' ".expect(\"Unreachable: A null byte was specifically inserted\")"
        entries = [(mapKey idx, mapValue idx) | idx <- M.elems (progIndexes prog)]
        lazy_static =
            LazyStatic
                { staticName = "IDXIDMAPC",
                  staticDoc = Just "A map of `IdxId`s to their name as an `&'static CStr`",
                  keyType = "program::IdxId",
                  valueType = "&'static ::std::ffi::CStr",
                  staticEntries = entries,
                  c_api = True
                }

-- The data required to make a static HashMap
data LazyStatic = LazyStatic {
    -- The name of the static (Rust convention calls for SCREAMING_SNAKE_CASE)
    staticName    :: String,
    -- The documentation for the static, displayed as a doc comment
    staticDoc     :: Maybe String,
    -- The static's HashMap's key's type
    keyType       :: String,
    -- The static's HashMap's value's type
    valueType     :: String,
    -- (key, value) pairs of each entry to be placed in the HashMap
    staticEntries :: [(Doc, Doc)],
    -- Whether or not the map should be feature-gated
    c_api :: Bool
}

-- Creates a static variable holding a `FnvHashMap` pre-allocated and pre-filled with the supplied values
--
-- The generated code will take roughly this form:
-- ```rust
-- /// {documentation}
-- #[cfg(feature = "c_api")] // only if c_api is true
-- pub static {static name}: ::once_cell::sync::Lazy<::fnv::FnvHashMap<{key}, {value}>> =
--     ::once_cell::sync::Lazy::new(|| {
--         let mut map = ::fnv::FnvHashMap::with_capacity_and_hasher({length elements}, ::fnv::FnvBuildHasher::default());
--         // For each element
--         map.insert({key}, {value});
--
--         map
--     });
-- ```
createLazyStatic :: LazyStatic -> Doc
createLazyStatic lazy_static =
    doc_comment
        $$ (if (c_api lazy_static) then "#[cfg(feature = \"c_api\")]\n" else "")
        <> "pub static" <+> static_name <> ": ::once_cell::sync::Lazy<::fnv::FnvHashMap<" <> key_type <> "," <+> value_type <> ">> ="
        $$ "    ::once_cell::sync::Lazy::new(|| {"
        -- Pre-allocate the HashMap, maps using a hasher other than `RandomState` can't use `with_capacity()`, so we
        -- use `with_capacity_and_hasher()`, giving it our pre-allocation capacity and a default hasher provided by fnv
        $$ "        let mut map = ::fnv::FnvHashMap::with_capacity_and_hasher(" <> (int map_len) <> ", ::fnv::FnvBuildHasher::default());"
        $$          (nest' . nest' $ vcat entries)
        $$ "        map"
        $$ "    });"
    where
        -- Get the total number of entries so we can pre-allocate the map
        map_len = length (staticEntries lazy_static)
        entries = map (hashMapIndex "map") (staticEntries lazy_static)
        static_name = text $ staticName lazy_static
        doc_comment = maybe empty (\doc -> "///" <+> text doc) $ staticDoc lazy_static
        key_type = text $ keyType lazy_static
        value_type = text $ valueType lazy_static

        -- Creates a `<map>.insert(<key>, <value>);` to insert a value
        hashMapIndex :: String -> (Doc, Doc) -> Doc
        hashMapIndex map_name (key, value) =
            text map_name <> ".insert(" <> key <> "," <+> value <> ");"

mkFunc :: (?statics::CrateStatics, ?crate_graph::CrateGraph, ?specname::String) => DatalogProgram -> Function -> Doc
mkFunc d f@Function{..} | isJust funcDef =
    "pub fn" <+> mkFuncNameShort d f <> tvars <> (parens $ hsep $ punctuate comma $ map mkArg funcArgs) <+> "->" <+> mkType d scope funcType $$
    "{"                                                                                                                                              $$
    (nest' $ mkExpr d (CtxFunc f) (fromJust funcDef) EVal)                                                                                           $$
    "}"
                        | -- generate commented out prototypes of extern functions for user convenvience.
                          otherwise = "/* fn" <+> (mkFuncNameShort d f) <> tvars <> (parens $ hsep $ punctuate comma $ map mkArg funcArgs) <+> "->" <+> mkType d scope funcType <+> "*/"
    where
    scope = Just $ nameScope f
    mkArg :: FuncArg -> Doc
    mkArg a = pp (name a) <> ":" <+> "&" <> (if argMut a then "mut" else empty) <+> mkType d scope a
    tvars = case funcTypeVars f of
                 []  -> empty
                 tvs -> "<" <> (hcat $ punctuate comma $ map ((<> ": ::ddlog_rt::Val") . pp) tvs) <> ">"

-- Find all delayed relations used in the program and assign each a RelId.
allocDelayedRelIds :: DatalogProgram -> CompilerMonad ()
allocDelayedRelIds d = do
    mapM_ (\Rule{..} ->
            mapM_ (\case
                    RHSLiteral{..} | atomIsDelayed rhsAtom -> do
                        let rel_delay = (atomRelation rhsAtom, atomDelay rhsAtom)
                        -- Allocate delayed relation indexes after the last
                        -- non-delayed relation index.
                        let fst_index = length (progRelations d) + 1
                        relid <- gets $ (M.lookup rel_delay) . cDelayedRelIds
                        case relid of
                             Just _ -> return ()
                             Nothing -> modify $ \c -> c{ cDelayedRelIds = M.insert rel_delay (fst_index + M.size (cDelayedRelIds c))
                                                                                    $ cDelayedRelIds c }
                    _ -> return ()) ruleRHS)
          $ progRules d

-- Precompute the set of arrangements used by the program.  This is done as a separate
-- compiler pass to maximize arrangement sharing: if a particular key is only used in
-- semijoin operators, then it is sufficient to create a cheaper 'Arrangement.Set' for it.
-- If it is also used in a join operator or an index, then we create an 'Arrangement.Map'
-- and share it between all relevant joins, indexes, and semijoins.
createArrangements :: DatalogProgram -> CompilerMonad ()
createArrangements d = do
    -- Iterate through all rules in the program; precompute the set of arrangements for each
    -- relation.
    mapM_ (createRelArrangements d) $ progRelations d
    -- Arrangements for all program indexes.
    mapM_ (createIndexArrangement d) $ progIndexes d

createIndexArrangement :: DatalogProgram -> Index -> CompilerMonad ()
createIndexArrangement d idx@Index{..} = do
    let marr = arrangeInput d idxAtom
               $ map (\f -> (eVar $ name f, error $ "createIndexArrangement " ++ show idx ++ ": " ++ name f ++ " context should not be used")) idxVars
    case marr of
         Nothing -> error $ "createIndexArrangement " ++ show idx ++ ": failed to compute index arrangement."
         Just arr -> addJoinArrangement d (atomRelation idxAtom) arr (Just idx)

createRelArrangements :: DatalogProgram -> Relation -> CompilerMonad ()
createRelArrangements d rel = mapM_ (createRuleArrangements d) $ relRules d $ name rel

createRuleArrangements :: DatalogProgram -> Rule -> CompilerMonad ()
createRuleArrangements d rule = do
    -- Arrange the first atom in the rule if needed
    let arr = ruleArrangeFstLiteral d rule
    when (isJust arr)
         $ addJoinArrangement d (atomRelation $ rhsAtom $ head $ ruleRHS rule) (fromJust arr) Nothing
    mapM_ (createRuleArrangement d rule) [1..(length (ruleRHS rule) - 1)]

createRuleArrangement :: DatalogProgram -> Rule -> Int -> CompilerMonad ()
createRuleArrangement d rule idx = do
    let rhs = ruleRHS rule !! idx
    let ctx = CtxRuleRAtom rule idx
    let rel = getRelation d $ atomRelation $ rhsAtom rhs
    let (arr, _) = normalizeArrangement d ctx $ atomVal $ rhsAtom rhs
    -- If the literal does not introduce new variables, it's a semijoin.
    let is_semi = null $ ruleRHSNewVars d rule idx
    case rhs of
         RHSLiteral True _ | is_semi   -> addSemijoinArrangement d (name rel) arr
                           | otherwise -> addJoinArrangement d (name rel) arr Nothing
         RHSLiteral False _            -> addAntijoinArrangement d (name rel) arr
         _                             -> return ()

-- Generate Rust struct for ProgNode.
compileSCC :: (?cfg::Config, ?crate_graph::CrateGraph, ?specname::String) => DatalogProgram -> Statics -> DepGraph -> Int -> [G.Node] -> CompilerMonad ProgNode
compileSCC d statics dep i nodes | recursive = compileSCCNode d statics relnames
                                 | otherwise =
    case depnode of
         DepNodeRel rel -> compileRelNode d statics rel
         DepNodeApply a -> return $ let ?statics = moduleStatics statics $ applyModule a
                                    in compileApplyNode d a i
    where
    recursive = any (\(from, to) -> elem from nodes && elem to nodes) $ G.edges dep
    relnames = map ((\(DepNodeRel rel) -> rel) . fromJust . G.lab dep) nodes
    depnode = fromJust $ G.lab dep $ head nodes

compileRelNode :: (?cfg::Config, ?specname::String, ?crate_graph::CrateGraph) => DatalogProgram -> Statics -> String -> CompilerMonad ProgNode
compileRelNode d statics relname = do
    rel <- compileRelation d statics relname
    return $ RelNode rel

{-
Generates a call to transformer function.  Assumes the following calling convention illustrated by
the example below:

* Every input relation corresponds to an argument of type &Collection<>
* This argument must be followed by an extra argument of closure type that extracts a typed record
  from 'Value'.
* Input functions correspond to input argument of corresponding function type
* There is an additional input argument per output relation that wraps an instance of the
  corresponding record type into a value.
* The function returns a tuple of one or more computed collections

DDlog declaration:

extern transformer Scc(Edges:   relation['E],
                       from:    function(e: 'E): 'N,
                       to:      function(e: 'E): 'N)
    -> (SCCLabels: relation [('N, 'N)])

Corresponding Rust declaration:
pub fn graph_SCC<S,V,E,N,EF,LF>(edges: &Collection<S,V,Weight>, _edges: EF,
                                from: fn(&E) -> N,
                                to:   fn(&E) -> N,
                                _scclabels: LF) -> (Collection<S,V,Weight>)
where
     S: Scope,
     S::Timestamp: Lattice + Ord,
     V: Val,
     N: Val,
     E: Val,
     EF: Fn(V) -> E + 'static,
     LF: Fn((N,N)) -> V + 'static
-}
compileApplyNode :: (?cfg :: Config, ?crate_graph::CrateGraph, ?specname::String) => DatalogProgram -> Apply -> Int -> ProgNode
compileApplyNode d Apply{..} idx =
    ApplyNode applyModule idx $
               "pub fn __apply_" <> pp idx <+> "() -> Box<"
            $$ "    dyn for<'a> Fn("
            $$ "        &mut ::fnv::FnvHashMap<"
            $$ "            program::RelId,"
            $$ "            collection::Collection<"
            $$ "            scopes::Child<'a, worker::Worker<communication::Allocator>, program::TS>,"
            $$ "                DDValue,"
            $$ "                Weight,"
            $$ "            >,"
            $$ "        >,"
            $$ "    ),"
            $$ "> {"
            $$ "    Box::new(|collections| {"
            $$ "        let (" <> commaSep outputs <> ") =" <+> rnameScoped (Just applyModule) applyTransformer <> (parens $ commaSep inputs) <> ";"
            $$ (nest' $ nest' $ vcat update_collections)
            $$ "    })"
            $$ "}"
    where
    Transformer{..} = getTransformer d applyTransformer
    inputs =
        concatMap
            ( \(i, ti) ->
                if hotypeIsFunction (hofType ti)
                    then [rnameScoped (Just applyModule) i]
                    else ["collections.get(&(" <> relId d i <> ")).unwrap()", extractValue (relType $ getRelation d i)]
            )
            (zip applyInputs transInputs)
            ++ map (\_ -> parens $ "|v| v.into_ddvalue()") applyOutputs
    outputs = map rnameFlat applyOutputs
    update_collections = map (\o -> "collections.insert(" <> relId d o <> "," <+> rnameFlat o <> ");") applyOutputs
    extractValue :: Type -> Doc
    extractValue t = parens $
            "|" <> vALUE_VAR <> ": DDValue| unsafe {<" <> mkType d (Just applyModule) t <> ">::from_ddvalue_unchecked(" <> vALUE_VAR <> ") }"

compileSCCNode :: (?cfg::Config, ?specname::String, ?crate_graph::CrateGraph) => DatalogProgram -> Statics -> [String] -> CompilerMonad ProgNode
compileSCCNode d statics relnames = do
    prels <- mapM (\rel -> do prel <- compileRelation d statics rel
                              -- Only `distinct` relation if its weights can grow
                              -- unboundedly inside fixed point computation.
                              return $ RecProgRel prel $ not $ relIsBounded d rel) relnames
    return $ SCCNode prels

{- Generate Rust representation of relation and associated rules.

//Example code generated by this function:
let ancestor = {
    let ancestorset = ancestorset.clone();
    Relation {
        name:         "ancestor".to_string(),
        input:        false,
        id:           2,
        rules:        vec![
            Rule{
                rel: 1,
                xforms: vec![]
            },
            Rule{
                rel: 2,
                xforms: vec![XForm::Join{
                    afun:        &(arrange_by_snd as ArrangeFunc<Value>),
                    arrangement: (1,0),
                    jfun:        &(jfun as JoinFunc<Value>)
                }]
            }],
        arrangements: vec![
            Arrangement{
                name: "arrange_by_ancestor".to_string(),
                afun: &(arrange_by_fst as ArrangeFunc<Value>)
            },
            Arrangement{
                name: "arrange_by_self".to_string(),
                afun: &(arrange_by_self as ArrangeFunc<Value>)
            }],
        change_cb:    Arc::new(move |v,pol| set_update("ancestor", &ancestorset, v, pol))
    }
};
-}
compileRelation :: (?cfg::Config, ?specname::String, ?crate_graph::CrateGraph) => DatalogProgram -> Statics -> String -> CompilerMonad ProgRel
compileRelation d statics rn = do
    let rel@Relation{..} = getRelation d rn
    let -- Each rule is stored in a static variable in the module where the rule is declared.
        rule_name :: Int -> Doc
        rule_name i = "__Rule_" <> rnameFlat (name rel) <> "_" <> pp i
        -- Each fact is stored in a static variable in the module where the rule is declared.
        fact_name :: Int -> Doc
        fact_name i = "__Fact_" <> rnameFlat (name rel) <> "_" <> pp i
        -- Each arrangement is stored in a static variable in the module where the relation is declared.
        arng_name :: Int -> Doc
        arng_name i = "__Arng_" <> rnameFlat (name rel) <> "_" <> pp i
    -- collect all rules for this relation
    let (facts, rules) =
                partition (null . ruleRHS)
                $ relRules d rn
    rules' <- mapIdxM (\rl i -> do
                        rl' <- let ?statics = moduleStatics statics (ruleModule rl)
                               in compileRule d rl 0 True
                        return (ruleModule rl, "pub static" <+> rule_name i <+> ": ::once_cell::sync::Lazy<program::Rule> = ::once_cell::sync::Lazy::new(|| {" $$ nest' rl' $$ "});")) rules
    let facts' = mapIdx (\fact i ->
                          let fact' = let ?statics = moduleStatics statics (ruleModule fact)
                                      in compileFact d fact
                          in (ruleModule fact,
                              ridentScoped Nothing (ruleModule fact) (render $ fact_name i) <> ".clone()",
                              "pub static" <+> fact_name i <+> ": ::once_cell::sync::Lazy<(program::RelId, DDValue)> = ::once_cell::sync::Lazy::new(|| {" $$ nest' fact' $$ "});")) facts
    let (key_func, key_code) = maybe ("::core::option::Option::None", Nothing)
                               (\k -> let ?statics = moduleStatics statics (nameScope rel) in
                                      let (func_name, code) = compileKey d rel k in
                                      ( "::core::option::Option::Some(" <> ridentScoped Nothing (nameScope rel) (render func_name) <> ")"
                                      , Just code))
                               relPrimaryKey
    let cb = if relRole == RelOutput
                then "change_cb: ::core::option::Option::Some(::std::sync::Arc::clone(&__update_cb)),"
                else "change_cb: ::core::option::Option::None,"
    arrangements <- gets $ (M.! rn) . cArrangements
    let compiled_arrangements = mapIdx (\arng i ->
                                         let ?statics = moduleStatics statics (nameScope rel) in
                                         let arng' = mkArrangement d rel arng in
                                         "pub static" <+> arng_name i <+> ": ::once_cell::sync::Lazy<program::Arrangement> = ::once_cell::sync::Lazy::new(|| {" $$ nest' arng' $$ "});") arrangements

    let (position, _) = relPos
    let location = text (replace "\\" "\\\\" $ sourceName position) <> ":" <> int (sourceLine position) <> ":" <> int (sourceColumn position)
    let code =
            "let" <+> rnameFlat rn <+> "=" <+> "::differential_datalog::program::Relation {"
            $$ "    name: ::std::borrow::Cow::from(\"" <> pp rn <+> "@" <+> location <> "\"),"
            $$ "    input:" <+> (if relRole == RelInput then "true" else "false") <> ","
            $$ "    distinct:" <+> (if relRole == RelOutput && relSemantics == RelSet && not (relIsDistinctByConstruction d rel)
                                           then "true"
                                           else "false") <> ","
            $$ "    caching_mode:" <+> "::differential_datalog::program::CachingMode::" <> (case relSemantics of
                                           RelMultiset -> "Multiset"
                                           RelSet -> "Set"
                                           RelStream -> "Stream") <> ","
            $$ "    key_func:" <+> key_func <> ","
            $$ "    id:" <+> relId d rn <> ","
            $$ "    rules: vec!["
            $$ (nest' $ nest' $ vcat $ mapIdx (\rule i -> ridentScoped Nothing (ruleModule rule) (render $ rule_name i) <> ".clone(),") rules)
            $$ "    ],"
            $$ "    arrangements: vec!["
            $$ (nest' $ nest' $ vcat $ mapIdx (\_ i -> (pp $ ridentScoped Nothing (nameScope rel) (render $ arng_name i)) <> ".clone(),") arrangements)
            $$ "    ],"
            $$ (nest' cb)
            $$ "};"
    return ProgRel{ prelName            = rn
                  , prelCode            = code
                  , prelRules           = rules'
                  , prelFacts           = facts'
                  , prelKey             = key_code
                  , prelArrangements    = compiled_arrangements
                  }

compileKey :: (?cfg::Config, ?specname::String, ?statics::CrateStatics, ?crate_graph::CrateGraph) => DatalogProgram -> Relation -> KeyExpr -> (Doc, Doc)
compileKey d rel@Relation{..} KeyExpr{..} =
    let v = mkDDValue d (CtxKey rel) keyExpr
        func_name = "__Key_" <> rnameFlat (name rel)
    in ( func_name
       , "pub fn" <+> func_name <> "(" <> kEY_VAR <> ": &DDValue) -> DDValue {"                                                         $$
         "    let ref" <+> pp keyVar <+> "= *unsafe {<" <> mkType d (Just $ nameScope rel) rel <> ">::from_ddvalue_ref_unchecked(" <> kEY_VAR <> ") };"  $$
         "    " <> v                                                                                                                    $$
         "}")

{- Generate Rust representation of a ground fact -}
compileFact :: (?cfg::Config, ?specname::String, ?statics::CrateStatics, ?crate_graph::CrateGraph) => DatalogProgram -> Rule -> Doc
compileFact d rl@Rule{..} =
    let rel = atomRelation $ lhsAtom $ head ruleLHS
        v = mkDDValue d (CtxRuleLAtom rl 0) $ atomVal $ lhsAtom $ head ruleLHS
    in "(" <> relId d rel <> "," <+> v <> ") /*" <> pp rl <> "*/"

-- If the rule contains a join, antijoin, or aggregation operator then its first literal
-- will have to be arranged before applying the operator.  This function checks
-- if this arrangement can be stored as an 'Arranged' collection and, if so, returns
-- the normalized representation of the arrangement.  (This requires that two
-- additional conditions are met: (1) all operators between the first literal and
-- the join operator are filters and (2) the required arrangement allows normalized
-- representation (e.g., we cannot arrange by arbitrary arithmetic expressions at
-- the moment)).
ruleArrangeFstLiteral :: DatalogProgram -> Rule -> Maybe Expr
ruleArrangeFstLiteral d rl@Rule{..} | null ruleRHS = Nothing
                                    | otherwise = input_arrangement
  where
    -- RHSCondition's between fstatom and the next operator that is not an RHSCondition.
    conds = takeWhile (rhsIsCondition . (ruleRHS !!)) [1 .. length ruleRHS - 1]
    -- Index of the next operator
    rhs_idx = length conds + 1
    -- Next RHS operator to process
    rhs = ruleRHS !! rhs_idx
    -- Input arrangement expected by the next operator (if any)
    arrange_input_by = if rhs_idx == length ruleRHS
                          then Nothing
                          else rhsInputArrangement d rl rhs_idx rhs
    -- If we're at the start of the rule and need to arrange the input relation, generate
    -- arrangement pattern.
    input_arrangement = if all (rhsIsFilterCondition . (ruleRHS !!)) conds
                           then maybe Nothing (arrangeInput d (rhsAtom $ head ruleRHS) . fst) arrange_input_by
                           else Nothing


{- Generate Rust representation of a Datalog rule

// Example Rust code generated by this function
-}

-- Recursively scan the body of the rule; generate XFormCollection.
-- 'last_rhs_idx' is the last RHS operator already processed by 'compileRule' invocations
-- up the call stack.  '0' means we're starting to process the rule and need
-- to generate the top-level 'Rule' data structure.
--
-- 'input_val' - true if the relation generated by the last operator is a Value, not
-- tuple.  This is the case when we've only encountered antijoins so far (since antijoins
-- do not rearrange the input relation).
compileRule :: (?cfg::Config, ?specname::String, ?crate_graph::CrateGraph, ?statics::CrateStatics) => DatalogProgram -> Rule -> Int -> Bool -> CompilerMonad Doc
compileRule d rl@Rule{..} last_rhs_idx input_val = {-trace ("compileRule " ++ show rl ++ " / " ++ show last_rhs_idx) $-} do
    -- First relation in the body of the rule
    let fstatom = rhsAtom $ head ruleRHS
    let fstrel = getRelation d $ atomRelation fstatom
    -- RHSConditions between last_rhs_idx and the next operator that is not an RHSCondition.
    let conds = takeWhile (rhsIsCondition . (ruleRHS !!)) [last_rhs_idx + 1 .. length ruleRHS - 1]
    -- Index of the next operator
    let rhs_idx = last_rhs_idx + length conds + 1
    -- Next RHS operator to process
    let rhs = ruleRHS !! rhs_idx
    -- Input arrangement expected by the next operator (if any)
    let arrange_input_by = if rhs_idx == length ruleRHS
                              then Nothing
                              else rhsInputArrangement d rl rhs_idx rhs
    -- Input arrangement expected by the next operator (if any)
    let input_arrangement = ruleArrangeFstLiteral d rl
    -- If we're at the start of the rule and need to arrange the input relation, generate
    -- arrangement pattern.
    input_arrangement_id <- if last_rhs_idx == 0 && not (relIsStream fstrel) && isJust input_arrangement
                               then getJoinArrangement (atomRelation fstatom) $ fromJust input_arrangement
                               else return Nothing
    -- Open up input constructor; bring Datalog variables into scope
    let open = if input_val
               then openAtom  d ("&" <> vALUE_VAR) rl 0 (rhsAtom $ head ruleRHS) "return ::core::option::Option::None"
               else openTuple d rl ("&" <> vALUE_VAR) $ rhsVarsAfter d rl last_rhs_idx
    -- Apply filters and assignments between last_rhs_idx and rhs_idx
    let filters = mkFilters d rl last_rhs_idx
    let prefix = open $+$ vcat filters
    -- Generate XFormCollection or XFormArrangement for the 'rhs' operator.
    let mkArrangedOperator conditions inpval =
            case rhs of
                 RHSLiteral True a  -> mkJoin d conditions inpval a rl rhs_idx
                 RHSLiteral False a -> mkAntijoin d conditions inpval a rl rhs_idx
                 RHSGroupBy{}       -> mkGroupBy d conditions inpval rl rhs_idx
                 _                  -> error $ "compileRule: operator " ++ show rhs ++ " does not expect arranged input"
    let mkCollectionOperator | rhs_idx == length ruleRHS
                             = return $ mkHead d prefix rl
                             | otherwise =
            case rhs of
                 RHSFlatMap vs e -> mkFlatMap d prefix rl rhs_idx vs e
                 RHSInspect e | not (null filters) || input_val -> mkFilterMap d prefix rl rhs_idx -- filter inputs before inspecting them
                              | otherwise -> mkInspect d prefix rl rhs_idx e input_val -- pass input_val to be future-proof
                 RHSLiteral True a  -> -- Streaming join.
                                       mkJoin d conds input_val a rl rhs_idx
                 _ -> error "compileRule: operator requires arranged input"

    let is_stream_xform = rulePrefixIsStream d rl rhs_idx
    -- If: input to the operator is an arranged collection
    -- Then: create Rule::ArrangementRule, pass conditions as input to mkJoin/Antijoin
    -- Else:
    --     If: At the start of the rule:
    --     Then: Generate Rule::CollectionRule;
    --     If: Next operator requires arranged input
    --     Then: Generate Arrange operator;
    relid <- atomRelId d fstatom
    case input_arrangement_id of
       Just arid -> do
            xform <- mkArrangedOperator conds input_val
            return $ "/*" <+> pp rl <+> "*/"
                $$ "::differential_datalog::program::Rule::ArrangementRule {"
                $$ "    description: ::std::borrow::Cow::Borrowed(" <> pp (show $ render $ rulePPStripped rl) <> "),"
                $$ "    arr: (" <> relid <> "," <+> pp arid <> "),"
                $$ "    xform:" <+> xform <> ","
                $$ "}"
       Nothing -> do
            xform <- case arrange_input_by of
                        Just (key_vars, val_vars) | not (is_stream_xform && rhsIsPositiveLiteral rhs) -> do
                            -- Evaluate arrange_input_by in the context of 'rhs'
                            let key_str = parens $ commaSep $ map (pp . fst) key_vars
                            let akey = mkTupleValue d rl key_vars
                            let aval = mkVarsTupleValue rl $ map (, EReference) val_vars
                            let afun = braces' $ prefix $$ "::core::option::Option::Some((" <> akey <> "," <+> aval <> "))"
                            xform' <- mkArrangedOperator [] False
                            let arranged_xform =
                                    "::differential_datalog::program::XFormCollection::Arrange {"
                                    $$ "    description: ::std::borrow::Cow::Borrowed(" <> (pp $ show $ show $ "arrange" <+> rulePPPrefix rl (last_rhs_idx + 1) <+> "by" <+> key_str) <> "),"
                                    $$ "    afun: {"
                                    $$ "        fn __ddlog_generated_arrangement_function(" <> vALUE_VAR <> ": ::differential_datalog::ddval::DDValue) ->"
                                    $$ "            ::core::option::Option<(::differential_datalog::ddval::DDValue, ::differential_datalog::ddval::DDValue)>"
                                    $$ (nest' $ nest' afun)
                                    $$ "        __ddlog_generated_arrangement_function"
                                    $$ "    },"
                                    $$ "    next: ::std::boxed::Box::new(" <> xform' <> "),"
                                    $$ "}"
                            if is_stream_xform
                            then do let post_conds = takeWhile (rhsIsCondition . (ruleRHS !!)) [rhs_idx + 1 .. length ruleRHS - 1]
                                    next <- compileRule d rl (rhs_idx + length post_conds) False
                                    return $ "::differential_datalog::program::XFormCollection::StreamXForm {"
                                        $$ "    description: ::std::borrow::Cow::Borrowed(" <> (pp $ show $ show $ "stream-transform" <+> rulePPPrefix rl (last_rhs_idx + 1)) <> "),"
                                        $$ "    xform: ::std::boxed::Box::new(::core::option::Option::Some(" <> arranged_xform <> ")),"
                                        $$ "    next: ::std::boxed::Box::new(" <> next <> "),"
                                        $$ "}"
                            else
                                return arranged_xform

                        _ -> mkCollectionOperator
            return $
                -- input_val ensures that CollectionRule is only generated once.
                -- There is a use-case where Inspect operator is converted into a FilterMap and then an Inspect in rust code.
                -- In order to do that, mkFilterMap's 'next' is another call to compileRule with the previous rule index.
                -- Since mkFilterMap already handled the case input_val and if it is the first rule, the recursive call to compileRule
                -- to generate Inspect rust code does not need another CollectionRule.
                if last_rhs_idx == 0 && input_val
                   then "/*" <+> pp rl <+> "*/"                                         $$
                        "::differential_datalog::program::Rule::CollectionRule {"                               $$
                        "    description: ::std::borrow::Cow::from(" <> pp (show $ render $ rulePPStripped rl) <> "),"    $$
                        "    rel:" <+> relid <> ","                                     $$
                        "    xform: ::core::option::Option::Some(" <> xform <> "),"                              $$
                        "}"
                   else "::core::option::Option::Some(" <> xform <> ")"


-- 'Join', 'Antijoin', 'Semijoin', and 'GroupBy' operators take arranged collection
-- as input.  'rhsInputArrangement' returns the arrangement expected by the operator
-- or Nothing if the operator takes a flat collection (e.g., it's a 'FlatMap').
--
-- The first component of the return tuple is the list of expressions that must be used
-- as the key to index the input collection.  The second component lists variables that
-- will form the value of the arrangement.
rhsInputArrangement :: DatalogProgram -> Rule -> Int -> RuleRHS -> Maybe ([(Expr, ECtx)], [Var])
rhsInputArrangement d rl rhs_idx (RHSLiteral _ atom) =
    let ctx = CtxRuleRAtom rl rhs_idx
        (_, vmap) = normalizeArrangement d ctx $ atomVal atom
    in Just $ (map (\(_,e,c) -> (e,c)) vmap,
               -- variables visible before join that are still in use after it
               (rhsVarsAfter d rl (rhs_idx - 1)) `intersect` (rhsVarsAfter d rl rhs_idx))
rhsInputArrangement d rl rhs_idx (RHSGroupBy _ _ grpby) =
    let ctx = CtxRuleRGroupBy rl rhs_idx
    in Just $ (map (\(v, ctx') -> (eVar v, ctx')) $ exprVarOccurrences ctx grpby,
               -- all visible variables to preserve multiset semantics
               rhsVarsAfter d rl (rhs_idx - 1))
rhsInputArrangement _ _  _       _ = Nothing

mkFlatMap :: (?cfg::Config, ?specname::String, ?crate_graph::CrateGraph, ?statics::CrateStatics) => DatalogProgram -> Doc -> Rule -> Int -> Expr -> Expr -> CompilerMonad Doc
mkFlatMap d prefix rl idx vs e = do
    let vars = mkVarsTupleValue rl $ map (, EVal) $ rhsVarsAfter d rl idx
    -- New variables introduced by this FlatMap.
    let newvars = map name $ exprVarDecls d (CtxRuleRFlatMapVars rl idx) vs
    -- Flatten
    let flatten = "let __flattened =" <+> mkExpr d (CtxRuleRFlatMap rl idx) e EVal <> ";"
    -- Clone variables before passing them to the closure.
    let clones = vcat $ map ((\vname -> "let" <+> vname <+> "=" <+> cloneRef vname <> ";") . pp . name)
                      $ filter (\v -> notElem (name v) newvars) $ rhsVarsAfter d rl idx
    let fmfun = braces'
                $ prefix  $$
                  flatten $$
                  clones  $$
                  "Some(Box::new(__flattened.into_iter().map(move |" <> mkIrrefutablePatExpr d EVal (CtxRuleRFlatMapVars rl idx) vs EVal <> "|" <+> vars <> ")))"
    next <- compileRule d rl idx False
    return $
        "XFormCollection::FlatMap{"                                                                                         $$
        "    description: std::borrow::Cow::from(" <> (pp $ show $ show $ rulePPPrefix rl $ idx + 1) <> "),"                $$
        (nest' $ "fmfun: {fn __f(" <> vALUE_VAR <> ": DDValue) -> ::std::option::Option<Box<dyn Iterator<Item=DDValue>>>" $$ fmfun $$ "__f},")$$
        "    next: Box::new(" <> next <> ")"                                                                                $$
        "}"

mkFilterMap :: (?cfg::Config, ?statics::CrateStatics, ?crate_graph::CrateGraph, ?specname::String) => DatalogProgram -> Doc -> Rule -> Int -> CompilerMonad Doc
mkFilterMap d prefix rl idx = do
    let v = mkVarsTupleValue rl $ map (, EReference) $ rhsVarsAfter d rl idx
    let fmfun = braces' $ prefix $$
                          "Some" <> parens v
    next <- compileRule d rl (idx - 1) False  -- Use the previous index to evaluate the rule again and call mkInspect
    return $
        "XFormCollection::FilterMap{"                                                                                       $$
        "    description: std::borrow::Cow::from(" <> (pp $ show $ show $ rulePPPrefix rl $ idx + 1) <> "),"                $$
        nest' ("fmfun: {fn __f(" <> vALUE_VAR <> ": DDValue) -> ::std::option::Option<DDValue>" $$ fmfun $$ "__f},")        $$
        "    next: Box::new(" <> next <> ")"                                                                                $$
        "}"

mkInspect :: (?cfg::Config, ?statics::CrateStatics, ?crate_graph::CrateGraph, ?specname::String) => DatalogProgram -> Doc -> Rule -> Int -> Expr -> Bool -> CompilerMonad Doc
mkInspect d prefix rl idx e input_val = do
    -- Inspected
    let weight = "let ddlog_weight = &(" <> wEIGHT_VAR <+> "as" <+> rnameScoped Nothing wEIGHT_TYPE <> ");"
    let timestamp = if ruleIsRecursive d rl
        then "let ddlog_timestamp = &" <> rnameScoped Nothing nESTED_TS_TYPE <> "{epoch:" <+> tIMESTAMP_VAR <> ".0 as" <+> rnameScoped Nothing ePOCH_TYPE <> ", iter:" <+> rnameScoped Nothing iTERATION_TYPE <> "::from(" <> tIMESTAMP_VAR <> ".1)};"
        else "let ddlog_timestamp = &(" <> tIMESTAMP_VAR <> ".0 as" <+> rnameScoped Nothing ePOCH_TYPE <> ");"
    let inspected = (braces $ mkExpr d (CtxRuleRInspect rl idx) e EVal) <> ";"
    let ifun = braces'
                $ weight $$
                  timestamp $$
                  prefix $$
                  inspected
    next <- compileRule d rl idx input_val
    return $
        "XFormCollection::Inspect{"                                                                                         $$
        "    description: std::borrow::Cow::from(" <+> (pp $ show $ show $ rulePPPrefix rl $ idx + 1) <> "),"               $$
        (nest' $ "ifun: {fn __f(" <> vALUE_VAR <> ": &DDValue, " <> tIMESTAMP_VAR <> ": TupleTS, " <> wEIGHT_VAR <> ": Weight) -> ()" $$ ifun $$ "__f},")$$
        "    next: Box::new(" <> next <> ")"                                                                                $$
        "}"

mkGroupBy :: (?cfg::Config, ?statics::CrateStatics, ?crate_graph::CrateGraph, ?specname::String) => DatalogProgram -> [Int] -> Bool -> Rule -> Int -> CompilerMonad Doc
mkGroupBy d filters input_val rl@Rule{..} idx = do
    let rhs@RHSGroupBy{..} = ruleRHS !! idx
    let ctx = CtxRuleRProject rl idx
    let gctx = CtxRuleRGroupBy rl idx
    let Just (_, group_vars) = rhsInputArrangement d rl idx rhs
    -- Filter inputs before grouping
    let ffun = mkFFun d rl filters
    -- Compute projection.
    let open = if input_val
               then openAtom d vALUE_VAR rl 0 (rhsAtom $ head ruleRHS) "unreachable!()"
               else openTuple d rl vALUE_VAR group_vars
    let project = "{fn __f(" <> vALUE_VAR <> ": &DDValue) -> " <+> mkType d (Just ruleModule) (exprType d ctx rhsProject) $$
                  (braces' $ open $$ mkExpr d ctx rhsProject EVal)                                            $$
                  "::std::sync::Arc::new(__f)}"
    -- * Create 'struct Group'
    -- * Apply filters following group_by.
    -- * Return variables still in scope after the last filter.
    let grp = rnameScoped Nothing gROUP_TYPE <> "::new_by_ref(" <> (mkExpr d gctx rhsGroupBy EVal) <> "," <+> gROUP_VAR <> "," <+> project <> ")"
    let aggregate = "let ref" <+> pp rhsVar <+> "= unsafe{" <> grp <> "};"
    let post_filters = mkFilters d rl idx
        last_idx = idx + length post_filters
    let result = mkVarsTupleValue rl $ map (, EReference) $ rhsVarsAfter d rl last_idx
    let key_vars = exprVars d gctx rhsGroupBy
    let open_key = openTuple d rl kEY_VAR key_vars
    let agfun = braces'
                $ open_key  $$
                  aggregate $$
                  vcat post_filters   $$
                  "Some(" <> result <> ")"
    -- If this is a streaming group-by, we're generating a `StreamXForm`, and
    -- only need to compile the group-by operator and subsequent filters.
    next <- if rulePrefixIsStream d rl idx
            then return "None"
            else compileRule d rl last_idx False
    return $
        "XFormArrangement::Aggregate{"                                                                                           $$
        "    description: std::borrow::Cow::from(" <> (pp $ show $ show $ rulePPPrefix rl $ idx + 1) <> "),"                     $$
        "    ffun:" <+> ffun <> ","                                                                                              $$
        "    aggfun: {fn __f(" <> kEY_VAR <> ": &DDValue," <+> gROUP_VAR <> ": &[(&DDValue, Weight)]) -> ::std::option::Option<DDValue>" $$ agfun $$ "__f}," $$
        "    next: Box::new(" <> next <> ")"                                                                                    $$
        "}"

-- Generate Rust code to filter records and bring variables into scope.
-- The Rust code returns None if the record does not pass the filter.
-- Only extracts new variables declared in this atom, e.g., in the example
-- below `v1` and `v2` are new variables:
--
-- let (v1,v2) /*v1,v2 are references*/ = match &v {
--     Value::Rel1(v1,v2,v3) => (v1,v2),
--     _ => return None
-- };
openAtom :: (?cfg::Config, ?specname::String, ?statics::CrateStatics, ?crate_graph::CrateGraph) => DatalogProgram -> Doc -> Rule -> Int -> Atom -> Doc -> Doc
openAtom d var rl idx Atom{..} on_error =
    let rel = getRelation d atomRelation
        t = relType rel
        t' = mkRelType d (ruleModule rl) t
        varnames = map (pp . name) $ exprVarDecls d (CtxRuleRAtom rl idx) atomVal
        var_clones = tuple $ map cloneRef varnames
        vars = tuple $ map ("ref" <+>) varnames
        mtch = mkMatch (mkPatExpr d (CtxRuleRAtom rl idx) atomVal EReference False) var_clones on_error
    in "let" <+> vars <+> "= match *unsafe { <" <> t' <> ">::from_ddvalue_ref_unchecked(" <> var <> ") } {" $$
       (nest' mtch)                                                                    $$
       "};"

-- Generate Rust code to open up tuples and bring variables into scope.
openTuple :: (?cfg::Config, ?crate_graph::CrateGraph, ?specname::String) => DatalogProgram -> Rule -> Doc -> [Var] -> Doc
openTuple d rl var vs =
    let t = tTuple $ map (varType d) vs
        t' = mkRelType d (ruleModule rl) t
        pat = tupleStruct (Just $ ruleModule rl) $ map (("ref" <+>) . pp . name) vs
    in "let" <+> pat <+> "= *unsafe { <" <> t' <> ">::from_ddvalue_ref_unchecked(" <+> var <+> ") };"

-- Type 'typ x' is used as the value type of a relation.
-- Returns compiled representation of the type.
mkRelType :: (WithType a, ?crate_graph::CrateGraph, ?specname::String) => DatalogProgram -> ModuleName -> a -> Doc
mkRelType d scope x =
    let t = typeNormalize d x
    in mkType d (Just scope) t

-- Generate valid Rust identifier that encodes the type.
mkTypeIdentifier :: DatalogProgram -> Type -> Doc
mkTypeIdentifier d t' =
    case t of
         TTuple{..}     -> "__Tuple" <> pp (length typeTupArgs) <> "__" <>
                            (hcat $ punctuate "_" $ map (mkTypeIdentifier d) typeTupArgs)
         TBool{}        -> "__Boolval"
         TInt{}         -> "__Intval"
         TString{}      -> "__Stringval"
         TBit{..}       -> "__Bitval" <> pp typeWidth
         TSigned{..}    -> "__Signedval" <> pp typeWidth
         TDouble{}      -> "__Doubleval"
         TFloat{}       -> "__Floatval"
         TUser{}        -> consuser
         TOpaque{}      -> consuser
         TVar{..}       -> pp tvarName
         TFunction{..}  -> "__Closure" <> (hcat $ punctuate "_" $ map mk_arg_type typeFuncArgs)
                                       <> "_ret_"
                                       <> (mkTypeIdentifier d typeRetType)
         _              -> error $ "unexpected type " ++ show t ++ " in Compile.mkTypeIdentifier'"
    where
    t = typeNormalize d t'
    consuser = rnameFlat (typeName t) <>
               case typeArgs t of
                    [] -> empty
                    as -> "__" <> (hcat $ punctuate "_" $ map (mkTypeIdentifier d) as)
    mk_arg_type :: ArgType -> Doc
    mk_arg_type atype = (if atypeMut atype then "mut_" else "imm_") <> mkTypeIdentifier d (typ atype)


mkDDValue :: (?cfg::Config, ?specname::String, ?statics::CrateStatics, ?crate_graph::CrateGraph) => DatalogProgram -> ECtx -> Expr -> Doc
mkDDValue d ctx e =
    (parens $ mkExpr d ctx e EVal) <> ".into_ddvalue()"

mkTupleValue :: (?cfg::Config, ?statics::CrateStatics, ?crate_graph::CrateGraph, ?specname::String) => DatalogProgram -> Rule -> [(Expr, ECtx)] -> Doc
mkTupleValue d rl es =
    (parens $ tupleStruct (Just $ ruleModule rl) $ map (\(e, ctx) -> mkExpr d ctx e EVal) es) <> ".into_ddvalue()"

mkVarsTupleValue :: (?cfg::Config, ?crate_graph::CrateGraph, ?specname::String) => Rule -> [(Var, EKind)] -> Doc
mkVarsTupleValue rl vs =
    let clone (v, EReference) = cloneRef $ pp $ name v
        clone (v, _) = (pp $ name v) <> ".clone()"
    in (parens $ tupleStruct (Just $ ruleModule rl) $ map clone vs) <> ".into_ddvalue()"

mkFieldTupleValue :: (?cfg::Config, ?crate_graph::CrateGraph, ?specname::String) => ModuleName -> [Field] -> Doc
mkFieldTupleValue scope fs =
    (parens $ tupleStruct (Just scope) $ map (cloneRef . pp . name) fs) <> ".into_ddvalue()"

-- Compile all contiguous RHSCondition terms following 'last_idx'
mkFilters :: (?statics::CrateStatics, ?specname::String, ?crate_graph::CrateGraph) => DatalogProgram -> Rule -> Int -> [Doc]
mkFilters d rl@Rule{..} last_idx =
    mapIdx (\rhs i -> mkFilter d (CtxRuleRCond rl $ i + last_idx + 1) $ rhsExpr rhs)
    $ takeWhile (\case
                  RHSCondition{} -> True
                  _              -> False)
    $ drop (last_idx+1) ruleRHS

-- Implement RHSCondition semantics in Rust; brings new variables into
-- scope if this is an assignment
mkFilter :: (?statics::CrateStatics, ?crate_graph::CrateGraph, ?specname::String) => DatalogProgram -> ECtx -> Expr -> Doc
mkFilter d ctx (E e@ESet{}) = mkAssignFilter d ctx e
mkFilter d ctx e            = mkCondFilter d ctx e

mkAssignFilter :: (?statics::CrateStatics, ?specname::String, ?crate_graph::CrateGraph) => DatalogProgram -> ECtx -> ENode -> Doc
mkAssignFilter d ctx e@(ESet _ l r) =
    "let" <+> vardecls <+> "= match" <+> r' <+> "{"                 $$
    (nest' mtch)                                                    $$
    "};"
    where
    r' = mkExpr d (CtxSetR e ctx) r EVal
    mtch = mkMatch (mkPatExpr d (CtxSetL e ctx) l EVal False) vars "return None"
    expr_vardecls = exprVarDecls d (CtxSetL e ctx) l
    varnames = map (pp . name) expr_vardecls
    vars = tuple varnames
    vardecls = (tuple $ map ("ref" <+>) varnames) <> ":" <+> (tuple $ map (mkType d (Just $ ctxModule ctx) . varType d) expr_vardecls)
mkAssignFilter _ _ e = error $ "Compile.mkAssignFilter: unexpected expression " ++ show e

mkCondFilter :: (?statics::CrateStatics, ?specname::String, ?crate_graph::CrateGraph) => DatalogProgram -> ECtx -> Expr -> Doc
mkCondFilter d ctx e =
    "if !" <> mkExpr d ctx e EVal <+> "{return None;};"

-- Generate the ffun field of `XFormArrangement::Join/Semijoin/Aggregate`.
-- This field is used if input to the operator is an arranged relation.
mkFFun :: (?cfg::Config, ?specname::String, ?statics::CrateStatics, ?crate_graph::CrateGraph) => DatalogProgram -> Rule -> [Int] -> Doc
mkFFun _ Rule{} [] = "None"
mkFFun d rl@Rule{..} input_filters =
   let open = openAtom d vALUE_VAR rl 0 (rhsAtom $ ruleRHS !! 0) ("return false")
       checks = parens $ vcat $ punctuate " &&"
                $ map (\i -> mkExpr d (CtxRuleRCond rl i) (rhsExpr $ ruleRHS !! i) EVal) input_filters
   in "Some({fn __f(" <> vALUE_VAR <> ": &DDValue) -> bool"  $$
      (braces' $ open $$ checks)                             $$
      "    __f"                                              $$
      "})"

-- Different kinds of relational joins.
data JoinKind = JoinArngStream -- Join arrangement with stream
              | JoinStreamArng -- Join stream with arrangement
              | JoinArngArng   -- Join two arrangements
              deriving(Eq)

-- Compile XForm::Join or XForm::Semijoin
-- Returns generated xform and index of the last RHS term consumed by
-- the XForm
mkJoin :: (?cfg::Config, ?specname::String, ?statics::CrateStatics, ?crate_graph::CrateGraph) => DatalogProgram -> [Int] -> Bool -> Atom -> Rule -> Int -> CompilerMonad Doc
mkJoin d input_filters input_val atom rl@Rule{..} join_idx = do
    let rel = getRelation d $ atomRelation atom
    -- Are we constructing a streaming join or a regular arranged join?
    let join_kind = if relIsStream rel
                    then JoinArngStream
                    else if rulePrefixIsStream d rl join_idx
                         then JoinStreamArng
                         else JoinArngArng
    -- Build arrangement to join with
    let ctx = CtxRuleRAtom rl join_idx
    let (arr, _) = normalizeArrangement d ctx $ atomVal atom
    -- If the operator does not introduce new variables then it's a semijoin.
    let semijoin = null $ ruleRHSNewVars d rl join_idx
    semi_arr_idx <- getSemijoinArrangement (atomRelation atom) arr
    join_arr_idx <- getJoinArrangement (atomRelation atom) arr
    let (aid, is_semi) = if join_kind == JoinArngStream
                         then (0, semijoin)
                         else if semijoin
                              -- Treat semijoin as normal join if we only have a map arrangement for it.
                              then maybe (fromJust join_arr_idx, False) (,True) semi_arr_idx
                              else (fromJust join_arr_idx, False)
    -- Variables from previous terms that will be used in terms
    -- following the join.
    let Just (key_vars, post_join_vars) = rhsInputArrangement d rl join_idx $ ruleRHS !! join_idx
    -- Open input Value or tuple.
    let open_input v = if input_val
                       then openAtom d v rl 0 (rhsAtom $ ruleRHS !! 0) "return ::std::option::Option::None"
                       else openTuple d rl v post_join_vars
    -- Filter inputs using 'input_filters'
    let ffun = mkFFun d rl input_filters
    -- simplify pattern to only extract new variables from it
    let simplify (E e@EStruct{..})  = E e{exprStructFields = map (\(n,v) -> (n, simplify v)) exprStructFields}
        simplify (E e@ETuple{..})   = E e{exprTupleFields = map simplify exprTupleFields}
        simplify (E e@EVar{..}) | isNothing $ lookupVar d ctx exprVar
                                 = E e
        simplify (E e@EBinding{..}) = E e{exprPattern = simplify exprPattern}
        simplify (E e@ETyped{..})   = E e{exprExpr = simplify exprExpr}
        simplify (E e@ERef{..})     = E e{exprPattern = simplify exprPattern}
        simplify _                  = ePHolder
    -- Join function: open up both values, apply filters.
    let open = let input = case join_kind of
                                JoinStreamArng -> openTuple d rl vALUE_VAR1 post_join_vars
                                _ -> open_input vALUE_VAR1 in
               if is_semi
               then input
               else input $$
                    openAtom d vALUE_VAR2 rl join_idx (atom{atomVal = simplify $ atomVal atom}) "return ::std::option::Option::None"
    let filters = mkFilters d rl join_idx
        last_idx = join_idx + length filters
    -- If we're at the end of the rule, generate head atom; otherwise
    -- return all live variables in a tuple
    (ret, next) <- if last_idx == length ruleRHS - 1
        then return (mkDDValue d (CtxRuleLAtom rl 0) (atomVal $ lhsAtom $ head $ ruleLHS), "::std::option::Option::None")
        else do let ret = mkVarsTupleValue rl $ map (, EReference) $ rhsVarsAfter d rl last_idx
                next <- compileRule d rl last_idx False
                return (ret, next)
    let jfun = braces' $ open                     $$
                         vcat filters             $$
                         "::std::option::Option::Some" <> parens ret
    let descr = "::std::borrow::Cow::Borrowed(" <> pp (show $ show $ rulePPPrefix rl $ join_idx + 1) <> "),"
    relid <- atomRelId d atom
    return $ case join_kind of
        JoinArngStream -> renderArrangedStreamJoin d rel arr is_semi jfun ffun relid descr next
        JoinStreamArng | is_semi ->
            "XFormCollection::StreamSemijoin{"                                                                                 $$
            "    description:" <+> descr                                                                                       $$
            "    arrangement: (" <> relid <> "," <> pp aid <> "),"                                                             $$
            "    afun: {fn __f(" <> vALUE_VAR <> ": DDValue) -> ::std::option::Option<(DDValue, DDValue)>"                     $$
            nest' afun                                                                                                         $$
            "    __f},"                                                                                                        $$
            "    jfun: {fn __f(" <> vALUE_VAR1 <> ": &DDValue) -> ::std::option::Option<DDValue>"                              $$
            nest' jfun                                                                                                         $$
            "    __f},"                                                                                                        $$
            "    next: Box::new(" <> next <> ")"                                                                               $$
            "}"
                       | otherwise ->
            "XFormCollection::StreamJoin{"                                                                                     $$
            "    description:" <+> descr                                                                                       $$
            "    arrangement: (" <> relid <> "," <> pp aid <> "),"                                                             $$
            "    afun: {fn __f(" <> vALUE_VAR <> ": DDValue) -> ::std::option::Option<(DDValue, DDValue)>"                     $$
            nest' afun                                                                                                         $$
            "    __f},"                                                                                                        $$
            "    jfun: {fn __f(" <> vALUE_VAR1 <> ": &DDValue," <+> vALUE_VAR2 <> ": &DDValue) -> ::std::option::Option<DDValue>" $$
            nest' jfun                                                                                                         $$
            "    __f},"                                                                                                        $$
            "    next: Box::new(" <> next <> ")"                                                                               $$
            "}"
            where
            akey = mkTupleValue d rl key_vars
            aval = mkVarsTupleValue rl $ map (, EReference) post_join_vars
            afun = braces' $
                       open_input ("&" <> vALUE_VAR)                                                                $$
                       (vcat $ map (\i -> mkFilter d (CtxRuleRCond rl i) $ rhsExpr $ ruleRHS !! i) input_filters)   $$
                       "Some((" <> akey <> "," <+> aval <> "))"
        JoinArngArng | is_semi ->
            "XFormArrangement::Semijoin {"                                                                                   $$
            "    description:" <+> descr                                                                                    $$
            "    ffun:" <+> ffun <> ","                                                                                     $$
            "    arrangement: (" <> relid <> "," <> pp aid <> "),"                                                          $$
            "    jfun: {fn __f(_: &DDValue," <+> vALUE_VAR1 <> ": &DDValue, _" <> vALUE_VAR2 <> ": &()) -> ::std::option::Option<DDValue>" $$
            nest' jfun                                                                                                      $$
            "    __f},"                                                                                                     $$
            "    next: Box::new(" <> next  <> ")"                                                                           $$
            "}"
                    | otherwise ->
            "XFormArrangement::Join {"                                                                                       $$
            "    description:" <+> descr                                                                                    $$
            "    ffun:" <+> ffun <> ","                                                                                     $$
            "    arrangement: (" <> relid <> "," <> pp aid <> "),"                                                          $$
            "    jfun: {fn __f(_: &DDValue," <+> vALUE_VAR1 <> ": &DDValue," <+> vALUE_VAR2 <> ": &DDValue) -> ::std::option::Option<DDValue>"  $$
            nest' jfun                                                                                                      $$
            "    __f},"                                                                                                     $$
            "    next: Box::new(" <> next <> ")"                                                                            $$
            "}"

renderArrangedStreamJoin :: (?cfg::Config, ?crate_graph::CrateGraph, ?specname::String, ?statics::CrateStatics) => DatalogProgram -> Relation -> Expr -> Bool -> Doc -> Doc -> Doc -> Doc -> Doc -> Doc
renderArrangedStreamJoin program relation arrangement is_semi join_func filter_func relid description next =
    let v2 = (if is_semi then "_" else empty) <> vALUE_VAR2
        key_func = braces' $ mkArrangementKey program relation arrangement True
            in
                "::differential_datalog::program::XFormArrangement::StreamJoin {"
                $$ "    description:" <+> description
                $$ "    ffun:" <+> filter_func <> ","
                $$ "    rel:" <+> relid <> ","
                $$ "    kfun: {"
                $$ "        fn __ddlog_generated_stream_join_key_function(" <> vALUE_VAR <> ": &::differential_datalog::ddval::DDValue)"
                $$ "            -> ::std::option::Option<::differential_datalog::ddval::DDValue>"
                $$ nest' (nest' key_func)
                $$ "        __ddlog_generated_stream_join_key_function"
                $$ "    },"
                $$ "    jfun: {"
                $$ "        fn __ddlog_generated_stream_join_function(" <> vALUE_VAR1 <> ": &::differential_datalog::ddval::DDValue," <+> v2 <> ": &::differential_datalog::ddval::DDValue)"
                $$ "            -> ::std::option::Option<::differential_datalog::ddval::DDValue>"
                $$ nest' (nest' join_func)
                $$ "        __ddlog_generated_stream_join_function"
                $$ "    },"
                $$ "    next: ::std::boxed::Box::new(" <> next <> ")"
                $$ "}"

-- Compile XForm::Antijoin
mkAntijoin :: (?cfg::Config, ?specname::String, ?crate_graph::CrateGraph, ?statics::CrateStatics) => DatalogProgram -> [Int] -> Bool -> Atom -> Rule -> Int -> CompilerMonad Doc
mkAntijoin d input_filters input_val atom@Atom{..} rl ajoin_idx = do
    -- create arrangement to anti-join with
    let ctx = CtxRuleRAtom rl ajoin_idx
    let (arr, _) = normalizeArrangement d ctx atomVal
    -- Filter inputs using 'input_filters'
    let ffun = mkFFun d rl input_filters
    aid <- fromJust <$> getAntijoinArrangement atomRelation arr
    next <- compileRule d rl ajoin_idx input_val
    relid <- atomRelId d atom
    return $ "::differential_datalog::program::XFormArrangement::Antijoin {"
        $$ "    description: ::std::borrow::Cow::Borrowed(" <> pp (show $ show $ rulePPPrefix rl $ ajoin_idx + 1) <> "),"
        $$ "    ffun:" <+> ffun <> ","
        $$ "    arrangement: (" <> relid <> "," <+> pp aid <> "),"
        $$ "    next: ::std::boxed::Box::new(" <> next <> ")"
        $$ "}"

-- Normalized representation of an arrangement.  In general, an arrangement is characterized
-- by its key function 'key: Value->Option<Value>' that filters the input collection and computes
-- the value to index it by.  'key' can in principle be an arbitrary function, but we restrict
-- the set of supported functions to those that can be represented as a pattern, e.g.,
-- 'C1{_, C2{_0,_},_1}' represents a function that only matches records that satisfy the pattern
-- and returns a tuple '(_0,_1)'.  (Note that the ordering of auxiliary variables matters: the
-- above pattern is not equivalent to 'C1{_, C2{_1,_},_0}').
--
-- The following two functions are used to extract patterns from rules. Consider the following
-- rule:  '... :- A(x,z,B{_,y}), C(y.f1, D{z})'.  Given this rule we would like to generate
-- arrangements of 'C' and 'A'.
--
-- 'normalizeArrangement' computes the former: 'C{_0, D{_1}}', along with the mapping from aux
--    variables to expressions over original variables: '_0 -> y.f1, _1 -> z'.
--
-- 'arrangeInput' takes the mapping produced by 'normalizeArrangement' and computes the
--    latter: 'A{_,_1,B{_,Q{_0}}}'.  Note how the field expression 'y.f1' forced variable 'y'
--    to expand into a pattern 'Q{_0}'
--

-- Normalize pattern expression for use in arrangement:
-- * Eliminate parts of the pattern that do not constrain its value
-- * Replace subexpressions with variables.
normalizeArrangement :: DatalogProgram -> ECtx -> Expr -> (Expr, [(String, Expr, ECtx)])
normalizeArrangement d patctx pat = (renamed, vmap)
    where
    pat' = exprFoldCtx (normalizePattern d) patctx pat
    (renamed, (_, vmap)) = runState (rename patctx pat') (0, [])
    rename :: ECtx -> Expr -> State (Int, [(String, Expr, ECtx)]) Expr
    rename ctx (E e) =
        case e of
             EStruct{..}             -> do
                fs' <- mapIdxM (\(n,e') i -> (n,) <$> rename (CtxStruct e ctx (i,n)) e') exprStructFields
                return $ E e{exprStructFields = fs'}
             ETuple{..}              -> do
                fs' <- mapIdxM (\e' i -> rename (CtxTuple e ctx i) e') exprTupleFields
                return $ E e{exprTupleFields = fs'}
             EBool{}                 -> return $ E e
             EInt{}                  -> return $ E e
             EDouble{}               -> return $ E e
             EFloat{}                -> return $ E e
             EString{}               -> return $ E e
             EBit{}                  -> return $ E e
             ESigned{}               -> return $ E e
             EPHolder{}              -> return $ E e
             ETyped{..}              -> do
                e' <- rename (CtxTyped e ctx) exprExpr
                return $ E e{exprExpr = e'}
             ERef{..}                -> do
                e' <- rename (CtxRef e ctx) exprPattern
                return $ E e{exprPattern = e'}
             _ | exprIsConst (E e)   -> return $ E e
             _                       -> do
                vid <- gets fst
                let vname = "_" ++ show vid
                modify $ \(_, _vmap) -> (vid + 1, _vmap ++ [(vname, E e, ctx)])
                return $ eVar vname

-- Simplify away parts of the pattern that do not constrain its value.
normalizePattern :: DatalogProgram -> ECtx -> ENode -> Expr
normalizePattern d ctx e =
    case e of
         -- replace new variables with placeholders
         EVar{..}    | isNothing $ lookupVar d ctx exprVar
                                 -> ePHolder
         -- replace tuples and unique constructors populated with placeholders
         -- with a placeholder
         EStruct{..} | consIsUnique d exprConstructor && all ((== ePHolder) . snd) exprStructFields
                                 -> ePHolder
         ETuple{..}  | all (== ePHolder) exprTupleFields
                                 -> ePHolder
         EBinding{..}            -> exprPattern
         ERef{..}    | exprPattern == ePHolder
                                 -> ePHolder
         _                       -> E e

-- 'fstatom' is the first atom in the body of a rule.
-- 'arrange_input_by' - list of expressions to arrange by that correspond to
-- '_0', '_1', ...   Returns normalized arrangement or 'Nothing' if
-- the arrangement cannot be represented in a normalized form
--
-- 'arrangeInput A(y, _, x) [x.f, y] = Just A(_1, _, C{.f=_0})'
-- 'arrangeInput A(y, _, x) [x.f+y] = Nothing' (since we currently can only arrange by
-- one or more fields and not any kind of derived expressions)
--
-- Step 1: replace each expression in 'arrange_input_by' by '_n' variables
-- Step 2: eliminate all other variables and patterns that do not constrain the value
arrangeInput :: DatalogProgram -> Atom -> [(Expr, ECtx)] -> Maybe Expr
arrangeInput d fstatom arrange_input_by = do
    fstatom' <- foldIdxM subst (atomVal fstatom) arrange_input_by
    let vars = map (\i -> "_" ++ show i) [0..length arrange_input_by - 1]
    return $ normalize vars fstatom'
  where
    subst :: Expr -> (Expr, ECtx) -> Int -> Maybe Expr
    subst e arrange_by i =
        if exprIsVarOrField $ fst arrange_by
           then let (e', found) = runState (exprFoldM (\x -> subst' x arrange_by i) e) False
                -- 'found' can only be false here if 'fstatom' contains a binding 'x@expr',
                -- where both x and some field in 'expr' occur in 'arrange_input_by'
                -- OR if more than one fields of the same structure are matched.
                -- TODO: support the above cases.
                in if found then Just e' else Nothing
           else Nothing

    subst' :: ENode -> (Expr, ECtx) -> Int -> State Bool Expr
    subst' e@EBinding{..} ab i | exprVar == fieldExprVar (fst ab) = do {put True; return $ substVar ab i}
                               | otherwise                        = return $ E e
    subst' e@EVar{..}     ab i | exprVar == fieldExprVar (fst ab) = do {put True; return $ substVar ab i}
                               | otherwise                        = return $ E e
    -- We could just return 'e' unmodified in all other cases, but
    -- we enumerate them explicitly to catch unhandled cases.
    subst' e@ERef{}     _  _ = return $ E e
    subst' e@EStruct{}  _  _ = return $ E e
    subst' e@ETuple{}   _  _ = return $ E e
    subst' e@EBool{}    _  _ = return $ E e
    subst' e@EInt{}     _  _ = return $ E e
    subst' e@EDouble{}  _  _ = return $ E e
    subst' e@EFloat{}   _  _ = return $ E e
    subst' e@EString{}  _  _ = return $ E e
    subst' e@EBit{}     _  _ = return $ E e
    subst' e@ESigned{}  _  _ = return $ E e
    subst' e@EPHolder{} _  _ = return $ E e
    subst' e@ETyped{}   _  _ = return $ E e
    subst' e            _  _ | exprIsConst (E e)
                             = return $ E e
    subst' e            _  _ = error $ "Unexpected expression " ++ show e ++ " in Compile.arrangeInput.subst'"

    fieldExprVar (E EVar{..})   = exprVar
    fieldExprVar (E ETyped{..}) = fieldExprVar exprExpr
    fieldExprVar (E EField{..}) = fieldExprVar exprStruct
    fieldExprVar (E ETupField{..}) = fieldExprVar exprTuple
    fieldExprVar e              = error $ "Compile.arrangeInput.fieldExprVar " ++ show e

    substVar :: (Expr, ECtx) -> Int -> Expr
    substVar ab i = substVar' ab (eVar $ "_" ++ show i)

    substVar' :: (Expr, ECtx) -> Expr -> Expr
    substVar' (E EVar{}, _) e' = e'
    substVar' (E par@(ETyped _ e _), ctx) e' = substVar' (e, ctx') e'
        where ctx' = CtxTyped par ctx
    substVar' (E par@(EField _ e f), ctx) e' = substVar' (e, ctx') e''
        where ctx' = CtxField par ctx
              etype = exprType' d ctx' e
              (TStruct _ [cons]) = typDeref' d etype
              estruct = eStruct (name cons)
                                (map (\a -> (IdentifierWithPos nopos $ name a, if name a == f then e' else ePHolder)) $ consArgs cons)
                                (typDeref' d etype)
              e'' = foldl' (\_e _ -> eRef _e) estruct [(1::Int)..nref etype]
    substVar' (E par@(ETupField _ e idx), ctx) e' = substVar' (e, ctx') e''
        where ctx' = CtxTupField par ctx
              etype = exprType' d ctx' e
              TTuple _ args = typDeref' d etype
              etup = eTuple $ mapIdx (\_ i -> if i == idx then e' else ePHolder) args
              e'' = foldl' (\_e _ -> eRef _e) etup [(1::Int)..nref etype]
    substVar' e                  _           = error $ "Unexpected expression " ++ show e ++ " in Compile.arrangeInput.substVar'"

    -- Number of nested `Ref<>` in type `t`
    nref rt@(TOpaque _ _ [t]) | isSharedRef d rt = 1 + nref (typ' d t)
    nref TStruct{} = 0
    nref TTuple{} = 0
    nref t = error $ "Unexpected type " ++ show t ++ " in Compile.arrangeInput.substVar'.nref"

    -- strip all variables but vs
    normalize :: [String] -> Expr -> Expr
    normalize vs e = exprFold (normalize' vs) e

    normalize' :: [String] -> ENode -> Expr
    normalize' vs e@EVar{..}   | elem exprVar vs                   = E e
                               | otherwise                         = ePHolder
    normalize' _  EStruct{..}  | consIsUnique d exprConstructor && all ((== ePHolder) . snd) exprStructFields
                               = ePHolder
    normalize' _  ETuple{..}   | all (== ePHolder) exprTupleFields = ePHolder
    normalize' _  EBinding{..} = exprPattern
    normalize' _  ERef{..}     | exprPattern == ePHolder           = ePHolder
    normalize' _  e                                                = E e

-- Compile XForm::FilterMap that generates the head of the rule
mkHead :: (?cfg::Config, ?specname::String, ?statics::CrateStatics, ?crate_graph::CrateGraph) => DatalogProgram -> Doc -> Rule -> Doc
mkHead d prefix rl =
    "XFormCollection::FilterMap{"                                                                               $$
    "    description: std::borrow::Cow::from(" <> (pp $ show $ show $ "head of" <+> rulePPStripped rl) <> "),"  $$
    nest' ("fmfun: {fn __f(" <> vALUE_VAR <> ": DDValue) -> ::std::option::Option<DDValue>" $$ fmfun $$ "__f},")$$
    "    next: Box::new(" <> next <> ")"                                                                        $$
    "}"
    where
    v = mkDDValue d (CtxRuleLAtom rl 0) (atomVal $ lhsAtom $ head $ ruleLHS rl)
    fmfun = braces' $ prefix $$
                      "Some" <> parens v
    -- 'simplifyDifferentiation' guarantees that differentiation can only occur
    -- in a rule of the form '__diff_R[x] :- R'[x]'.
    next = case last $ ruleRHS rl of
                RHSLiteral{rhsAtom=Atom{atomDiff = True}} ->
                    "Some(XFormCollection::Differentiate{"                                                                              $$
                    "    description: std::borrow::Cow::from(" <> (pp $ show $ show $ "differentiate" <+> rulePPStripped rl) <> "),"    $$
                    "    next: Box::new(None)"                                                                                          $$
                    "})"
                _ -> "None"

-- Variables in the RHS of the rule declared before or in i'th term
-- and used after the term.
rhsVarsAfter :: DatalogProgram -> Rule -> Int -> [Var]
rhsVarsAfter d rl i =
    case ruleRHS rl !! i of
         -- Inspect operators cannot change the collection it inspects. No variables are dropped.
         RHSInspect _ -> rhsVarsAfter d rl (i-1)
         _            -> filter (\f -> -- If an grouping occurs in the remaining part of the rule,
                                       -- keep all variables to preserve multiset semantics
                                       if any rhsIsGroupBy $ drop (i+1) (ruleRHS rl)
                                          then True
                                          else elem f $ (ruleLHSVars d rl) `union`
                                                        (concatMap (ruleRHSTermVars d rl) [i+1..length (ruleRHS rl) - 1]))
                                $ ruleRHSVars d rl (i+1)

mkProg :: (?crate_graph :: CrateGraph, ?specname :: String) => DatalogProgram -> CompilerState -> [ProgNode] -> Doc
mkProg d cstate nodes =
    "pub fn prog(__update_cb: std::sync::Arc<dyn program::RelationCallback>) -> program::Program {"
        $$ (nest' relations)
        $$ "    let nodes: std::vec::Vec<program::ProgNode> = vec!["
        $$ (nest' $ nest' program_nodes)
        $$ "    ];"
        $$ "    let delayed_rels = vec![" <> delayed_rels <> "];"
        $$ "    let init_data: std::vec::Vec<(program::RelId, DDValue)> = vec![" <> facts <> "];"
        $$ "    program::Program {"
        $$ "        nodes,"
        $$ "        delayed_rels,"
        $$ "        init_data,"
        $$ "    }"
        $$ "}"
  where
    relations =
        vcat $
            map (\ProgRel{..} -> prelCode) $
                concatMap nodeRels nodes

    program_nodes = nest' $ vcommaSep $ map mkNode nodes
    facts =
        vcommaSep $
            concatMap ((map sel2) . prelFacts) $
                concatMap nodeRels nodes
    delayed_rels = vcommaSep
                   $ map (\((rel, delay), delayed_relid) ->
                           "program::DelayedRelation {"                                 $$
                           "    id:" <+> pp delayed_relid <> ","                        $$
                           "    rel_id:" <+> pp (relId d rel) <> ","                    $$
                           "    delay:" <+> pp (toInteger $ delayDelay delay) <> ","    $$
                           "}")
                   $ M.toList $ cDelayedRelIds cstate

mkNode :: (?crate_graph::CrateGraph, ?specname::String) => ProgNode -> Doc
mkNode (RelNode ProgRel{..}) =
    "program::ProgNode::Rel{rel:" <+> rnameFlat prelName <> "}"
mkNode (SCCNode rels) =
    "program::ProgNode::Scc{rels: vec![" <>
    (commaSep $ map (\RecProgRel{..} ->
                      "program::RecursiveRelation{rel: " <> (rnameFlat $ prelName rprelRel) <>
                      ", distinct: " <> (if rprelDistinct then "true" else "false") <> "}") rels) <> "]}"
mkNode (ApplyNode mname i _) =
    "program::ProgNode::Apply{tfun:" <+> ridentScoped Nothing mname ("__apply_" ++ show i) <> "}"

mkArrangement :: (?cfg::Config, ?statics::CrateStatics, ?crate_graph::CrateGraph, ?specname::String) => DatalogProgram -> Relation -> Arrangement -> Doc
mkArrangement d rel ArrangementMap{..} =
    let filter_key = mkArrangementKey d rel arngPattern False
        afun = braces' $
               "let __cloned =" <+> vALUE_VAR <> ".clone();"                                                $$
               filter_key <> ".map(|x|(x,__cloned))"
    in "program::Arrangement::Map{"                                                                              $$
       "   name: std::borrow::Cow::from(r###\"" <> pp arngPattern <> " /*join*/\"###),"                          $$
       (nest' $ "afun: {fn __f(" <> vALUE_VAR <> ": DDValue) -> ::std::option::Option<(DDValue,DDValue)>" $$ afun $$ "__f},")  $$
       "    queryable:" <+> (if null arngIndexes then "false" else "true")                                       $$
       "}"

mkArrangement d rel ArrangementSet{..} =
    let filter_key = mkArrangementKey d rel arngPattern False
        fmfun = braces' filter_key
        -- Arrangement contains distinct elements by construction and does
        -- not require expensive `distinct()` or `distinct_total()` applied to
        -- it if it is defined over all fields of a distinct relation (i.e.,
        -- the pattern expression does not contain placeholders).
        distinct_by_construction = relIsDistinct d rel && (not $ exprContainsPHolders arngPattern)
    in "program::Arrangement::Set{"                                                                                                 $$
       "    name: std::borrow::Cow::from(r###\"" <> pp arngPattern <> " /*" <> (if arngDistinct then "antijoin" else "semijoin") <> "*/\"###)," $$
       (nest' $ "fmfun: {fn __f(" <> vALUE_VAR <> ": DDValue) -> ::std::option::Option<DDValue>" $$ fmfun $$ "__f},")                             $$
       "    distinct:" <+> (if arngDistinct && not distinct_by_construction then "true" else "false")                               $$
       "}"

-- Generate part of the arrangement computation that filters inputs and computes the key part of the
-- arrangement.
mkArrangementKey :: (?cfg::Config, ?statics::CrateStatics, ?crate_graph::CrateGraph, ?specname::String) => DatalogProgram -> Relation -> Expr -> Bool -> Doc
mkArrangementKey d rel pat is_ref =
    -- extract variables with types from pattern.
    let getvars :: Type -> Expr -> [Field]
        getvars t (E EStruct{..}) =
            concatMap (\(e,t') -> getvars t' e)
            $ zip (map snd exprStructFields) (map typ $ consArgs $ fromJust $ find ((== exprConstructor) . name) cs)
            where TStruct _ cs = typ' d t
        getvars t (E ETuple{..})  =
            concatMap (\(e,t') -> getvars t' e) $ zip exprTupleFields ts
            where TTuple _ ts = typ' d t
        getvars t (E ETyped{..})  = getvars t exprExpr
        getvars t (E ERef{..})    =
            getvars t' exprPattern
            where TOpaque _ _ [t'] = typ' d t
        getvars t (E EVar{..})    = [Field nopos [] exprVar t]
        getvars _ _               = []

        relt = relType rel
        -- Order variables by their integer value: '_0', '_1', ...
        patvars = mkFieldTupleValue (nameScope rel)
                  $ sortBy (\f1 f2 -> compare ((read $ tail $ name f1)::Int) (read $ tail $ name f2))
                  $ getvars relt pat
        relt' = mkRelType d (nameScope rel) relt
        res = "Some(" <> patvars <> ")"
        -- Manufacture fake context to make sure 'pattern' has a known type in 'mkPatExpr'.
        -- Make sure that the context belongs to 'rel', so that the expression is
        -- generated within the same module.
        pattern_ctx = CtxTyped (ETyped nopos pat $ relType rel)
                               (CtxIndex $ Index nopos "" [] $ Atom nopos (name rel) delayZero False ePHolder)
        from_ddvalue = if is_ref then "from_ddvalue_ref_unchecked" else "from_ddvalue_unchecked"
        mtch = mkMatch (mkPatExpr d pattern_ctx pat EReference False) res "None"
        in
            "match unsafe {" <+> "<" <> relt' <> ">::" <> from_ddvalue <> "(" <> vALUE_VAR <> ") } {"
            $$ nest' mtch
            $$ "}"

-- Encodes Rust match pattern.
--
-- The first element is a Rust match pattern, the second
-- element is a (possibly empty) condition attached to the pattern,
-- e.g., the DDlog pattern
-- 'Constructor{f1= x, f2= "foo"}' compiles into
-- 'TypeName::Constructor{f1: x, f2=_0}' match and '*_0 == "foo".to_string())'
-- condition, where '_0' is an auxiliary variable of type 'String'.
--
-- The third field is only used if the input pattern contains 'ERef' expressions.
-- Rust does not allow pattern matching across smart pointers like 'Arc'; therefore
-- the content of an 'Arc' must be matched in a separare nested match expression.
-- (Note, this only works if there is a single pattern to match, which is why 'ERef'
-- is only allowed in rules.) Each tuple in the list (one per 'ERef' in the original
-- DDlog pattern) is an expression to match and the corresponding Rust pattern.
data Match = Match { mPattern::Doc, mCond::Doc, mSubpatterns::[(Doc, Match)] }

mkMatch :: Match -> Doc -> Doc -> Doc
mkMatch (Match pat cond subpatterns) if_matches if_misses =
    pat <+> "=>" <+> res <> "," $$
    "_ =>" <+> if_misses
    where
    res = if cond == empty
             then subpattern
             else "if" <+> cond <+> "{" $+$ nest' (nest' subpattern $+$ "} else {" <+> if_misses <+> "}")
    subpattern = case subpatterns of
        [] -> if_matches
        (e,p):subpatterns' ->
            "match" <+> e <+> "{"                                                                    $$
            (nest' $ mkMatch p{mSubpatterns = mSubpatterns p ++ subpatterns'} if_matches if_misses)  $$
            "}"

-- Compile Datalog pattern expression to Rust.
--
-- 'kind' - variables bound by this pattern must be of this kind.
-- 'mut' - true iff match expression is mutable, and hence pattern variables
--         should be mutable too.
--
-- Assumes:
-- * the expression being matched is of kind EVal
-- * if 'kind == EVal', the pattern does not contain any 'ERef's
mkPatExpr :: (?statics::CrateStatics, ?specname::String, ?crate_graph::CrateGraph) => DatalogProgram -> ECtx -> Expr -> EKind -> Bool -> Match
mkPatExpr d ctx (E e) kind mut = evalState (mkPatExpr' d EVal ctx e kind mut) 0

allocPatVar :: State Int Doc
allocPatVar = do
    i <- get
    put $ i+1
    return $ "_" <> pp i <> "_"

-- Computes prefix to be attached to variables in pattern, given
-- the kind of expression being matched and the kind we want for the
-- new variable.
varprefix :: EKind -> EKind -> Bool -> Doc
varprefix EReference EReference _     = empty
varprefix _          EReference True  = "ref mut"
varprefix _          EReference False = "ref"
varprefix EVal       EVal       _     = empty
varprefix inkind     varkind    _     = error $ "varprefix " ++ show inkind ++ " " ++ show varkind

mkPatExpr' :: (?statics::CrateStatics, ?crate_graph::CrateGraph, ?specname::String) => DatalogProgram -> EKind -> ECtx -> ENode -> EKind -> Bool -> State Int Match
mkPatExpr' _ inkind _   EVar{..}        varkind mut  = return $ Match (varprefix inkind varkind mut <+> pp exprVar) empty []
mkPatExpr' _ inkind _   EVarDecl{..}    varkind mut  = return $ Match (varprefix inkind varkind mut <+> pp exprVName) empty []
mkPatExpr' _ _      _   (EBool _ True)  _       _    = return $ Match "true" empty []
mkPatExpr' _ _      _   (EBool _ False) _       _    = return $ Match "false" empty []
mkPatExpr' _ inkind _   EString{..}     varkind _    = do
    vname <- allocPatVar
    return $ Match (varprefix inkind varkind False <+> vname) (vname <> ".as_str() ==" <+> "\"" <> pp exprString <> "\"") []
mkPatExpr' d inkind ctx e@EStruct{..}   varkind mut  = do
    fields <- mapIdxM (\(f, E e') i -> (f,) <$> mkPatExpr' d inkind (CtxStruct e ctx (i,f)) e' varkind mut) exprStructFields
    let t = consType d exprConstructor
        struct_name = name t
        pat = mkConstructorName (Just $ ctxModule ctx) struct_name (fromJust $ tdefType t) exprConstructor <>
              (braces $ hsep $ punctuate comma $ map (\(fname, m) -> pp fname <> ":" <+> mPattern m) fields)
        cond = hsep $ intersperse "&&" $ filter (/= empty)
                                       $ map (\(_,m) -> mCond m) fields
        subpatterns = concatMap (\(_, m) -> mSubpatterns m) fields
    return $ Match pat cond subpatterns
mkPatExpr' d inkind ctx e@ETuple{..}    varkind mut  = do
    fields <- mapIdxM (\(E f) i -> mkPatExpr' d inkind (CtxTuple e ctx i) f varkind mut) exprTupleFields
    let pat = tupleStruct (Just $ ctxModule ctx) $ map (pp . mPattern) fields
        cond = hsep $ intersperse "&&" $ filter (/= empty)
                                       $ map (pp . mCond) fields
        subpatterns = concatMap mSubpatterns fields
    return $ Match pat cond subpatterns
mkPatExpr' _ _      _   EPHolder{}      _       _    = return $ Match "_" empty []
mkPatExpr' d inkind ctx e@ETyped{..}    varkind mut  = mkPatExpr' d inkind (CtxTyped e ctx) (enode exprExpr) varkind mut
mkPatExpr' d inkind ctx e@EBinding{..}  varkind _    = do
    -- Rust does not allow variable declarations inside bindings.
    -- To bypass this, we convert the bind pattern into a nested pattern.
    -- Unfortunately, this means that binding can only be used when there
    -- is a single pattern to match against, e.g., in an atom or an assignment
    -- clause in a rule.
    Match pat cond subpatterns <- mkPatExpr' d inkind (CtxBinding e ctx) (enode exprPattern) varkind False
    return $ Match (varprefix inkind varkind False <+> pp exprVar) empty ((pp exprVar, Match pat cond []):subpatterns)
mkPatExpr' d inkind ctx e@ERef{..}      varkind _    = do
    vname <- allocPatVar
    subpattern <- mkPatExpr' d EReference {- deref() returns reference -}
                             (CtxRef e ctx) (enode exprPattern) varkind False
    return $ Match (varprefix inkind varkind False <+> vname)
                   empty [("(" <> deref (pp vname, varkind, undefined) <> ").deref()" , subpattern)]
mkPatExpr' d inkind ctx e               varkind _    = do
    vname <- allocPatVar
    return $ Match (varprefix inkind varkind False <+> vname)
                   (deref (vname, varkind, undefined) <+> "==" <+> mkExpr d ctx (E e) EVal)
                   []

-- Irrefutable patterns are patterns that are guaranteed to match any expression
-- of compatible type.  Such patterns consist of any combination of variable
-- declarations, tuples, constructors (for types with unique constructors), and
-- placeholders.
--
-- Irrefutable patterns currently occur in for-loop patterns and in the LHS of
-- the FlatMap operator.
-- 'inkind' - the expression being deconstructed is of this kind.
-- 'varkind' - variables bound by this pattern must be of this kind.
mkIrrefutablePatExpr :: (?statics::CrateStatics, ?crate_graph::CrateGraph, ?specname::String) => DatalogProgram -> EKind -> ECtx -> Expr -> EKind -> Doc
mkIrrefutablePatExpr d inkind ctx (E e) varkind = pat
    where
    Match pat _ _ = evalState (mkPatExpr' d inkind ctx e varkind False) 0

-- Convert Datalog expression to Rust.
-- We generate the code so that all variables are references and must
-- be dereferenced before use or cloned when passed to a constructor,
-- assigned to another variable or returned.
mkExpr :: (?statics::CrateStatics, ?crate_graph::CrateGraph, ?specname::String) => DatalogProgram -> ECtx -> Expr -> EKind -> Doc
mkExpr d ctx e k =
    case k of
         EVal       -> val e'
         EReference -> ref e'
         ELVal      -> lval e'
         ENoReturn  -> sel1 e'
    where
    e' = exprFoldCtx (mkExpr_ d) ctx e

mkExpr_ :: (?statics::CrateStatics, ?crate_graph::CrateGraph, ?specname::String) => DatalogProgram -> ECtx -> ExprNode (Doc, EKind, ENode) -> (Doc, EKind, ENode)
mkExpr_ d ctx e = (t', k', e')
    where (t', k') = mkExpr' d ctx e
          e' = exprMap (E . sel3) e

-- Compiled expressions are represented as '(Doc, EKind)' tuple, where
-- the second components is the kind of the compiled representation
mkExpr' :: (?statics::CrateStatics, ?crate_graph::CrateGraph, ?specname::String) => DatalogProgram -> ECtx -> ExprNode (Doc, EKind, ENode) -> (Doc, EKind)

-- Expression is compiled to a lazy static.
mkExpr' d ctx e | isJust static_idx = (parens $ "&*crate::__STATIC_" <> pp (fromJust static_idx), EReference)
    where
    e' = exprMap (E . sel3) e
    static_idx = lookupStatic d (E e') ctx ?statics

-- All variables are references
mkExpr' _ _ EVar{..}    = (pp exprVar, EReference)

-- Special case of a function call expression where function name is specified
-- without type annotation.  This should only be the case if the function name
-- uniquely identifies the function.  In this case we can find the function and
-- generate function call expression without determining its exact type.  This
-- is a bit hacky, but is helpful when generating code for function calls
-- generated by 'Debug.hs'.
mkExpr' d ctx EApply{exprFunc = (_, _, EFunc{exprFuncName}), exprArgs} =
    (-- Avoid using code generated by 'mkExpr EFunc', which requires
     -- establishing function type.  Lazy evaluation means that we won't
     -- execute that code.
     mkFuncName d (Just $ ctxModule ctx) func
     <> (parens $ commaSep
                $ map (\(a, mut) -> if mut then mutref a else ref a)
                $ zip exprArgs (map argMut funcArgs)), kind)
    where
    [fname] = exprFuncName
    [func@Function{..}] = getFuncs d fname $ Just $ length exprArgs
    kind = if funcGetReturnByRefAttr d func then EReference else EVal

-- Function arguments are passed as read-only or mutable references
-- Functions return values unless they are labeled return-by-reference.
mkExpr' d ctx e@(EApply{..}) =
    if is_closure
    then (sel1 exprFunc <>  ".call"
          <> (parens $ tuple
                     $ map (\(a, mut) -> if mut then mutref a else ref a)
                     $ zip exprArgs (map atypeMut arg_types)), kind)
    else (sel1 exprFunc
          <> (parens $ commaSep
                     $ map (\(a, mut) -> if mut then mutref a else ref a)
                     $ zip exprArgs (map atypeMut arg_types)), kind)
    where
    e' = exprMap (E . sel3) e
    efunc = E $ sel3 exprFunc
    efunc_ctx = CtxApplyFunc e' ctx
    TFunction _ arg_types _ = exprType' d efunc_ctx efunc
    (kind, is_closure) =
        case exprStripTypeAnnotations efunc efunc_ctx of
             (E efunc'@EFunc{}, ctx'') -> let (f, _) = funcExprGetFunc d ctx'' efunc'
                                          in (if funcGetReturnByRefAttr d f then EReference else EVal, False)
             -- TODO: support return_by_ref attribute on closures?
             _ -> (EVal, True)

-- If the function is referenced inside a function invocation, simply return the
-- name of the function; otherwise wrap the function in a closure.
mkExpr' d ctx e@EFunc{} =
    (res, EVal)
    where
    arg_deref :: FuncArg -> Doc
    arg_deref a = if argMut a then "&mut *" else "&*"
    -- Clone return value if function returns by-reference.
    clone_ref = if funcGetReturnByRefAttr d f then ".clone()" else empty
    res = case parctx ctx of
               CtxApplyFunc{} -> fname <> targs
               _ -> "(Box::new(::ddlog_rt::ClosureImpl{"                                                 $$
                    "    description: \"" <> fname <> "\","                                                             $$
                    "    captured: (),"                                                                                 $$
                    "    f:" <+> (braces' $ "fn __f(__args:" <> (tuple $ map mkarg funcArgs) <> ", __captured: &()) ->" <+> ret_type_code       $$
                                            (if length funcArgs == 1
                                             then "{unsafe{" <> fname <> "(" <> arg_deref (funcArgs !! 0) <> "__args)}" <> clone_ref <> "}"
                                             else "{unsafe{" <> fname <> "(" <> commaSep (mapIdx (\a i -> arg_deref a <> "__args." <> pp i) funcArgs) <> ")}" <> clone_ref <> "}") $$
                                            "__f")                                                                      $$
                    "}) as Box<dyn ::ddlog_rt::Closure<(" <> commaSep (map mkarg funcArgs) <> ")," <+> ret_type_code <> ">>)"
    local_module = Just $ ctxModule ctx

    e' = exprMap (E . sel3) e
    (f@Function{..}, tmap) = funcExprGetFunc d ctx e'
    fname = mkFuncName d local_module f
    ret_type_code = mkType d local_module $ typeSubstTypeArgs tmap funcType
    targs = case funcTypeVars f of
                 []  -> empty
                 tvs -> -- Extern functions can have type arguments that don't match their DDlog declaration.
                        if isJust funcDef
                        then "::<" <> commaSep (map (\tv -> mkType d local_module $ tmap M.! tv) tvs) <> ">"
                        else empty

    mkarg :: FuncArg -> Doc
    mkarg a = (if argMut a then "*mut" else "*const") <+> (mkType d local_module $ typeSubstTypeArgs tmap $ typ a)

    -- Transitively unwrap type annotations.
    parctx :: ECtx -> ECtx
    parctx (CtxTyped _ ctx_) = parctx ctx_
    parctx ctx_ = ctx_

mkExpr' d ctx e@EClosure{..} =
    (braces' $ "(Box::new(::ddlog_rt::ClosureImpl{"                                                         $$
               -- Strip type annotations for readability.
               "    description:" <+> (pp $ show $ show $ pp $ exprStripTypeAnnotationsRec (E e') ctx) <> ","              $$
               "    captured:" <+> (tuple $ map (\v -> pp (name v) <> ".clone()") captured_vars) <> ","                    $$
               "    f:" <+> braces' ("fn __f(__args:" <> tuple arg_type_docs <> "," <+>
                                            "__captured: &" <> (tuple $ map (mkType d local_module . varType d) captured_vars) <> ") ->" <+> ret_type_code    $$
                                     (braces' $ vcat ref_captured_vars $$
                                                vcat ref_args          $$
                                                val exprExpr)          $$
                                     "__f") $$
               "}) as Box<dyn ::ddlog_rt::Closure<(" <> commaSep arg_type_docs <> ")," <+> ret_type_code <> ">>)"
    , EVal)
    where
    e' = exprMap (E . sel3) e
    local_module = Just $ ctxModule ctx
    TFunction _ arg_types ret_type = exprType' d ctx (E e')
    ret_type_code = mkType d local_module ret_type
    arg_type_docs = map (\t -> (if atypeMut t then "*mut" else "*const") <+> mkType d local_module t)
                        arg_types
    -- Captured variables are exactly the free variables that occur in 'e'.
    captured_vars = exprFreeVars d ctx $ E e'
    -- Create references to captured variables inside the closure.
    ref_captured_vars = if length captured_vars == 1
                        then ["let" <+> pp (name $ captured_vars !! 0) <+> "= __captured;"]
                        else mapIdx (\v i -> "let" <+> pp (name v) <+> "= &__captured." <> pp i <> ";") captured_vars
    arg_deref :: ClosureExprArg -> Doc
    arg_deref a = if (atypeMut $ fromJust $ ceargType a) then "&mut *" else "&*"
    ref_args = if length exprClosureArgs == 1
               then ["let" <+> pp (name $ exprClosureArgs !! 0) <+> "= unsafe{" <> arg_deref (exprClosureArgs !! 0) <> "__args};"]
               else mapIdx (\a i -> "let" <+> pp (name a) <+> "= unsafe{" <> arg_deref a <> "__args." <> pp i <> "};") exprClosureArgs

-- Field access automatically dereferences subexpression
mkExpr' _ _ EField{..} = (sel1 exprStruct <> "." <> pp exprField, ELVal)
mkExpr' _ _ ETupField{..} = ("(" <> sel1 exprTuple <> "." <> pp exprTupField <> ")", ELVal)
mkExpr' _ _ (EBool _ True) = ("true", EVal)
mkExpr' _ _ (EBool _ False) = ("false", EVal)
mkExpr' _ _ EInt{..} = (mkInt exprIVal, EVal)
mkExpr' _ _ EDouble{..} = ("::ordered_float::OrderedFloat::<f64>" <> (parens $ pp exprDVal), EVal)
mkExpr' _ _ EFloat{..} = ("::ordered_float::OrderedFloat::<f32>" <> (parens $ pp exprFVal), EVal)
mkExpr' _ _ EString{..} = ("String::from(r###\"" <> pp exprString <> "\"###)", EVal)
mkExpr' d ctx EBit{..} | exprWidth <= 128 = (parens $ pp exprIVal <+> "as" <+> mkType d (Just $ ctxModule ctx) (tBit exprWidth), EVal)
                       | otherwise        = (mkBigUintType <> "::parse_bytes(b\"" <> pp exprIVal <> "\", 10)", EVal)
mkExpr' d ctx ESigned{..} | exprWidth <= 128 = (parens $ pp exprIVal <+> "as" <+> mkType d (Just $ ctxModule ctx) (tSigned exprWidth), EVal)
                          | otherwise        = (mkBigIntType <> "::parse_bytes(b\"" <> pp exprIVal <> "\", 10)", EVal)

-- Struct fields must be values
mkExpr' d ctx EStruct{..} | ctxInSetL ctx
                          = (tname <> fieldlvals, ELVal)
                          | isstruct
                          = (parens $ tname <> fieldvals, EVal)
                          | otherwise
                          = (parens $ tname <> "::" <> nameLocal exprConstructor <> fieldvals, EVal)
    where fieldvals  = braces $ commaSep $ map (\(fname, v) -> pp fname <> ":" <+> val v) exprStructFields
          fieldlvals = braces $ commaSep $ map (\(fname, v) -> pp fname <> ":" <+> lval v) exprStructFields
          tdef = consType d exprConstructor
          isstruct = isStructType $ fromJust $ tdefType tdef
          tname = rnameScoped (Just $ ctxModule ctx) $ name tdef

-- Tuple fields must be values
mkExpr' _ ctx ETuple{..} | ctxInSetL ctx
                         = (tupleStruct (Just $ ctxModule ctx) $ map lval exprTupleFields, ELVal)
                         | otherwise
                         = (tupleStruct (Just $ ctxModule ctx) $ map val exprTupleFields, EVal)

mkExpr' d ctx e@ESlice{..} = (mkSlice d (val exprOp, w) exprH exprL, EVal)
    where
    e' = exprMap (E . sel3) e
    TBit _ w = exprType' d (CtxSlice e' ctx) $ E $ sel3 exprOp

-- Match expression is a reference
mkExpr' d ctx e@EMatch{..} = (doc, EVal)
    where
    e' = exprMap (E . sel3) e
    m = deref exprMatchExpr
    -- Is the match expression mutable? Evaluate its mutability
    -- in the parent context, as everything is immutable in CtxMatchExpr
    -- context.
    mut = exprIsVarOrFieldLVal d ctx (E $ sel3 exprMatchExpr)
    doc = ("match" <+> m <+> "{")
          $$
          (nest' $ vcat $ punctuate comma cases)
          $$
          "}"
    cases = mapIdx (\(c,v) idx -> let Match pat cond [] = mkPatExpr d (CtxMatchPat e' ctx idx) (E $ sel3 c) EReference mut
                                      cond' = if cond == empty then empty else ("if" <+> cond) in
                                  pat <+> cond' <+> "=>" <+> val v) exprCases

mkExpr' _  _ EVarDecl{..} = ("ref mut" <+> pp exprVName, ELVal)

mkExpr' _ ctx ESeq{..} | ctxIsSeq2 ctx || ctxIsFunc ctx
                       = (body, EVal)
                       | otherwise
                       = (braces' body, EVal)
    where
    body = (sel1 exprLeft <> ";") $$ val exprRight

mkExpr' _ _ EITE{..} = (doc, EVal)
    where
    doc = ("if" <+> deref exprCond <+> "{") $$
          (nest' $ val exprThen)            $$
          ("}" <+> "else" <+> "{")          $$
          (nest' $ val exprElse)            $$
          "}"

mkExpr' d ctx e@EFor{..} = (doc, EVal)
    where
    e' = exprMap (E . sel3) e
    ikind = snd $ typeIterType d $ exprType d (CtxForIter e' ctx) $ E $ sel3 exprIter
    pat = mkIrrefutablePatExpr d ikind (CtxForVars e' ctx) (E $ sel3 exprLoopVars) EReference
    doc = ("for" <+> pat <+> "in" <+> sel1 exprIter <> ".iter() {") $$
          (nest' $ val exprBody)                                    $$
          "}"

-- Desonctruction expressions in LHS are compiled into let statements, other assignments
-- are compiled into normal assignments.  Note: assignments in rule
-- atoms are handled by a different code path.
mkExpr' d ctx e@ESet{..} | islet     = ("let" <+> assign <> optsemi, EVal)
                         | otherwise = (assign, EVal)
    where
    e' = exprMap (E . sel3) e
    islet = exprIsDeconstruct d $ E $ sel3 exprLVal
    t = if islet
        then ":" <+> (mkType d (Just $ ctxModule ctx) $ exprType d (CtxSetL e' ctx) $ E $ sel3 exprLVal)
        else empty
    assign = lval exprLVal <> t <+> "=" <+> val exprRVal
    optsemi = if not (ctxIsSeq1 ctx) then ";" else empty

mkExpr' _ _ EBreak{} = ("break", ENoReturn)
mkExpr' _ _ EContinue{} = ("continue", ENoReturn)
mkExpr' _ _ EReturn{..} = ("return" <+> val exprRetVal, ENoReturn)

-- operators take values or lvalues and return values
mkExpr' d ctx e@EBinOp{..} = (v', EVal)
    where
    -- Comparison operators can operate on either values or references.  Avoid
    -- unnecessary cloning by passing values by reference to comparison operators,
    -- casting both to immutable references to avoid Rust compiler errors due to
    -- comparing mutable and immutable references.
    (e1, e2) = if bopIsComparison exprBOp
                  then (parens ("&*" <> ref exprLeft), parens ("&*" <> ref exprRight))
                  else (val exprLeft, val exprRight)
    e' = exprMap (E . sel3) e
    t  = exprType' d ctx (E e')
    t1 = exprType' d (CtxBinOpL e' ctx) (E $ sel3 exprLeft)
    t2 = exprType' d (CtxBinOpR e' ctx) (E $ sel3 exprRight)
    v = case exprBOp of
             Concat | t == tString
                    -> case sel3 exprRight of
                            EString _ s -> "::ddlog_rt::string_append_str(" <> e1 <> ", r###\"" <> pp s <> "\"###)"
                            _           -> "::ddlog_rt::string_append(" <> e1 <> "," <+> ref exprRight <> ")"
             op     -> mkBinOp d op (e1, t1) (e2, t2)

    -- Truncate bitvector result in case the type used to represent it
    -- in Rust is larger than the bitvector width.
    v' = if elem exprBOp bopsRequireTruncation
            then mkTruncate v t
            else v

mkExpr' d ctx e@EUnOp{..} = (v, EVal)
    where
    arg =  val exprOp
    e' = exprMap (E . sel3) e
    t = exprType' d ctx (E e')
    v = case exprUOp of
             Not    -> parens $ "!" <> arg
             BNeg   -> mkTruncate (parens $ "!" <> arg) t
             UMinus | smallInt d t
                    -> mkTruncate (parens $ arg <> ".wrapping_neg()") t
             UMinus | isFP d t
                    -> "::ordered_float::OrderedFloat" <> (parens $ "-" <> arg <> ".into_inner()")
             UMinus -> mkTruncate (parens $ "-" <> arg) t
mkExpr' _ _ EPHolder{} = ("_", ELVal)

-- * LHS of assignment will get type ascription elsewhere (see 'ESet').
-- * Do type coercion for integer constants
mkExpr' d ctx ETyped{..} | ctxInSetL ctx = (e', categ)
                         | isint && toDouble
                                         = (parens $ e' <> ".to_double()", categ)
                         | isint && toFloat
                                         = (parens $ e' <+> ".to_float()", categ)
                         | isint         = (parens $ e' <+> "as" <+> mkType d (Just $ ctxModule ctx) exprTSpec, categ)
                         | otherwise     = (e', categ)
    where
    (e', categ, e) = exprExpr
    isint = case e of
                 EInt{} -> True
                 _      -> False
    toDouble = isDouble d exprTSpec
    toFloat = isFloat d exprTSpec

mkExpr' d ctx EAs{..} | bothIntegers && narrow_from && narrow_to && width_cmp /= GT
                      -- use Rust's type cast syntax to convert between
                      -- primitive types; no need to truncate the result if
                      -- target width is greater than or equal to source
                      = (parens $ val exprExpr <+> "as" <+> mkType d local_module exprTSpec, EVal)
                      | bothIntegers && narrow_from && narrow_to
                      -- apply lossy type conversion between primitive Rust types;
                      -- truncate the result if needed
                      = (mkTruncate (parens $ val exprExpr <+> "as" <+> mkType d local_module exprTSpec) to_type,
                         EVal)
                      | bothIntegers && width_cmp == GT && tfrom == tto
                      -- from_type is wider than to_type, but they both
                      -- correspond to the same Rust type: truncate from_type
                      -- (e & ((1 << w) - 1))
                      = ("(" <> val exprExpr <+>
                         "& ((" <> tfrom <> "::one() <<" <> pp (typeWidth to_type) <> ") -" <+> tfrom <> "::one()))"
                        , EVal)
                      | bothIntegers && width_cmp == GT && isBigInt d from_type
                      -- from_type is bigint, to_type is s8/16/32/64/128: truncate from_type,
                      -- and convert to unsigned bit vector using `truncate_to_uN` method and then
                      -- coerce to signed bit vector:
                      -- e.truncate_to_uN() as <to_type>
                      = ("(" <> val exprExpr <+> ".truncate_to_u" <> pp (typeWidth to_type) <> "() as" <+> tto <> ")", EVal)
                      | bothIntegers && width_cmp == GT
                      -- from_type is wider than to_type: truncate from_type and
                      -- then convert:
                      -- (e & ((1 << w) - 1)).to_<to_type>().unwrap()
                      = ("(" <> val exprExpr <+>
                         "& ((" <> tfrom <> "::one() <<" <> pp (typeWidth to_type) <> ") -" <+> tfrom <> "::one()))" <>
                         ".to_" <> tto <> "().unwrap()", EVal)
                      | bothIntegers && tto == tfrom
                      -- from_type is same width or narrower than to_type (only case left) and
                      -- they both correspond to the same Rust type
                      = (val exprExpr, EVal)
                      | bothIntegers
                      = (parens $ tto <> "::from_" <> nameLocal (render tfrom) <> "(" <> val exprExpr <> ")", EVal)

                      -- convert long integers to FP
                      | isFloat d to_type && (tfrom == mkBigIntType || tfrom == mkBigUintType)
                      = (parens $ "(" <> val exprExpr <> ").to_float()", EVal)
                      | isDouble d to_type && (tfrom == mkBigIntType || tfrom == mkBigUintType)
                      = (parens $ "(" <> val exprExpr <> ").to_double()", EVal)
                      -- convert integer to float
                      | isFloat d to_type && isInteger d from_type
                      = (parens $ "::ordered_float::OrderedFloat(" <> val exprExpr <> " as f32)", EVal)
                      | isDouble d to_type && isInteger d from_type
                      = (parens $ "::ordered_float::OrderedFloat(" <> val exprExpr <> " as f64)", EVal)
                      -- convert some FP to double
                      | isDouble d to_type && isFP d from_type
                      = (parens $ "::ordered_float::OrderedFloat::<f64>::from((*" <> val exprExpr <> ") as f64)", EVal)
                      -- convert some FP to float
                      | isFloat d to_type && isFP d from_type
                      = (parens $ "::ordered_float::OrderedFloat::<f32>::from((*" <> val exprExpr <> ") as f32)", EVal)
                      | otherwise
                      = error $ "Unexpected cast from " ++ (show from_type) ++ " to " ++ (show to_type)
    where
    e' = sel3 exprExpr
    from_type = exprType' d (CtxAs e' ctx) $ E e'
    to_type   = typ' d exprTSpec
    local_module = Just $ ctxModule ctx
    tfrom = mkType d local_module from_type  -- Rust type
    tto   = mkType d local_module to_type    -- Rust type
    bothIntegers = (isInteger d from_type) && (isInteger d to_type)
    narrow_from = (isBit d from_type || isSigned d from_type) && typeWidth from_type <= 128
    narrow_to   = (isBit d to_type || isSigned d to_type)  && typeWidth to_type <= 128
    width_cmp = if ((isBit d from_type || isSigned d from_type) &&
                    (isBit d to_type || isSigned d to_type))
                   then compare (typeWidth from_type) (typeWidth to_type)
                   else if isBigInt d from_type && isBigInt d to_type
                           then EQ
                           else if isBigInt d to_type then LT else GT

mkExpr' _ _ e = error $ "Compile.mkExpr': unexpected expression at " ++ show (pos e)

-- 'local' is true iff the function is being used in the same crate where it was
-- declared, i.e., the 'types' crate.
mkFuncName :: (?crate_graph::CrateGraph, ?specname::String) => DatalogProgram -> Maybe ModuleName -> Function -> Doc
mkFuncName d local_module f =
    ridentScoped local_module (nameScope f) (render $ mkFuncNameShort d f)

-- Rust function name without the scope.
mkFuncNameShort :: DatalogProgram -> Function -> Doc
mkFuncNameShort d f | length namesakes == 1 = nameLocal $ name f
                    | otherwise =
    (nameLocal $ name f) <> "_" <> args
    where
    args = hcat $ punctuate "_" $ map (mkTypeIdentifier d) $ (map typ $ funcArgs f) ++ [funcType f]
    namesakes = progFunctions d M.! (name f)


-- 'local' is true iff the type is being used inside the 'types' crate.
mkType :: (WithType a, ?crate_graph::CrateGraph, ?specname::String) => DatalogProgram -> Maybe ModuleName -> a -> Doc
mkType d scope x = mkType' d scope $ typ x

mkType' :: (?crate_graph::CrateGraph, ?specname::String) => DatalogProgram -> Maybe ModuleName -> Type -> Doc
mkType' _ _       TBool{}                    = "bool"
mkType' _ _       TInt{}                     = mkBigIntType
mkType' _ _       TString{}                  = "String"
mkType' _ _       TBit{..} | typeWidth <= 8  = "u8"
                           | typeWidth <= 16 = "u16"
                           | typeWidth <= 32 = "u32"
                           | typeWidth <= 64 = "u64"
                           | typeWidth <= 128= "u128"
                           | otherwise       = mkBigUintType
mkType' _ _       t@TSigned{..} | typeWidth == 8  = "i8"
                                | typeWidth == 16 = "i16"
                                | typeWidth == 32 = "i32"
                                | typeWidth == 64 = "i64"
                                | typeWidth == 128= "i128"
                                | otherwise  = errorWithoutStackTrace $ "Only machine widths (8/16/32/64/128) supported: " ++ show t
mkType' _ _       TDouble{}                  = "::ordered_float::OrderedFloat<f64>"
mkType' _ _       TFloat{}                   = "::ordered_float::OrderedFloat<f32>"
mkType' d scope   TTuple{..} | length typeTupArgs == 0
                                    = parens $ commaSep $ map (mkType' d scope) typeTupArgs
mkType' d scope   TTuple{..}        = tupleTypeName scope typeTupArgs <>
                                      if null typeTupArgs
                                         then empty
                                         else "<" <> (commaSep $ map (mkType' d scope) typeTupArgs) <> ">"
mkType' d scope   TUser{..} | tdefGetAliasAttr d (getType d typeName)
                                    = mkType' d scope $ fromJust $ tdefType $ getType d typeName
mkType' d scope   TUser{..}         = rnameScoped scope typeName <>
                                      if null typeArgs
                                         then empty
                                         else "<" <> (commaSep $ map (mkType' d scope) typeArgs) <> ">"
mkType' d scope   TOpaque{..}       = rnameScoped scope typeName <>
                                      if null typeArgs
                                         then empty
                                         else "<" <> (commaSep $ map (mkType' d scope) typeArgs) <> ">"
mkType' _ _       TVar{..}          = pp tvarName
mkType' d scope   TFunction{..}     = "Box<dyn" <+> rnameScoped scope "ddlog_rt::Closure" <> "<" <> (tuple $ map mkarg typeFuncArgs) <> "," <+> ret_type_code <> ">>"
                                      where
                                      mkarg a = (if atypeMut a then "*mut" else "*const") <+> mkType' d scope (typ a)
                                      ret_type_code = mkType' d scope typeRetType
mkType' _ _       t                 = error $ "Compile.mkType' " ++ show t

smallInt :: DatalogProgram -> Type -> Bool
smallInt d t = ((isSigned d t || isBit d t) && (typeWidth (typ' d t) <= 128))

mkBigUintType :: Doc
mkBigUintType = "::ddlog_bigint::Uint"

mkBigIntType :: Doc
mkBigIntType = "::ddlog_bigint::Int"

mkBinOp :: (?crate_graph::CrateGraph, ?specname::String) => DatalogProgram -> BOp -> (Doc, Type) -> (Doc, Type) -> Doc
mkBinOp d op (e1, t1) (e2, t2) =
    case op of
        Eq     -> parens $ e1 <+> "==" <+> e2
        Neq    -> parens $ e1 <+> "!=" <+> e2
        Lt     -> parens $ e1 <+> "<"  <+> e2
        Gt     -> parens $ e1 <+> ">"  <+> e2
        Lte    -> parens $ e1 <+> "<=" <+> e2
        Gte    -> parens $ e1 <+> ">=" <+> e2
        And    -> parens $ e1 <+> "&&" <+> e2
        Or     -> parens $ e1 <+> "||" <+> e2
        Impl   -> parens $ "!" <> e1 <+> "||" <+> e2
        Mod    | smallInt d t1
               -> parens $ e1 <> ".wrapping_rem(" <> e2 <> ")"
               | otherwise
               -> parens $ e1 <+> "%" <+> e2
        Div    | smallInt d t1
               -> parens $ e1 <> ".wrapping_div(" <> e2 <> ")"
               | isFP d t1
               -> "::ordered_float::OrderedFloat" <> (parens $ e1 <> ".into_inner()" <+> "/" <+> e2 <> ".into_inner()")
               | otherwise
               -> parens $ e1 <+> "/" <+> e2
        ShiftR | smallInt d t1
               -> parens $ e1 <> ".wrapping_shr(" <> e2 <> ")"
               | otherwise
               -> parens $ e1 <+> ">>" <+> e2
        ShiftL | smallInt d t1
               -> parens $ e1 <> ".wrapping_shl(" <> e2 <> ")"
               | otherwise
               -> parens $ e1 <+> "<<" <+> e2
        BAnd   -> parens $ e1 <+> "&"  <+> e2
        BOr    -> parens $ e1 <+> "|"  <+> e2
        BXor   -> parens $ e1 <+> "^"  <+> e2
        Plus   | smallInt d t1
               -> parens $ e1 <> ".wrapping_add(" <> e2 <> ")"
               | isFP d t1
               -> "::ordered_float::OrderedFloat" <> (parens $ e1 <> ".into_inner()" <+> "+" <+> e2 <> ".into_inner()")
               | otherwise
               -> parens $ e1 <+> "+" <+> e2
        Minus  | smallInt d t1
               -> parens $ e1 <> ".wrapping_sub(" <> e2 <> ")"
               | isFP d t1
               -> "::ordered_float::OrderedFloat" <> (parens $ e1 <> ".into_inner()"<+> "-" <+> e2 <> ".into_inner()")
               | otherwise
               -> parens $ e1 <+> "-" <+> e2
        Times  | smallInt d t1
               -> parens $ e1 <> ".wrapping_mul(" <> e2 <> ")"
               | isFP d t1
               -> "::ordered_float::OrderedFloat" <> (parens $ e1 <> ".into_inner()" <+> "*" <+> e2 <> ".into_inner()")
               | otherwise
               -> parens $ e1 <+> "*" <+> e2
        Concat -> mkConcat d (e1, typeWidth t1) (e2, typeWidth t2)

-- These operators require truncating the output value to correct
-- width.
bopsRequireTruncation :: [BOp]
bopsRequireTruncation = [ShiftL, Plus, Minus, Times]

-- Produce code to cast bitvector to a different-width BV.
-- The value of 'e' must fit in the new width.
castBV :: (?crate_graph::CrateGraph, ?specname::String) => DatalogProgram -> Doc -> Int -> Int -> Doc
castBV d e w1 w2 | t1 == t2
                 = e
                 | w1 <= 128 && w2 <= 128
                 = parens $ e <+> "as" <+> t2
                 | w2 > 128
                 = mkBigUintType <> "::from_" <> nameLocal (render t1) <> "(" <> e <> ")"
                 | otherwise
                 = e <> "to_" <> t2 <> "().unwrap()"
    where
    t1 = mkType d (error "mkType bit<> should not depend on scope") $ tBit w1
    t2 = mkType d (error "mkType bit<> should not depend on scope") $ tBit w2

-- Concatenate two bitvectors
mkConcat :: (?crate_graph::CrateGraph, ?specname::String) => DatalogProgram -> (Doc, Int) -> (Doc, Int) -> Doc
mkConcat d (e1, w1) (e2, w2) =
    parens $ e1'' <+> "|" <+> e2'
    where
    e1' = castBV d e1 w1 (w1+w2)
    e2' = castBV d e2 w2 (w1+w2)
    e1'' = parens $ e1' <+> "<<" <+> pp w2

mkSlice :: (?crate_graph::CrateGraph, ?specname::String) => DatalogProgram -> (Doc, Int) -> Int -> Int -> Doc
mkSlice d (e, w) h l = castBV d res w (h - l + 1)
    where
    res = parens $ (parens $ e <+> ">>" <+> pp l) <+> "&" <+> mask
    mask = mkBVMask (h - l + 1)

mkBVMask :: Int -> Doc
mkBVMask w | w > 128   = mkBigUintType <> "::parse_bytes(b\"" <> m <> "\", 16)"
           | otherwise = "0x" <> m
    where
    m = pp $ showHex (((1::Integer) `shiftL` w) - 1) ""

mkTruncate :: Doc -> Type -> Doc
mkTruncate v t =
    case t of
         TBit{..}    | needsTruncation typeWidth
                     -> parens $ v <+> "&" <+> mask typeWidth
         _           -> v
    where
    needsTruncation :: Int -> Bool
    needsTruncation w = mask w /= empty
    mask :: Int -> Doc
    mask w | w < 8 || w > 8  && w < 16 || w > 16 && w < 32 || w > 32 && w < 64 || w > 64 && w < 128
           = mkBVMask w
    mask _ = empty

mkInt :: Integer -> Doc
mkInt v | v <= (toInteger (maxBound::Word128)) && v >= (toInteger (minBound::Word128))
        = mkBigIntType <> "::from_u128(" <> pp v <> ")"
        | v <= (toInteger (maxBound::Int128))  && v >= (toInteger (minBound::Int128))
        = mkBigIntType <> "::from_i128(" <> pp v <> ")"
        | otherwise
        = mkBigIntType <> "::parse_bytes(b\"" <> pp v <> "\", 10)"

-- Compute the atom or tuple of variables after the prefix of length n.
-- If this is the last term, then it is an expression of the LHS variables for each head
-- of the rule.
-- If this is the first term, then it's the atom of the RHSLiteral.
-- Otherwise, this is the variables from rhsVarsAfter converted into an ETuple expression.
recordAfterPrefix :: DatalogProgram -> Rule -> Int -> [Expr]
recordAfterPrefix d rl i =
  if i == length (ruleRHS rl) - 1
     then  map (atomVal . lhsAtom) $ ruleLHS rl
     else if i == 0
             then [eVar $ exprVar $ enode $ atomVal $ rhsAtom $ head $ ruleRHS rl ]
             else [eTuple $ map (eVar . name) (rhsVarsAfter d rl i) ]
