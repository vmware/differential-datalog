{-
Copyright (c) 2018-2020 VMware, Inc.
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
    rustProjectDir,
    mkValConstructorName,
    mkConstructorName,
    mkType,
    mkValue,
    tupleStruct,
    rname,
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
import Data.String.Utils
import System.FilePath
import System.Directory
import qualified Data.ByteString.Char8 as BS
import Numeric
import qualified Data.Set as S
import qualified Data.Map as M
import qualified Data.Graph.Inductive as G
import Data.WideWord
--import Debug.Trace

import Language.DifferentialDatalog.Config
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
import Language.DifferentialDatalog.Index
import Language.DifferentialDatalog.Optimize
import Language.DifferentialDatalog.ECtx
import Language.DifferentialDatalog.Type
import Language.DifferentialDatalog.Rule
import Language.DifferentialDatalog.FlatBuffer
import Language.DifferentialDatalog.Attribute
import Language.DifferentialDatalog.Var

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

-- Extract static template header before the "/*- !!!!!!!!!!!!!!!!!!!! -*/"
-- separator from file; substitute "datalog_example" with 'specname' in
-- the header.
header :: String -> String -> Doc
header template specname =
    let h = case splitOn "/*- !!!!!!!!!!!!!!!!!!!! -*/" template of
                []  -> error "Missing separator in lib.rs"
                x:_ -> x
    in pp $ replace "datalog_example" specname h

-- 'types' crate containing DDlog type declarations.
typesHeader :: String -> Doc
typesHeader specname = header (BS.unpack $ $(embedFile "rust/template/types/lib.rs")) specname

-- 'value' crate that declares relations and value types.
valueHeader :: String -> Doc
valueHeader specname = header (BS.unpack $ $(embedFile "rust/template/value/lib.rs")) specname

-- Main crate containing compiled program rules.
mainHeader :: String -> Doc
mainHeader specname = header (BS.unpack $ $(embedFile "rust/template/src/lib.rs")) specname

-- Top-level 'Cargo.toml'.
mainCargo :: String -> [String] -> Doc
mainCargo specname crate_types =
    (pp $ replace "datalog_example" specname $ BS.unpack $ $(embedFile "rust/template/Cargo.toml")) $$
    "crate-type = [" <> (hsep $ punctuate "," $ map (\t -> "\"" <> pp t <> "\"") $ "rlib" : crate_types) <> "]"

-- 'types/Cargo.toml' - imports Rust dependencies.
typesCargo :: String -> Doc -> Doc
typesCargo specname toml_code =
    (pp $ replace "datalog_example" specname $ BS.unpack $ $(embedFile "rust/template/types/Cargo.toml")) $$
    "" $$ toml_code

rustProjectDir :: String -> String
rustProjectDir specname = specname ++ "_ddlog"

-- IMPORTANT: KEEP THIS IN SYNC WITH FILE LIST IN 'build.rs'.
templateFiles :: String -> [(String, String)]
templateFiles specname =
    map (mapSnd (BS.unpack)) $
        [ (dir </> "src/build.rs"               , $(embedFile "rust/template/src/build.rs"))
        , (dir </> "src/main.rs"                , $(embedFile "rust/template/src/main.rs"))
        , (dir </> "src/api.rs"                 , $(embedFile "rust/template/src/api.rs"))
        , (dir </> "src/ovsdb.rs"               , $(embedFile "rust/template/src/ovsdb.rs"))
        , (dir </> "src/update_handler.rs"      , $(embedFile "rust/template/src/update_handler.rs"))
        , (dir </> "ddlog.h"                    , $(embedFile "rust/template/ddlog.h"))
        , (dir </> "ddlog_ovsdb_test.c"         , $(embedFile "rust/template/ddlog_ovsdb_test.c"))
        ]
    where dir = rustProjectDir specname

-- Rust differential_datalog library
rustLibFiles :: String -> [(String, String)]
rustLibFiles specname =
    map (mapSnd (BS.unpack)) $
        [ (dir </> "differential_datalog/Cargo.toml"                      , $(embedFile "rust/template/differential_datalog/Cargo.toml"))
        , (dir </> "differential_datalog/arcval.rs"                       , $(embedFile "rust/template/differential_datalog/arcval.rs"))
        , (dir </> "differential_datalog/callback.rs"                     , $(embedFile "rust/template/differential_datalog/callback.rs"))
        , (dir </> "differential_datalog/ddlog.rs"                        , $(embedFile "rust/template/differential_datalog/ddlog.rs"))
        , (dir </> "differential_datalog/ddval.rs"                        , $(embedFile "rust/template/differential_datalog/ddval.rs"))
        , (dir </> "differential_datalog/int.rs"                          , $(embedFile "rust/template/differential_datalog/int.rs"))
        , (dir </> "differential_datalog/lib.rs"                          , $(embedFile "rust/template/differential_datalog/lib.rs"))
        , (dir </> "differential_datalog/profile.rs"                      , $(embedFile "rust/template/differential_datalog/profile.rs"))
        , (dir </> "differential_datalog/profile_statistics.rs"           , $(embedFile "rust/template/differential_datalog/profile_statistics.rs"))
        , (dir </> "differential_datalog/program.rs"                      , $(embedFile "rust/template/differential_datalog/program.rs"))
        , (dir </> "differential_datalog/record.rs"                       , $(embedFile "rust/template/differential_datalog/record.rs"))
        , (dir </> "differential_datalog/replay.rs"                       , $(embedFile "rust/template/differential_datalog/replay.rs"))
        , (dir </> "differential_datalog/test.rs"                         , $(embedFile "rust/template/differential_datalog/test.rs"))
        , (dir </> "differential_datalog/test_value.rs"                   , $(embedFile "rust/template/differential_datalog/test_value.rs"))
        , (dir </> "differential_datalog/test_record.rs"                  , $(embedFile "rust/template/differential_datalog/test_record.rs"))
        , (dir </> "differential_datalog/uint.rs"                         , $(embedFile "rust/template/differential_datalog/uint.rs"))
        , (dir </> "differential_datalog/valmap.rs"                       , $(embedFile "rust/template/differential_datalog/valmap.rs"))
        , (dir </> "differential_datalog/variable.rs"                     , $(embedFile "rust/template/differential_datalog/variable.rs"))
        , (dir </> "cmd_parser/Cargo.toml"                                , $(embedFile "rust/template/cmd_parser/Cargo.toml"))
        , (dir </> "cmd_parser/lib.rs"                                    , $(embedFile "rust/template/cmd_parser/lib.rs"))
        , (dir </> "cmd_parser/parse.rs"                                  , $(embedFile "rust/template/cmd_parser/parse.rs"))
        , (dir </> "distributed_datalog/Cargo.toml"                       , $(embedFile "rust/template/distributed_datalog/Cargo.toml"))
        , (dir </> "distributed_datalog/src/assign.rs"                    , $(embedFile "rust/template/distributed_datalog/src/assign.rs"))
        , (dir </> "distributed_datalog/src/accumulate/mod.rs"            , $(embedFile "rust/template/distributed_datalog/src/accumulate/mod.rs"))
        , (dir </> "distributed_datalog/src/accumulate/accumulator.rs"    , $(embedFile "rust/template/distributed_datalog/src/accumulate/accumulator.rs"))
        , (dir </> "distributed_datalog/src/accumulate/observer.rs"       , $(embedFile "rust/template/distributed_datalog/src/accumulate/observer.rs"))
        , (dir </> "distributed_datalog/src/accumulate/test.rs"           , $(embedFile "rust/template/distributed_datalog/src/accumulate/test.rs"))
        , (dir </> "distributed_datalog/src/accumulate/txndistributor.rs" , $(embedFile "rust/template/distributed_datalog/src/accumulate/txndistributor.rs"))
        , (dir </> "distributed_datalog/src/instantiate.rs"               , $(embedFile "rust/template/distributed_datalog/src/instantiate.rs"))
        , (dir </> "distributed_datalog/src/lib.rs"                       , $(embedFile "rust/template/distributed_datalog/src/lib.rs"))
        , (dir </> "distributed_datalog/src/observe/mod.rs"               , $(embedFile "rust/template/distributed_datalog/src/observe/mod.rs"))
        , (dir </> "distributed_datalog/src/observe/observable.rs"        , $(embedFile "rust/template/distributed_datalog/src/observe/observable.rs"))
        , (dir </> "distributed_datalog/src/observe/observer.rs"          , $(embedFile "rust/template/distributed_datalog/src/observe/observer.rs"))
        , (dir </> "distributed_datalog/src/observe/test.rs"              , $(embedFile "rust/template/distributed_datalog/src/observe/test.rs"))
        , (dir </> "distributed_datalog/src/read_config.rs"               , $(embedFile "rust/template/distributed_datalog/src/read_config.rs"))
        , (dir </> "distributed_datalog/src/schema.rs"                    , $(embedFile "rust/template/distributed_datalog/src/schema.rs"))
        , (dir </> "distributed_datalog/src/server.rs"                    , $(embedFile "rust/template/distributed_datalog/src/server.rs"))
        , (dir </> "distributed_datalog/src/sinks/file.rs"                , $(embedFile "rust/template/distributed_datalog/src/sinks/file.rs"))
        , (dir </> "distributed_datalog/src/sinks/mod.rs"                 , $(embedFile "rust/template/distributed_datalog/src/sinks/mod.rs"))
        , (dir </> "distributed_datalog/src/sources/file.rs"              , $(embedFile "rust/template/distributed_datalog/src/sources/file.rs"))
        , (dir </> "distributed_datalog/src/sources/mod.rs"               , $(embedFile "rust/template/distributed_datalog/src/sources/mod.rs"))
        , (dir </> "distributed_datalog/src/tcp_channel/message.rs"       , $(embedFile "rust/template/distributed_datalog/src/tcp_channel/message.rs"))
        , (dir </> "distributed_datalog/src/tcp_channel/mod.rs"           , $(embedFile "rust/template/distributed_datalog/src/tcp_channel/mod.rs"))
        , (dir </> "distributed_datalog/src/tcp_channel/receiver.rs"      , $(embedFile "rust/template/distributed_datalog/src/tcp_channel/receiver.rs"))
        , (dir </> "distributed_datalog/src/tcp_channel/sender.rs"        , $(embedFile "rust/template/distributed_datalog/src/tcp_channel/sender.rs"))
        , (dir </> "distributed_datalog/src/tcp_channel/socket.rs"        , $(embedFile "rust/template/distributed_datalog/src/tcp_channel/socket.rs"))
        , (dir </> "distributed_datalog/src/tcp_channel/txnbuf.rs"        , $(embedFile "rust/template/distributed_datalog/src/tcp_channel/txnbuf.rs"))
        , (dir </> "distributed_datalog/src/test.rs"                      , $(embedFile "rust/template/distributed_datalog/src/test.rs"))
        , (dir </> "distributed_datalog/src/txnmux.rs"                    , $(embedFile "rust/template/distributed_datalog/src/txnmux.rs"))
        , (dir </> "distributed_datalog/src/zookeeper.rs"                 , $(embedFile "rust/template/distributed_datalog/src/zookeeper.rs"))
        , (dir </> "ovsdb/Cargo.toml"                                     , $(embedFile "rust/template/ovsdb/Cargo.toml"))
        , (dir </> "ovsdb/lib.rs"                                         , $(embedFile "rust/template/ovsdb/lib.rs"))
        , (dir </> "ovsdb/test.rs"                                        , $(embedFile "rust/template/ovsdb/test.rs"))
        , (dir </> "types/log.rs"                                         , $(embedFile "rust/template/types/log.rs"))
        , (dir </> "value/Cargo.toml"                                     , $(embedFile "rust/template/value/Cargo.toml"))
        , (dir </> ".cargo/config"                                        , $(embedFile "rust/template/.cargo/config"))
        ]
    where dir = rustProjectDir specname


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

-- Rust expression kind
data EKind = EVal         -- normal value
           | ELVal        -- l-value that can be written to or moved
           | EReference   -- reference (mutable or immutable)
           | ENoReturn    -- expression does not return (e.g., break, continue, or return).
           deriving (Eq, Show)

-- convert any expression into reference
ref :: (Doc, EKind, ENode) -> Doc
ref (x, EReference, _)  = x
ref (x, ENoReturn, _)   = x
ref (x, _, _)           = parens $ "&" <> x

-- dereference expression if it is a reference; leave it alone
-- otherwise
deref :: (Doc, EKind, ENode) -> Doc
deref (x, EReference, _) = parens $ "*" <> x
deref (x, _, _)          = x

-- convert any expression into mutable reference
mutref :: (Doc, EKind, ENode) -> Doc
mutref (x, EReference, _)  = x
mutref (x, ENoReturn, _)   = x
mutref (x, _, _)           = parens $ "&mut" <> x

-- convert any expression to EVal by cloning it if necessary
val :: (Doc, EKind, ENode) -> Doc
val (x, EVal, _)        = x
val (x, ENoReturn, _)   = x
val (x, EReference, _)  = cloneRef x
val (x, _, _)           = x <> ".clone()"

-- convert expression to l-value
lval :: (Doc, EKind, ENode) -> Doc
lval (x, ELVal, _)      = x
-- this can only be mutable reference in a valid program
lval (x, EReference, _) = parens $ "*" <> x
lval (x, EVal, _)       = error $ "Compile.lval: cannot convert value to l-value: " ++ show x
lval (x, ENoReturn, _)  = error $ "Compile.lval: cannot convert expression to l-value: " ++ show x

-- When calling `clone()` on a reference, Rust occasionally decides to clone
-- the reference instead of the referenced object.  This function makes sure to
-- dereference it first.
cloneRef :: Doc -> Doc
cloneRef r = "(*" <> r <> ").clone()"

-- Compiled relation: Rust code for the 'struct Relation' plus ground facts for this relation.
data ProgRel = ProgRel {
    prelName    :: String,
    prelCode    :: Doc,
    prelFacts   :: [Doc]
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
              | ApplyNode Doc
              | RelNode   ProgRel

nodeRels :: ProgNode -> [ProgRel]
nodeRels (SCCNode rels) = map rprelRel rels
nodeRels (RelNode rel)  = [rel]
nodeRels (ApplyNode _)  = []

{- State accumulated by the compiler as it traverses the program -}
type CompilerMonad = State CompilerState

data CompilerState = CompilerState {
    cTypes        :: S.Set Type,
    cArrangements :: M.Map String [Arrangement]
}

emptyCompilerState :: CompilerState
emptyCompilerState = CompilerState {
    cTypes        = S.empty,
    cArrangements = M.empty
}

-- Convert name to a valid Rust identifier by replacing "::" with "_"
rname :: String -> Doc
rname = pp . replace "::" "_"

mkRelEnum :: DatalogProgram -> Doc
mkRelEnum d =
    "#[derive(Copy,Clone,Debug,PartialEq,Eq,Hash)]"                                                                                       $$
    "pub enum Relations {"                                                                                                                $$
    (nest' $ vcat $ punctuate comma $ map (\rel -> rname (name rel) <+> "=" <+> pp (relIdentifier d rel)) $ M.elems $ progRelations d)    $$
    "}"

mkIdxEnum :: DatalogProgram -> Doc
mkIdxEnum d =
    "#[derive(Copy,Clone,Debug,PartialEq,Eq,Hash)]"                                                                                       $$
    "pub enum Indexes {"                                                                                                                  $$
    (nest' $ vcat $ punctuate comma $ map (\idx -> rname (name idx) <+> "=" <+> pp (idxIdentifier d idx)) $ M.elems $ progIndexes d)      $$
    "}"

relId :: String -> Doc
relId rel = "Relations::" <> rname rel <+> "as RelId"

-- t must be normalized
addType :: Type -> CompilerMonad ()
addType t = modify $ \s -> s{cTypes = S.insert t $ cTypes s}

-- Create a new arrangement for use in a join operator:
-- * If the arrangement exists, do nothing
-- * If a semijoin arrangement with the same pattern exists,
--   promote it to a join arrangement
-- * Otherwise, add the new arrangement
addJoinArrangement :: String -> Expr -> Maybe Index -> CompilerMonad ()
addJoinArrangement relname pattern midx = do
    arrs <- gets $ (M.! relname) . cArrangements
    let existing_idx = findIndex (\a -> (arngPattern a == pattern) && (arngIsDistinct a == False)) arrs
    let idxs = nub $ maybeToList midx ++ maybe [] (arngUsedInIndexes . (arrs !!)) existing_idx
    let join_arr = ArrangementMap pattern idxs
    let arrs' = maybe (arrs ++ [join_arr])
                      (\idx -> take idx arrs ++ [join_arr] ++ drop (idx+1) arrs)
                      existing_idx
    modify $ \s -> s{cArrangements = M.insert relname arrs' $ cArrangements s}

-- Create a new arrangement for use in a semijoin operator:
-- * If a semijoin, antijoin or join arrangement with the same pattern exists, do nothing
-- * Otherwise, add the new arrangement
addSemijoinArrangement :: String -> Expr -> CompilerMonad ()
addSemijoinArrangement relname pattern = do
    arrs <- gets $ (M.! relname) . cArrangements
    let arrs' = if isJust $ find ((==pattern) . arngPattern) arrs
                   then arrs
                   else arrs ++ [ArrangementSet pattern False]
    modify $ \s -> s{cArrangements = M.insert relname arrs' $ cArrangements s}

-- Create a new arrangement for use in a antijoin operator:
-- * If the arrangement exists, do nothing
-- * If a semijoin arrangement with the same pattern exists, promote it to
--   an antijoin by setting 'distinct' to true
-- * Otherwise, add the new arrangement
addAntijoinArrangement :: String -> Expr -> CompilerMonad ()
addAntijoinArrangement relname pattern = do
    arrs <- gets $ (M.! relname) . cArrangements
    let antijoin_arr = ArrangementSet pattern True
    let semijoin_idx = elemIndex (ArrangementSet pattern False) arrs
    let arrs' = if elem antijoin_arr arrs
                   then arrs
                   else maybe (arrs ++ [antijoin_arr])
                              (\idx -> take idx arrs ++ [antijoin_arr] ++ drop (idx+1) arrs)
                              semijoin_idx
    modify $ \s -> s{cArrangements = M.insert relname arrs' $ cArrangements s}

-- Find an arrangement of the form 'ArrangementSet pattern _'
getSemijoinArrangement :: String -> Expr -> CompilerMonad (Maybe Int)
getSemijoinArrangement relname pattern = do
    arrs <- gets $ (M.! relname) . cArrangements
    return $ findIndex (\case
                         ArrangementSet pattern' _ -> pattern' == pattern
                         _                         -> False)
                       arrs

-- Find an arrangement of the form 'ArrangementMap pattern _'
getJoinArrangement :: String -> Expr -> CompilerMonad (Maybe Int)
getJoinArrangement relname pattern = do
    arrs <- gets $ (M.! relname) . cArrangements
    return $ findIndex (\case
                         ArrangementMap{arngPattern} -> arngPattern == pattern
                         _ -> False) arrs

-- Find an arrangement of the form 'ArrangementSet pattern True'
getAntijoinArrangement :: String -> Expr -> CompilerMonad (Maybe Int)
getAntijoinArrangement relname pattern = do
    arrs <- gets $ (M.! relname) . cArrangements
    return $ elemIndex (ArrangementSet pattern True) arrs

-- Rust does not like parenthesis around singleton tuples
tuple :: [Doc] -> Doc
tuple [x] = x
tuple xs = parens $ hsep $ punctuate comma xs

tupleTypeName :: [a] -> Doc
tupleTypeName xs = "tuple" <> pp (length xs)

-- Rust does not implement Eq and other traits for tuples with >12 elements.
-- Use structs with n fields for longer tuples.
tupleStruct :: [Doc] -> Doc
tupleStruct [x]                  = x
tupleStruct xs | length xs <= 12 = tuple xs
               | otherwise       = tupleTypeName xs <> tuple xs

-- Structs with a single constructor are compiled into Rust structs;
-- structs with multiple constructor are compiled into Rust enums.
isStructType :: Type -> Bool
isStructType TStruct{..} | length typeCons == 1 = True
isStructType TStruct{..}                        = False
isStructType t                                  = error $ "Compile.isStructType " ++ show t

mkConstructorName :: String -> Type -> String -> Doc
mkConstructorName tname t c =
    if isStructType t
       then rname tname
       else rname tname <> "::" <> rname c

-- | Create a compilable Cargo crate.  If the crate already exists, only writes files
-- modified by the recompilation.
--
-- 'specname' - will be used as Cargo package and library names
--
-- 'rs_code' - additional Rust code to be added to the generated program 'lib.rs'.
--
-- 'toml_code' - code to be added to the generated 'Cargo.toml' file
--
-- 'dir' - directory for the crate; will be created if does not exist
--
-- 'crate_types' - list of Cargo library crate types, e.g., [\"staticlib\"],
--                  [\"cdylib\"], [\"staticlib\", \"cdylib\"]
compile :: (?cfg::Config) => DatalogProgram -> String -> Doc -> Doc -> FilePath -> [String] -> IO ()
compile d_unoptimized specname rs_code toml_code dir crate_types = do
    -- Create dir if it does not exist.
    createDirectoryIfMissing True (dir </> rustProjectDir specname)
    -- dump dependency graph to file
    updateFile (dir </> rustProjectDir specname </> specname <.> "dot")
               (depGraphToDot $ progDependencyGraph d_unoptimized)
    -- Apply optimizations; make sure the program has at least one relation.
    let d = addDummyRel $ optimize d_unoptimized
    when (confDumpDebug ?cfg) $
        writeFile (replaceExtension (confDatalogFile ?cfg) ".opt.ast") (show d)
    let (types, value, main) = compileLib d specname rs_code
    when (confJava ?cfg) $
        compileFlatBufferBindings d specname (dir </> rustProjectDir specname)
    -- Substitute specname template files; write files if changed.
    mapM_ (\(path, content) -> do
            let path' = dir </> path
                content' = replace "datalog_example" specname content
            updateFile path' content')
          $ templateFiles specname
    -- Update rustLibFiles if they changed.
    mapM_ (\(path, content) -> do
            let path' = dir </> path
            updateFile path' content)
         $ rustLibFiles specname
    -- Generate lib files if changed.
    updateFile (dir </> rustProjectDir specname </> "types/Cargo.toml") (render $ typesCargo specname toml_code)
    updateFile (dir </> rustProjectDir specname </> "types/lib.rs")     (render types)
    updateFile (dir </> rustProjectDir specname </> "value/lib.rs")     (render value)
    updateFile (dir </> rustProjectDir specname </> "Cargo.toml")       (render $ mainCargo specname crate_types)
    updateFile (dir </> rustProjectDir specname </> "src/lib.rs")       (render main)
    return ()

-- | Compile Datalog program into Rust code.
--
-- Returns Rust code for three crates that comp:
-- * 'types' crate containing DDlog type declarations, functions, and all imported
--   Rust library code.
-- * 'value' crate that declares relations and value types.
-- * 'main' crate that contains rule definitions in Rust and imports the other two.
--
compileLib :: (?cfg::Config) => DatalogProgram -> String -> Doc -> (Doc, Doc, Doc)
compileLib d specname rs_code = (typesLib, valueLib, mainLib)
    where
    statics = collectStatics d
    typesLib = typesHeader specname         $+$
               rs_code                      $+$
               typedefs                     $+$
               mkStatics d statics          $+$
               funcs
    valueLib = valueHeader specname         $+$
               mkValType d (cTypes cstate)  $+$ -- 'Value' enum type
               mkValueFromRecord d          $+$ -- Function to convert cmd_parser::Record to Value
               mkIndexesIntoArrId d cstate  $+$
               mkRelEnum d                  $+$ -- 'enum Relations'
               mkIdxEnum d                      -- 'enum Indexes'
    mainLib = mainHeader specname           $+$
              prog
    -- Compute ordered SCCs of the dependency graph.  These will define the
    -- structure of the program.
    depgraph = progDependencyGraph d
    sccs = G.topsort' $ G.condensation depgraph
    -- Initialize arrangements map
    arrs = M.fromList $ map (, []) $ M.keys $ progRelations d
    -- Initialize types
    -- Make sure that empty tuple is always in Value, so it can be
    -- used to implement Value::default()
    types = S.fromList $ (tTuple []) : (map (typeNormalize d . relType) $ M.elems $ progRelations d)
    -- Compile SCCs
    (prog, cstate) = let ?statics = statics in
                     runState (do -- First pass: compute arrangements
                                  createArrangements d
                                  -- Second pass: compile relations
                                  nodes <- mapM (compileSCC d depgraph) sccs
                                  mkProg nodes)
                              $ emptyCompilerState { cArrangements = arrs
                                                   , cTypes        = types }
    -- Type declarations
    typedefs = vcat $ map (mkTypedef d) $ M.elems $ progTypedefs d
    -- Functions
    (fdef, fextern) = partition (isJust . funcDef) $ concat $ M.elems $ progFunctions d
    funcs = let ?statics = statics in vcat $ (map (mkFunc d) fextern ++ map (mkFunc d) fdef)

-- This type stores the set of statically evaluated constant sub-expressions.
-- We need to track the type of each expression, as generic functions like
-- 'set_empty()' can have different types in different contexts.
-- The value associated with each key is the unique index of the static
-- expression used to generate a name for it.
type Statics = M.Map (Expr, Type) Int

addStatic :: DatalogProgram -> Expr -> ECtx -> Statics -> Statics
addStatic d e ctx statics =
    M.alter (maybe (Just $ M.size statics) Just) (e, exprType'' d ctx e) statics

deleteStatic :: Expr -> Type -> Statics -> Statics
deleteStatic e t statics = M.delete (e,t) statics

lookupStatic :: DatalogProgram -> Expr -> ECtx -> Statics -> Maybe Int
lookupStatic d e ctx statics = M.lookup (e, exprType'' d ctx e) statics

-- Extract all subexpressions of the program to be evaluated as lazy statics.
-- Rust lazy statics are computed once on the first access.  The current policy
-- is to evaluate function calls that take constant arguments and are side
-- effect-free statically.
collectStatics :: DatalogProgram -> Statics
collectStatics d = execState (progExprMapCtxM d (checkStaticExpr)) M.empty
    where
    checkStaticExpr :: ECtx -> ENode -> State (Statics) Expr
    checkStaticExpr ctx e@EApply{} | null (exprFreeVars d ctx (E e))
                                     && (not $ exprIsPolymorphic d ctx (E e))
                                     && exprIsPure d ctx (E e) = do
        modify (addStatic d (E e) ctx)
        return $ E e
    checkStaticExpr _   e = return $ E e

-- Generate Rust lazy static declarations, one for each static expression
-- computed by 'collectStatics'.
mkStatics :: DatalogProgram -> Statics -> Doc
mkStatics d statics =
    vcat $
    map (\((e, t), i) ->
            let static_name = "__STATIC_" <> pp i in
            let ?statics = deleteStatic e t statics in
            let e' = mkExpr d CtxTop (eTyped e t) EVal in
            "lazy_static!{ pub static ref" <+> static_name <> ":" <+> mkType t <+> "=" <+> e' <+> "; }")
        $ M.toList statics

-- Add dummy relation to the spec if it does not contain any.
-- Otherwise, we have to tediously handle this corner case in various
-- parts of the compiler.
-- Likewise, add a dummy index if the spec does not contain any.
addDummyRel :: DatalogProgram -> DatalogProgram
addDummyRel d =
    d {progRelations = rels, progIndexes = idxs}
    where
    rels = if (M.null $ progRelations d) || (M.null $ progIndexes d)
              then M.insert "__Null" (Relation nopos RelInternal RelSet "__Null" (tTuple []) Nothing) (progRelations d)
              else progRelations d
    idxs = if M.null $ progIndexes d
              then M.singleton "__Null_by_none" $ Index nopos "__Null_by_none" [] $ Atom nopos "__Null" $ eTuple []
              else progIndexes d

mkTypedef :: DatalogProgram -> TypeDef -> Doc
mkTypedef d tdef@TypeDef{..} =
    vcat (map (\attr -> "#[" <> pp attr <> "]") rustAttrs) $$
    case tdefType of
         Just TStruct{..} | length typeCons == 1
                          -> let (fields, extras) = unzip $ map (mkField tdefName True) $ consArgs $ head typeCons in
                             derive_struct                                                             $$
                             "pub struct" <+> rname tdefName <> targs <+> "{"                          $$
                             (nest' $ vcat $ punctuate comma fields)                                   $$
                             "}"                                                                       $$
                             impl_abomonate                                                            $$
                             mkFromRecord d tdef                                                       $$
                             mkStructIntoRecord tdef                                                   $$
                             mkStructMutator tdef                                                      $$
                             display                                                                   $$
                             vcat extras
                          | otherwise
                          -> let (constructors, extras) = unzip $ map mkConstructor typeCons in
                             derive_enum                                                               $$
                             "pub enum" <+> rname tdefName <> targs <+> "{"                            $$
                             (nest' $ vcat $ punctuate comma constructors)                             $$
                             "}"                                                                       $$
                             impl_abomonate                                                            $$
                             mkFromRecord d tdef                                                       $$
                             mkEnumIntoRecord tdef                                                     $$
                             mkEnumMutator tdef                                                        $$
                             display                                                                   $$
                             default_enum                                                              $$
                             vcat extras
         Just t           -> "pub type" <+> rname tdefName <+> targs <+> "=" <+> mkType t <> ";"
         Nothing          -> empty -- The user must provide definitions of opaque types
    where
    rustAttrs = getRustAttrs d tdefAttrs
    derive_serialize = if tdefGetCustomSerdeAttr d tdef then empty else ", Serialize, Deserialize"
    derive_struct = "#[derive(Eq, Ord, Clone, Hash, PartialEq, PartialOrd, Default" <> derive_serialize <> ")]"
    derive_enum = "#[derive(Eq, Ord, Clone, Hash, PartialEq, PartialOrd" <> derive_serialize <> ")]"
    targs = if null tdefArgs
               then empty
               else "<" <> (hsep $ punctuate comma $ map pp tdefArgs) <> ">"
    targs_traits = if null tdefArgs
                      then empty
                      else "<" <> (hsep $ punctuate comma $ map ((<> ": Val") . pp) tdefArgs) <> ">"
    targs_disp = if null tdefArgs
                    then empty
                    else "<" <> (hsep $ punctuate comma $ map ((<> ": fmt::Debug") . pp) tdefArgs) <> ">"
    targs_def = if null tdefArgs
                   then empty
                   else "<" <> (hsep $ punctuate comma $ map ((<> ": Default") . pp) tdefArgs) <> ">"

    -- Generate struct field, including #-annotations.
    -- If this field requires its own serde module (due to the deserialize_map_from_array attribute),
    -- this code is returned in the second component of the tuple.
    mkField :: String -> Bool -> Field -> (Doc, Doc)
    mkField cons pub f = ( from_arr_attr $$
                           vcat (map (\attr -> "#[" <> pp attr <> "]") rattrs) $$
                           (if pub then "pub" else empty) <+> pp (name f) <> ":" <+> mkType f
                        , from_array_module)
        where rattrs = getRustAttrs d $ fieldAttrs f
              TOpaque _ _ [ktype, vtype] = typ' d f
              from_arr_attr = maybe empty (\_ -> "#[serde(with=\"" <> from_array_module_name <> "\")]")
                              $ fieldGetDeserializeArrayAttr d f
              from_array_module = maybe empty (\fname -> let kfunc = getFunc d fname [vtype] in
                                                         "deserialize_map_from_array!(" <>
                                                         from_array_module_name <> "," <>
                                                         mkType ktype <> "," <> mkType vtype <> "," <>
                                                         mkFuncName d kfunc <> ");")
                                  $ fieldGetDeserializeArrayAttr d f
              from_array_module_name = "__serde_" <> rname cons <> "_" <> pp (name f)

    mkConstructor :: Constructor -> (Doc, Doc)
    mkConstructor c = (cons, vcat extras)
        where
        (fields, extras) = unzip $ map (mkField (name c) False) $ consArgs c
        args = vcat $ punctuate comma fields
        rattrs = getRustAttrs d $ consAttrs c
        cons = vcat (map (\attr -> "#[" <> pp attr <> "]") rattrs) $$
               if null $ consArgs c
                  then rname (name c)
                  else rname (name c) <+> "{" $$
                       nest' args $$
                       "}"

    impl_abomonate = "impl" <+> targs_traits <+> "abomonation::Abomonation for" <+> rname tdefName <> targs <> "{}"

    display = "impl" <+> targs_disp <+> "fmt::Display for" <+> rname tdefName <> targs <+> "{"                 $$
              "    fn fmt(&self, __formatter: &mut fmt::Formatter) -> fmt::Result {"                           $$
              "        match self {"                                                                           $$
              (nest' $ nest' $ nest' $ vcat $ punctuate comma $ map mkDispCons $ typeCons $ fromJust tdefType) $$
              "        }"                                                                                      $$
              "    }"                                                                                          $$
              "}"                                                                                              $$
              "impl" <+> targs_disp <+> "fmt::Debug for" <+> rname tdefName <> targs <+> "{"                   $$
              "    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {"                                     $$
              "        fmt::Display::fmt(&self, f)"                                                            $$
              "    }"                                                                                          $$
              "}"
    mkDispCons :: Constructor -> Doc
    mkDispCons c@Constructor{..} =
        cname <> "{" <> (hcat $ punctuate comma $ map (pp . name) consArgs) <> "} => {" $$
        (nest' $
            "__formatter.write_str(\"" <> (pp $ name c) <> "{\")?;" $$
            (vcat $
             mapIdx (\a i -> (if isString d a
                                 then "record::format_ddlog_str(" <> (pp $ name a) <> ", __formatter)?;"
                                 else "fmt::Debug::fmt(" <> (pp $ name a) <> ", __formatter)?;")
                             $$
                             (if i + 1 < length consArgs
                                 then "__formatter.write_str(\",\")?;"
                                 else empty))
                    consArgs) $$
            "__formatter.write_str(\"}\")")
        $$
        "}"
        where cname = mkConstructorName tdefName (fromJust tdefType) (name c)

    default_enum =
              "impl "  <+> targs_def <+> "Default for" <+> rname tdefName <> targs <+> "{"                     $$
              "    fn default() -> Self {"                                                                     $$
              "        " <> cname <> "{" <> def_args <> "}"                                                    $$
              "    }"                                                                                          $$
              "}"
        where c = head $ typeCons $ fromJust tdefType
              cname = mkConstructorName tdefName (fromJust tdefType) (name c)
              def_args = commaSep $ map (\a -> (pp $ name a) <+> ": Default::default()") $ consArgs c
{-
Generate FromRecord trait implementation for a struct type:

impl <T: FromRecord> FromRecord for DummyEnum<T> {
    fn from_record(val: &Record) -> Result<Self, String> {
        match val {
            Record::Struct(constr, args) => {
                match constr.as_ref() {
                    "Constr1" if args.len() == 2 => {
                        Ok(DummyEnum::Constr1{f1: <Bbool>::from_record(&args[0])?,
                                              f2: String::from_record(&args[1])?})
                    },
                    "Constr2" if args.len() == 3 => {
                        Ok(DummyEnum::Constr2{f1: <T>::from_record(&args[0])?,
                                              f2: <BigInt>::from_record(&args[1])?,
                                              f3: <Foo<T>>::from_record(&args[2])?})
                    },
                    "Constr3" if args.len() == 1 => {
                        Ok(DummyEnum::Constr3{f1: <(bool,bool)>::from_record(&args[0])?})
                    },
                    c => Result::Err(format!("unknown constructor {} of type DummyEnum in {:?}", c, *val))
                }
            },
            v => {
                Result::Err(format!("not a struct {:?}", *v))
            }
        }
    }
}
-}
mkFromRecord :: DatalogProgram -> TypeDef -> Doc
mkFromRecord d t@TypeDef{..} | tdefGetCustomFromRecord d t = empty
                             | otherwise =
    "impl" <+> targs_bounds <+> "record::FromRecord for" <+> rname (name t) <> targs <+> "{"                                    $$
    "    fn from_record(val: &record::Record) -> result::Result<Self, String> {"                                                $$
    "        match val {"                                                                                                       $$
    "            record::Record::PosStruct(constr, _args) => {"                                                                 $$
    "                match constr.as_ref() {"                                                                                   $$
    (nest' $ nest' $ nest' $ nest' $ nest' pos_constructors)                                                                    $$
    "                    c => result::Result::Err(format!(\"unknown constructor {} of type" <+> rname (name t) <+> "in {:?}\", c, *val))" $$
    "                }"                                                                                                         $$
    "            },"                                                                                                            $$
    "            record::Record::NamedStruct(constr, _args) => {"                                                               $$
    "                match constr.as_ref() {"                                                                                   $$
    (nest' $ nest' $ nest' $ nest' $ nest' named_constructors)                                                                  $$
    "                    c => result::Result::Err(format!(\"unknown constructor {} of type" <+> rname (name t) <+> "in {:?}\", c, *val))" $$
    "                }"                                                                                                         $$
    "            },"                                                                                                            $$
    "            record::Record::Serialized(format, s) => {"                                                                    $$
    "                if format == \"json\" {"                                                                                   $$
    "                    serde_json::from_str(&*s).map_err(|e|format!(\"{}\", e))"                                              $$
    "                } else {"                                                                                                  $$
    "                    result::Result::Err(format!(\"unsupported serialization format '{}'\", format))"                       $$
    "                }"                                                                                                         $$
    "            },"                                                                                                            $$
    "            v => {"                                                                                                        $$
    "                result::Result::Err(format!(\"not a struct {:?}\", *v))"                                                   $$
    "            }"                                                                                                             $$
    "        }"                                                                                                                 $$
    "    }"                                                                                                                     $$
    "}"
    where
    targs = "<" <> (hcat $ punctuate comma $ map pp tdefArgs) <> ">"
    targs_bounds = "<" <> (hcat $ punctuate comma $ map ((<> ": record::FromRecord + serde::de::DeserializeOwned + Default") . pp) tdefArgs) <> ">"
    pos_constructors = vcat $ map mkposcons $ typeCons $ fromJust tdefType
    mkposcons :: Constructor -> Doc
    mkposcons c@Constructor{..} =
        "\"" <> pp (name c) <> "\"" <+> "if _args.len() ==" <+> (pp $ length consArgs) <+> "=> {" $$
        "    Ok(" <> cname <> "{" <> (hsep $ punctuate comma fields) <> "})"     $$
        "},"
        where
        cname = mkConstructorName tdefName (fromJust tdefType) (name c)
        fields = mapIdx (\f i -> pp (name f) <> ": <" <> (mkType f) <> ">::from_record(&_args[" <> pp i <> "])?") consArgs
    named_constructors = vcat $ map mknamedcons $ typeCons $ fromJust tdefType
    mknamedcons :: Constructor -> Doc
    mknamedcons c@Constructor{..} =
        "\"" <> pp (name c) <> "\" => {" $$
        "    Ok(" <> cname <> "{" <> (hsep $ punctuate comma fields) <> "})"     $$
        "},"
        where
        cname = mkConstructorName tdefName (fromJust tdefType) (name c)
        fields = map (\f -> pp (name f) <> ": record::arg_extract::<" <> mkType f <> ">(_args, \"" <> (pp $ unddname f) <> "\")?") consArgs

mkStructIntoRecord :: TypeDef -> Doc
mkStructIntoRecord t@TypeDef{..} =
    "decl_struct_into_record!(" <> rname (name t) <> "[\"" <> pp (name t) <> "\"]" <> targs <> "," <+> args <> ");"
    where
    targs = "<" <> (hcat $ punctuate comma $ map pp tdefArgs) <> ">"
    args = commaSep $ map (pp . name) $ consArgs $ head $ typeCons $ fromJust tdefType

mkStructMutator :: TypeDef -> Doc
mkStructMutator t@TypeDef{..} =
    "decl_record_mutator_struct!(" <> rname (name t) <> ", " <> targs <> "," <+> args <> ");"
    where
    targs = "<" <> (hcat $ punctuate comma $ map pp tdefArgs) <> ">"
    args = commaSep $ map (\arg -> pp (name arg) <> ":" <+> mkType arg) $ consArgs $ head $ typeCons $ fromJust tdefType

mkEnumIntoRecord :: TypeDef -> Doc
mkEnumIntoRecord t@TypeDef{..} =
    "decl_enum_into_record!(" <> rname (name t) <> "," <+> targs <> "," <+> cons <> ");"
    where
    targs = "<" <> (hcat $ punctuate comma $ map pp tdefArgs) <> ">"
    cons = commaSep $ map (\c -> (rname $ name c) <> "[\"" <> pp (name c) <> "\"]" <> "{" <> (commaSep $ map (pp . name) $ consArgs c) <> "}")
                    $ typeCons $ fromJust tdefType

mkEnumMutator :: TypeDef -> Doc
mkEnumMutator t@TypeDef{..} =
    "decl_record_mutator_enum!(" <> rname (name t) <> "," <+> targs <> "," <+> cons <> ");"
    where
    targs = "<" <> (hcat $ punctuate comma $ map pp tdefArgs) <> ">"
    cons = commaSep $ map (\c -> (rname $ name c) <> "{" <> (commaSep $ map (\arg -> pp (name arg) <> ":" <+> mkType arg) $ consArgs c) <> "}")
                    $ typeCons $ fromJust tdefType

unddname :: (WithName a) => a -> String
unddname x = if isPrefixOf "__" (name x) && elem short reservedNames
                then short
                else name x
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
mkValueFromRecord :: (?cfg::Config) => DatalogProgram -> Doc
mkValueFromRecord d@DatalogProgram{..} =
    mkRelationsTryFromStr d                                                                         $$
    mkIsOutputRels d                                                                                $$
    mkIsInputRels d                                                                                 $$
    mkRelationsTryFromRelId d                                                                       $$
    mkRelId2Name d                                                                                  $$
    mkRelId2NameC                                                                                   $$
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
    "pub fn relval_from_record(rel: Relations, _rec: &record::Record) -> result::Result<DDValue, String> {" $$
    "    match rel {"                                                                               $$
    (nest' $ nest' $ vcommaSep entries)                                                             $$
    "    }"                                                                                         $$
    "}"                                                                                             $$
    "pub fn relkey_from_record(rel: Relations, _rec: &record::Record) -> result::Result<DDValue, String> {" $$
    "    match rel {"                                                                               $$
    (nest' $ nest' $ vcommaSep key_entries)                                                         $$
    "        _ => Err(format!(\"relation {:?} does not have a primary key\", rel))"                 $$
    "    }"                                                                                         $$
    "}"                                                                                             $$
    "pub fn idxkey_from_record(idx: Indexes, _rec: &record::Record) -> result::Result<DDValue, String> {"   $$
    "    match idx {"                                                                               $$
    (nest' $ nest' $ vcommaSep idx_entries)                                                         $$
    "    }"                                                                                         $$
    "}"
    where
    entries = map mkrelval $ M.elems progRelations
    mkrelval :: Relation ->  Doc
    mkrelval rel@Relation{..} =
        "Relations::" <> rname(name rel) <+> "=> {"                                                                     $$
        "    Ok(Value::" <> mkValConstructorName d t <> (parens $ "<" <> mkType t <> ">::from_record(_rec)?).into_ddvalue()")   $$
        "}"
        where t = typeNormalize d relType
    key_entries = map mkrelkey $ filter (isJust . relPrimaryKey) $ M.elems progRelations
    mkrelkey :: Relation ->  Doc
    mkrelkey rel@Relation{..} =
        "Relations::" <> rname(name rel) <+> "=> {"                                                                     $$
        "    Ok(Value::" <> mkValConstructorName d t <> (parens $ "<" <> mkType t <> ">::from_record(_rec)?).into_ddvalue()")   $$
        "}"
        where t = typeNormalize d $ fromJust $ relKeyType d rel
    idx_entries = map mkidxkey $ M.elems progIndexes
    mkidxkey :: Index ->  Doc
    mkidxkey idx@Index{..} =
        "Indexes::" <> rname (name idx) <+> "=> {"                                                                      $$
        "    Ok(Value::" <> mkValConstructorName d t <> (parens $ "<" <> mkType t <> ">::from_record(_rec)?).into_ddvalue()")   $$
        "}"
        where t = typeNormalize d $ idxKeyType idx

-- Convert string to `enum Relations`
mkRelationsTryFromStr :: DatalogProgram -> Doc
mkRelationsTryFromStr d =
    "impl TryFrom<&str> for Relations {"                                  $$
    "    type Error = ();"                                                $$
    "    fn try_from(rname: &str) -> result::Result<Self, Self::Error> {" $$
    "         match rname {"                                              $$
                  (nest' $ nest' $ vcat $ entries)                        $$
    "             _  => Err(())"                                          $$
    "         }"                                                          $$
    "    }"                                                               $$
    "}"
    where
    entries = map mkrel $ M.elems $ progRelations d
    mkrel :: Relation -> Doc
    mkrel rel = "\"" <> pp (name rel) <> "\" => Ok(Relations::" <> rname (name rel) <> "),"

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
    mkrel rel = "Relations::" <> rname (name rel) <> " => true,"

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
    mkrel rel = "Relations::" <> rname (name rel) <> " => true,"

-- Convert RelId to enum Relations
mkRelationsTryFromRelId :: DatalogProgram -> Doc
mkRelationsTryFromRelId d =
    "impl TryFrom<RelId> for Relations {"                                 $$
    "    type Error = ();"                                                $$
    "    fn try_from(rid: RelId) -> result::Result<Self, Self::Error> {"  $$
    "         match rid {"                                                $$
                  (nest' $ nest' $ vcat $ entries)                        $$
    "             _  => Err(())"                                          $$
    "         }"                                                          $$
    "    }"                                                               $$
    "}"
    where
    entries = map mkrel $ M.elems $ progRelations d
    mkrel :: Relation -> Doc
    mkrel rel = pp (relIdentifier d rel) <+> "=> Ok(Relations::" <> rname (name rel) <> "),"

mkRelId2Name :: DatalogProgram -> Doc
mkRelId2Name d =
    "pub fn relid2name(rid: RelId) -> Option<&'static str> {" $$
    "   match rid {"                                          $$
    (nest' $ nest' $ vcat $ entries)                          $$
    "       _  => None"                                       $$
    "   }"                                                    $$
    "}"
    where
    entries = map mkrel $ M.elems $ progRelations d
    mkrel :: Relation -> Doc
    mkrel rel = pp (relIdentifier d rel) <+> "=> Some(&\"" <> pp (name rel) <> "\"),"

mkRelId2NameC :: Doc
mkRelId2NameC =
    "pub fn relid2cname(rid: RelId) -> Option<&'static ffi::CStr> {"        $$
    "    RELIDMAPC.get(&rid).map(|c: &'static ffi::CString|c.as_ref())"                            $$
    "}"

mkRelIdMap :: DatalogProgram -> Doc
mkRelIdMap d =
    "lazy_static! {"                                                        $$
    "    pub static ref RELIDMAP: FnvHashMap<Relations, &'static str> = {"  $$
    "        let mut m = FnvHashMap::default();"                            $$
    (nest' $ nest' $ vcat entries)                                          $$
    "        m"                                                             $$
    "   };"                                                                 $$
    "}"
    where
    entries = map mkrel $ M.elems $ progRelations d
    mkrel :: Relation -> Doc
    mkrel rel = "m.insert(Relations::" <> rname (name rel) <> ", \"" <> pp (name rel) <> "\");"

mkRelIdMapC :: DatalogProgram -> Doc
mkRelIdMapC d =
    "lazy_static! {"                                                            $$
    "    pub static ref RELIDMAPC: FnvHashMap<RelId, ffi::CString> = {"         $$
    "        let mut m = FnvHashMap::default();"                                $$
    (nest' $ nest' $ vcat entries)                                              $$
    "        m"                                                                 $$
    "   };"                                                                     $$
    "}"
    where
    entries = map mkrel $ M.elems $ progRelations d
    mkrel :: Relation -> Doc
    mkrel rel = "m.insert(" <> pp (relIdentifier d rel) <>
        ", ffi::CString::new(\"" <> pp (name rel) <> "\").unwrap_or_else(|_|ffi::CString::new(r\"Cannot convert relation name to C string\").unwrap()));"

mkInputRelIdMap :: DatalogProgram -> Doc
mkInputRelIdMap d =
    "lazy_static! {"                                                                $$
    "    pub static ref INPUT_RELIDMAP: FnvHashMap<Relations, &'static str> = {"    $$
    "        let mut m = FnvHashMap::default();"                                    $$
    (nest' $ nest' $ vcat entries)                                                  $$
    "        m"                                                                     $$
    "    };"                                                                        $$
    "}"
    where
    entries = map mkrel $ filter ((== RelInput) . relRole) $ M.elems $ progRelations d
    mkrel :: Relation -> Doc
    mkrel rel = "m.insert(Relations::" <> rname (name rel) <> ", \"" <> pp (name rel) <> "\");"

mkOutputRelIdMap :: DatalogProgram -> Doc
mkOutputRelIdMap d =
    "lazy_static! {"                                                                $$
    "    pub static ref OUTPUT_RELIDMAP: FnvHashMap<Relations, &'static str> = {"   $$
    "        let mut m = FnvHashMap::default();"                                    $$
    (nest' $ nest' $ vcat entries)                                                  $$
    "        m"                                                                     $$
    "    };"                                                                        $$
    "}"
    where
    entries = map mkrel $ filter ((== RelOutput) . relRole) $ M.elems $ progRelations d
    mkrel :: Relation -> Doc
    mkrel rel = "m.insert(Relations::" <> rname (name rel) <> ", \"" <> pp (name rel) <> "\");"

-- Convert string to `enum Indexes`
mkIndexesTryFromStr :: DatalogProgram -> Doc
mkIndexesTryFromStr d =
    "impl TryFrom<&str> for Indexes {"                                    $$
    "    type Error = ();"                                                $$
    "    fn try_from(iname: &str) -> result::Result<Self, Self::Error> {" $$
    "         match iname {"                                              $$
                  (nest' $ nest' $ vcat $ entries)                        $$
    "             _  => Err(())"                                          $$
    "         }"                                                          $$
    "    }"                                                               $$
    "}"
    where
    entries = map mkidx $ M.elems $ progIndexes d
    mkidx :: Index -> Doc
    mkidx idx = "\"" <> pp (name idx) <> "\" => Ok(Indexes::" <> rname (name idx) <> "),"

mkIndexesIntoArrId :: DatalogProgram -> CompilerState -> Doc
mkIndexesIntoArrId d CompilerState{..} =
    "pub fn indexes2arrid(idx: Indexes) -> ArrId {"                  $$
    "    match idx {"                                                $$
    (nest' $ nest' $ vcat $ entries)                                 $$
    "    }"                                                          $$
    "}"
    where
    entries = map mkidx $ M.elems $ progIndexes d
    mkidx :: Index -> Doc
    mkidx idx = "Indexes::" <> rname (name idx) <+> "=> (" <+> pp (relIdentifier d $ idxRelation d idx)
                                                    <> "," <+> pp (findidx idx) <> "),"
    findidx :: Index -> Int
    findidx idx@Index{..} =
        fromJust
        $ findIndex (elem idx . arngUsedInIndexes)
        $ cArrangements M.! atomRelation idxAtom

-- Convert IdxId to `enum Indexes`
mkIndexesTryFromIdxId :: DatalogProgram -> Doc
mkIndexesTryFromIdxId d =
    "impl TryFrom<IdxId> for Indexes {"                                   $$
    "    type Error = ();"                                                $$
    "    fn try_from(iid: IdxId) -> result::Result<Self, Self::Error> {"  $$
    "         match iid {"                                                $$
                  (nest' $ nest' $ vcat $ entries)                        $$
    "             _  => Err(())"                                          $$
    "         }"                                                          $$
    "    }"                                                               $$
    "}"
    where
    entries = map mkidx $ M.elems $ progIndexes d
    mkidx :: Index -> Doc
    mkidx idx = pp (idxIdentifier d idx) <+> "=> Ok(Indexes::" <> rname (name idx) <> "),"

mkIdxId2Name :: DatalogProgram -> Doc
mkIdxId2Name d =
    "pub fn indexid2name(iid: IdxId) -> Option<&'static str> {" $$
    "   match iid {"                                            $$
    (nest' $ nest' $ vcat $ entries)                            $$
    "       _  => None"                                         $$
    "   }"                                                      $$
    "}"
    where
    entries = map mkidx $ M.elems $ progIndexes d
    mkidx :: Index -> Doc
    mkidx idx = pp (idxIdentifier d idx) <+> "=> Some(&\"" <> pp (name idx) <> "\"),"

mkIdxId2NameC :: Doc
mkIdxId2NameC =
    "pub fn indexid2cname(iid: IdxId) -> Option<&'static ffi::CStr> {"      $$
    "    IDXIDMAPC.get(&iid).map(|c: &'static ffi::CString|c.as_ref())"     $$
    "}"

mkIdxIdMap :: DatalogProgram -> Doc
mkIdxIdMap d =
    "lazy_static! {"                                                        $$
    "    pub static ref IDXIDMAP: FnvHashMap<Indexes, &'static str> = {"    $$
    "        let mut m = FnvHashMap::default();"                            $$
    (nest' $ nest' $ vcat entries)                                          $$
    "        m"                                                             $$
    "   };"                                                                 $$
    "}"
    where
    entries = map mkidx $ M.elems $ progIndexes d
    mkidx :: Index -> Doc
    mkidx idx = "m.insert(Indexes::" <> rname (name idx) <> ", \"" <> pp (name idx) <> "\");"

mkIdxIdMapC :: DatalogProgram -> Doc
mkIdxIdMapC d =
    "lazy_static! {"                                                            $$
    "    pub static ref IDXIDMAPC: FnvHashMap<IdxId, ffi::CString> = {"         $$
    "        let mut m = FnvHashMap::default();"                                $$
    (nest' $ nest' $ vcat entries)                                              $$
    "        m"                                                                 $$
    "   };"                                                                     $$
    "}"
    where
    entries = map mkidx $ M.elems $ progIndexes d
    mkidx :: Index -> Doc
    mkidx idx = "m.insert(" <> pp (idxIdentifier d idx) <>
        ", ffi::CString::new(\"" <> pp (name idx) <> "\").unwrap_or_else(|_|ffi::CString::new(r\"Cannot convert index name to C string\").unwrap()));"

mkFunc :: (?statics::Statics) => DatalogProgram -> Function -> Doc
mkFunc d f@Function{..} | isJust funcDef =
    "pub fn" <+> mkFuncName d f <> tvars <> (parens $ hsep $ punctuate comma $ map mkArg funcArgs) <+> "->" <+> mkType funcType      $$
    "{"                                                                                                                              $$
    (nest' $ mkExpr d (CtxFunc f) (fromJust funcDef) EVal)                                                                           $$
    "}"
                        | -- generate commented out prototypes of extern functions for user convenvience.
                          otherwise = "/* fn" <+> rname (name f) <> tvars <> (parens $ hsep $ punctuate comma $ map mkArg funcArgs) <+> "->" <+> mkType funcType <+> "*/"
    where
    mkArg :: FuncArg -> Doc
    mkArg a = pp (name a) <> ":" <+> "&" <> (if argMut a then "mut" else empty) <+> mkType a
    tvars = case funcTypeVars f of
                 []  -> empty
                 tvs -> "<" <> (hcat $ punctuate comma $ map ((<> ": Val") . pp) tvs) <> ">"

-- Generate Value type as an enum with one entry per type in types
mkValType :: (?cfg::Config) => DatalogProgram -> S.Set Type -> Doc
mkValType d types =
    "pub mod Value" $$
    (braces' $
        "use super::*;"               $$
        (vcat $ map mkVal $ S.toList types))
    where
    mkVal :: Type -> Doc
    mkVal t =
        "#[derive(Default, Eq, Ord, Clone, Hash, PartialEq, PartialOrd, Serialize, Deserialize, Debug)]"            $$
        "pub struct" <+> tname <+> parens ("pub" <+> mkTypeScoped "super::" t) <> ";"                               $$
        "impl abomonation::Abomonation for" <+> tname <+> "{}"                                                      $$
        "impl fmt::Display for" <+> tname <+> "{"                                                                   $$
        "    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {"                                            $$
        "        self.clone().into_record().fmt(f)"                                                                 $$
        "    }"                                                                                                     $$
        "}"                                                                                                         $$
        "impl record::IntoRecord for" <+> tname <+> "{"                                                             $$
        "    fn into_record(self) -> record::Record {"                                                              $$
        "        self.0.into_record()"                                                                              $$
        "    }"                                                                                                     $$
        "}"                                                                                                         $$
        "impl record::Mutator<" <> tname <> "> for record::Record {"                                                $$
        "    fn mutate(&self, v: &mut" <+> tname <+> ") -> result::Result<(), std::string::String> {"               $$
        "        self.mutate(&mut v.0)"                                                                             $$
        "    }"                                                                                                     $$
        "}"                                                                                                         $$
        "//#[typetag::serde]"                                                                                       $$
        "decl_ddval_convert!{" <> tname <> "}"
        where tname = mkValConstructorName d t

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
         Just arr -> addJoinArrangement (atomRelation idxAtom) arr (Just idx)

createRelArrangements :: DatalogProgram -> Relation -> CompilerMonad ()
createRelArrangements d rel = mapM_ (createRuleArrangements d) $ relRules d $ name rel

createRuleArrangements :: DatalogProgram -> Rule -> CompilerMonad ()
createRuleArrangements d rule = do
    -- Arrange the first atom in the rule if needed
    let arr = ruleArrangeFstLiteral d rule
    when (isJust arr)
         $ addJoinArrangement (atomRelation $ rhsAtom $ head $ ruleRHS rule) (fromJust arr) Nothing
    mapM_ (createRuleArrangement d rule) [1..(length (ruleRHS rule) - 1)]

createRuleArrangement :: DatalogProgram -> Rule -> Int -> CompilerMonad ()
createRuleArrangement d rule idx = do
    let rhs = ruleRHS rule !! idx
    let ctx = CtxRuleRAtom rule idx
    let rel = getRelation d $ atomRelation $ rhsAtom rhs
    let (arr, _) = normalizeArrangement d ctx $ atomVal $ rhsAtom rhs
    -- If the literal does not introduce new variables, it's a semijoin
    let is_semi = null $ ruleRHSNewVars d rule idx
    case rhs of
         RHSLiteral True _ | is_semi   -> addSemijoinArrangement (name rel) arr
                           | otherwise -> addJoinArrangement (name rel) arr Nothing
         RHSLiteral False _            -> addAntijoinArrangement (name rel) arr
         _                             -> return ()

-- Generate Rust struct for ProgNode
compileSCC :: (?cfg::Config, ?statics::Statics) => DatalogProgram -> DepGraph -> [G.Node] -> CompilerMonad ProgNode
compileSCC d dep nodes | recursive = compileSCCNode d relnames
                       | otherwise = case depnode of
                                          DepNodeRel rel -> compileRelNode d rel
                                          DepNodeApply a -> return $ compileApplyNode d a
    where
    recursive = any (\(from, to) -> elem from nodes && elem to nodes) $ G.edges dep
    relnames = map ((\(DepNodeRel rel) -> rel) . fromJust . G.lab dep) nodes
    depnode = fromJust $ G.lab dep $ head nodes

compileRelNode :: (?cfg::Config, ?statics::Statics) => DatalogProgram -> String -> CompilerMonad ProgNode
compileRelNode d relname = do
    rel <- compileRelation d relname
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

extern transformer SCC(Edges:   relation['E],
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
compileApplyNode :: (?cfg::Config) => DatalogProgram -> Apply -> ProgNode
compileApplyNode d Apply{..} = ApplyNode $
    "{fn transformer() -> Box<dyn for<'a> Fn(&mut FnvHashMap<RelId, collection::Collection<scopes::Child<'a, worker::Worker<communication::Allocator>, TS>,DDValue,Weight>>)> {" $$
    "    Box::new(|collections| {"                                                                                                                             $$
    "        let (" <> commaSep outputs <> ") =" <+> rname applyTransformer <> (parens $ commaSep inputs) <> ";"                                               $$
    (nest' $ nest' $ vcat update_collections)                                                                                                                  $$
    "    })"                                                                                                                                                   $$
    "}; transformer}"
  where
    Transformer{..} = getTransformer d applyTransformer
    inputs = concatMap (\(i, ti) ->
                         if hotypeIsFunction (hofType ti)
                            then [rname i]
                            else ["collections.get(&(" <> relId i <> ")).unwrap()", extractValue d (relType $ getRelation d i)])
                       (zip applyInputs transInputs)
             ++
             map (\o -> parens $ "|v|" <> mkValue d "v" (relType $ getRelation d o) <> ".into_ddvalue()") applyOutputs
    outputs = map rname applyOutputs
    update_collections = map (\o -> "collections.insert(" <> relId o <> "," <+> rname o <> ");") applyOutputs

extractValue :: (?cfg::Config) => DatalogProgram -> Type -> Doc
extractValue d t = parens $
        "|" <> vALUE_VAR <> ": DDValue| unsafe { Value::" <> mkValConstructorName d t' <> "::from_ddvalue(" <> vALUE_VAR <> ") }.0.clone()"
    where t' = typeNormalize d t

compileSCCNode :: (?cfg::Config, ?statics::Statics) => DatalogProgram -> [String] -> CompilerMonad ProgNode
compileSCCNode d relnames = do
    prels <- mapM (\rel -> do prel <- compileRelation d rel
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
compileRelation :: (?cfg::Config, ?statics::Statics) => DatalogProgram -> String -> CompilerMonad ProgRel
compileRelation d rn = do
    let rel@Relation{..} = getRelation d rn
    -- collect all rules for this relation
    let (facts, rules) =
                partition (null . ruleRHS)
                $ relRules d rn
    rules' <- mapM (\rl -> compileRule d rl 0 True) rules
    facts' <- mapM (compileFact d) facts
    key_func <- maybe (return "None")
                      (\k -> do lambda <- compileKey d rel k
                                return $ "Some(" <> lambda <> ")")
                relPrimaryKey

    let cb = if relRole == RelOutput
                then "change_cb:    Some(sync::Arc::new(sync::Mutex::new(__update_cb.clone())))"
                else "change_cb:    None"
    arrangements <- gets $ (M.! rn) . cArrangements
    compiled_arrangements <- mapM (mkArrangement d rel) arrangements
    let code =
            "Relation {"                                                                                        $$
            "    name:         \"" <> pp rn <> "\".to_string(),"                                                $$
            "    input:        " <> (if relRole == RelInput then "true" else "false") <> ","                    $$
            "    distinct:     " <> (if relRole == RelOutput && relSemantics == RelSet && not (relIsDistinctByConstruction d rel)
                                        then "true"
                                        else "false") <> ","  $$
            "    caching_mode: " <> (case relSemantics of
                                          RelMultiset -> "CachingMode::Multiset"
                                          RelSet -> "CachingMode::Set"
                                          RelStream -> "CachingMode::Stream") <> ","         $$
            "    key_func:     " <> key_func <> ","                                                             $$
            "    id:           " <> relId rn <> ","                                                             $$
            "    rules:        vec!["                                                                           $$
            (nest' $ nest' $ vcat (punctuate comma rules') <> "],")                                             $$
            "    arrangements: vec!["                                                                           $$
            (nest' $ nest' $ vcat (punctuate comma compiled_arrangements) <> "],")                              $$
            (nest' cb)                                                                                          $$
            "}"
    return ProgRel{ prelName = rn, prelCode = code, prelFacts = facts' }

compileKey :: (?cfg::Config, ?statics::Statics) => DatalogProgram -> Relation -> KeyExpr -> CompilerMonad Doc
compileKey d rel@Relation{..} KeyExpr{..} = do
    v <- mkValue' d (CtxKey rel) keyExpr
    return $
        "(|" <> kEY_VAR <> ": &DDValue| {"                                                                                                      $$
        "    let ref" <+> pp keyVar <+> "= unsafe { Value::" <> mkValConstructorName d relType <> "::from_ddvalue_ref(" <> kEY_VAR <> ") }.0;"  $$
        "    " <> v                                                                                                                             $$
        "})"

{- Generate Rust representation of a ground fact -}
compileFact :: (?cfg::Config, ?statics::Statics) => DatalogProgram -> Rule -> CompilerMonad Doc
compileFact d rl@Rule{..} = do
    let rel = atomRelation $ head ruleLHS
    v <- mkValue' d (CtxRuleL rl 0) $ atomVal $ head ruleLHS
    return $ "(" <> relId rel <> "," <+> v <> ") /*" <> pp rl <> "*/"


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
-- to generate the top-level 'Rule data structure'
--
-- 'input_val' - true if the relation generated by the last operator is a Value, not
-- tuple.  This is the case when we've only encountered antijoins so far (since antijoins
-- do not rearrange the input relation).
compileRule :: (?cfg::Config, ?statics::Statics) => DatalogProgram -> Rule -> Int -> Bool -> CompilerMonad Doc
compileRule d rl@Rule{..} last_rhs_idx input_val = {-trace ("compileRule " ++ show rl ++ " / " ++ show last_rhs_idx) $-} do
    -- First relation in the body of the rule
    let fstatom = rhsAtom $ head ruleRHS
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
    input_arrangement_id <- if last_rhs_idx == 0 && isJust input_arrangement
                               then getJoinArrangement (atomRelation fstatom) $ fromJust input_arrangement
                               else return Nothing
    -- Open up input constructor; bring Datalog variables into scope
    open <- if input_val
               then openAtom  d ("&" <> vALUE_VAR) rl 0 (rhsAtom $ head ruleRHS) "return None"
               else openTuple d ("&" <> vALUE_VAR) $ rhsVarsAfter d rl last_rhs_idx
    -- Apply filters and assignments between last_rhs_idx and rhs_idx
    let filters = mkFilters d rl last_rhs_idx
    let prefix = open $+$ vcat filters
    -- Generate XFormCollection or XFormArrangement for the 'rhs' operator.
    let mkArrangedOperator conditions inpval =
            case rhs of
                 RHSLiteral True a  -> mkJoin d conditions inpval a rl rhs_idx
                 RHSLiteral False a -> mkAntijoin d conditions inpval a rl rhs_idx
                 RHSAggregate{}     -> mkAggregate d conditions inpval rl rhs_idx
                 _                  -> error $ "compileRule: operator " ++ show rhs ++ " does not expect arranged input"
    let mkCollectionOperator | rhs_idx == length ruleRHS
                             = mkHead d prefix rl
                             | otherwise =
            case rhs of
                 RHSFlatMap v e -> mkFlatMap d prefix rl rhs_idx v e
                 RHSInspect e | not (null filters) || input_val -> mkFilterMap d prefix rl rhs_idx -- filter inputs before inspecting them
                              | otherwise -> mkInspect d prefix rl rhs_idx e input_val -- pass input_val to be future-proof
                 _ -> error "compileRule: operator requires arranged input"

    -- If: input to the operator is an arranged collection
    -- Then: create Rule::ArrangementRule, pass conditions as input to mkJoin/Antijoin
    -- Else:
    --     If: At the start of the rule:
    --     Then: Generate Rule::CollectionRule;
    --     If: Next operator requires arranged input
    --     Then: Generate Arrange operator;
    case input_arrangement_id of
       Just arid -> do
            xform <- mkArrangedOperator conds input_val
            return $ "/*" <+> pp rl <+> "*/"                                                    $$
                     "Rule::ArrangementRule {"                                                  $$
                     "    description:" <+> pp (show $ show rl) <> ".to_string(),"              $$
                     "    arr: (" <+> relId (atomRelation fstatom) <> "," <+> pp arid <> "),"   $$
                     "    xform:" <+> xform                                                     $$
                     "}"
       Nothing -> do
            xform <- case arrange_input_by of
                        Just (key_vars, val_vars) -> do
                            -- Evaluate arrange_input_by in the context of 'rhs'
                            let key_str = parens $ commaSep $ map (pp . fst) key_vars
                            akey <- mkTupleValue d key_vars
                            aval <- mkVarsTupleValue d $ map (, EReference) val_vars
                            let afun = braces'
                                       $ prefix $$
                                         "Some((" <> akey <> "," <+> aval <> "))"
                            xform' <- mkArrangedOperator [] False
                            return $ "XFormCollection::Arrange {"                                                                                    $$
                                     "    description:" <+> (pp $ show $ show $ "arrange" <+> rulePPPrefix rl (last_rhs_idx+1) <+> "by" <+> key_str) <+> ".to_string(),"                                                                                                                             $$
                                     (nest' $ "afun: &{fn __f(" <> vALUE_VAR <> ": DDValue) -> Option<(DDValue,DDValue)>" $$ afun $$ "__f},")        $$
                                     "    next: Box::new(" <> xform' <> ")"                                                                          $$
                                     "}"
                        Nothing -> mkCollectionOperator
            return $
                -- input_val ensures that CollectionRule is only generated once.
                -- There is a use-case where Inspect operator is converted into a FilterMap and then an Inspect in rust code.
                -- In order to do that, mkFilterMap's 'next' is another call to compileRule with the previous rule index.
                -- Since mkFilterMap already handled the case input_val and if it is the first rule, the recursive call to compileRule
                -- to generate Inspect rust code does not need another CollectionRule.
                if last_rhs_idx == 0 && input_val
                   then "/*" <+> pp rl <+> "*/"                                         $$
                        "Rule::CollectionRule {"                                        $$
                        "    description:" <+> pp (show $ show rl) <> ".to_string(),"   $$
                        "    rel:" <+> relId (atomRelation fstatom) <> ","              $$
                        "    xform: Some(" <> xform <> ")"                              $$
                        "}"
                   else "Some(" <> xform <> ")"


-- 'Join', 'Antijoin', 'Semijoin', and 'Aggregate' operators take arranged collection
-- as input.  'rhsInputArrangement' returns the arrangement expected by the operator
-- or Nothing if the operator takes a flat collection (e.g., it's a 'FlatMap').
--
-- The first component of the return tuple is the list of expressions that must be used
-- to index the input collection.  The second component lists variables that
-- will form the value of the arrangement.
rhsInputArrangement :: DatalogProgram -> Rule -> Int -> RuleRHS -> Maybe ([(Expr, ECtx)], [Var])
rhsInputArrangement d rl rhs_idx (RHSLiteral _ atom) =
    let ctx = CtxRuleRAtom rl rhs_idx
        (_, vmap) = normalizeArrangement d ctx $ atomVal atom
    in Just $ (map (\(_,e,c) -> (e,c)) vmap,
               -- variables visible before join that are still in use after it
               (rhsVarsAfter d rl (rhs_idx - 1)) `intersect` (rhsVarsAfter d rl rhs_idx))
rhsInputArrangement d rl rhs_idx (RHSAggregate _ grpby _ _) =
    let ctx = CtxRuleRGroupBy rl rhs_idx
    in Just $ (map (\(v, ctx') -> (eVar v, ctx')) $ exprVarOccurrences ctx grpby,
               -- all visible variables to preserve multiset semantics
               rhsVarsAfter d rl (rhs_idx - 1))
rhsInputArrangement _ _  _       _ = Nothing

mkFlatMap :: (?cfg::Config, ?statics::Statics) => DatalogProgram -> Doc -> Rule -> Int -> String -> Expr -> CompilerMonad Doc
mkFlatMap d prefix rl idx v e = do
    vars <- mkVarsTupleValue d $ map (, EVal) $ rhsVarsAfter d rl idx
    -- Flatten
    let flatten = "let __flattened =" <+> mkExpr d (CtxRuleRFlatMap rl idx) e EVal <> ";"
    -- Clone variables before passing them to the closure.
    let clones = vcat $ map ((\vname -> "let" <+> vname <+> "=" <+> cloneRef vname <> ";") . pp . name)
                      $ filter ((/= v) . name) $ rhsVarsAfter d rl idx
    let fmfun = braces'
                $ prefix  $$
                  flatten $$
                  clones  $$
                  "Some(Box::new(__flattened.into_iter().map(move |" <> pp v <> "|" <> vars <> ")))"
    next <- compileRule d rl idx False
    return $
        "XFormCollection::FlatMap{"                                                                                         $$
        "    description:" <+> (pp $ show $ show $ rulePPPrefix rl $ idx + 1) <+> ".to_string(),"                           $$
        (nest' $ "fmfun: &{fn __f(" <> vALUE_VAR <> ": DDValue) -> Option<Box<dyn Iterator<Item=DDValue>>>" $$ fmfun $$ "__f},")$$
        "    next: Box::new(" <> next <> ")"                                                                                $$
        "}"

mkFilterMap :: (?cfg::Config, ?statics::Statics) => DatalogProgram -> Doc -> Rule -> Int -> CompilerMonad Doc
mkFilterMap d prefix rl idx = do
    v <- mkVarsTupleValue d $ map (, EReference) $ rhsVarsAfter d rl idx
    let fmfun = braces' $ prefix $$
                          "Some" <> parens v
    next <- compileRule d rl (idx - 1) False  -- Use the previous index to evaluate the rule again and call mkInspect
    return $
        "XFormCollection::FilterMap{"                                                                                       $$
        "    description:" <+> (pp $ show $ show $ rulePPPrefix rl $ idx + 1) <+> ".to_string(),"                           $$
        nest' ("fmfun: &{fn __f(" <> vALUE_VAR <> ": DDValue) -> Option<DDValue>" $$ fmfun $$ "__f},")                      $$
        "    next: Box::new(" <> next <> ")"                                                                                $$
        "}"

mkInspect :: (?cfg::Config, ?statics::Statics) => DatalogProgram -> Doc -> Rule -> Int -> Expr -> Bool -> CompilerMonad Doc
mkInspect d prefix rl idx e input_val = do
    -- Inspected
    let weight = "let ddlog_weight = &(" <> wEIGHT_VAR <> " as std_DDWeight);"
    let timestamp = if ruleIsRecursive d rl
        then "let ddlog_timestamp = &std_DDNestedTS{epoch:" <+> tIMESTAMP_VAR <> ".0 as std_DDEpoch, iter:" <+> tIMESTAMP_VAR <> ".1.x as std_DDIteration};"
        else "let ddlog_timestamp = &(" <> tIMESTAMP_VAR <> ".0 as std_DDEpoch);"
    let inspected = (braces $ mkExpr d (CtxRuleRInspect rl idx) e EVal) <> ";"
    let ifun = braces'
                $ weight $$
                  timestamp $$
                  prefix $$
                  inspected
    next <- compileRule d rl idx input_val
    return $
        "XFormCollection::Inspect{"                                                                                         $$
        "    description:" <+> (pp $ show $ show $ rulePPPrefix rl $ idx + 1) <+> ".to_string(),"                           $$
        (nest' $ "ifun: &{fn __f(" <> vALUE_VAR <> ": &DDValue, " <> tIMESTAMP_VAR <> ": TupleTS, " <> wEIGHT_VAR <> ": Weight) -> ()" $$ ifun $$ "__f},")$$
        "    next: Box::new(" <> next <> ")"                                                                                $$
        "}"

mkAggregate :: (?cfg::Config, ?statics::Statics) => DatalogProgram -> [Int] -> Bool -> Rule -> Int -> CompilerMonad Doc
mkAggregate d filters input_val rl@Rule{..} idx = do
    let rhs@RHSAggregate{..} = ruleRHS !! idx
    let ctx = CtxRuleRAggregate rl idx
    let gctx = CtxRuleRGroupBy rl idx
    let Just (_, group_vars) = rhsInputArrangement d rl idx rhs
    -- Filter inputs before grouping
    ffun <- mkFFun d rl filters
    -- Function to extract the argument of aggregation function from 'Value'
    open <- if input_val
               then openAtom d vALUE_VAR rl 0 (rhsAtom $ head ruleRHS) "unreachable!()"
               else openTuple d vALUE_VAR group_vars
    let project = "{fn __f(" <> vALUE_VAR <> ": &DDValue) -> " <+> mkType (exprType d ctx rhsAggExpr) $$
                  (braces' $ open $$ mkExpr d ctx rhsAggExpr EVal)                                   $$
                  "std::rc::Rc::new(__f)}"
    -- Aggregate function:
    -- - compute aggregate
    -- - return variables still in scope after this term
    let ktype = ruleAggregateKeyType d rl idx
    let vtype = ruleAggregateValType d rl idx
    let tmap = ruleAggregateTypeParams d rl idx
    let agg_func = getFunc d rhsAggFunc [tOpaque gROUP_TYPE [ktype, vtype]]
    -- Pass group-by variables to the aggregate function.
    let grp = "&std_Group::new(&" <> (mkExpr d gctx rhsGroupBy EVal) <> "," <+> gROUP_VAR <> "," <+> project <> ")"
    let tparams = commaSep $ map (\tvar -> mkType (tmap M.! tvar)) $ funcTypeVars agg_func
    let aggregate = "let" <+> pp rhsVar <+> "=" <+> rname rhsAggFunc <>
                    "::<" <> tparams <> ">(" <> grp <> ");"
    result <- mkVarsTupleValue d $ map (\v -> (v, if name v == rhsVar then EVal else EReference)) $ rhsVarsAfter d rl idx
    let key_vars = exprVars d gctx rhsGroupBy
    open_key <- openTuple d kEY_VAR key_vars
    let agfun = braces'
                $ open_key  $$
                  aggregate $$
                  result
    next <- compileRule d rl idx False
    return $
        "XFormArrangement::Aggregate{"                                                                                           $$
        "    description:" <+> (pp $ show $ show $ rulePPPrefix rl $ idx + 1) <> ".to_string(),"                                 $$
        "    ffun:" <+> ffun <> ","                                                                                              $$
        "    aggfun: &{fn __f(" <> kEY_VAR <> ": &DDValue," <+> gROUP_VAR <> ": &[(&DDValue, Weight)]) -> DDValue" $$ agfun $$ "__f}," $$
        "    next: Box::new(" <> next <> ")"                                                                                     $$
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
openAtom :: (?cfg::Config, ?statics::Statics) => DatalogProgram -> Doc -> Rule -> Int -> Atom -> Doc -> CompilerMonad Doc
openAtom d var rl idx Atom{..} on_error = do
    let rel = getRelation d atomRelation
    let t = relType rel
    constructor <- mkValConstructorName' d t
    let varnames = map (pp . name) $ exprVarDecls d (CtxRuleRAtom rl idx) atomVal
        var_clones = tuple $ map cloneRef varnames
        vars = tuple $ map ("ref" <+>) varnames
        mtch = mkMatch (mkPatExpr d (CtxRuleRAtom rl idx) atomVal EReference) var_clones on_error
    return $
        "let" <+> vars <+> "= match unsafe { " <+> constructor <> "::from_ddvalue_ref(" <> var <> ") }.0 {" $$
        (nest' mtch)                                                                                        $$
        "};"

-- Generate Rust code to open up tuples and bring variables into scope.
openTuple :: (?cfg::Config) => DatalogProgram -> Doc -> [Var] -> CompilerMonad Doc
openTuple d var vs = do
    let t = tTuple $ map (varType d) vs
    cons <- mkValConstructorName' d t
    let pattern = tupleStruct $ map (("ref" <+>) . pp . name) vs
    return $ "let" <+> pattern <+> "= unsafe {" <+> cons <> "::from_ddvalue_ref(" <+> var <+> ") }.0;"

-- Generate Rust constructor name for a type;
-- add type to CompilerMonad
mkValConstructorName' :: DatalogProgram -> Type -> CompilerMonad Doc
mkValConstructorName' d t = do
    let t' = typeNormalize d t
    addType t'
    return $ "Value::" <> mkValConstructorName d t'

-- Generate Rust constructor name for a type
mkValConstructorName :: DatalogProgram -> Type -> Doc
mkValConstructorName d t' =
    case t of
         TTuple{..}  -> "__Tuple" <> pp (length typeTupArgs) <> "__" <>
                        (hcat $ punctuate "_" $ map (mkValConstructorName d) typeTupArgs)
         TBool{}     -> "__Boolval"
         TInt{}      -> "__Intval"
         TString{}   -> "__Stringval"
         TBit{..}    -> "__Bitval" <> pp typeWidth
         TSigned{..} -> "__Signedval" <> pp typeWidth
         TDouble{}   -> "__Doubleval"
         TFloat{}    -> "__Floatval"
         TUser{}     -> consuser
         TOpaque{}   -> consuser
         TVar{..}    -> pp tvarName
         _           -> error $ "unexpected type " ++ show t ++ " in Compile.mkValConstructorName'"
    where
    t = typeNormalize d t'
    consuser = rname (typeName t) <>
               case typeArgs t of
                    [] -> empty
                    as -> "__" <> (hcat $ punctuate "_" $ map (mkValConstructorName d) as)

mkValue' :: (?cfg::Config, ?statics::Statics) => DatalogProgram -> ECtx -> Expr -> CompilerMonad Doc
mkValue' d ctx e = do
    let t = exprType d ctx e
    constructor <- mkValConstructorName' d t
    return $ constructor <> (parens $ mkExpr d ctx e EVal) <> ".into_ddvalue()"

mkValue :: (?cfg::Config) => DatalogProgram -> Doc -> Type -> Doc
mkValue d v t = "Value::" <> mkValConstructorName d (typeNormalize d t) <> (parens v)

mkTupleValue :: (?cfg::Config, ?statics::Statics) => DatalogProgram -> [(Expr, ECtx)] -> CompilerMonad Doc
mkTupleValue d es = do
    let t = tTuple $ map (\(e, ctx) -> exprType'' d ctx e) es
    constructor <- mkValConstructorName' d t
    return $ constructor <> (parens $ tupleStruct $ map (\(e, ctx) -> mkExpr d ctx e EVal) es) <> ".into_ddvalue()"

mkVarsTupleValue :: (?cfg::Config) => DatalogProgram -> [(Var, EKind)] -> CompilerMonad Doc
mkVarsTupleValue d vs = do
    let t = tTuple $ map (varType d . fst) vs
    constructor <- mkValConstructorName' d t
    let clone (v, EReference) = cloneRef $ pp $ name v
        clone (v, _) = (pp $ name v) <> ".clone()"
    return $ constructor <> (parens $ tupleStruct $ map clone vs) <> ".into_ddvalue()"

mkFieldTupleValue :: (?cfg::Config) => DatalogProgram -> [Field] -> CompilerMonad Doc
mkFieldTupleValue d fs = do
    let t = tTuple $ map typ fs
    constructor <- mkValConstructorName' d t
    return $ constructor <> (parens $ tupleStruct $ map (cloneRef . pp . name) fs) <> ".into_ddvalue()"

-- Compile all contiguous RHSCondition terms following 'last_idx'
mkFilters :: (?statics::Statics) => DatalogProgram -> Rule -> Int -> [Doc]
mkFilters d rl@Rule{..} last_idx =
    mapIdx (\rhs i -> mkFilter d (CtxRuleRCond rl $ i + last_idx + 1) $ rhsExpr rhs)
    $ takeWhile (\case
                  RHSCondition{} -> True
                  _              -> False)
    $ drop (last_idx+1) ruleRHS

-- Implement RHSCondition semantics in Rust; brings new variables into
-- scope if this is an assignment
mkFilter :: (?statics::Statics) => DatalogProgram -> ECtx -> Expr -> Doc
mkFilter d ctx (E e@ESet{..}) = mkAssignFilter d ctx e
mkFilter d ctx e              = mkCondFilter d ctx e

mkAssignFilter :: (?statics::Statics) => DatalogProgram -> ECtx -> ENode -> Doc
mkAssignFilter d ctx e@(ESet _ l r) =
    "let" <+> vardecls <+> "= match" <+> r' <+> "{"                 $$
    (nest' mtch)                                                    $$
    "};"
    where
    r' = mkExpr d (CtxSetR e ctx) r EVal
    mtch = mkMatch (mkPatExpr d (CtxSetL e ctx) l EVal) vars "return None"
    expr_vardecls = exprVarDecls d (CtxSetL e ctx) l
    varnames = map (pp . name) expr_vardecls
    vars = tuple varnames
    vardecls = (tuple $ map ("ref" <+>) varnames) <> ":" <+> (tuple $ map (mkType . varType d) expr_vardecls)
mkAssignFilter _ _ e = error $ "Compile.mkAssignFilter: unexpected expression " ++ show e

mkCondFilter :: (?statics::Statics) => DatalogProgram -> ECtx -> Expr -> Doc
mkCondFilter d ctx e =
    "if !" <> mkExpr d ctx e EVal <+> "{return None;};"

-- Generate the ffun field of `XFormArrangement::Join/Semijoin/Aggregate`.
-- This field is used if input to the operator is an arranged relation.
mkFFun :: (?cfg::Config, ?statics::Statics) => DatalogProgram -> Rule -> [Int] -> CompilerMonad Doc
mkFFun _ Rule{} [] = return "None"
mkFFun d rl@Rule{..} input_filters = do
   open <- openAtom d vALUE_VAR rl 0 (rhsAtom $ ruleRHS !! 0) ("return false")
   let checks = hsep $ punctuate " &&"
                $ map (\i -> mkExpr d (CtxRuleRCond rl i) (rhsExpr $ ruleRHS !! i) EVal) input_filters
   return $ "Some(&{fn __f(" <> vALUE_VAR <> ": &DDValue) -> bool" $$
            (braces' $ open $$ checks)                             $$
            "    __f"                                              $$
            "})"

-- Compile XForm::Join or XForm::Semijoin
-- Returns generated xform and index of the last RHS term consumed by
-- the XForm
mkJoin :: (?cfg::Config, ?statics::Statics) => DatalogProgram -> [Int] -> Bool -> Atom -> Rule -> Int -> CompilerMonad Doc
mkJoin d input_filters input_val atom rl@Rule{..} join_idx = do
    -- Build arrangement to join with
    let ctx = CtxRuleRAtom rl join_idx
    let (arr, _) = normalizeArrangement d ctx $ atomVal atom
    -- If the operator does not introduce new variables then it's a semijoin
    let semijoin = null $ ruleRHSNewVars d rl join_idx
    semi_arr_idx <- getSemijoinArrangement (atomRelation atom) arr
    join_arr_idx <- getJoinArrangement (atomRelation atom) arr
    -- Treat semijoin as normal join if we only have a map arrangement for it
    let (aid, is_semi) = if semijoin
                            then maybe (fromJust join_arr_idx, False) (,True) semi_arr_idx
                            else (fromJust join_arr_idx, False)
    -- Variables from previous terms that will be used in terms
    -- following the join.
    let post_join_vars = (rhsVarsAfter d rl (join_idx - 1)) `intersect`
                         (rhsVarsAfter d rl join_idx)
    -- Open input Value or tuple.  We may need to do this twice: once to filter
    -- input before join and once to filter output of the join.
    let open_input v = if input_val
                          then openAtom d v rl 0 (rhsAtom $ ruleRHS !! 0) "return None"
                          else openTuple d v post_join_vars
    -- Filter inputs using 'input_filters'
    ffun <- mkFFun d rl input_filters

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
    open <- if is_semi
               then open_input vALUE_VAR1
               else liftM2 ($$) (open_input vALUE_VAR1)
                           (openAtom d vALUE_VAR2 rl join_idx (atom{atomVal = simplify $ atomVal atom}) "return None")
    let filters = mkFilters d rl join_idx
        last_idx = join_idx + length filters
    -- If we're at the end of the rule, generate head atom; otherwise
    -- return all live variables in a tuple
    (ret, next) <- if last_idx == length ruleRHS - 1
        then (, "None") <$> mkValue' d (CtxRuleL rl 0) (atomVal $ head $ ruleLHS)
        else do ret <- mkVarsTupleValue d $ map (, EReference) $ rhsVarsAfter d rl last_idx
                next <- compileRule d rl last_idx False
                return (ret, next)
    let jfun = braces' $ open                     $$
                         vcat filters             $$
                         "Some" <> parens ret
    return $
        if is_semi
           then "XFormArrangement::Semijoin{"                                                                                   $$
                "    description:" <+> (pp $ show $ show $ rulePPPrefix rl $ join_idx + 1) <> ".to_string(),"                   $$
                "    ffun:" <+> ffun <> ","                                                                                     $$
                "    arrangement: (" <> relId (atomRelation atom) <> "," <> pp aid <> "),"                                      $$
                "    jfun: &{fn __f(_: &DDValue ," <> vALUE_VAR1 <> ": &DDValue,_" <> vALUE_VAR2 <> ": &()) -> Option<DDValue>"       $$
                nest' jfun                                                                                                      $$
                "    __f},"                                                                                                     $$
                "    next: Box::new(" <> next  <> ")"                                                                           $$
                "}"
           else "XFormArrangement::Join{"                                                                                       $$
                "    description:" <+> (pp $ show $ show $ rulePPPrefix rl $ join_idx + 1) <> ".to_string(),"                   $$
                "    ffun:" <+> ffun <> ","                                                                                     $$
                "    arrangement: (" <> relId (atomRelation atom) <> "," <> pp aid <> "),"                                      $$
                "    jfun: &{fn __f(_: &DDValue ," <> vALUE_VAR1 <> ": &DDValue," <> vALUE_VAR2 <> ": &DDValue) -> Option<DDValue>"     $$
                nest' jfun                                                                                                      $$
                "    __f},"                                                                                                     $$
                "    next: Box::new(" <> next <> ")"                                                                            $$
                "}"

-- Compile XForm::Antijoin
mkAntijoin :: (?cfg::Config, ?statics::Statics) => DatalogProgram -> [Int] -> Bool -> Atom -> Rule -> Int -> CompilerMonad Doc
mkAntijoin d input_filters input_val Atom{..} rl@Rule{..} ajoin_idx = do
    -- create arrangement to anti-join with
    let ctx = CtxRuleRAtom rl ajoin_idx
    let (arr, _) = normalizeArrangement d ctx atomVal
    -- Filter inputs using 'input_filters'
    ffun <- mkFFun d rl input_filters
    aid <- fromJust <$> getAntijoinArrangement atomRelation arr
    next <- compileRule d rl ajoin_idx input_val
    return $ "XFormArrangement::Antijoin {"                                                                   $$
             "    description:" <+> (pp $ show $ show $ rulePPPrefix rl $ ajoin_idx + 1) <> ".to_string(),"   $$
             "    ffun:" <+> ffun <> ","                                                                      $$
             "    arrangement: (" <> relId atomRelation <> "," <> pp aid <> "),"                              $$
             "    next: Box::new(" <> next <> ")"                                                             $$
             "}"

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
                fs' <- mapM (\(n,e') -> (n,) <$> rename (CtxStruct e ctx n) e') exprStructFields
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
                                (map (\a -> (name a, if name a == f then e' else ePHolder)) $ consArgs cons)
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
mkHead :: (?cfg::Config, ?statics::Statics) => DatalogProgram -> Doc -> Rule -> CompilerMonad Doc
mkHead d prefix rl = do
    v <- mkValue' d (CtxRuleL rl 0) (atomVal $ head $ ruleLHS rl)
    let fmfun = braces' $ prefix $$
                          "Some" <> parens v
    return $
        "XFormCollection::FilterMap{"                                                               $$
        "    description:" <+> (pp $ show $ show $ "head of" <+> pp rl) <+> ".to_string(),"         $$
        nest' ("fmfun: &{fn __f(" <> vALUE_VAR <> ": DDValue) -> Option<DDValue>" $$ fmfun $$ "__f},")  $$
        "    next: Box::new(None)"                                                                  $$
        "}"

-- Variables in the RHS of the rule declared before or in i'th term
-- and used after the term.
rhsVarsAfter :: DatalogProgram -> Rule -> Int -> [Var]
rhsVarsAfter d rl i =
    case ruleRHS rl !! i of
         -- Inspect operators cannot change the collection it inspects. No variables are dropped.
         RHSInspect _ -> rhsVarsAfter d rl (i-1)
         _            -> filter (\f -> -- If an aggregation occurs in the remaining part of the rule,
                                       -- keep all variables to preserve multiset semantics
                                       if any rhsIsAggregate $ drop (i+1) (ruleRHS rl)
                                          then True
                                          else elem f $ (ruleLHSVars d rl) `union`
                                                        (concatMap (ruleRHSTermVars d rl) [i+1..length (ruleRHS rl) - 1]))
                                $ ruleRHSVars d rl (i+1)

mkProg :: [ProgNode] -> CompilerMonad Doc
mkProg nodes = do
    let rels = vcat $
               map (\ProgRel{..} -> "let" <+> rname prelName <+> "=" <+> prelCode <> ";")
                   (concatMap nodeRels nodes)
    let facts = concatMap prelFacts $ concatMap nodeRels nodes
    let pnodes = map mkNode nodes
        prog = "Program {"                                      $$
               "    nodes: vec!["                               $$
               (nest' $ nest' $ vcat $ punctuate comma pnodes)  $$
               "    ],"                                         $$
               "    init_data: vec!["                           $$
               (nest' $ nest' $ vcat $ punctuate comma facts)   $$
               "    ]"                                          $$
               "}"
    return $
        "pub fn prog(__update_cb: Box<dyn CBFn>) -> Program {"  $$
        (nest' $ rels $$ prog)                                                 $$
        "}"

mkNode :: ProgNode -> Doc
mkNode (RelNode (ProgRel rel _ _)) =
    "ProgNode::Rel{rel:" <+> rname rel <> "}"
mkNode (SCCNode rels) =
    "ProgNode::SCC{rels: vec![" <>
    (commaSep $ map (\RecProgRel{..} ->
                      "RecursiveRelation{rel: " <> (rname $ prelName rprelRel) <>
                      ", distinct: " <> (if rprelDistinct then "true" else "false") <> "}") rels) <> "]}"
mkNode (ApplyNode fun) =
    "ProgNode::Apply{tfun:" <+> fun <> "}"

mkArrangement :: (?cfg::Config, ?statics::Statics) => DatalogProgram -> Relation -> Arrangement -> CompilerMonad Doc
mkArrangement d rel ArrangementMap{..} = do
    filter_key <- mkArrangementKey d rel arngPattern
    let afun = braces' $
               "let __cloned =" <+> vALUE_VAR <> ".clone();"                                                $$
               filter_key <> ".map(|x|(x,__cloned))"
    return $
        "Arrangement::Map{"                                                                                       $$
        "   name: r###\"" <> pp arngPattern <> " /*join*/\"###.to_string(),"                                      $$
        (nest' $ "afun: &{fn __f(" <> vALUE_VAR <> ": DDValue) -> Option<(DDValue,DDValue)>" $$ afun $$ "__f},")  $$
        "    queryable:" <+> (if null arngIndexes then "false" else "true")                                       $$
        "}"

mkArrangement d rel ArrangementSet{..} = do
    filter_key <- mkArrangementKey d rel arngPattern
    let fmfun = braces' filter_key
    -- Arrangement contains distinct elements by construction and does
    -- not require expensive `distinct()` or `distinct_total()` applied to
    -- it if it is defined over all fields of a distinct relation (i.e.,
    -- the pattern expression does not contain placeholders).
    let distinct_by_construction = relIsDistinct d rel && (not $ exprContainsPHolders arngPattern)
    return $
        "Arrangement::Set{"                                                                                                          $$
        "    name: r###\"" <> pp arngPattern <> " /*" <> (if arngDistinct then "antijoin" else "semijoin") <> "*/\"###.to_string()," $$
        (nest' $ "fmfun: &{fn __f(" <> vALUE_VAR <> ": DDValue) -> Option<DDValue>" $$ fmfun $$ "__f},")                             $$
        "    distinct:" <+> (if arngDistinct && not distinct_by_construction then "true" else "false")                               $$
        "}"

-- Generate part of the arrangement computation that filters inputs and computes the key part of the
-- arrangement.
mkArrangementKey :: (?cfg::Config, ?statics::Statics) => DatalogProgram -> Relation -> Expr -> CompilerMonad Doc
mkArrangementKey d rel pattern = do
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
    let t = relType rel
    -- Order variables by their integer value: '_0', '_1', ...
    patvars <- mkFieldTupleValue d
               $ sortBy (\f1 f2 -> compare ((read $ tail $ name f1)::Int) (read $ tail $ name f2))
               $ getvars t pattern
    constructor <- mkValConstructorName' d t
    let res = "Some(" <> patvars <> ")"
    -- Manufacture fake context to make sure 'pattern' has a known type
    -- in 'mkPatExpr'.
    let pattern_ctx = CtxTyped (ETyped nopos pattern $ relType rel) CtxTop
    let mtch = mkMatch (mkPatExpr d pattern_ctx pattern EReference) res "None"
    return $ "match" <+> "unsafe {" <+> constructor <> "::from_ddvalue(" <> vALUE_VAR <> ") }.0 {"  $$
             nest' mtch                                                                     $$
             "}"

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
--
-- Assumes:
-- * the expression being matched is of kind EVal
-- * if 'kind == EVal', the pattern does not contain any 'ERef's
mkPatExpr :: (?statics::Statics) => DatalogProgram -> ECtx -> Expr -> EKind -> Match
mkPatExpr d ctx (E e) kind = evalState (mkPatExpr' d EVal ctx e kind) 0

allocPatVar :: State Int Doc
allocPatVar = do
    i <- get
    put $ i+1
    return $ "_" <> pp i <> "_"

-- Computes prefix to be attached to variables in pattern, given
-- the kind of relation being matched and the kind we want for the
-- new variable.
varprefix :: EKind -> EKind -> Doc
varprefix EReference EReference = empty
varprefix _          EReference = "ref"
varprefix EVal       EVal       = empty
varprefix inkind     varkind    = error $ "varprefix " ++ show inkind ++ " " ++ show varkind

mkPatExpr' :: (?statics::Statics) => DatalogProgram -> EKind -> ECtx -> ENode -> EKind -> State Int Match
mkPatExpr' _ inkind _   EVar{..}        varkind   = return $ Match (varprefix inkind varkind <+> pp exprVar) empty []
mkPatExpr' _ inkind _   EVarDecl{..}    varkind   = return $ Match (varprefix inkind varkind <+> pp exprVName) empty []
mkPatExpr' _ _      _   (EBool _ True)  _         = return $ Match "true" empty []
mkPatExpr' _ _      _   (EBool _ False) _         = return $ Match "false" empty []
mkPatExpr' _ inkind _   EString{..}     varkind   = do
    vname <- allocPatVar
    return $ Match (varprefix inkind varkind <+> vname) (vname <> ".as_str() ==" <+> "\"" <> pp exprString <> "\"") []
mkPatExpr' d inkind ctx e@EStruct{..}   varkind   = do
    fields <- mapM (\(f, E e') -> (f,) <$> mkPatExpr' d inkind (CtxStruct e ctx f) e' varkind) exprStructFields
    let t = consType d exprConstructor
        struct_name = name t
        pat = mkConstructorName struct_name (fromJust $ tdefType t) exprConstructor <>
              (braces $ hsep $ punctuate comma $ map (\(fname, m) -> pp fname <> ":" <+> mPattern m) fields)
        cond = hsep $ intersperse "&&" $ filter (/= empty)
                                       $ map (\(_,m) -> mCond m) fields
        subpatterns = concatMap (\(_, m) -> mSubpatterns m) fields
    return $ Match pat cond subpatterns
mkPatExpr' d inkind ctx e@ETuple{..}    varkind   = do
    fields <- mapIdxM (\(E f) i -> mkPatExpr' d inkind (CtxTuple e ctx i) f varkind) exprTupleFields
    let pat = tupleStruct $ map (pp . mPattern) fields
        cond = hsep $ intersperse "&&" $ filter (/= empty)
                                       $ map (pp . mCond) fields
        subpatterns = concatMap mSubpatterns fields
    return $ Match pat cond subpatterns
mkPatExpr' _ _      _   EPHolder{}      _         = return $ Match "_" empty []
mkPatExpr' d inkind ctx e@ETyped{..}    varkind   = mkPatExpr' d inkind (CtxTyped e ctx) (enode exprExpr) varkind
mkPatExpr' d inkind ctx e@EBinding{..}  varkind   = do
    -- Rust does not allow variable declarations inside bindings.
    -- To bypass this, we convert the bind pattern into a nested pattern.
    -- Unfortunately, this means that binding can only be used when there
    -- is a single pattern to match against, e.g., in an atom or an assignment
    -- clause in a rule.
    Match pat cond subpatterns <- mkPatExpr' d inkind (CtxBinding e ctx) (enode exprPattern) varkind
    return $ Match (varprefix inkind varkind <+> pp exprVar) empty ((pp exprVar, Match pat cond []):subpatterns)
mkPatExpr' d inkind ctx e@ERef{..}      varkind   = do
    vname <- allocPatVar
    subpattern <- mkPatExpr' d EReference {- deref() returns reference -}
                             (CtxRef e ctx) (enode exprPattern) varkind
    return $ Match (varprefix inkind varkind <+> vname)
                   empty [("(" <> deref (pp vname, varkind, undefined) <> ").deref()" , subpattern)]
mkPatExpr' d inkind ctx e               varkind   = do
    vname <- allocPatVar
    return $ Match (varprefix inkind varkind <+> vname)
                   (deref (vname, varkind, undefined) <+> "==" <+> mkExpr d ctx (E e) EVal)
                   []

-- Convert Datalog expression to Rust.
-- We generate the code so that all variables are references and must
-- be dereferenced before use or cloned when passed to a constructor,
-- assigned to another variable or returned.
mkExpr :: (?statics::Statics) => DatalogProgram -> ECtx -> Expr -> EKind -> Doc
mkExpr d ctx e k =
    case k of
         EVal       -> val e'
         EReference -> ref e'
         ELVal      -> lval e'
         ENoReturn  -> sel1 e'
    where
    e' = exprFoldCtx (mkExpr_ d) ctx e

mkExpr_ :: (?statics::Statics) => DatalogProgram -> ECtx -> ExprNode (Doc, EKind, ENode) -> (Doc, EKind, ENode)
mkExpr_ d ctx e = (t', k', e')
    where (t', k') = mkExpr' d ctx e
          e' = exprMap (E . sel3) e

-- Compiled expressions are represented as '(Doc, EKind)' tuple, where
-- the second components is the kind of the compiled representation
mkExpr' :: (?statics::Statics) => DatalogProgram -> ECtx -> ExprNode (Doc, EKind, ENode) -> (Doc, EKind)

-- Expression is compiled to a lazy static.
mkExpr' d ctx e | isJust static_idx = (parens $ "&*__STATIC_" <> pp (fromJust static_idx), EReference)
    where
    e' = exprMap (E . sel3) e
    static_idx = lookupStatic d (E e') ctx ?statics

-- All variables are references
mkExpr' _ _ EVar{..}    = (pp exprVar, EReference)

-- Function arguments are passed as read-only or mutable references
-- Functions return real values unless they are labeled return-by-reference.
mkExpr' d ctx e@(EApply{..})  =
    (mkFuncName d f <> (parens $ commaSep
                        $ map (\(a, mut) -> if mut then mutref a else ref a)
                        $ zip exprArgs (map argMut $ funcArgs f)), kind)
    where
    f = applyExprGetFunc d ctx $ exprMap (E . sel3) e
    kind = if funcGetReturnByRefAttr d f then EReference else EVal

-- Field access automatically dereferences subexpression
mkExpr' _ _ EField{..} = (sel1 exprStruct <> "." <> pp exprField, ELVal)
mkExpr' _ _ ETupField{..} = ("(" <> sel1 exprTuple <> "." <> pp exprTupField <> ")", ELVal)
mkExpr' _ _ (EBool _ True) = ("true", EVal)
mkExpr' _ _ (EBool _ False) = ("false", EVal)
mkExpr' _ _ EInt{..} = (mkInt exprIVal, EVal)
mkExpr' _ _ EDouble{..} = ("OrderedFloat::<f64>" <> (parens $ pp exprDVal), EVal)
mkExpr' _ _ EFloat{..} = ("OrderedFloat::<f32>" <> (parens $ pp exprFVal), EVal)
mkExpr' _ _ EString{..} = ("String::from(r###\"" <> pp exprString <> "\"###)", EVal)
mkExpr' _ _ EBit{..} | exprWidth <= 128 = (parens $ pp exprIVal <+> "as" <+> mkType (tBit exprWidth), EVal)
                     | otherwise        = ("Uint::parse_bytes(b\"" <> pp exprIVal <> "\", 10)", EVal)
mkExpr' _ _ ESigned{..} | exprWidth <= 128 = (parens $ pp exprIVal <+> "as" <+> mkType (tSigned exprWidth), EVal)
                        | otherwise        = ("Int::parse_bytes(b\"" <> pp exprIVal <> "\", 10)", EVal)

-- Struct fields must be values
mkExpr' d ctx EStruct{..} | ctxInSetL ctx
                          = (tname <> fieldlvals, ELVal)
                          | isstruct
                          = (parens $ tname <> fieldvals, EVal)
                          | otherwise
                          = (parens $ tname <> "::" <> rname exprConstructor <> fieldvals, EVal)
    where fieldvals  = braces $ commaSep $ map (\(fname, v) -> pp fname <> ":" <+> val v) exprStructFields
          fieldlvals = braces $ commaSep $ map (\(fname, v) -> pp fname <> ":" <+> lval v) exprStructFields
          tdef = consType d exprConstructor
          isstruct = isStructType $ fromJust $ tdefType tdef
          tname = rname $ name tdef

-- Tuple fields must be values
mkExpr' _ ctx ETuple{..} | ctxInSetL ctx
                         = (tupleStruct $ map lval exprTupleFields, ELVal)
                         | otherwise
                         = (tupleStruct $ map val exprTupleFields, EVal)

mkExpr' d ctx e@ESlice{..} = (mkSlice (val exprOp, w) exprH exprL, EVal)
    where
    e' = exprMap (E . sel3) e
    TBit _ w = exprType' d (CtxSlice e' ctx) $ E $ sel3 exprOp

-- Match expression is a reference
mkExpr' d ctx e@EMatch{..} = (doc, EVal)
    where
    e' = exprMap (E . sel3) e
    m = {-if exprIsVarOrFieldLVal d (CtxMatchExpr (exprMap (E . sel3) e) ctx) (E $ sel3 exprMatchExpr)
           then mutref exprMatchExpr
           else ref exprMatchExpr -}
        deref exprMatchExpr
    doc = ("match" <+> m <+> "{")
          $$
          (nest' $ vcat $ punctuate comma cases)
          $$
          "}"
    cases = mapIdx (\(c,v) idx -> let Match pat cond [] = mkPatExpr d (CtxMatchPat e' ctx idx) (E $ sel3 c) EReference
                                      cond' = if cond == empty then empty else ("if" <+> cond) in
                                  pat <+> cond' <+> "=>" <+> val v) exprCases

-- Variables are mutable references
mkExpr' _ _ EVarDecl{..} = ("ref mut" <+> pp exprVName, ELVal)

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
    -- If collection iterates by value, convert value into reference.
    opt_ref = if (snd . typeIterType d) $ exprType d (CtxForIter e' ctx) (E $ sel3 exprIter)
                 then empty
                 else "ref"
    doc = ("for" <+> opt_ref <+> pp exprLoopVar <+> "in" <+> sel1 exprIter <> ".iter() {") $$
          (nest' $ val exprBody)                                                   $$
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
        then ":" <+> (mkType $ exprType d (CtxSetL e' ctx) $ E $ sel3 exprLVal)
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
                            EString _ s -> "string_append_str(" <> e1 <> ", r###\"" <> pp s <> "\"###)"
                            _           -> "string_append(" <> e1 <> "," <+> ref exprRight <> ")"
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
                    -> "OrderedFloat" <> (parens $ "-" <> arg <> ".into_inner()")
             UMinus -> mkTruncate (parens $ "-" <> arg) t
mkExpr' _ _ EPHolder{} = ("_", ELVal)

-- * LHS of assignment will get type ascription elsewhere (see 'ESet').
-- * 'continue', 'break', 'return' do not need type ascriptions.
-- * Do type coercion for integer constants
-- * Otherwise, introduce an intermediate variable with explicit type
mkExpr' d ctx ETyped{..} | ctxInSetL ctx = (e', categ)
                         | isint && toDouble
                                         = (parens $ e' <> ".to_double()", categ)
                         | isint && toFloat
                                         = (parens $ e' <+> ".to_float()", categ)
                         | isint         = (parens $ e' <+> "as" <+> mkType exprTSpec, categ)
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
                      = (parens $ val exprExpr <+> "as" <+> mkType exprTSpec, EVal)
                      | bothIntegers && narrow_from && narrow_to
                      -- apply lossy type conversion between primitive Rust types;
                      -- truncate the result if needed
                      = (mkTruncate (parens $ val exprExpr <+> "as" <+> mkType exprTSpec) to_type,
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
                      = (parens $ tto <> "::from_" <> tfrom <> "(" <> val exprExpr <> ")", EVal)

                      -- convert long integers to FP
                      | isFloat d to_type && (tfrom == "Int" || tfrom == "Uint")
                      = (parens $ "(" <> val exprExpr <> ").to_float()", EVal)
                      | isDouble d to_type && (tfrom == "Int" || tfrom == "Uint")
                      = (parens $ "(" <> val exprExpr <> ").to_double()", EVal)
                      -- convert integer to float
                      | isFloat d to_type && isInteger d from_type
                      = (parens $ "OrderedFloat(" <> val exprExpr <> " as f32)", EVal)
                      | isDouble d to_type && isInteger d from_type
                      = (parens $ "OrderedFloat(" <> val exprExpr <> " as f64)", EVal)
                      -- convert some FP to double
                      | isDouble d to_type && isFP d from_type
                      = (parens $ "OrderedFloat::<f64>::from((*" <> val exprExpr <> ") as f64)", EVal)
                      -- convert some FP to float
                      | isFloat d to_type && isFP d from_type
                      = (parens $ "OrderedFloat::<f32>::from((*" <> val exprExpr <> ") as f32)", EVal)
                      | otherwise
                      = error $ "Unexpected cast from " ++ (show from_type) ++ " to " ++ (show to_type)
    where
    e' = sel3 exprExpr
    from_type = exprType' d (CtxAs e' ctx) $ E e'
    to_type   = typ' d exprTSpec
    tfrom = mkType from_type  -- Rust type
    tto   = mkType to_type    -- Rust type
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

mkFuncName :: DatalogProgram -> Function -> Doc
mkFuncName d f | length namesakes == 1 = rname $ name f
               | otherwise =
    (rname $ name f) <> "_" <> targ0 <> "_" <> pp (length $ funcArgs f)
    where
    arg0 = funcArgs f !! 0
    targ0 = mkValConstructorName d $ typ arg0
    namesakes = progFunctions d M.! (name f)

mkType :: (WithType a) => a -> Doc
mkType x = mkType' empty $ typ x

-- DDlog user-defined types are declared in `scope`.
mkTypeScoped :: (WithType a) => Doc -> a -> Doc
mkTypeScoped scope x = mkType' scope $ typ x

mkType' :: Doc -> Type -> Doc
mkType' _       TBool{}                    = "bool"
mkType' _       TInt{}                     = "Int"
mkType' _       TString{}                  = "String"
mkType' _       TBit{..} | typeWidth <= 8  = "u8"
                         | typeWidth <= 16 = "u16"
                         | typeWidth <= 32 = "u32"
                         | typeWidth <= 64 = "u64"
                         | typeWidth <= 128= "u128"
                         | otherwise       = "Uint"
mkType' _       t@TSigned{..} | typeWidth == 8  = "i8"
                              | typeWidth == 16 = "i16"
                              | typeWidth == 32 = "i32"
                              | typeWidth == 64 = "i64"
                              | typeWidth == 128= "i128"
                              | otherwise       = errorWithoutStackTrace $ "Only machine widths (8/16/32/64/128) supported: " ++ show t
mkType' _       TDouble{}                  = "OrderedFloat<f64>"
mkType' _       TFloat{}                   = "OrderedFloat<f32>"
mkType' scope   TTuple{..} | length typeTupArgs <= 12
                                  = parens $ commaSep $ map (mkType' scope) typeTupArgs
mkType' scope   TTuple{..}        = tupleTypeName typeTupArgs <>
                                     if null typeTupArgs
                                        then empty
                                        else "<" <> (commaSep $ map (mkType' scope) typeTupArgs) <> ">"
mkType' scope   TUser{..}         = scope <> rname typeName <>
                                     if null typeArgs
                                        then empty
                                        else "<" <> (commaSep $ map (mkType' scope) typeArgs) <> ">"
mkType' scope   TOpaque{..}       = scope <> rname typeName <>
                                     if null typeArgs
                                        then empty
                                        else "<" <> (commaSep $ map (mkType' scope) typeArgs) <> ">"
mkType' _       TVar{..}          = pp tvarName
mkType' _       t                 = error $ "Compile.mkType' " ++ show t

smallInt :: DatalogProgram -> Type -> Bool
smallInt d t = ((isSigned d t || isBit d t) && (typeWidth (typ' d t) <= 128))

mkBinOp :: DatalogProgram -> BOp -> (Doc, Type) -> (Doc, Type) -> Doc
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
               -> "OrderedFloat" <> (parens $ e1 <> ".into_inner()" <+> "/" <+> e2 <> ".into_inner()")
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
               -> "OrderedFloat" <> (parens $ e1 <> ".into_inner()" <+> "+" <+> e2 <> ".into_inner()")
               | otherwise
               -> parens $ e1 <+> "+" <+> e2
        Minus  | smallInt d t1
               -> parens $ e1 <> ".wrapping_sub(" <> e2 <> ")"
               | isFP d t1
               -> "OrderedFloat" <> (parens $ e1 <> ".into_inner()"<+> "-" <+> e2 <> ".into_inner()")
               | otherwise
               -> parens $ e1 <+> "-" <+> e2
        Times  | smallInt d t1
               -> parens $ e1 <> ".wrapping_mul(" <> e2 <> ")"
               | isFP d t1
               -> "OrderedFloat" <> (parens $ e1 <> ".into_inner()" <+> "*" <+> e2 <> ".into_inner()")
               | otherwise
               -> parens $ e1 <+> "*" <+> e2
        Concat -> mkConcat (e1, typeWidth t1) (e2, typeWidth t2)

-- These operators require truncating the output value to correct
-- width.
bopsRequireTruncation :: [BOp]
bopsRequireTruncation = [ShiftL, Plus, Minus, Times]

-- Produce code to cast bitvector to a different-width BV.
-- The value of 'e' must fit in the new width.
castBV :: Doc -> Int -> Int -> Doc
castBV e w1 w2 | t1 == t2
               = e
               | w1 <= 128 && w2 <= 128
               = parens $ e <+> "as" <+> t2
               | w2 > 128
               = "Uint::from_" <> t1 <> "(" <> e <> ")"
               | otherwise
               = e <> "to_" <> t2 <> "().unwrap()"
    where
    t1 = mkType $ tBit w1
    t2 = mkType $ tBit w2

-- Concatenate two bitvectors
mkConcat :: (Doc, Int) -> (Doc, Int) -> Doc
mkConcat (e1, w1) (e2, w2) =
    parens $ e1'' <+> "|" <+> e2'
    where
    e1' = castBV e1 w1 (w1+w2)
    e2' = castBV e2 w2 (w1+w2)
    e1'' = parens $ e1' <+> "<<" <+> pp w2

mkSlice :: (Doc, Int) -> Int -> Int -> Doc
mkSlice (e, w) h l = castBV res w (h - l + 1)
    where
    res = parens $ (parens $ e <+> ">>" <+> pp l) <+> "&" <+> mask
    mask = mkBVMask (h - l + 1)

mkBVMask :: Int -> Doc
mkBVMask w | w > 128   = "Uint::parse_bytes(b\"" <> m <> "\", 16)"
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
        = "Int::from_u128(" <> pp v <> ")"
        | v <= (toInteger (maxBound::Int128))  && v >= (toInteger (minBound::Int128))
        = "Int::from_i128(" <> pp v <> ")"
        | otherwise
        = "Int::parse_bytes(b\"" <> pp v <> "\", 10)"

-- Compute the atom or tuple of variables after the prefix of length n.
-- If this is the last term, then it is an expression of the LHS variables for each head
-- of the rule.
-- If this is the first term, then it's the atom of the RHSLiteral.
-- Otherwise, this is the variables from rhsVarsAfter converted into an ETuple expression.
recordAfterPrefix :: DatalogProgram -> Rule -> Int -> [Expr]
recordAfterPrefix d rl i =
  if i == length (ruleRHS rl) - 1
     then  map atomVal $ ruleLHS rl
     else if i == 0
             then [eVar $ exprVar $ enode $ atomVal $ rhsAtom $ head $ ruleRHS rl ]
             else [eTuple $ map (eVar . name) (rhsVarsAfter d rl i) ]
