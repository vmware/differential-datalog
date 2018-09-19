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

{-# LANGUAGE RecordWildCards, FlexibleContexts, LambdaCase, TupleSections, OverloadedStrings, TemplateHaskell, QuasiQuotes #-}

{- |
Module     : Compile
Description: Compile 'DatalogProgram' to Rust.  See program.rs for corresponding Rust declarations.
-}

module Language.DifferentialDatalog.Compile (
    compile,
    isStructType,
    mkValConstructorName',
    mkConstructorName,
    mkType
) where

import Control.Monad.State
import Text.PrettyPrint
import Data.Tuple
import Data.Tuple.Select
import Data.Maybe
import Data.List
import Data.Int
import Data.Word
import Data.Bits
import Data.FileEmbed
import Data.String.Utils
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

import Language.DifferentialDatalog.PP
import Language.DifferentialDatalog.Name
import Language.DifferentialDatalog.Pos
import Language.DifferentialDatalog.Ops
import Language.DifferentialDatalog.Util
import Language.DifferentialDatalog.Syntax
import Language.DifferentialDatalog.NS
import Language.DifferentialDatalog.Expr
import Language.DifferentialDatalog.DatalogProgram
import Language.DifferentialDatalog.Module
import Language.DifferentialDatalog.ECtx
import Language.DifferentialDatalog.Type
import Language.DifferentialDatalog.Rule
import qualified Language.DifferentialDatalog.FFI as FFI

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

-- Rust imports
header :: String -> Doc
header specname = pp $ replace "datalog_example" specname $ BS.unpack $ $(embedFile "rust/template/lib.rs")

ffiheader :: String -> Doc
ffiheader specname = pp $ replace "datalog_example" specname $ BS.unpack $ $(embedFile "rust/template/ffi.rs")

cheader :: String -> Doc
cheader specname = pp $ replace "datalog_example" specname $ BS.unpack $ $(embedFile "rust/template/ffi.h")

--cargoFile = BS.unpack $(embedFile "rust/template/Cargo.toml")

templateFiles :: String -> [(String, String)]
templateFiles specname =
    map (mapSnd (BS.unpack)) $
        [ (joinPath [specname, "Cargo.toml"]  , $(embedFile "rust/template/Cargo.toml"))
        , (joinPath [specname, "main.rs"]     , $(embedFile "rust/template/main.rs"))
        , (joinPath [specname, "ffi_test.rs"] , $(embedFile "rust/template/ffi_test.rs"))
        , (joinPath [specname, "valmap.rs"]   , $(embedFile "rust/template/valmap.rs"))
        ]

-- Rust differential_datalog library
rustLibFiles :: [(String, String)]
rustLibFiles =
    map (mapSnd (BS.unpack)) $
        [ ("differential_datalog/Cargo.toml"  , $(embedFile "rust/differential_datalog/Cargo.toml"))
        , ("differential_datalog/int.rs"      , $(embedFile "rust/differential_datalog/int.rs"))
        , ("differential_datalog/uint.rs"     , $(embedFile "rust/differential_datalog/uint.rs"))
        , ("differential_datalog/variable.rs" , $(embedFile "rust/differential_datalog/variable.rs"))
        , ("differential_datalog/profile.rs"  , $(embedFile "rust/differential_datalog/profile.rs"))
        , ("differential_datalog/program.rs"  , $(embedFile "rust/differential_datalog/program.rs"))
        , ("differential_datalog/lib.rs"      , $(embedFile "rust/differential_datalog/lib.rs"))
        , ("differential_datalog/test.rs"     , $(embedFile "rust/differential_datalog/test.rs"))
        , ("cmd_parser/Cargo.toml"            , $(embedFile "rust/cmd_parser/Cargo.toml"))
        , ("cmd_parser/lib.rs"                , $(embedFile "rust/cmd_parser/lib.rs"))
        , ("cmd_parser/parse.rs"              , $(embedFile "rust/cmd_parser/parse.rs"))
        , ("cmd_parser/from_record.rs"        , $(embedFile "rust/cmd_parser/from_record.rs"))
        ]


{- The following types model corresponding entities in program.rs -}

-- Arrangement is uniquely identified by its _normalized_ pattern
-- expression.  The normalized pattern only contains variables
-- involved in the arrangement key, with normalized names (so that
-- two patterns isomorphic modulo variable names have the same
-- normalized representation) and only expand constructors that
-- either contain a key variable or are non-unique.
data Arrangement = Arrangement {
    arngPattern :: Expr
} deriving Eq

-- Rust expression kind
data EKind = EVal   -- normal value
           | ELVal  -- l-value that can be written to or moved
           | ERef   -- reference (mutable or immutable)
           deriving (Eq)

-- convert any expression into reference
ref :: (Doc, EKind, ENode) -> Doc
ref (x, ERef, _)  = x
ref (x, _, _)     = parens $ "&" <> x

-- dereference expression if it is a reference; leave it alone
-- otherwise
deref :: (Doc, EKind, ENode) -> Doc
deref (x, ERef, _) = parens $ "*" <> x
deref (x, _, _)    = x

-- convert any expression into mutable reference
mutref :: (Doc, EKind, ENode) -> Doc
mutref (x, ERef, _)  = x
mutref (x, _, _)     = parens $ "&mut" <> x

-- convert any expression to EVal by cloning it if necessary
val :: (Doc, EKind, ENode) -> Doc
val (x, EVal, _) = x
val (x, _, _)    = x <> ".clone()"

-- convert expression to l-value
lval :: (Doc, EKind, ENode) -> Doc
lval (x, ELVal, _) = x
-- this can only be mutable reference in a valid program
lval (x, ERef, _)  = parens $ "*" <> x
lval (x, EVal, _)  = error $ "Compile.lval: cannot convert value to l-value: " ++ show x

-- Relation is a function that takes a list of arrangements and produces a Doc containing Rust
-- code for the relation (since we won't know all required arrangements till we finish scanning
-- the program)
type ProgRel = (String, [Doc] -> Doc)

data ProgNode = SCCNode [ProgRel]
              | RelNode ProgRel

nodeRels :: ProgNode -> [ProgRel]
nodeRels (SCCNode rels) = rels
nodeRels (RelNode rel)  = [rel]

{- State accumulated by the compiler as it traverses the program -}
type CompilerMonad = State CompilerState

type ArrId = Int

data CompilerState = CompilerState {
    cTypes        :: S.Set Type,
    cArrangements :: M.Map String [Arrangement]
}

emptyCompilerState :: CompilerState
emptyCompilerState = CompilerState {
    cTypes        = S.empty,
    cArrangements = M.empty
}

mkRelEnum :: DatalogProgram -> Doc
mkRelEnum d =
    "#[derive(Copy,Clone,Debug)]"                                                                               $$
    "pub enum Relations {"                                                                                      $$
    (nest' $ vcat $ punctuate comma $ mapIdx (\rel i -> pp rel <+> "=" <+> pp i) $ M.keys $ progRelations d)    $$
    "}"

relId :: String -> Doc
relId rel = "Relations::" <> pp rel <+> "as RelId"

-- t must be normalized
addType :: Type -> CompilerMonad ()
addType t = modify $ \s -> s{cTypes = S.insert t $ cTypes s}

-- Create a new arrangement or return existing arrangement id
addArrangement :: String -> Arrangement -> CompilerMonad ArrId
addArrangement relname arr = do
    arrs <- gets $ (M.! relname) . cArrangements
    let (arrs', aid) = case findIndex (==arr) arrs of
                            Nothing -> (arrs ++ [arr], length arrs)
                            Just i  -> (arrs, i)
    modify $ \s -> s{cArrangements = M.insert relname arrs' $ cArrangements s}
    return aid

-- Rust does not like parenthesis around singleton tuples
tuple :: [Doc] -> Doc
tuple [x] = x
tuple xs = parens $ hsep $ punctuate comma xs

-- Structs with a single constructor are compiled into Rust structs;
-- structs with multiple constructor are compiled into Rust enums.
isStructType :: Type -> Bool
isStructType TStruct{..} | length typeCons == 1 = True
isStructType TStruct{..}                        = False
isStructType t                                  = error $ "Compile.isStructType " ++ show t

mkConstructorName :: String -> Type -> String -> Doc
mkConstructorName tname t c =
    if isStructType t
       then pp tname
       else pp tname <> "::" <> pp c

-- | Create a compilable Cargo crate.  If the crate already exists, only writes files
-- modified by the recompilation.
--
-- 'specname' - will be used as Cargo package and library names
--
-- 'imports' - user-provided string inserted after the generic header
-- from and before autogenerated code in lib.rs.
--
-- 'dir' - directory for the crate; will be created if does not
-- exists
compile :: DatalogProgram -> String -> String -> FilePath -> IO ()
compile d specname imports dir = do
    let (lib, rust_ffi, c_ffi) = compileLib d specname imports
    -- Create dir if it does not exist.
    createDirectoryIfMissing True dir
    -- Update rustLibFiles if they changed.
    mapM_ (\(path, content) -> do
            let path' = joinPath [dir, path]
            updateFile path' content)
         rustLibFiles
    -- Substitute specname template files; write files if changed.
    mapM_ (\(path, content) -> do
            let path' = joinPath [dir, path]
                content' = replace "datalog_example" specname content
            updateFile path' content')
          $ templateFiles specname
    -- Generate lib.rs file if changed.
    updateFile (joinPath [dir, specname, "lib.rs"]) (render lib)
    -- Generate ffi.rs file if changed.
    updateFile (joinPath [dir, specname, "ffi.rs"]) (render rust_ffi)
    -- Update matching C header file.
    updateFile (joinPath [dir, specname, specname ++ ".h"]) (render c_ffi)
    return ()

-- Replace file content if changed
updateFile :: FilePath -> String -> IO ()
updateFile path content = do
    createDirectoryIfMissing True $ takeDirectory path
    exists <- doesFileExist path
    let tmppath = addExtension path "tmp"
    if exists
       then do
            oldcontent <- readFile path
            when (oldcontent /= content) $ do
                writeFile tmppath content
                renameFile tmppath path
       else writeFile path content

-- | Compile Datalog program into Rust code that creates 'struct Program' representing
-- the program for the Rust Datalog library
compileLib :: DatalogProgram -> String -> String -> (Doc, Doc, Doc)
compileLib d specname imports =
    (header specname      $+$
     pp imports           $+$
     typedefs             $+$
     mkValueFromRecord d' $+$ -- Function to convert cmd_parser::Record to Value
     mkRelEnum d'         $+$ -- Relations enum
     valtype              $+$
     funcs                $+$
     prog
    ,
     ffiheader specname   $+$
     rust_ffi
    ,
     cheader specname     $+$
     c_ffi)
    where
    -- Massage program to Rust-friendly form:
    -- * Rename program entities to Rust-friendly names
    -- * Transform away rules with multiple heads
    -- * Make sure the program has at least one relation
    d' = addDummyRel $ progExpandMultiheadRules $ progRustizeNamespace d
    (rust_ffi, c_ffi) = FFI.mkFFIInterface d'
    -- Compute ordered SCCs of the dependency graph.  These will define the
    -- structure of the program.
    depgraph = progDependencyGraph d'
    sccs = G.topsort' $ G.condensation depgraph
    -- Initialize arrangements map
    arrs = M.fromList $ map ((, []) . snd) $ G.labNodes depgraph
    -- Initialize types
    -- Make sure that empty tuple is always in Value, so it can be
    -- used to implement Value::default()
    types = S.fromList $ (tTuple []) : (map (typeNormalize d' . relType) $ M.elems $ progRelations d')
    -- Compile SCCs
    (prog, cstate) = runState (do nodes <- mapM (compileSCC d' depgraph) sccs
                                  mkProg d' nodes)
                              $ emptyCompilerState{cArrangements = arrs,
                                                   cTypes        = types}
    -- Type declarations
    typedefs = vcat $ map mkTypedef $ M.elems $ progTypedefs d'
    -- Functions
    (fdef, fextern) = partition (isJust . funcDef) $ M.elems $ progFunctions d'
    funcs = vcat $ (map (mkFunc d') fextern ++ map (mkFunc d') fdef)
    -- 'Value' enum type
    valtype = mkValType d' $ cTypes cstate

-- Add dummy relation to the spec if it does not contain any.
-- Otherwise, we have to tediously handle this corner case in various
-- parts of the compiler.
addDummyRel :: DatalogProgram -> DatalogProgram
addDummyRel d | not $ M.null $ progRelations d = d
              | otherwise = d {progRelations = M.singleton "Null" $ Relation nopos True "Null" $ tTuple []}

mkTypedef :: TypeDef -> Doc
mkTypedef tdef@TypeDef{..} =
    case tdefType of
         Just TStruct{..} | length typeCons == 1
                          -> derive                                                                    $$
                             "pub struct" <+> pp tdefName <> targs <+> "{"                             $$
                             (nest' $ vcat $ punctuate comma $ map mkField $ consArgs $ head typeCons) $$
                             "}"                                                                       $$
                             impl_abomonate                                                            $$
                             mkFromRecord tdef                                                         $$
                             display
                          | otherwise
                          -> derive                                                                    $$
                             "pub enum" <+> pp tdefName <> targs <+> "{"                               $$
                             (nest' $ vcat $ punctuate comma $ map mkConstructor typeCons)             $$
                             "}"                                                                       $$
                             impl_abomonate                                                            $$
                             mkFromRecord tdef                                                         $$
                             display
         Just t           -> "type" <+> pp tdefName <+> targs <+> "=" <+> mkType t <> ";"
         Nothing          -> empty -- The user must provide definitions of opaque types
    where
    derive = "#[derive(Eq, Ord, Clone, Hash, PartialEq, PartialOrd, Serialize, Deserialize)]"
    targs = if null tdefArgs
               then empty
               else "<" <> (hsep $ punctuate comma $ map pp tdefArgs) <> ">"
    targs_traits = if null tdefArgs
                      then empty
                      else "<" <> (hsep $ punctuate comma $ map ((<> ": Val") . pp) tdefArgs) <> ">"
    targs_disp = if null tdefArgs
                    then empty
                    else "<" <> (hsep $ punctuate comma $ map ((<> ": fmt::Display + fmt::Debug") . pp) tdefArgs) <> ">"


    mkField :: Field -> Doc
    mkField f = pp (name f) <> ":" <+> mkType f

    mkConstructor :: Constructor -> Doc
    mkConstructor c =
        let args = vcat $ punctuate comma $ map mkField $ consArgs c in
        if null $ consArgs c
           then pp (name c)
           else pp (name c) <+> "{" $$
                nest' args $$
                "}"

    impl_abomonate = "impl" <+> targs_traits <+> "Abomonation for" <+> pp tdefName <> targs <> "{}"

    display = "impl" <+> targs_disp <+> "fmt::Display for" <+> pp tdefName <> targs <+> "{"                    $$
              "    fn fmt(&self, __formatter: &mut fmt::Formatter) -> fmt::Result {"                           $$
              "        match self {"                                                                           $$
              (nest' $ nest' $ nest' $ vcat $ punctuate comma $ map mkDispCons $ typeCons $ fromJust tdefType) $$
              "        }"                                                                                      $$
              "    }"                                                                                          $$
              "}"                                                                                              $$
              "impl" <+> targs_disp <+> "fmt::Debug for" <+> pp tdefName <> targs <+> "{"                      $$
              "    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {"                                     $$
              "        fmt::Display::fmt(&self, f)"                                                            $$
              "    }"                                                                                          $$
              "}"
    mkDispCons :: Constructor -> Doc
    mkDispCons c@Constructor{..} =
        cname <> "{" <> (hcat $ punctuate comma $ map (pp . name) consArgs) <> "} =>" <+>
        "write!(__formatter, \"" <> cname <> "{{" <> (hcat $ punctuate comma $ map (\_ -> "{:?}") consArgs) <> "}}\"," <+>
        (hcat $ punctuate comma $ map (("*" <>) . pp . name) consArgs) <> ")"
        where cname = mkConstructorName tdefName (fromJust tdefType) (name c)

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
mkFromRecord :: TypeDef -> Doc
mkFromRecord t@TypeDef{..} =
    "impl" <+> targs_bounds <+> "FromRecord for" <+> pp (name t) <> targs <+> "{"                                               $$
    "    fn from_record(val: &Record) -> Result<Self, String> {"                                                                $$
    "        match val {"                                                                                                       $$
    "            Record::Struct(constr, args) => {"                                                                             $$
    "                match constr.as_ref() {"                                                                                   $$
    (nest' $ nest' $ nest' $ nest' $ nest' constructors)                                                                        $$
    "                    c => Result::Err(format!(\"unknown constructor {} of type" <+> pp (name t) <+> "in {:?}\", c, *val))"  $$
    "                }"                                                                                                         $$
    "            },"                                                                                                            $$
    "            v => {"                                                                                                        $$
    "                Result::Err(format!(\"not a struct {:?}\", *v))"                                                           $$
    "            }"                                                                                                             $$
    "        }"                                                                                                                 $$
    "    }"                                                                                                                     $$
    "}"
    where
    targs = "<" <> (hcat $ punctuate comma $ map pp tdefArgs) <> ">"
    targs_bounds = "<" <> (hcat $ punctuate comma $ map ((<> ": FromRecord") . pp) tdefArgs) <> ">"
    constructors = vcat $ map mkcons $ typeCons $ fromJust tdefType
    mkcons :: Constructor -> Doc
    mkcons c@Constructor{..} =
        "\"" <> pp (name c) <> "\"" <+> "if args.len() ==" <+> (pp $ length consArgs) <+> "=> {" $$
        "    Ok(" <> cname <> "{" <> (hsep $ punctuate comma fields) <> "})"     $$
        "},"
        where
        cname = mkConstructorName tdefName (fromJust tdefType) (name c)
        fields = mapIdx (\f i -> pp (name f) <> ": <" <> (mkType f) <> ">::from_record(&args[" <> pp i <> "])?") consArgs

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
mkValueFromRecord :: DatalogProgram -> Doc
mkValueFromRecord d@DatalogProgram{..} =
    mkRelname2Id d                                                                          $$
    mkRelId2Relations d                                                                     $$
    "pub fn relval_from_record(rel: Relations, rec: &Record) -> Result<Value, String> {"    $$
    "    match rel {"                                                                       $$
    (nest' $ nest' $ vcat $ punctuate comma entries)                                        $$
    "    }"                                                                                 $$
    "}"
    where
    entries = map mkrel $ M.elems progRelations
    mkrel :: Relation ->  Doc
    mkrel rel@Relation{..} =
        "Relations::" <> pp (name rel) <+> "=> {"                                                      $$
        "    Ok(Value::" <> mkValConstructorName' d t <> "(<" <> mkType t <> ">::from_record(rec)?))"  $$
        "}"
        where t = typeNormalize d relType

-- Convert string to RelId
mkRelname2Id :: DatalogProgram -> Doc
mkRelname2Id d =
    "pub fn relname2id(rname: &str) -> Option<Relations> {" $$
    "   match rname {"                                      $$
    (nest' $ nest' $ vcat $ entries)                        $$
    "       _  => None"                                     $$
    "   }"                                                  $$
    "}"
    where
    entries = map mkrel $ M.elems $ progRelations d
    mkrel :: Relation -> Doc
    mkrel rel = "\"" <> pp (name rel) <> "\" => Some(Relations::" <> pp (name rel) <> "),"

-- Convert string to RelId
mkRelId2Relations :: DatalogProgram -> Doc
mkRelId2Relations d =
    "pub fn relid2rel(rid: RelId) -> Option<Relations> {"   $$
    "   match rid {"                                        $$
    (nest' $ nest' $ vcat $ entries)                        $$
    "       _  => None"                                     $$
    "   }"                                                  $$
    "}"
    where
    entries = mapIdx mkrel $ M.elems $ progRelations d
    mkrel :: Relation -> Int -> Doc
    mkrel rel i = pp i <+> "=> Some(Relations::" <> pp (name rel) <> "),"

mkFunc :: DatalogProgram -> Function -> Doc
mkFunc d f@Function{..} | isJust funcDef =
    "fn" <+> pp (name f) <> tvars <> (parens $ hsep $ punctuate comma $ map mkArg funcArgs) <+> "->" <+> mkType funcType <+> "{"  $$
    (nest' $ mkExpr d (CtxFunc f) (fromJust funcDef) EVal)                                                                        $$
    "}"
                        | -- generate commented out prototypes of extern functions for user convenvience.
                          otherwise = "/* fn" <+> pp (name f) <> tvars <> (parens $ hsep $ punctuate comma $ map mkArg funcArgs) <+> "->" <+> mkType funcType <+> "*/"
    where
    mkArg :: Field -> Doc
    mkArg a = pp (name a) <> ":" <+> "&" <> mkType a

    tvars = case funcTypeVars f of
                 []  -> empty
                 tvs -> "<" <> (hcat $ punctuate comma $ map ((<> ": Val") . pp) tvs) <> ">"

-- Generate Value type as an enum with one entry per type in types
mkValType :: DatalogProgram -> S.Set Type -> Doc
mkValType d types = 
    "#[derive(Eq, Ord, Clone, Hash, PartialEq, PartialOrd, Serialize, Deserialize, Debug)]" $$
    "pub enum Value {"                                                                      $$
    (nest' $ vcat $ punctuate comma $ map mkValCons $ S.toList types)                       $$
    "}"                                                                                     $$
    "unsafe_abomonate!(Value);"                                                             $$
    "impl Default for Value {"                                                              $$
    "    fn default() -> Value {" <> tuple0 <> "}"                                          $$
    "}"                                                                                     $$
    "impl fmt::Display for Value {"                                                         $$
    "    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {"                            $$
    "        match self {"                                                                  $$
    (nest' $ nest' $ nest' $ vcat $ punctuate comma $ map mkdisplay $ S.toList types)       $$
    "        }"                                                                             $$
    "    }"                                                                                 $$
    "}"
    where
    consname t = mkValConstructorName' d t
    mkValCons :: Type -> Doc
    mkValCons t = consname t <> (parens $ mkType t)
    tuple0 = "Value::" <> mkValConstructorName' d (tTuple []) <> "(())"
    mkdisplay :: Type -> Doc
    mkdisplay t = "Value::" <> consname t <+> "(v) => write!(f, \"{:?}\", *v)"

-- Generate Rust struct for ProgNode
compileSCC :: DatalogProgram -> DepGraph -> [G.Node] -> CompilerMonad ProgNode
compileSCC d dep nodes | recursive = compileSCCNode d relnames
                       | otherwise =  compileRelNode d (head relnames)
    where
    recursive = any (\(from, to) -> elem from nodes && elem to nodes) $ G.edges dep
    relnames = map (fromJust . G.lab dep) nodes

compileRelNode :: DatalogProgram -> String -> CompilerMonad ProgNode
compileRelNode d relname = do
    rel <- compileRelation d relname
    return $ RelNode rel

compileSCCNode :: DatalogProgram -> [String] -> CompilerMonad ProgNode
compileSCCNode d relnames = do
    rels <- mapM (compileRelation d) relnames
    return $ SCCNode rels

{- Generate Rust representation of relation and associated rules.

//Example code generated by this function:
let ancestorset: Arc<Mutex<ValSet<Value>>> = Arc::new(Mutex::new(FnvHashSet::default()));
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
compileRelation :: DatalogProgram -> String -> CompilerMonad ProgRel
compileRelation d rname = do
    let Relation{..} = getRelation d rname
    -- collect all rules for this relation
    let rules = filter (not . null . ruleRHS)
                $ filter ((== rname) . atomRelation . head . ruleLHS)
                $ progRules d
    rules' <- mapM (compileRule d) rules
    let f arrangements =
            "Relation {"                                                                $$
            "    name:         \"" <> pp rname <> "\".to_string(),"                     $$
            "    input:        " <> (if relGround then "true" else "false") <> ","      $$
            "    id:           Relations::" <> pp rname <+> "as RelId,"                 $$
            "    rules:        vec!["                                                   $$
            (nest' $ nest' $ vcat (punctuate comma rules') <> "],")                     $$
            "    arrangements: vec!["                                                   $$
            (nest' $ nest' $ vcat (punctuate comma arrangements) <> "],")               $$
            "    change_cb:    __update_cb.clone()"                                     $$
            "}"
    return (rname, f)

{- Generate Rust representation of a Datalog rule

// Example Rust code generated by this function
Rule{
    rel: 2,
    xforms: vec![XForm::Join{
        afun:        &(arrange_by_snd as ArrangeFunc<Value>),
        arrangement: (1,0),
        jfun:        &(jfun as JoinFunc<Value>)
    }]
}
-}
compileRule :: DatalogProgram -> Rule -> CompilerMonad Doc
compileRule d rl@Rule{..} = do
    let fstrel = atomRelation $ rhsAtom $ head ruleRHS
    xforms <- compileRule' d rl 0
    return $ "/*" <+> pp rl <+> "*/"                             $$
             "Rule{"                                             $$
             "    rel: Relations::" <> pp fstrel <+> "as RelId," $$
             "    xforms: vec!["                                 $$
             (nest' $ nest' $ vcat $ punctuate comma xforms)     $$
             "    ]}"

-- Generates one XForm in the chain
compileRule' :: DatalogProgram -> Rule -> Int -> CompilerMonad [Doc]
compileRule' d rl@Rule{..} last_rhs_idx = {-trace ("compileRule' " ++ show rl ++ " / " ++ show last_rhs_idx) $-} do
    -- Open up input constructor; bring Datalog variables into scope
    open <- if last_rhs_idx == 0
               then openAtom  d vALUE_VAR $ rhsAtom $ head ruleRHS
               else openTuple d vALUE_VAR $ rhsVarsAfter d rl last_rhs_idx
    -- Apply filters and assignments between last_rhs_idx and the next
    -- join or antijoin
    let filters = mkFilters d rl last_rhs_idx
    let prefix = open $+$ vcat filters
    -- index of the next join
    let join_idx = last_rhs_idx + length filters + 1
    if {-trace ("join_idx = " ++ show join_idx) $-} join_idx == length ruleRHS
       then do
           head <- mkHead d prefix rl
           return [head]
       else do
           (xform, last_idx') <-
               case ruleRHS !! join_idx of
                    RHSLiteral True a  -> mkJoin d prefix a rl join_idx
                    RHSLiteral False a -> (, join_idx) <$> mkAntijoin d prefix a rl join_idx
                    RHSFlatMap v e     -> (, join_idx) <$> mkFlatMap d prefix rl join_idx v e
           if {-trace ("last_idx' = " ++ show last_idx') $-} last_idx' < length ruleRHS
              then do rest <- compileRule' d rl last_idx'
                      return $ xform:rest
              else return [xform]

mkFlatMap :: DatalogProgram -> Doc -> Rule -> Int -> String -> Expr -> CompilerMonad Doc
mkFlatMap d prefix rl idx v e = do
    vars <- mkVarsTupleValue d $ rhsVarsAfter d rl idx
    -- Clone variables before passing them to the closure.
    let clones = vcat $ map ((\vname -> "let" <+> vname <+> "=" <+> vname <> ".clone();") . pp . name)
                      $ filter ((/= v) . name) $ rhsVarsAfter d rl idx
    let set = mkExpr d (CtxRuleRFlatMap rl idx) e EVal
        fmfun = braces'
                $ prefix $$
                  clones $$
                  "Some(Box::new(" <> set <> ".into_iter().map(move |" <> pp v <> "|" <> vars <> ")))"
    return $
        "XForm::FlatMap{"                                                                                                $$
        (nest' $ "fmfun: &{fn __f(" <> vALUE_VAR <> ": Value) -> Option<Box<Iterator<Item=Value>>>" $$ fmfun $$ "__f},") $$
        "}"

-- Generate Rust code to filter records and bring variables into scope.
-- The Rust code returns None if the record does not pass the filter.
--
-- let (v1,v2) /*v1,v2 are references*/ = match &v {
--     Value::Rel1(v1,v2) => (v1,v2),
--     _ => return None
-- };
openAtom :: DatalogProgram -> Doc -> Atom -> CompilerMonad Doc
openAtom d var Atom{..} = do
    let rel = getRelation d atomRelation
    constructor <- mkValConstructorName d $ relType rel
    let varnames = map pp $ exprVars atomVal
        vars = tuple varnames
        (pattern, cond) = mkPatExpr d "ref" atomVal
        cond_str = if cond == empty then empty else ("if" <+> cond)
    return $
        "let" <+> vars <+> "= match " <> var <> "{"                                    $$
        "    " <> constructor <> parens pattern <+> cond_str <+> "=> " <> vars <> ","  $$
        "    _ => return None"                                                         $$
        "};"

-- Generate Rust code to open up tuples and bring variables into scope.
openTuple :: DatalogProgram -> Doc -> [Field] -> CompilerMonad Doc
openTuple d var vs = do
    (tup, cons) <- mkVarsTupleValuePat d vs
    let vars = tuple $ map (pp . name) vs
    return $
        "let" <+> vars <+> "= match" <+> var <+> "{"                                              $$
        "    " <> tup <> "=>" <+> vars <> ","                                                     $$
        "    _ => panic!(\"Unexpected value {:?} (expected" <+> cons <+> ")\", " <> var <> ")"    $$
        "};"

-- Generate Rust constructor name for a type
mkValConstructorName :: DatalogProgram -> Type -> CompilerMonad Doc
mkValConstructorName d t = do
    let t' = typeNormalize d t
    addType t'
    return $ "Value::" <> mkValConstructorName' d t'

-- Assumes that t is normalized
mkValConstructorName' :: DatalogProgram -> Type -> Doc
mkValConstructorName' d t =
    case t of
         TTuple{..}  -> "tuple" <> pp (length typeTupArgs) <> "__" <>
                        (hcat $ punctuate "_" $ map (mkValConstructorName' d) typeTupArgs)
         TBool{}     -> "bool"
         TInt{}      -> "int"
         TString{}   -> "string"
         TBit{..}    -> "bit" <> pp typeWidth
         TUser{}     -> consuser
         TOpaque{}   -> consuser
         _           -> error $ "unexpected type " ++ show t ++ " in Compile.mkValConstructorName"
    where
    consuser = pp (typeName t) <>
               case typeArgs t of
                    [] -> empty
                    as -> "__" <> (hcat $ punctuate "_" $ map (mkValConstructorName' d) as)

mkValue :: DatalogProgram -> ECtx -> Expr -> CompilerMonad Doc
mkValue d ctx e = do
    constructor <- mkValConstructorName d $ exprType d ctx e
    return $ constructor <> (parens $ mkExpr d ctx e EVal)

mkTupleValue :: DatalogProgram -> ECtx -> [Expr] -> CompilerMonad Doc
mkTupleValue d ctx es = do
    constructor <- mkValConstructorName d $ tTuple $ map (exprType'' d ctx) es
    return $ constructor <> (parens $ tuple $ map (\e -> mkExpr d ctx e EVal) es)

mkVarsTupleValue :: DatalogProgram -> [Field] -> CompilerMonad Doc
mkVarsTupleValue d vs = do
    constructor <- mkValConstructorName d $ tTuple $ map typ vs
    return $ constructor <> (parens $ tuple $ map ((<> ".clone()") . pp . name) vs)

mkVarsTupleValuePat :: DatalogProgram -> [Field] -> CompilerMonad (Doc, Doc)
mkVarsTupleValuePat d vs = do
    constructor <- mkValConstructorName d $ tTuple $ map typ vs
    return $ (constructor <> (parens $ tuple $ map (("ref" <+>) . pp . name) vs), constructor)

-- Compile all contiguous RHSCondition terms following 'last_idx'
mkFilters :: DatalogProgram -> Rule -> Int -> [Doc]
mkFilters d rl@Rule{..} last_idx =
    mapIdx (\rhs i -> mkFilter d (CtxRuleRCond rl $ i + last_idx + 1) $ rhsExpr rhs)
    $ takeWhile (\case
                  RHSCondition{} -> True
                  _              -> False)
    $ drop (last_idx+1) ruleRHS

-- Implement RHSCondition semantics in Rust; brings new variables into
-- scope if this is an assignment
mkFilter :: DatalogProgram -> ECtx -> Expr -> Doc
mkFilter d ctx (E e@ESet{..}) = mkAssignFilter d ctx e
mkFilter d ctx e              = mkCondFilter d ctx e

mkAssignFilter :: DatalogProgram -> ECtx -> ENode -> Doc
mkAssignFilter d ctx e@(ESet _ l r) =
    "let" <+> vardecls <+> "= match" <+> r' <+> "{"                 $$
    (nest' $ pattern <+> cond_str <+> "=> " <+> vars <> ",")        $$
    "    _ => return None"                                          $$
    "};"
    where
    r' = mkExpr d (CtxSetR e ctx) r EVal
    (pattern, cond) = mkPatExpr d empty l
    varnames = map (pp . fst) $ exprVarDecls (CtxSetL e ctx) l
    vars = tuple varnames
    vardecls = tuple $ map ("ref" <+>) varnames
    cond_str = if cond == empty then empty else ("if" <+> cond)

mkCondFilter :: DatalogProgram -> ECtx -> Expr -> Doc
mkCondFilter d ctx e =
    "if !" <> mkExpr d ctx e EVal <+> "{return None;};"

-- Compile XForm::Join
-- Returns generated xform and index of the last RHS term consumed by
-- the XForm
mkJoin :: DatalogProgram -> Doc -> Atom -> Rule -> Int -> CompilerMonad (Doc, Int)
mkJoin d prefix atom rl@Rule{..} join_idx = do
    -- Build arrangement to join with
    let ctx = CtxRuleRAtom rl join_idx
        (arr, vmap) = normalizeArrangement d (getRelation d $ atomRelation atom) ctx $ atomVal atom
    aid <- addArrangement (atomRelation atom) arr
    -- Variables from previous terms that will be used in terms
    -- following the join.
    let post_join_vars = (rhsVarsAfter d rl (join_idx - 1)) `intersect`
                         (rhsVarsAfter d rl join_idx)
    -- Arrange variables from previous terms
    akey <- mkTupleValue d ctx $ map snd vmap
    aval <- mkVarsTupleValue d post_join_vars
    let afun = braces' $ prefix $$
                         "Some((" <> akey <> "," <+> aval <> "))"
    -- simplify pattern to only extract new variables from it
    let strip (E e@EStruct{..}) = E $ e{exprStructFields = map (\(n,v) -> (n, strip v)) exprStructFields}
        strip (E e@ETuple{..})  = E $ e{exprTupleFields = map strip exprTupleFields}
        strip (E e@EVar{..}) | isNothing $ lookupVar d ctx exprVar
                                = E e
        strip (E e@ETyped{..})  = E e{exprExpr = strip exprExpr}
        strip _                 = ePHolder
    -- Join function: open up both values, apply filters.
    open <- liftM2 ($$) (openTuple d ("*" <> vALUE_VAR1) post_join_vars)
                        (openAtom d ("*" <> vALUE_VAR2) atom{atomVal = strip $ atomVal atom})
    let filters = mkFilters d rl join_idx
        last_idx = join_idx + length filters
    -- If we're at the end of the rule, generate head atom; otherwise
    -- return all live variables in a tuple
    (ret, last_idx') <- if last_idx == length ruleRHS - 1
        then (, last_idx + 1) <$> mkValue d (CtxRuleL rl 0) (atomVal $ head $ ruleLHS)
        else (, last_idx)     <$> (mkVarsTupleValue d $ rhsVarsAfter d rl last_idx)
    let jfun = braces' $ open                     $$
                         vcat filters             $$
                         "Some" <> parens ret
    let doc = "XForm::Join{"                                                                                                                      $$
              (nest' $ "afun: &{fn __f(" <> vALUE_VAR <> ": Value) -> Option<(Value,Value)>" $$ afun $$ "__f},")                                  $$
              "    arrangement: (Relations::" <> pp (atomRelation atom) <+> "as RelId," <> pp aid <> "),"                                         $$
              (nest' $ "jfun: &{fn __f(_: &Value ," <> vALUE_VAR1 <> ": &Value," <> vALUE_VAR2 <> ": &Value) -> Option<Value>" $$ jfun $$ "__f}") $$
              "}"
    return (doc, last_idx')

-- Compile XForm::Antijoin
mkAntijoin :: DatalogProgram -> Doc -> Atom -> Rule -> Int -> CompilerMonad Doc
mkAntijoin d prefix Atom{..} rl@Rule{..} ajoin_idx = do
    akey <- mkValue d (CtxRuleRAtom rl ajoin_idx) atomVal
    aval <- mkVarsTupleValue d $ rhsVarsAfter d rl ajoin_idx
    let afun = braces' $ prefix $$
                         "Some((" <> akey <> "," <+> aval <> "))"
    return $ "XForm::Antijoin{"                                                                                 $$
             (nest' $ "afun: &{fn __f(" <> vALUE_VAR <> ": Value) -> Option<(Value,Value)>" $$ afun $$ "__f},") $$
             "    rel:  Relations::" <> pp atomRelation <+> "as RelId"                                          $$
             "}"

-- Normalize pattern expression for use in arrangement
normalizeArrangement :: DatalogProgram -> Relation -> ECtx -> Expr -> (Arrangement, [(String, Expr)])
normalizeArrangement d rel ctx pat = (Arrangement renamed, vmap)
    where
    pat' = exprFoldCtx (normalizePattern d) ctx pat
    (renamed, (_, vmap)) = runState (rename pat') (0, [])
    rename :: Expr -> State (Int, [(String, Expr)]) Expr
    rename (E e) =
        case e of
             EStruct{..}             -> do
                fs' <- mapM (\(n,e) -> (n,) <$> rename e) exprStructFields
                return $ E e{exprStructFields = fs'}
             ETuple{..}              -> do
                fs' <- mapM rename exprTupleFields
                return $ E e{exprTupleFields = fs'}
             EBool{}                 -> return $ E e
             EInt{}                  -> return $ E e
             EString{}               -> return $ E e
             EBit{}                  -> return $ E e
             EPHolder{}              -> return $ E e
             ETyped{..}              -> do
                e' <- rename exprExpr
                return $ E e{exprExpr = e'}
             _                       -> do
                vid <- gets fst
                let vname = "_" ++ show vid
                modify $ \(_, vmap) -> (vid + 1, vmap ++ [(vname, E e)])
                return $ eVar vname

-- Simplify away parts of the pattern that do not constrain its value.
normalizePattern :: DatalogProgram -> ECtx -> ENode -> Expr
normalizePattern _ ctx e | not (ctxInRuleRHSPattern ctx) = E e
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
         _                       -> E e

-- Compile XForm::FilterMap that generates the head of the rule
mkHead :: DatalogProgram -> Doc -> Rule -> CompilerMonad Doc
mkHead d prefix rl = do
    v <- mkValue d (CtxRuleL rl 0) (atomVal $ head $ ruleLHS rl)
    let fmfun = braces' $ prefix $$
                          "Some" <> parens v
    return $
        "XForm::FilterMap{"                                                                       $$
        nest' ("fmfun: &{fn __f(" <> vALUE_VAR <> ": Value) -> Option<Value>" $$ fmfun $$ "__f}") $$
        "}"

-- Variables in the RHS of the rule declared before or in i'th term
-- and visible after the term.
rhsVarsAfter :: DatalogProgram -> Rule -> Int -> [Field]
rhsVarsAfter d rl i =
    filter (\f -> elem (name f) $ (map name $ ruleLHSVars d rl) `union`
                                  (concatMap (ruleRHSTermVars rl) [i+1..length (ruleRHS rl) - 1]))
           $ ruleRHSVars d rl (i+1)

mkProg :: DatalogProgram -> [ProgNode] -> CompilerMonad Doc
mkProg d nodes = do
    rels <- vcat <$>
            mapM (\(rname, rel) -> do
                  relarrs <- gets ((M.! rname) . cArrangements)
                  arrs <- mapM (mkArrangement d (getRelation d rname)) relarrs
                  return $ "let" <+> pp rname <+> "=" <+> rel arrs <> ";")
                 (concatMap nodeRels nodes)
    let pnodes = map mkNode nodes
        prog = "Program {"                                      $$
               "    nodes: vec!["                               $$
               (nest' $ nest' $ vcat $ punctuate comma pnodes)  $$
               "]}"
    return $
        "pub fn prog(__update_cb: UpdateCallback<Value>) -> Program<Value> {"  $$
        (nest' $ rels $$ prog)                                                 $$
        "}"

mkNode :: ProgNode -> Doc
mkNode (RelNode (rel,_)) =
    "ProgNode::RelNode{rel:" <+> pp rel <> "}"
mkNode (SCCNode rels)    =
    "ProgNode::SCCNode{rels: vec![" <> (commaSep $ map (pp . fst) rels) <> "]}"

mkArrangement :: DatalogProgram -> Relation -> Arrangement -> CompilerMonad Doc
mkArrangement d rel (Arrangement pattern) = do
    let (pat, cond) = mkPatExpr d "ref" pattern
        cond' = if cond == empty then empty else ("if" <+> cond)
    -- extract variables with types from pattern, in the order
    -- consistent with that returned by 'rename'.
    let getvars :: Type -> Expr -> [Field]
        getvars t (E EStruct{..}) =
            concatMap (\(e,t) -> getvars t e)
            $ zip (map snd exprStructFields) (map typ $ consArgs $ fromJust $ find ((== exprConstructor) . name) cs)
            where TStruct _ cs = typ' d t
        getvars t (E ETuple{..})  =
            concatMap (\(e,t) -> getvars t e) $ zip exprTupleFields ts
            where TTuple _ ts = typ' d t
        getvars t (E ETyped{..})  = getvars t exprExpr
        getvars t (E EVar{..})    = [Field nopos exprVar t]
        getvars _ _               = []
    patvars <- mkVarsTupleValue d $ getvars (relType rel) pattern
    constructor <- mkValConstructorName d $ relType rel
    let afun = braces' $
               "match" <+> vALUE_VAR <+> "{"                                                                                  $$
               (nest' $ constructor <> parens pat <+> cond' <+> "=> Some((" <> patvars <> "," <+> vALUE_VAR <> ".clone())),") $$
               "    _ => None"                                                                                                $$
               "}"
    return $
        "Arrangement{"                                                                                      $$
        "   name: r###\"" <> pp pattern <> "\"###.to_string(),"                                             $$
        (nest' $ "afun: &{fn __f(" <> vALUE_VAR <> ": Value) -> Option<(Value,Value)>" $$ afun $$ "__f}")   $$
        "}"

-- Compile Datalog pattern expression to Rust.
-- The first element in the return tuple is a Rust match pattern, the second
-- element is a (possibly empty condition attached to the pattern),
-- e.g., the Datalog pattern
-- 'Constructor{f1= x, f2= "foo"}' compiles into
-- '(TypeName::Constructor{f1: x, f2=_0}, *_0 == "foo".to_string())'
-- where '_0' is an auxiliary variable of type 'String'
--
-- 'varprefix' is a prefix to be added to variables declared in the
-- pattern, e.g., "ref".
mkPatExpr :: DatalogProgram -> Doc -> Expr -> (Doc, Doc)
mkPatExpr d varprefix e = evalState (exprFoldM (mkPatExpr' d varprefix) e) 0

mkPatExpr' :: DatalogProgram -> Doc -> ExprNode (Doc, Doc) -> State Int (Doc, Doc)
mkPatExpr' _ varprefix EVar{..}                  = return (varprefix <+> pp exprVar, empty)
mkPatExpr' _ varprefix EVarDecl{..}              = return (varprefix <+> pp exprVName, empty)
mkPatExpr' _ _         (EBool _ True)            = return ("true", empty)
mkPatExpr' _ _         (EBool _ False)           = return ("false", empty)
mkPatExpr' _ varprefix EInt{..}                  = do
    i <- get
    put $ i+1
    let vname = pp $ "_" <> pp i <> "_"
    return (varprefix <+> vname, "*" <> vname <+> "==" <+> mkInt exprIVal)
mkPatExpr' _ varprefix EString{..}               = do
    i <- get
    put $ i+1
    let vname = pp $ "_" <> pp i <> "_"
    return (varprefix <+> vname, "*" <> vname <+> "==" <+> "\"" <> pp exprString <> "\"" <> ".to_string()")
mkPatExpr' _ _         EBit{..} | exprWidth <= 128= return (pp exprIVal, empty)
mkPatExpr' _ varprefix EBit{..}                  = do
    i <- get
    put $ i+1
    let vname = pp $ "_" <> pp i <> "_"
    return (varprefix <+> vname, "*" <> vname <+> "==" <+> "Uint::parse_bytes(b\"" <> pp exprIVal <> "\", 10)")
mkPatExpr' d _         EStruct{..}               = return (e, cond)
    where
    t = consType d exprConstructor
    struct_name = name t
    e = pp struct_name <>
        (if isStructType (fromJust $ tdefType t) then empty else ("::" <> pp exprConstructor)) <>
        (braces $ hsep $ punctuate comma $ map (\(fname, (e, _)) -> pp fname <> ":" <+> e) exprStructFields)
    cond = hsep $ intersperse "&&" $ filter (/= empty)
                                   $ map (\(_,(_,c)) -> c) exprStructFields
mkPatExpr' d _         ETuple{..}                = return (e, cond)
    where
    e = "(" <> (hsep $ punctuate comma $ map (pp . fst) exprTupleFields) <> ")"
    cond = hsep $ intersperse "&&" $ filter (/= empty)
                                   $ map (pp . snd) exprTupleFields
mkPatExpr' _ _         EPHolder{}                = return ("_", empty)
mkPatExpr' _ _         ETyped{..}                = return exprExpr
mkPatExpr' _ _         e                         = error $ "Compile.mkPatExpr' " ++ (show $ exprMap fst e)

-- Convert Datalog expression to Rust.
-- We generate the code so that all variables are references and must
-- be dereferenced before use or cloned when passed to a constructor,
-- assigned to another variable or returned.
mkExpr :: DatalogProgram -> ECtx -> Expr -> EKind -> Doc
mkExpr d ctx e k | k == EVal  = val e'
                 | k == ERef  = ref e'
                 | k == ELVal = lval e'
    where
    e' = exprFoldCtx (mkExpr_ d) ctx e

mkExpr_ :: DatalogProgram -> ECtx -> ExprNode (Doc, EKind, ENode) -> (Doc, EKind, ENode)
mkExpr_ d ctx e = (t', k', e')
    where (t', k') = mkExpr' d ctx e
          e' = exprMap (E . sel3) e

-- Compiled expressions are represented as '(Doc, EKind)' tuple, where
-- the second components is the kind of the compiled representation
mkExpr' :: DatalogProgram -> ECtx -> ExprNode (Doc, EKind, ENode) -> (Doc, EKind)
-- All variables are references
mkExpr' _ _ EVar{..}    = (pp exprVar, ERef)

-- Function arguments are passed as read-only references
-- Functions return real values.
mkExpr' _ _ EApply{..}  =
    (pp exprFunc <> (parens $ commaSep $ map ref exprArgs), EVal)

-- Field access automatically dereferences subexpression
mkExpr' _ _ EField{..} = (sel1 exprStruct <> "." <> pp exprField, ELVal)

mkExpr' _ _ (EBool _ True) = ("true", EVal)
mkExpr' _ _ (EBool _ False) = ("false", EVal)
mkExpr' _ _ EInt{..} = (mkInt exprIVal, EVal)
mkExpr' _ _ EString{..} = ("r###\"" <> pp exprString <> "\"###.to_string()", EVal)
mkExpr' _ _ EBit{..} | exprWidth <= 128 = (parens $ pp exprIVal <+> "as" <+> mkType (tBit exprWidth), EVal)
                     | otherwise        = ("Uint::parse_bytes(b\"" <> pp exprIVal <> "\", 10)", EVal)

-- Struct fields must be values
mkExpr' d ctx EStruct{..} | ctxInSetL ctx
                          = (tname <> fieldlvals, ELVal)
                          | isstruct
                          = (tname <> fieldvals, EVal)
                          | otherwise
                          = (tname <> "::" <> pp exprConstructor <> fieldvals, EVal)
    where fieldvals  = braces $ commaSep $ map (\(fname, v) -> pp fname <> ":" <+> val v) exprStructFields
          fieldlvals = braces $ commaSep $ map (\(fname, v) -> pp fname <> ":" <+> lval v) exprStructFields
          tdef = consType d exprConstructor
          isstruct = isStructType $ fromJust $ tdefType tdef
          tname = pp $ name tdef

-- Tuple fields must be values
mkExpr' _ ctx ETuple{..} | ctxInSetL ctx
                         = (tuple $ map lval exprTupleFields, ELVal)
                         | otherwise
                         = (tuple $ map val exprTupleFields, EVal)

mkExpr' d ctx e@ESlice{..} = (mkSlice (val exprOp, w) exprH exprL, EVal)
    where
    e' = exprMap (E . sel3) e
    TBit _ w = exprType' d (CtxSlice e' ctx) $ E $ sel3 exprOp

-- Match expression is a reference
mkExpr' d ctx e@EMatch{..} = (doc, EVal)
    where
    m = {-if exprIsVarOrFieldLVal d (CtxMatchExpr (exprMap (E . sel3) e) ctx) (E $ sel3 exprMatchExpr)
           then mutref exprMatchExpr
           else ref exprMatchExpr -}
        deref exprMatchExpr
    doc = ("match" <+> m <+> "{")
          $$
          (nest' $ vcat $ punctuate comma cases)
          $$
          "}"
    cases = map (\(c,v) -> let (match, cond) = mkPatExpr d "ref" (E $ sel3 c)
                               cond' = if cond == empty then empty else ("if" <+> cond) in
                           match <+> cond' <+> "=>" <+> val v) exprCases

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

-- Desonctruction expressions in LHS are compiled into let statements, other assignments
-- are compiled into normal assignments.  Note: assignments in rule
-- atoms are handled by a different code path.
mkExpr' d ctx ESet{..} | islet     = ("let" <+> assign <> optsemi, EVal)
                       | otherwise = (assign, EVal)
    where
    islet = exprIsDeconstruct d $ E $ sel3 exprLVal
    assign = lval exprLVal <+> "=" <+> val exprRVal
    optsemi = if not (ctxIsSeq1 ctx) then ";" else empty

-- operators take values or lvalues and return values
mkExpr' d ctx e@EBinOp{..} = (v', EVal)
    where
    e1 = val exprLeft
    e2 = val exprRight
    e' = exprMap (E . sel3) e
    t  = exprType' d ctx (E e')
    t1 = exprType' d (CtxBinOpL e' ctx) (E $ sel3 exprLeft)
    t2 = exprType' d (CtxBinOpR e' ctx) (E $ sel3 exprRight)
    v = case exprBOp of
             Concat | t == tString
                    -> parens $ e1 <+> "+" <+> ref exprRight
             Concat -> mkConcat (e1, typeWidth t1) (e1, typeWidth t2)
             Impl   -> parens $ "!" <> e1 <+> "||" <+> e2
             op     -> parens $ e1 <+> mkBinOp op <+> e2
    -- Truncate bitvector result in case the type used to represent it
    -- in Rust is larger than the bitvector width.
    v' = if elem exprBOp bopsRequireTruncation
            then mkTruncate v t
            else v

mkExpr' d ctx e@EUnOp{..} = (v, EVal)
    where
    arg =  val exprOp
    e' = exprMap (E . sel3) e
    v = case exprUOp of
             Not    -> parens $ "!" <> arg
             BNeg   -> mkTruncate (parens $ "!" <> arg) $ exprType' d ctx (E e')

mkExpr' _ _ EPHolder{} = ("_", ELVal)

-- keep type ascriptions in LHS of assignment and in integer constants
mkExpr' _ ctx ETyped{..} | ctxIsSetL ctx = (e' <+> ":" <+> mkType exprTSpec, cat)
                         | isint         = (parens $ e' <+> "as" <+> mkType exprTSpec, cat)
                         | otherwise     = (e', cat)
    where
    (e', cat, e) = exprExpr
    isint = case e of
                 EInt{} -> True
                 _      -> False


mkType :: (WithType a) => a -> Doc
mkType x = mkType' $ typ x

mkType' :: Type -> Doc
mkType' TBool{}                    = "bool"
mkType' TInt{}                     = "Int"
mkType' TString{}                  = "String"
mkType' TBit{..} | typeWidth <= 8  = "u8"
                 | typeWidth <= 16 = "u16"
                 | typeWidth <= 32 = "u32"
                 | typeWidth <= 64 = "u64"
                 | typeWidth <= 128= "u128"
                 | otherwise       = "Uint"
mkType' TTuple{..}                 = parens $ commaSep $ map mkType' typeTupArgs
mkType' TUser{..}                  = pp typeName <>
                                    if null typeArgs
                                       then empty
                                       else "<" <> (commaSep $ map mkType' typeArgs) <> ">"
mkType' TOpaque{..}                = pp typeName <>
                                    if null typeArgs
                                       then empty
                                       else "<" <> (commaSep $ map mkType' typeArgs) <> ">"
mkType' TVar{..}                   = pp tvarName
mkType' t                          = error $ "Compile.mkType' " ++ show t

mkBinOp :: BOp -> Doc
mkBinOp Eq     = "=="
mkBinOp Neq    = "!="
mkBinOp Lt     = "<"
mkBinOp Gt     = ">"
mkBinOp Lte    = "<="
mkBinOp Gte    = ">="
mkBinOp And    = "&&"
mkBinOp Or     = "||"
mkBinOp Mod    = "%"
mkBinOp Div    = "/"
mkBinOp ShiftR = ">>"
mkBinOp BAnd   = "&"
mkBinOp BOr    = "|"
mkBinOp ShiftL = "<<"
mkBinOp Plus   = "+"
mkBinOp Minus  = "-"
mkBinOp Times  = "*"

-- These operators require truncating the output value to correct
-- width.
bopsRequireTruncation = [ShiftL, Plus, Minus, Times]

-- Produce code to cast bitvector to a different-width BV.
-- The value of 'e' must fit in the new width.
castBV :: Doc -> Int -> Int -> Doc
castBV e w1 w2 | t1 == t2
               = e
               | w1 <= 128 && w2 <= 128
               = parens $ e <+> "as" <+> t2
               | w2 > 128
               = "Uint::from_u128(" <> e <> ")"
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
         TBit{..} | needsTruncation typeWidth
                  -> parens $ v <+> "&" <+> mask typeWidth
         _        -> v
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
