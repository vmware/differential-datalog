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

{- |
Module     : Module
Description: DDlog's module system implemented as syntactic sugar over core syntax.
-}

{-# LANGUAGE RecordWildCards, FlexibleContexts, TupleSections, LambdaCase, OverloadedStrings #-}

module Language.DifferentialDatalog.Module(
    nameScope,
    nameLocal,
    scoped,
    parseDatalogProgram) where

import Prelude hiding((<>), mod, readFile, writeFile)
import Control.Monad.State.Lazy
import Control.Monad.Except
import qualified Data.Map as M
import qualified System.FilePath as F
import System.Directory
import System.FilePath.Posix
import Data.List
import Data.String.Utils
import Data.Maybe
import Data.Char
--import Debug.Trace
import Text.PrettyPrint

import Language.DifferentialDatalog.Pos
import Language.DifferentialDatalog.PP
import Language.DifferentialDatalog.Util
import Language.DifferentialDatalog.Parse
import Language.DifferentialDatalog.Name
import Language.DifferentialDatalog.NS
import Language.DifferentialDatalog.Syntax
import Language.DifferentialDatalog.DatalogProgram
import Language.DifferentialDatalog.Error
--import Language.DifferentialDatalog.Validate

data DatalogModule = DatalogModule {
    moduleName :: ModuleName,
    moduleFile :: FilePath,
    moduleDefs :: DatalogProgram
}

-- Standard library module name.
stdLibs :: [ModuleName]
stdLibs = [ModuleName ["std"], ModuleName ["internment"]]

stdImport :: ModuleName -> Import
stdImport lib = Import nopos lib (ModuleName [])

-- Import library imports.
stdImports :: [Import]
stdImports = map stdImport stdLibs

-- | Parse a datalog program along with all its imports; returns a "flat"
-- program without imports and the list of Rust files associated with each
-- module in the program.
--
-- 'roots' is the list of directories to search for imports
--
-- if 'import_std' is true, imports the standard libraries
-- to each module.
parseDatalogProgram :: [FilePath] -> Bool -> String -> FilePath -> IO (DatalogProgram, Doc, Doc)
parseDatalogProgram roots import_std fdata fname = do
    roots' <- nub <$> mapM canonicalizePath roots
    prog <- parseDatalogString fdata fname
    let prog' = if import_std
                   then prog { progImports = stdImports ++ progImports prog }
                   else prog
    let main_mod = DatalogModule (ModuleName []) fname prog'
    imports <- evalStateT (parseImports roots' main_mod) []
    let all_modules = main_mod : imports
    prog'' <- flattenNamespace all_modules
    -- collect Rust files associated with each module and place it in a separate Rust module
    rs <- ((\(macros, rs) -> (vcat $ nub $ concat macros) $+$ vcat rs) . unzip . catMaybes) <$>
          mapM ((\mod -> do
                    let rsfile = addExtension (dropExtension $ moduleFile mod) "rs"
                    -- top-level module name is empty
                    let mname = if moduleName mod == ModuleName []
                                   then "__top"
                                   else "__" <> (pp $ name2rust $ show $ moduleName mod)
                    rs_exists <- doesFileExist rsfile
                    if rs_exists
                       then do rs_code <- readFile rsfile
                               -- Extract lines of the form '#[macro_use] extern crate ...'
                               -- and place them in the root of the crate, as Rust does not allow
                               -- macro_use exports in a submodule
                               let (macro_lines, rs_lines) = partition (isPrefixOf "#[macro_use] extern crate")
                                                             $ lines rs_code
                               let rs_code' = vcat $ map pp rs_lines
                               return $ Just ( map pp macro_lines
                                             , "pub use" <+> mname <> "::*;"   $$
                                               "mod" <+> mname <+> "{"         $$
                                               (nest' $ "use super::*;")       $$
                                               (nest' rs_code')                $$
                                               "}")
                       else return Nothing))
               all_modules
    -- collect .toml files associated with modules
    toml <- (vcat . catMaybes) <$>
          mapM ((\mod -> do let tomlfile = addExtension (dropExtension $ moduleFile mod) "toml"
                            exists <- doesFileExist tomlfile
                            if exists
                               then do toml_code <- readFile tomlfile
                                       return $ Just $ pp toml_code
                               else return Nothing))
               all_modules
    return (prog'', rs, toml)

mergeModules :: (MonadError String me) => [DatalogProgram] -> me DatalogProgram
mergeModules mods = do
    let prog = DatalogProgram {
        progImports      = [],
        progTypedefs     = M.unions $ map progTypedefs mods,
        progFunctions    = M.unions $ map progFunctions mods,
        progTransformers = M.unions $ map progTransformers mods,
        progRelations    = M.unions $ map progRelations mods,
        progIndexes      = M.unions $ map progIndexes mods,
        progRules        = concatMap progRules mods,
        progApplys       = concatMap progApplys mods,
        progSources      = M.unions $ map progSources mods
    }
        jp = Just prog
    uniq jp (name2rust . name) (\m -> ("The following function name " ++ (funcName m) ++ " will cause name collisions"))
         $ M.elems $ progFunctions prog
    uniq jp (name2rust . name) (\m -> "The following transformer name " ++ (transName m) ++ " will cause name collisions")
         $ M.elems $ progTransformers prog
    uniq jp (name2rust . name) (\m -> "The following relation name " ++ (relName m) ++ " will cause name collisions")
         $ M.elems $ progRelations prog
    uniq jp (name2rust . name) (\m -> "The following index name " ++ (idxName m) ++ " will cause name collisions")
         $ M.elems $ progIndexes prog
    uniq jp (name2rust . name) (\m -> "The following type name " ++ (tdefName m) ++ " will cause name collisions")
         $ M.elems $ progTypedefs prog
    return prog

parseImports :: [FilePath] -> DatalogModule -> StateT [ModuleName] IO [DatalogModule]
parseImports roots mod = concat <$>
    mapM (\imp@Import{..} -> do
           exists <- gets $ elem importModule
           if exists
              then return []
              else parseImport roots mod imp)
         (progImports $ moduleDefs mod)

parseImport :: [FilePath] -> DatalogModule -> Import -> StateT [ModuleName] IO [DatalogModule]
parseImport roots mod imp = do
    when (importModule imp == moduleName mod)
         $ errorWithoutStackTrace $ "Module '" ++ show (moduleName mod) ++ "' is trying to import self"
    modify (importModule imp:)
    fname <- lift $ findModule roots mod $ importModule imp
    prog <- lift $ do fdata <- readFile fname
                      parseDatalogString fdata fname
    mapM_ (\imp' -> when (elem (importModule imp') stdLibs)
                    $ errorWithoutStackTrace $ "Module '" ++ show (importModule imp') ++ "' is part of the DDlog standard library and is imported automatically by all modules")
          $ progImports prog
    let prog_imports = filter (stdImport (importModule imp) /=) $ stdImports ++ progImports prog
    let mod' = DatalogModule (importModule imp) fname $ prog { progImports = prog_imports }
    imports <- parseImports roots mod'
    return $ mod' : imports

findModule :: [FilePath] -> DatalogModule -> ModuleName -> IO FilePath
findModule roots mod imp = do
    let fpath = (F.joinPath $ modulePath imp) F.<.> ".dl"
    let candidate_paths = map (F.</> fpath) roots
    mods <- filterM doesFileExist candidate_paths
    case mods of
         [m]   -> return m
         []    -> errorWithoutStackTrace $
                     "Module " ++ show imp ++ " imported by " ++ moduleFile mod ++
                     " not found. Paths searched:\n" ++
                     (intercalate "\n" candidate_paths)
         _     -> errorWithoutStackTrace $
                    "Found multiple candidates for module " ++ show imp ++ " imported by " ++ moduleFile mod ++ ":\n" ++
                    (intercalate "\n" mods)

type MMap = M.Map ModuleName DatalogModule

flattenNamespace :: [DatalogModule] -> IO DatalogProgram
flattenNamespace mods = do
    let mmap = M.fromList $ map (\m -> (moduleName m, m)) mods
    let prog = do mods' <- mapM (flattenNamespace1 mmap) mods
                  mergeModules mods'
    case prog of
         Left e  -> errorWithoutStackTrace e
         Right p -> return p

flattenNamespace1 :: (MonadError String me) => MMap -> DatalogModule -> me DatalogProgram
flattenNamespace1 mmap mod@DatalogModule{..} = do
    -- rename typedefs, functions, and relations declared in this module
    let types' = namedListToMap $ map (namedFlatten mod) (M.elems $ progTypedefs moduleDefs)
        funcs' = namedListToMap $ map (namedFlatten mod) (M.elems $ progFunctions moduleDefs)
        trans' = namedListToMap $ map (namedFlatten mod) (M.elems $ progTransformers moduleDefs)
        rels'  = namedListToMap $ map (namedFlatten mod) (M.elems $ progRelations moduleDefs)
        idxs'  = namedListToMap $ map (namedFlatten mod) (M.elems $ progIndexes moduleDefs)
    let prog1 = moduleDefs { progTypedefs     = types'
                           , progFunctions    = funcs'
                           , progTransformers = trans'
                           , progRelations    = rels'
                           , progIndexes      = idxs' }
    -- flatten relation references
    prog2 <- progAtomMapM prog1 (\a -> setName a <$> flattenRelName mmap mod (pos a) (name a))
    applys <- mapM (applyFlattenNames mod mmap) $ progApplys prog2
    let prog2' = prog2 { progApplys = applys }
    -- rename types
    prog3 <- progTypeMapM prog2' (typeFlatten mmap mod)
    -- rename constructors and functions
    prog4 <- progExprMapCtxM prog3 (\_ e -> exprFlatten mmap mod e)
    prog5 <- progRHSMapM prog4 (\case
                                 rhs@RHSAggregate{..} -> do
                                     f' <- flattenFuncName mmap mod (pos rhsAggExpr) rhsAggFunc
                                     return $ rhs{rhsAggFunc = f'}
                                 rhs -> return rhs)
    prog6 <- progAttributeMapM prog5 (attrFlatten mod mmap)
    return prog6

attrFlatten :: (MonadError String me) => DatalogModule -> MMap -> Attribute -> me Attribute
attrFlatten mod mmap a@Attribute{attrName="deserialize_from_array", attrVal=(E EApply{..})} = do
    -- The value of deserialize_from_array attribute is the name of the key
    -- function.
    f' <- flattenFuncName mmap mod (pos a) exprFunc
    return $ a{attrVal = eApply f' exprArgs}
attrFlatten _   _    a = return a

applyFlattenNames :: (MonadError String me) => DatalogModule -> MMap -> Apply -> me Apply
applyFlattenNames mod mmap a@Apply{..} = do
    trans <- flattenTransName mmap mod (pos a) applyTransformer
    inputs <- mapM (\i -> if isLower $ head i
                             then flattenFuncName mmap mod (pos a) i
                             else flattenRelName mmap mod (pos a) i) applyInputs
    outputs <- mapM (\o -> if isLower $ head o
                              then flattenFuncName mmap mod (pos a) o
                              else flattenRelName mmap mod (pos a) o) applyOutputs
    return a { applyTransformer = trans
             , applyInputs      = inputs
             , applyOutputs     = outputs }

nameScope :: String -> ModuleName
nameScope n = ModuleName $ init $ split "." n

nameLocal :: String -> String
nameLocal n = last $ split "." n

scoped :: ModuleName -> String -> String
scoped mod n = intercalate "." (modulePath mod ++ [n])

candidates :: (MonadError String me) => DatalogModule -> Pos -> String -> me [ModuleName]
candidates DatalogModule{..} p n = do
    let mod = nameScope n
    let mods = (map importModule $ filter ((==mod) . importAlias) $ progImports moduleDefs) ++
               (if mod == ModuleName [] then [moduleName] else [])
    when (null mods) $
        errBrief p $ "Unknown module " ++ show mod ++ ".  Did you forget to import it?"
    return mods

flattenName :: (MonadError String me) => (DatalogProgram -> String -> Maybe a) -> String -> MMap -> DatalogModule -> Pos -> String -> me String
flattenName lookup_fun entity mmap mod p c = do
    cand_mods <- candidates mod p c
    let lname = nameLocal c
    let cands = filter ((\m -> isJust $ lookup_fun (moduleDefs m) lname)) $ map (mmap M.!) cand_mods
    case cands of
         [m] -> return $ scoped (moduleName m) lname
         []  -> errBrief p $ "Unknown " ++ entity ++ " '" ++ c ++ "'"
         _   -> errBrief p $ "Conflicting definitions of " ++ entity ++ " " ++ c ++
                        " found in the following modules: " ++
                        (intercalate ", " $ map moduleFile cands)

flattenConsName :: (MonadError String me) => MMap -> DatalogModule -> Pos -> String -> me String
flattenConsName = flattenName lookupConstructor "constructor"

flattenTypeName :: (MonadError String me) => MMap -> DatalogModule -> Pos -> String -> me String
flattenTypeName = flattenName lookupType "type"

flattenFuncName :: (MonadError String me) => MMap -> DatalogModule -> Pos -> String -> me String
flattenFuncName = flattenName lookupFunc "function"

flattenRelName :: (MonadError String me) => MMap -> DatalogModule -> Pos -> String -> me String
flattenRelName = flattenName lookupRelation "relation"

flattenTransName :: (MonadError String me) => MMap -> DatalogModule -> Pos -> String -> me String
flattenTransName = flattenName lookupTransformer "transformer"

namedFlatten :: (WithName a) => DatalogModule -> a -> a
namedFlatten mod x = setName x $ scoped (moduleName mod) (name x)

typeFlatten :: (MonadError String me) => MMap -> DatalogModule -> Type -> me Type
typeFlatten mmap mod t = do
    case t of
         TStruct{..} -> do cs <- mapM (\c -> setName c <$> flattenConsName mmap mod (pos c) (name c)) typeCons
                           return $ t { typeCons = cs }
         TUser{..}   -> do n <- flattenTypeName mmap mod (pos t) typeName
                           return $ t { typeName = n }
         TOpaque{..} -> do n <- flattenTypeName mmap mod (pos t) typeName
                           return $ t { typeName = n }
         _           -> return t

exprFlatten :: (MonadError String me) => MMap -> DatalogModule -> ENode -> me Expr
exprFlatten mmap mod e@EApply{..} = do
    f <- flattenFuncName mmap mod (pos e) exprFunc
    return $ E $ e { exprFunc = f }
exprFlatten mmap mod e@EStruct{..} = do
    c <- flattenConsName mmap mod (pos e) exprConstructor
    return $ E $ e { exprConstructor = c }
exprFlatten _    _   e = return $ E e

name2rust :: String -> String
name2rust = replace "." "_"
