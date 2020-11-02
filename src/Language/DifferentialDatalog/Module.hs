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

{- |
Module     : Module
Description: DDlog's module system implemented as syntactic sugar over core syntax.
-}

{-# LANGUAGE RecordWildCards, FlexibleContexts, TupleSections, LambdaCase, OverloadedStrings #-}

module Language.DifferentialDatalog.Module(
    mOD_STD,
    DatalogModule(..),
    emptyModule,
    moduleNameToPath,
    moduleIsChildOf,
    mainModuleName,
    nameScope,
    nameLocal,
    nameLocalStr,
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
import Data.List.Split
import Data.Maybe
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
import Language.DifferentialDatalog.ECtx
import {-# SOURCE #-} Language.DifferentialDatalog.Expr
--import Language.DifferentialDatalog.Validate

-- DDlog standard library name.
mOD_STD :: String
mOD_STD = "ddlog_std"

-- 'child' is an immediate submodule of 'parent'.
moduleIsChildOf :: ModuleName -> ModuleName -> Bool
moduleIsChildOf (ModuleName child) (ModuleName parent) =
    parent `isPrefixOf` child && length child == length parent + 1

moduleNameToPath :: ModuleName -> FilePath
moduleNameToPath (ModuleName []) = "lib.rs"
moduleNameToPath (ModuleName [n]) = n <.> "rs"
moduleNameToPath (ModuleName (n:ns)) = n </> moduleNameToPath (ModuleName ns)

nameScope :: (WithName a) => a -> ModuleName
nameScope = ModuleName . init . splitOn "::" . name

nameLocal :: (WithName a) => a -> Doc
nameLocal = pp . nameLocalStr

nameLocalStr :: (WithName a) => a -> String
nameLocalStr = last . splitOn "::" . name

scoped :: ModuleName -> String -> String
scoped mod n = intercalate "::" (modulePath mod ++ [n])

mainModuleName :: ModuleName
mainModuleName = ModuleName []

data DatalogModule = DatalogModule {
    moduleName :: ModuleName,
    moduleFile :: FilePath,
    moduleDefs :: DatalogProgram
}

emptyModule :: ModuleName -> DatalogModule
emptyModule mname = DatalogModule {
    moduleName = mname,
    moduleFile = "",
    moduleDefs = emptyDatalogProgram
}

-- Standard library module name.
stdLibs :: [ModuleName]
stdLibs = [ModuleName [mOD_STD], ModuleName ["internment"]]

stdImport :: ModuleName -> Import
stdImport lib = Import nopos lib (ModuleName [])

-- Import library imports.
stdImports :: [Import]
stdImports = map stdImport stdLibs

-- | Parse a datalog program along with all its imports; returns a "flat"
-- program without imports.  In addition, returns `.rs` and `.toml` code
-- for each module.
--
-- 'roots' is the list of directories to search for imports
--
-- if 'import_std' is true, imports the standard libraries
-- to each module.
parseDatalogProgram :: [FilePath] -> Bool -> String -> FilePath -> IO ([DatalogModule], DatalogProgram, M.Map ModuleName (Doc, Doc))
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
    -- Collect '.rs' and '.toml' files associated with each module.
    rs <- M.fromList <$>
          mapM (\mod -> do
                   let rsfile = addExtension (dropExtension $ moduleFile mod) "rs"
                   let tomlfile = addExtension (dropExtension $ moduleFile mod) "toml"
                   rs_exists <- doesFileExist rsfile
                   toml_exists <- doesFileExist tomlfile
                   rs_code <- if rs_exists
                              then pp <$> readFile rsfile
                              else return empty
                   toml_code <- if toml_exists
                                then pp <$> readFile tomlfile
                                else return empty
                   return (moduleName mod, (rs_code, toml_code)))
               all_modules
    return (all_modules, prog'', rs)

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
    uniq jp (name2rust . name) (\m -> ("Function name '" ++ funcName m ++ "' will cause name collisions"))
         $ map head $ M.elems $ progFunctions prog
    uniq jp (name2rust . name) (\m -> "Transformer name '" ++ transName m ++ "' will cause name collisions")
         $ M.elems $ progTransformers prog
    uniq jp (name2rust . name) (\m -> "Relation name '" ++ relName m ++ "' will cause name collisions")
         $ M.elems $ progRelations prog
    uniq jp (name2rust . name) (\m -> "Index name '" ++ idxName m ++ "' will cause name collisions")
         $ M.elems $ progIndexes prog
    uniq jp (name2rust . name) (\m -> "Type name '" ++ tdefName m ++ "' will cause name collisions")
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
        funcs' = M.fromList $ (map (\fs -> (name $ head fs, fs)))
                            $ map (map (namedFlatten mod)) (M.elems $ progFunctions moduleDefs)
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
    prog4 <- progExprMapCtxM prog3 (exprFlatten mmap mod)
    prog5 <- progAttributeMapM prog4 (attrFlatten mod mmap)
    return prog5

attrFlatten :: (MonadError String me) => DatalogModule -> MMap -> Attribute -> me Attribute
attrFlatten mod mmap a@Attribute{attrName="deserialize_from_array", ..} = do
    -- The value of deserialize_from_array attribute is the name of the key
    -- function.
    e' <- exprFoldCtxM (exprFlatten mmap mod) CtxTop attrVal
    return $ a{attrVal = e'}
attrFlatten _   _    a = return a

applyFlattenNames :: (MonadError String me) => DatalogModule -> MMap -> Apply -> me Apply
applyFlattenNames mod mmap a@Apply{..} = do
    (trans_name, Transformer{..}) <- flattenTransName mmap mod (pos a) applyTransformer
    check (moduleDefs mod) (length applyInputs == length transInputs) (pos a)
          $ "Transformer '" ++ transName ++ "' expects " ++ show (length transInputs) ++ " inputs"
    inputs <- mapM (\(hot, i) -> case hot of
                                      HOTypeFunction{..} -> flattenFuncName' mmap mod (pos a) i (length hotArgs)
                                      HOTypeRelation{}   -> flattenRelName mmap mod (pos a) i)
                   $ zip (map hofType transInputs) applyInputs
    check (moduleDefs mod) (length applyOutputs == length transOutputs) (pos a)
          $ "Transformer '" ++ transName ++ "' expects " ++ show (length transOutputs) ++ " outputs"
    outputs <- mapM (\(hot, o) -> case hot of
                                       HOTypeFunction{..} -> flattenFuncName' mmap mod (pos a) o (length hotArgs)
                                       HOTypeRelation{}   -> flattenRelName mmap mod (pos a) o)
                    $ zip (map hofType transOutputs) applyOutputs
    return a { applyTransformer = trans_name
             , applyInputs      = inputs
             , applyOutputs     = outputs }

candidates :: (MonadError String me) => DatalogModule -> Pos -> String -> me [ModuleName]
candidates DatalogModule{..} p n = do
    let mod = nameScope n
    let mods = (map importModule $ filter ((==mod) . importAlias) $ progImports moduleDefs) ++
               (if mod == ModuleName [] then [moduleName] else [])
    when (null mods) $
        errBrief p $ "Unknown module " ++ show mod ++ ".  Did you forget to import it?"
    return mods

flattenName :: (MonadError String me) => (DatalogProgram -> String -> Maybe a) -> String -> MMap -> DatalogModule -> Pos -> String -> me (String, a)
flattenName lookup_fun entity mmap mod p c = do
    cand_mods <- candidates mod p c
    let lname = nameLocalStr c
    let cands = concatMap (\m -> maybeToList $ (m,) <$> lookup_fun (moduleDefs m) lname) $ map (mmap M.!) cand_mods
    case cands of
         [(m,x)] -> return $ (scoped (moduleName m) lname, x)
         []  -> errBrief p $ "Unknown " ++ entity ++ " '" ++ c ++ "'"
         _   -> errBrief p $ "Conflicting definitions of " ++ entity ++ " " ++ c ++
                        " found in the following modules: " ++
                        (intercalate ", " $ map (moduleFile . fst) cands)

flattenConsName :: (MonadError String me) => MMap -> DatalogModule -> Pos -> String -> me String
flattenConsName mmap mod p c = fst <$> flattenName lookupConstructor "constructor" mmap mod p c

flattenTypeName :: (MonadError String me) => MMap -> DatalogModule -> Pos -> String -> me String
flattenTypeName mmap mod p c = fst <$> flattenName lookupType "type" mmap mod p c

-- Function 'fname' can be declared in multiple modules.  Return all matching function
-- names; postpone disambiguation till type inference.
flattenFuncName :: (MonadError String me) => MMap -> DatalogModule -> Pos -> String -> Maybe Int -> me [String]
flattenFuncName mmap mod p fname nargs = do
    cand_mods <- candidates mod p fname
    let lname = nameLocalStr fname
    let cands = filter (\m -> isJust $ lookupFuncs (moduleDefs m) lname nargs) $ map (mmap M.!) cand_mods
    case cands of
         [] -> err (moduleDefs mod) p $ "Unknown function '" ++ fname ++ "'" ++ maybe "" (\n -> " with " ++ show n ++ " arguments") nargs
         ms -> return $ map (\m -> scoped (moduleName m) lname) ms

-- Like 'flattenFuncName', but additionally insists that there is a unique
-- matching function.
flattenFuncName' :: (MonadError String me) => MMap -> DatalogModule -> Pos -> String -> Int -> me String
flattenFuncName' mmap mod p fname nargs = do
    fname' <- flattenFuncName mmap mod p fname $ Just nargs
    check (moduleDefs mod) (length fname' > 0) p $ "Unknown function '" ++ fname ++ "'"
    check (moduleDefs mod) (length fname' == 1) p
          $ "Ambiguous function name '" ++ fname ++ "' may refer to\n  " ++
            (intercalate "\n  " fname')
    return $ head fname'

flattenRelName :: (MonadError String me) => MMap -> DatalogModule -> Pos -> String -> me String
flattenRelName mmap mod p c = fst <$> flattenName lookupRelation "relation" mmap mod p c

flattenTransName :: (MonadError String me) => MMap -> DatalogModule -> Pos -> String -> me (String, Transformer)
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

exprFlatten :: (MonadError String me) => MMap -> DatalogModule -> ECtx -> ENode -> me Expr
exprFlatten mmap mod _ e@EFunc{..} = do
    fs <- flattenFuncName mmap mod (pos e) (head exprFuncName) Nothing
    return $ E $ e { exprFuncName = fs }
-- The parser is not always able to distinguish function name from variable
-- name.  Here is where we disambiguate by checking if a variable with this name
-- is visible in the current scope.  If not, we seach for a function with this
-- name.  If found, convert expression to 'EFunc'; otherwise still treat it as a
-- variable (this is the case when we are declaring a variable inside a rule).
exprFlatten mmap mod ctx e@EVar{..} | ctxInRuleRHSPattern ctx = return $ E e
                                    | otherwise = do
    case lookupVar (moduleDefs mod) ctx exprVar of
         Nothing -> case exprFlatten mmap mod ctx $ EFunc (pos e) [exprVar] of
                         Left _     -> return $ E e
                         Right e'   -> return e'
         Just _  -> return $ E e
exprFlatten mmap mod _ e@EStruct{..} = do
    c <- flattenConsName mmap mod (pos e) exprConstructor
    return $ E $ e { exprConstructor = c }
exprFlatten _ _ _   e = return $ E e

name2rust :: String -> String
name2rust = replace "::" "_"
