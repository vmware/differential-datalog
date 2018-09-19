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

{-# LANGUAGE RecordWildCards, FlexibleContexts #-}

module Language.DifferentialDatalog.Module(
    parseDatalogProgram,
    progRustizeNamespace) where

import Control.Monad.State.Lazy
import Control.Monad.Except
import qualified Data.Map as M
import qualified System.FilePath as F
import System.Directory
import Data.List
import Data.String.Utils
import Debug.Trace

import Language.DifferentialDatalog.Pos
import Language.DifferentialDatalog.Util
import Language.DifferentialDatalog.Parse
import Language.DifferentialDatalog.Name
import Language.DifferentialDatalog.Syntax
import Language.DifferentialDatalog.DatalogProgram
import Language.DifferentialDatalog.Validate

data DatalogModule = DatalogModule {
    moduleName :: ModuleName,
    moduleDefs :: DatalogProgram
}

-- | Parse a datalog program along with all its imports; returns a "flat"
-- program without imports.
--
-- 'roots' is the list of directories to search for imports
--
-- if 'insert_preamble' is true, prepends the content of
-- 'datalogPreamble' to the file before parsing it.
parseDatalogProgram :: [FilePath] -> Bool -> String -> FilePath -> IO DatalogProgram
parseDatalogProgram roots insert_preamble fdata fname = do
    prog <- parseDatalogString insert_preamble fdata fname
    let main_mod = DatalogModule (ModuleName []) prog
    imports <- evalStateT (parseImports roots main_mod) []
    flattenNamespace $ main_mod : imports

mergeModules :: (MonadError String me) => [DatalogProgram] -> me DatalogProgram
mergeModules mods = do
    let prog = DatalogProgram {
        progImports    = [],
        progTypedefs   = M.unions $ map progTypedefs mods,
        progFunctions  = M.unions $ map progFunctions mods,
        progRelations  = M.unions $ map progRelations mods,
        progRules      = concatMap progRules mods
    }
    uniq (name2rust . name) (\_ -> "The following function declarations will cause name collision in Rust: ") 
         $ M.elems $ progFunctions prog
    uniq (name2rust . name) (\_ -> "The following relations will cause name collision in Rust: ") 
         $ M.elems $ progRelations prog
    uniq (name2rust . name) (\_ -> "The following type declarations will cause name collision in Rust: ") 
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
parseImport roots mod Import{..} = do
    modify (importModule:)
    prog <- lift $ do fname <- findModule roots mod importModule
                      fdata <- readFile fname
                      parseDatalogString False fdata fname
    let mod' = DatalogModule importModule prog
    imports <- parseImports roots mod'
    return $ mod' : imports

findModule :: [FilePath] -> DatalogModule -> ModuleName -> IO FilePath
findModule roots mod imp = do
    let fpath = (F.joinPath $ modulePath imp) F.<.> ".dl"
    let candidates = map (F.</> fpath) roots
    mods <- filterM doesFileExist candidates
    case mods of
         [m]   -> return m
         []    -> errorWithoutStackTrace $
                     "Module " ++ show imp ++ " imported by " ++ show (moduleName mod) ++ 
                     " not found. Paths searched:\n" ++
                     (intercalate "\n" candidates)
         _     -> errorWithoutStackTrace $ 
                    "Found multiple candidates for module " ++ show imp ++ " imported by " ++ show (moduleName mod) ++ ":\n" ++
                    (intercalate "\n" candidates)


flattenNamespace :: [DatalogModule] -> IO DatalogProgram
flattenNamespace mods = do
    let prog = do mods' <- mapM flattenNamespace1 mods 
                  mergeModules mods'
    case prog of
         Left e  -> errorWithoutStackTrace e
         Right p -> return p

flattenNamespace1 :: (MonadError String me) => DatalogModule -> me DatalogProgram
flattenNamespace1 mod@DatalogModule{..} = do
    -- rename typedefs, functions, and relations declared in this module
    types' <- namedListToMap <$> mapM (namedFlatten mod) (M.elems $ progTypedefs moduleDefs)
    funcs' <- namedListToMap <$> mapM (namedFlatten mod) (M.elems $ progFunctions moduleDefs)
    rels'  <- namedListToMap <$> mapM (namedFlatten mod) (M.elems $ progRelations moduleDefs)
    let prog1 = moduleDefs { progTypedefs   = types'
                           , progFunctions  = funcs'
                           , progRelations  = rels' }
    -- flatten relation references
    prog2 <- progAtomMapM prog1 (namedFlatten mod)
    -- rename types
    prog3 <- progTypeMapM prog2 (typeFlatten mod)
    -- rename constructors and functions
    prog4 <- progExprMapCtxM prog3 (\_ e -> exprFlatten mod e)
    return prog4

nameScope :: String -> Maybe ModuleName
nameScope n = 
    case init $ split "." n of
         [] -> Nothing
         xs -> Just $ ModuleName xs

scoped :: ModuleName -> String -> String
scoped mod n = intercalate "." (modulePath mod ++ [n])

flattenName :: (MonadError String me) => DatalogModule -> Pos -> String -> me String
flattenName DatalogModule{..} pos n =
    case nameScope n of
         Nothing   -> return $ scoped moduleName n
         Just mod -> case find ((==mod) . importAlias) $ progImports moduleDefs of
                          Nothing  -> err pos $ "Unknown module " ++ show mod ++ ".  Did you forget to import it?"
                          Just imp -> return $ scoped (importModule imp) n

namedFlatten :: (MonadError String me, WithName a, WithPos a) => DatalogModule -> a -> me a
namedFlatten mod x = setName x <$> flattenName mod (pos x) (name x)

typeFlatten :: (MonadError String me) => DatalogModule -> Type -> me Type
typeFlatten mod t = do
    case t of
         TStruct{..} -> do c <- mapM (namedFlatten mod) typeCons
                           return $ t { typeCons = c }
         TUser{..}   -> do n <- flattenName mod (pos t) typeName
                           return $ t { typeName = n }
         TOpaque{..} -> do n <- flattenName mod (pos t) typeName
                           return $ t { typeName = n }
         _           -> return t

exprFlatten :: (MonadError String me) => DatalogModule -> ENode -> me Expr
exprFlatten mod e@EApply{..} = do
    f <- flattenName mod (pos e) exprFunc
    return $ E $ e { exprFunc = f }
exprFlatten mod e@EStruct{..} = do
    c <- flattenName mod (pos e) exprConstructor
    return $ E $ e { exprConstructor = c }
exprFlatten _   e = return $ E e 


----------

name2rust :: String -> String
name2rust = replace "." "_"

named2rust :: (WithName a) => a -> a
named2rust x = setName x $ name2rust (name x)

-- | Replace "." with "_" in all identifiers in the program.  Must be called before converting
-- program to Rust.  The output of a function is not a valid DDlog program, as it may violate
-- capitalization rules.
progRustizeNamespace :: DatalogProgram -> DatalogProgram
progRustizeNamespace prog@DatalogProgram{..} =
    case validate prog4 of
         Left e -> error $ "progRustizeNamespace produced invalid program: " ++ e
         _      -> prog4
    where
    prog1 = prog {
        progTypedefs   = namedListToMap $ map named2rust (M.elems progTypedefs),
        progFunctions  = namedListToMap $ map named2rust (M.elems progFunctions),
        progRelations  = namedListToMap $ map named2rust (M.elems progRelations)
    }
    prog2 = progAtomMap prog1 named2rust
    prog3 = progTypeMap prog2 type2rust
    prog4 = progExprMapCtx prog3 (\_ e -> expr2rust e)

type2rust :: Type -> Type
type2rust t =
    case t of
         TStruct{..} -> t { typeCons = map named2rust typeCons }
         TUser{..}   -> t { typeName = name2rust typeName }
         TOpaque{..} -> t { typeName = name2rust typeName }
         _           -> t

expr2rust :: ENode -> Expr
expr2rust e@EApply{..}  = E $ e { exprFunc = name2rust exprFunc }
expr2rust e@EStruct{..} = E $ e { exprConstructor = name2rust exprConstructor }
expr2rust e             = E e
