{-# LANGUAGE ImplicitParams #-}

module Language.DifferentialDatalog.Module where

import qualified Data.Map as M
import Text.PrettyPrint
import Control.Monad.Trans.Except

import Language.DifferentialDatalog.Syntax
import Language.DifferentialDatalog.Name
import Language.DifferentialDatalog.Config

data DatalogModule
emptyModule :: ModuleName -> DatalogModule

mOD_STD :: String
mOD_RT :: String
moduleIsChildOf :: ModuleName -> ModuleName -> Bool
moduleNameToPath :: ModuleName -> FilePath
mainModuleName :: ModuleName
nameScope :: (WithName a) => a -> ModuleName
nameLocal :: (WithName a) => a -> Doc
nameLocalStr :: (WithName a) => a -> String
scoped :: ModuleName -> String -> String
parseDatalogProgram :: (?cfg::Config) => [FilePath] -> Bool -> String -> FilePath -> Bool -> ExceptT String IO ([DatalogModule], DatalogProgram, M.Map ModuleName (Doc, Doc, Doc))
stdLibs :: [ModuleName]
