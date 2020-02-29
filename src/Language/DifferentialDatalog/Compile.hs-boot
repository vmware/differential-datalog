{-# LANGUAGE ImplicitParams #-}

module Language.DifferentialDatalog.Compile where

import Text.PrettyPrint

import Language.DifferentialDatalog.Syntax
import Language.DifferentialDatalog.Type

data CompilerConfig = CompilerConfig {
    cconfJava         :: Bool
}

defaultCompilerConfig :: CompilerConfig

mkValConstructorName :: DatalogProgram -> Type -> Doc
mkConstructorName :: String -> Type -> String -> Doc
mkType :: (WithType a) => a -> Doc
rname :: String -> Doc

mkValue :: (?cfg::CompilerConfig) => DatalogProgram -> Doc -> Type -> Doc
tupleStruct :: [Doc] -> Doc
