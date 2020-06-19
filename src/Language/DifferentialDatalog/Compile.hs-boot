{-# LANGUAGE ImplicitParams #-}

module Language.DifferentialDatalog.Compile where

import Text.PrettyPrint

import Language.DifferentialDatalog.Config
import Language.DifferentialDatalog.Syntax
import {-# SOURCE #-} Language.DifferentialDatalog.Type

mkValConstructorName :: DatalogProgram -> Type -> Doc
mkConstructorName :: String -> Type -> String -> Doc
mkType :: (WithType a) => a -> Doc
rname :: String -> Doc

mkValue :: (?cfg::Config) => DatalogProgram -> Doc -> Type -> Doc
tupleStruct :: [Doc] -> Doc

recordAfterPrefix :: DatalogProgram -> Rule -> Int -> [Expr]
