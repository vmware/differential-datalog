{-# LANGUAGE ImplicitParams #-}

module Language.DifferentialDatalog.Compile where

import Text.PrettyPrint

import Language.DifferentialDatalog.Syntax
import {-# SOURCE #-} Language.DifferentialDatalog.Type

mkConstructorName :: Bool -> String -> Type -> String -> Doc
mkType :: (WithType a) => DatalogProgram -> Bool -> a -> Doc
rnameFlat :: String -> Doc
rnameScoped :: Bool -> String -> Doc

tupleStruct :: Bool -> [Doc] -> Doc

recordAfterPrefix :: DatalogProgram -> Rule -> Int -> [Expr]
