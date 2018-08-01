module Language.DifferentialDatalog.Compile where

import Text.PrettyPrint

import Language.DifferentialDatalog.Syntax
import Language.DifferentialDatalog.Type

isStructType :: Type -> Bool
mkValConstructorName' :: DatalogProgram -> Type -> Doc
mkConstructorName :: String -> Type -> String -> Doc
mkType :: (WithType a) => a -> Doc
