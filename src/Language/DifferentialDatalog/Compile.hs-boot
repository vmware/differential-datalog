{-# LANGUAGE ImplicitParams #-}

module Language.DifferentialDatalog.Compile where

import Text.PrettyPrint
import qualified Data.ByteString.Char8 as BS

import Language.DifferentialDatalog.Syntax
import Language.DifferentialDatalog.Crate
import {-# SOURCE #-} Language.DifferentialDatalog.Type

unpackFixNewline :: BS.ByteString -> String

mkConstructorName :: (?crate_graph::CrateGraph, ?specname::String) => Maybe ModuleName -> String -> Type -> String -> Doc
mkType :: (WithType a, ?crate_graph::CrateGraph, ?specname::String) => DatalogProgram -> Maybe ModuleName -> a -> Doc
rnameFlat :: String -> Doc
rnameScoped :: (?crate_graph::CrateGraph, ?specname::String) => Maybe ModuleName -> String -> Doc
tupleStruct :: (?crate_graph::CrateGraph, ?specname::String) => Maybe ModuleName -> [Doc] -> Doc
recordAfterPrefix :: DatalogProgram -> Rule -> Int -> [Expr]
