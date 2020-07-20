{-# LANGUAGE FlexibleContexts, ImplicitParams #-}

module Language.DifferentialDatalog.TypeInference where

import qualified Data.Map as M
import Control.Monad.Except

import Language.DifferentialDatalog.Pos
import Language.DifferentialDatalog.Syntax

inferTypes :: (MonadError String me) => DatalogProgram -> [(ECtx, Expr)] -> me [Expr]
inferTypeArgs :: (MonadError String me) => DatalogProgram -> Pos -> String -> [(Type, Type)] -> me (M.Map String Type)
unifyTypes :: DatalogProgram -> Type -> Type -> Bool
