{-# LANGUAGE FlexibleContexts #-}

module Language.DifferentialDatalog.Function where

import qualified Data.Map as M
import Control.Monad.Except
import Language.DifferentialDatalog.Pos
import Language.DifferentialDatalog.Syntax

funcTypeArgSubsts :: (MonadError String me) => DatalogProgram -> Pos -> Function -> [Type] -> Maybe Type -> me (M.Map String Type)
funcGroupArgTypes :: DatalogProgram -> Function -> [Type]
