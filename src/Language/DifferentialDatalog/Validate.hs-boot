{-# LANGUAGE FlexibleContexts #-}

module Language.DifferentialDatalog.Validate where

import Control.Monad.Except
import Language.DifferentialDatalog.Syntax

typeValidate :: (MonadError String me) => DatalogProgram -> [String] -> Type -> me ()
