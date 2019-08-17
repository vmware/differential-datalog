{-# LANGUAGE FlexibleContexts #-}

module Language.DifferentialDatalog.Type where

import Control.Monad.Except
import Language.DifferentialDatalog.Syntax
import Language.DifferentialDatalog.Pos

class WithType a where
    typ  :: a -> Type
    setType :: a -> Type -> a

instance WithType Type where
instance WithType Field where

ctxExpectType :: DatalogProgram -> ECtx -> Maybe Type
exprType :: DatalogProgram -> ECtx -> Expr -> Type
exprType' :: DatalogProgram -> ECtx -> Expr -> Type
sET_TYPES :: [String]
gROUP_TYPE :: String
checkIterable :: (MonadError String me, WithType a) => String -> Pos -> DatalogProgram -> a -> me ()
typeIterType :: DatalogProgram -> Type -> Maybe Type
exprTypeMaybe :: DatalogProgram -> ECtx -> Expr -> Maybe Type
