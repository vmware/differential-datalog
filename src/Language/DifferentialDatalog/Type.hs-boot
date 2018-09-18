module Language.DifferentialDatalog.Type where

import Language.DifferentialDatalog.Syntax

class WithType a where
    typ  :: a -> Type
    setType :: a -> Type -> a

instance WithType Type where
instance WithType Field where

ctxExpectType :: DatalogProgram -> ECtx -> Maybe Type
exprType :: DatalogProgram -> ECtx -> Expr -> Type
exprType' :: DatalogProgram -> ECtx -> Expr -> Type
