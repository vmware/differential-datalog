module Language.DifferentialDatalog.Type where

import Language.DifferentialDatalog.Syntax

class WithType a where
    typ  :: a -> Type

instance WithType Type where
instance WithType Field where

ctxExpectType :: DatalogProgram -> ECtx -> Maybe Type
