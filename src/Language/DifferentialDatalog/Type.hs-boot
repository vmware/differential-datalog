{-# LANGUAGE FlexibleContexts #-}

module Language.DifferentialDatalog.Type where

import Control.Monad.Except
import Language.DifferentialDatalog.Syntax
import Language.DifferentialDatalog.Pos
import {-# SOURCE #-} Language.DifferentialDatalog.Var

data ConsTree

class WithType a where
    typ  :: a -> Type
    setType :: a -> Type -> a

instance WithType Relation where
instance WithType Type where
instance WithType Field where
instance WithType FuncArg where
instance WithType ArgType where

typ'' :: (WithType a) => DatalogProgram -> a -> Type
typeStaticMemberTypes :: DatalogProgram -> Type -> [String]
typesMatch :: (WithType a, WithType b) => DatalogProgram -> a -> b -> Bool
checkTypesMatch :: (MonadError String me, WithType a, WithType b) => Pos -> DatalogProgram -> a -> b -> me ()
consTreeEmpty :: ConsTree -> Bool
consTreeAbduct :: DatalogProgram -> ConsTree -> Expr -> (ConsTree, ConsTree)
typeConsTree :: Type -> ConsTree
exprType :: DatalogProgram -> ECtx -> Expr -> Type
exprType' :: DatalogProgram -> ECtx -> Expr -> Type
exprType'' :: DatalogProgram -> ECtx -> Expr -> Type
gROUP_TYPE :: String
ePOCH_TYPE :: String
iTERATION_TYPE :: String
nESTED_TS_TYPE :: String
wEIGHT_TYPE :: String
exprNodeType :: DatalogProgram -> ECtx -> ExprNode Type -> Type
isBool :: (WithType a) => DatalogProgram -> a -> Bool
isBit :: (WithType a) => DatalogProgram -> a -> Bool
isSigned :: (WithType a) => DatalogProgram -> a -> Bool
isString :: (WithType a) => DatalogProgram -> a -> Bool
isBigInt :: (WithType a) => DatalogProgram -> a -> Bool
isInteger :: (WithType a) => DatalogProgram -> a -> Bool
isMap :: (WithType a) => DatalogProgram -> a -> Bool
isGroup :: (WithType a) => DatalogProgram -> a -> Bool
isStruct :: (WithType a) => DatalogProgram -> a -> Bool
isSharedRef :: (WithType a) => DatalogProgram -> a -> Bool
isFloat :: (WithType a) => DatalogProgram -> a -> Bool
isDouble :: (WithType a) => DatalogProgram -> a -> Bool
isFP :: (WithType a) => DatalogProgram -> a -> Bool
typ' :: (WithType a) => DatalogProgram -> a -> Type
typeMapM :: (Monad m) => (Type -> m Type) -> Type -> m Type
typeIsPolymorphic :: Type -> Bool
varType :: DatalogProgram -> Var -> Type
