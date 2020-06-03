{-# LANGUAGE FlexibleContexts #-}

module Language.DifferentialDatalog.Type where

import Control.Monad.Except
import Language.DifferentialDatalog.Syntax
import Language.DifferentialDatalog.Pos
import Language.DifferentialDatalog.Var
import qualified Data.Map as M

data ConsTree

class WithType a where
    typ  :: a -> Type
    setType :: a -> Type -> a

instance WithType Relation where
instance WithType Type where
instance WithType Field where
instance WithType FuncArg where

typeStaticMemberTypes :: DatalogProgram -> Type -> [String]
typesMatch :: (WithType a, WithType b) => DatalogProgram -> a -> b -> Bool
checkTypesMatch :: (MonadError String me, WithType a, WithType b) => Pos -> DatalogProgram -> a -> b -> me ()
consTreeEmpty :: ConsTree -> Bool
consTreeAbduct :: DatalogProgram -> ConsTree -> Expr -> (ConsTree, ConsTree)
typeConsTree :: Type -> ConsTree
ctxExpectType :: DatalogProgram -> ECtx -> Maybe Type
exprType :: DatalogProgram -> ECtx -> Expr -> Type
exprType' :: DatalogProgram -> ECtx -> Expr -> Type
exprType'' :: DatalogProgram -> ECtx -> Expr -> Type
sET_TYPES :: [String]
gROUP_TYPE :: String
ePOCH_TYPE :: String
iTERATION_TYPE :: String
nESTED_TS_TYPE :: String
wEIGHT_TYPE :: String
checkIterable :: (MonadError String me, WithType a) => String -> Pos -> DatalogProgram -> a -> me ()
typeIterType :: DatalogProgram -> Type -> Maybe (Type, Bool)
exprNodeType :: (MonadError String me) => DatalogProgram -> ECtx -> ExprNode Type -> me Type
exprTypeMaybe :: DatalogProgram -> ECtx -> Expr -> Maybe Type
unifyTypes :: (MonadError String me) => DatalogProgram -> Pos -> String -> [(Type, Type)] -> me (M.Map String Type)
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
varTypeMaybe :: DatalogProgram -> Var -> Maybe Type
varType :: DatalogProgram -> Var -> Type
