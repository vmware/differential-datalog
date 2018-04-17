module Language.DifferentialDatalog.Expr where

import Language.DifferentialDatalog.Syntax

exprFoldCtxM :: (Monad m) => (ECtx -> ExprNode b -> m b) -> ECtx -> Expr -> m b
exprMapM :: (Monad m) => (a -> m b) -> ExprNode a -> m (ExprNode b)
exprMap :: (a -> b) -> ExprNode a -> ExprNode b
exprFoldCtx :: (ECtx -> ExprNode b -> b) -> ECtx -> Expr -> b
exprFoldM :: (Monad m) => (ExprNode b -> m b) -> Expr -> m b
exprFold :: (ExprNode b -> b) -> Expr -> b
exprTraverseCtxWithM :: (Monad m) => (ECtx -> ExprNode a -> m a) -> (ECtx -> ExprNode a -> m ()) -> ECtx -> Expr -> m ()
exprTraverseCtxM :: (Monad m) => (ECtx -> ENode -> m ()) -> ECtx -> Expr -> m ()
exprTraverseM :: (Monad m) => (ENode -> m ()) -> Expr -> m ()
exprCollectCtxM :: (Monad m) => (ECtx -> ExprNode b -> m b) -> (b -> b -> b) -> ECtx -> Expr -> m b
exprCollectM :: (Monad m) => (ExprNode b -> m b) -> (b -> b -> b) -> Expr -> m b
exprCollectCtx :: (ECtx -> ExprNode b -> b) -> (b -> b -> b) -> ECtx -> Expr -> b
exprCollect :: (ExprNode b -> b) -> (b -> b -> b) -> Expr -> b
exprVars :: ECtx -> Expr -> [(String, ECtx)]
exprVarDecls :: ECtx -> Expr -> [(String, ECtx)]
isLExpr :: DatalogProgram -> ECtx -> Expr -> Bool
isLVar :: DatalogProgram -> ECtx -> String -> Bool
exprFuncs :: Expr -> [String]
exprFuncsRec :: DatalogProgram -> Expr -> [String]
