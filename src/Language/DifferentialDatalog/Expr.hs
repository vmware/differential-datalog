{-
Copyright (c) 2018 VMware, Inc.
SPDX-License-Identifier: MIT

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
-}

{-# LANGUAGE ImplicitParams, RecordWildCards, LambdaCase, TupleSections #-}

module Language.DifferentialDatalog.Expr (
    exprMapM,
    exprMap,
    exprFold,
    exprFoldM,
    exprTraverseCtxWithM,
    exprTraverseCtxM,
    exprTraverseM,
    exprFoldCtx,
    exprFoldCtxM,
    exprCollectCtxM,
    exprCollectM,
    exprCollectCtx,
    exprCollect,
    exprVars,
    exprVarDecls,
    exprFuncs,
    exprFuncsRec,
    isLExpr,
    isLVar
    --isLRel
    --exprSplitLHS,
    --exprSplitVDecl,
    --exprInline,
    --expr2Statement,
    --exprModifyResult,
    --ctxExpectsStat,
    --ctxMustReturn,
    --exprIsStatement,
    --exprVarSubst,
    --exprVarRename,
    --exprMutatorsNonRec
    --exprScalars
    --exprDeps
    --exprSubst
    --combineCascades
    ) where

import Data.List
import Data.Maybe
import Control.Monad
import Control.Monad.Identity
import Control.Monad.State
--import Debug.Trace

import Language.DifferentialDatalog.Ops
import Language.DifferentialDatalog.Syntax
import Language.DifferentialDatalog.NS
import Language.DifferentialDatalog.Util
import Language.DifferentialDatalog.Name
--import Language.DifferentialDatalog.Type

-- depth-first fold of an expression
exprFoldCtxM :: (Monad m) => (ECtx -> ExprNode b -> m b) -> ECtx -> Expr -> m b
exprFoldCtxM f ctx (E n) = exprFoldCtxM' f ctx n

exprFoldCtxM' :: (Monad m) => (ECtx -> ExprNode b -> m b) -> ECtx -> ENode -> m b
exprFoldCtxM' f ctx   (EVar p v)              = f ctx $ EVar p v
exprFoldCtxM' f ctx e@(EApply p fun as)       = f ctx =<< EApply p fun <$> (mapIdxM (\a i -> exprFoldCtxM f (CtxApply e ctx i) a) as)
exprFoldCtxM' f ctx e@(EField p s fl)         = do s' <- exprFoldCtxM f (CtxField e ctx) s
                                                   f ctx $ EField p s' fl
exprFoldCtxM' f ctx   (EBool p b)             = f ctx $ EBool p b
exprFoldCtxM' f ctx   (EInt p i)              = f ctx $ EInt p i
exprFoldCtxM' f ctx   (EString p s)           = f ctx $ EString p s
exprFoldCtxM' f ctx   (EBit p w v)            = f ctx $ EBit p w v
exprFoldCtxM' f ctx e@(EStruct p c fs)        = f ctx =<< EStruct p c <$> (mapM (\(fname, fl) -> (fname,) <$> exprFoldCtxM f (CtxStruct e ctx fname) fl) fs)
exprFoldCtxM' f ctx e@(ETuple p fs)           = f ctx =<< ETuple p <$> (mapIdxM (\fl i -> exprFoldCtxM f (CtxTuple e ctx i) fl) fs)
exprFoldCtxM' f ctx e@(ESlice p v h l)        = do v' <- exprFoldCtxM f (CtxSlice e ctx) v
                                                   f ctx $ ESlice p v' h l
exprFoldCtxM' f ctx e@(EMatch p m cs)         = do m' <- exprFoldCtxM f (CtxMatchExpr e ctx) m
                                                   cs' <- mapIdxM (\(e1, e2) i -> (,) <$> (exprFoldCtxM f (CtxMatchPat e ctx i) e1) <*>
                                                                                          (exprFoldCtxM f (CtxMatchVal e ctx i) e2)) cs
                                                   f ctx $ EMatch p m' cs'
exprFoldCtxM' f ctx   (EVarDecl p v)          = f ctx $ EVarDecl p v
exprFoldCtxM' f ctx e@(ESeq p l r)            = f ctx =<< ESeq p <$> exprFoldCtxM f (CtxSeq1 e ctx) l <*> 
                                                                     exprFoldCtxM f (CtxSeq2 e ctx) r
exprFoldCtxM' f ctx e@(EITE p i t el)         = f ctx =<< EITE p <$>
                                                          exprFoldCtxM f (CtxITEIf e ctx) i <*>
                                                          exprFoldCtxM f (CtxITEThen e ctx) t <*>
                                                          exprFoldCtxM f (CtxITEElse e ctx) el
exprFoldCtxM' f ctx e@(ESet p l r)            = f ctx =<< ESet p <$> exprFoldCtxM f (CtxSetL e ctx) l <*> 
                                                                     exprFoldCtxM f (CtxSetR e ctx) r
exprFoldCtxM' f ctx e@(EBinOp p op l r)       = f ctx =<< EBinOp p op <$> exprFoldCtxM f (CtxBinOpL e ctx) l <*>
                                                                          exprFoldCtxM f (CtxBinOpR e ctx) r
exprFoldCtxM' f ctx e@(EUnOp p op x)          = f ctx =<< EUnOp p op <$> (exprFoldCtxM f (CtxUnOp e ctx) x)
exprFoldCtxM' f ctx   (EPHolder p)            = f ctx $ EPHolder p
exprFoldCtxM' f ctx e@(ETyped p x t)          = do x' <- exprFoldCtxM f (CtxTyped e ctx) x
                                                   f ctx $ ETyped p x' t

exprMapM :: (Monad m) => (a -> m b) -> ExprNode a -> m (ExprNode b)
exprMapM g e = case e of
                   EVar p v            -> return $ EVar p v
                   EApply p f as       -> EApply p f <$> mapM g as
                   EField p s f        -> (\s' -> EField p s' f) <$> g s
                   EBool p b           -> return $ EBool p b
                   EInt p i            -> return $ EInt p i
                   EString p s         -> return $ EString p s
                   EBit p w v          -> return $ EBit p w v
                   EStruct p s fs      -> EStruct p s <$> mapM (\(fname, e) -> (fname,) <$> g e) fs
                   ETuple p fs         -> ETuple p <$> mapM g fs
                   ESlice p v h l      -> (\v' -> ESlice p v' h l) <$> g v
                   EMatch p m cs       -> EMatch p <$> g m <*> mapM (\(e1, e2) -> (,) <$> g e1 <*> g e2) cs
                   EVarDecl p v        -> return $ EVarDecl p v
                   ESeq p l r          -> ESeq p <$> g l <*> g r
                   EITE p i t el       -> EITE p <$> g i <*> g t <*> g el
                   ESet p l r          -> ESet p <$> g l <*> g r
                   EBinOp p op l r     -> EBinOp p op <$> g l <*> g r
                   EUnOp p op v        -> EUnOp p op <$> g v
                   EPHolder p          -> return $ EPHolder p
                   ETyped p v t        -> (\v' -> ETyped p v' t) <$> g v


exprMap :: (a -> b) -> ExprNode a -> ExprNode b
exprMap f e = runIdentity $ exprMapM (\e' -> return $ f e') e

exprFoldCtx :: (ECtx -> ExprNode b -> b) -> ECtx -> Expr -> b
exprFoldCtx f ctx e = runIdentity $ exprFoldCtxM (\ctx' e' -> return $ f ctx' e') ctx e

exprFoldM :: (Monad m) => (ExprNode b -> m b) -> Expr -> m b
exprFoldM f e = exprFoldCtxM (\_ e' -> f e') undefined e

exprFold :: (ExprNode b -> b) -> Expr -> b
exprFold f e = runIdentity $ exprFoldM (return . f) e

exprTraverseCtxWithM :: (Monad m) => (ECtx -> ExprNode a -> m a) -> (ECtx -> ExprNode a -> m ()) -> ECtx -> Expr -> m ()
exprTraverseCtxWithM g f ctx e = do {_ <- exprFoldCtxM (\ctx' e' -> do {f ctx' e'; g ctx' e'}) ctx e; return ()}

exprTraverseCtxM :: (Monad m) => (ECtx -> ENode -> m ()) -> ECtx -> Expr -> m ()
exprTraverseCtxM = exprTraverseCtxWithM (\_ x -> return $ E x)

exprTraverseM :: (Monad m) => (ENode -> m ()) -> Expr -> m ()
exprTraverseM f = exprTraverseCtxM (\_ x -> f x) undefined

exprCollectCtxM :: (Monad m) => (ECtx -> ExprNode b -> m b) -> (b -> b -> b) -> ECtx -> Expr -> m b
exprCollectCtxM f op ctx e = exprFoldCtxM g ctx e
    where g ctx' x = do x' <- f ctx' x
                        return $ case x of
                                     EVar _ _              -> x'
                                     EApply _ _ as         -> foldl' op x' as
                                     EField _ s _          -> x' `op` s
                                     EBool _ _             -> x'
                                     EInt _ _              -> x'
                                     EString _ _           -> x'
                                     EBit _ _ _            -> x'
                                     EStruct _ _ fs        -> foldl' (\a (_, x) -> op a x) x' fs
                                     ETuple _ fs           -> foldl' op x' fs
                                     ESlice _ v _ _        -> x' `op` v
                                     EMatch _ m cs         -> foldl' (\a (p,v) -> a `op` p `op` v) (x' `op` m) cs
                                     EVarDecl _ _          -> x'    
                                     ESeq _ l r            -> x' `op` l `op` r       
                                     EITE _ i t el         -> x' `op` i `op` t `op` el 
                                     ESet _ l r            -> x' `op` l `op` r
                                     EBinOp _ _ l r        -> x' `op` l `op` r  
                                     EUnOp _ _ v           -> x' `op` v
                                     EPHolder _            -> x'
                                     ETyped _ v _          -> x' `op` v

exprCollectM :: (Monad m) => (ExprNode b -> m b) -> (b -> b -> b) -> Expr -> m b
exprCollectM f op e = exprCollectCtxM (\_ e' -> f e') op undefined e

exprCollectCtx :: (ECtx -> ExprNode b -> b) -> (b -> b -> b) -> ECtx -> Expr -> b
exprCollectCtx f op ctx e = runIdentity $ exprCollectCtxM (\ctx' x -> return $ f ctx' x) op ctx e

exprCollect :: (ExprNode b -> b) -> (b -> b -> b) -> Expr -> b
exprCollect f op e = runIdentity $ exprCollectM (return . f) op e

-- enumerate all variables that occur in the expression
exprVars :: ECtx -> Expr -> [(String, ECtx)]
exprVars ctx e = exprCollectCtx (\ctx' e' -> case e' of
                                                  EVar _ v -> [(v, ctx')]
                                                  _        -> [])
                                (++) ctx e

-- Variables declared inside expression, visible in the code that follows the expression
exprVarDecls :: ECtx -> Expr -> [(String, ECtx)]
exprVarDecls ctx e = 
    exprFoldCtx (\ctx' e' -> 
                  case e' of
                       EStruct _ _ fs -> concatMap snd fs
                       ETuple _ fs    -> concat fs
                       EVarDecl _ v   -> [(v, ctx')]
                       ESet _ l _     -> l
                       ETyped _ e'' _ -> e''
                       _              -> []) ctx e

-- Non-recursively enumerate all functions invoked by the expression
exprFuncs :: Expr -> [String]
exprFuncs e = exprFuncs' [] e

exprFuncs' :: [String] -> Expr -> [String]
exprFuncs' acc e = nub $ exprCollect (\case
                                       EApply _ f _ -> if' (elem f acc) [] [f]
                                       _            -> []) 
                                     (++) e

-- Recursively enumerate all functions invoked by the expression
exprFuncsRec :: DatalogProgram -> Expr -> [String]
exprFuncsRec d e = exprFuncsRec' d [] e

exprFuncsRec' :: DatalogProgram -> [String] -> Expr -> [String]
exprFuncsRec' d acc e = 
    let new = exprFuncs' acc e in
    new ++ foldl' (\acc' f -> maybe acc' ((acc'++) . exprFuncsRec' d (acc++new++acc')) $ funcDef $ getFunc d f) [] new

isLExpr :: DatalogProgram -> ECtx -> Expr -> Bool
isLExpr d ctx e = exprFoldCtx (isLExpr' d) ctx e

isLExpr' :: DatalogProgram -> ECtx -> ExprNode Bool -> Bool
isLExpr' d ctx (EVar _ v)       = isLVar d ctx v
isLExpr' _ _   (EField _ e _)   = e
isLExpr' _ _   (ETuple _ as)    = and as
isLExpr' _ _   (EStruct _ _ as) = all snd as
isLExpr' _ _   (EVarDecl _ _)   = True
isLExpr' _ _   (EPHolder _)     = True
isLExpr' _ _   (ETyped _ e _)   = e
isLExpr' _ _   _                = False

isLVar :: DatalogProgram -> ECtx -> String -> Bool
isLVar d ctx v = isJust $ find ((==v) . name) $ fst $ ctxVars d ctx
