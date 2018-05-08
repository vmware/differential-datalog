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

{-# LANGUAGE TupleSections #-}

{- | 
Module     : DatalogProgram
Description: Helper functions for manipulating 'DatalogProgram'.
-}
module Language.DifferentialDatalog.DatalogProgram (
    progExprMapCtxM
) 
where

import Language.DifferentialDatalog.Util
import Language.DifferentialDatalog.Syntax
import Language.DifferentialDatalog.Expr

-- | Map function 'fun' over all expressions in a program
progExprMapCtxM :: (Monad m) => DatalogProgram -> (ECtx -> ENode -> m Expr) -> m DatalogProgram
progExprMapCtxM d fun = do
    funcs' <- mapM (\f -> do e <- case funcDef f of
                                       Nothing -> return Nothing
                                       Just e  -> Just <$> exprFoldCtxM fun (CtxFunc f) e
                             return f{funcDef = e})
                   $ progFunctions d
    rules' <- mapM (\r -> do lhs <- mapIdxM (\a i -> atomExprMapCtxM d fun (CtxRuleL r i) a) $ ruleLHS r
                             rhs <- mapIdxM (\x i -> rhsExprMapCtxM d fun r i x) $ ruleRHS r 
                             return r{ruleLHS = lhs, ruleRHS = rhs})
                   $ progRules d
    return d{ progFunctions = funcs'
            , progRules     = rules'}    

atomExprMapCtxM :: (Monad m) => DatalogProgram -> (ECtx -> ENode -> m Expr) -> (String -> ECtx) -> Atom -> m Atom
atomExprMapCtxM d fun fctx a = do 
    args <- mapM (\(m, e) -> (m,) <$> exprFoldCtxM fun (fctx m) e) $ atomArgs a
    return a{atomArgs = args}

rhsExprMapCtxM :: (Monad m) => DatalogProgram -> (ECtx -> ENode -> m Expr) -> Rule -> Int -> RuleRHS -> m RuleRHS
rhsExprMapCtxM d fun r rhsidx l@RHSLiteral{}   = do
    a <- atomExprMapCtxM d fun (CtxRuleRAtom r rhsidx) (rhsAtom l)
    return l{rhsAtom = a}
rhsExprMapCtxM d fun r rhsidx c@RHSCondition{} = do
    e <- exprFoldCtxM fun (CtxRuleRCond r rhsidx) (rhsExpr c)
    return c{rhsExpr = e}
rhsExprMapCtxM d fun r rhsidx a@RHSAggregate{} = do
    e <- exprFoldCtxM fun (CtxRuleRAggregate r rhsidx) (rhsAggExpr a)
    return a{rhsAggExpr = e}
rhsExprMapCtxM d fun r rhsidx m@RHSFlatMap{}   = do
    e <- exprFoldCtxM fun (CtxRuleRFlatMap r rhsidx) (rhsMapExpr m)
    return m{rhsMapExpr = e}
