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

{-# LANGUAGE TupleSections, LambdaCase, RecordWildCards #-}

{- | 
Module     : DatalogProgram
Description: Helper functions for manipulating 'DatalogProgram'.
-}
module Language.DifferentialDatalog.DatalogProgram (
    progExprMapCtxM,
    progDependencyGraph
) 
where

import qualified Data.Graph.Inductive as G
import qualified Data.Map as M
import Data.Maybe

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

atomExprMapCtxM :: (Monad m) => DatalogProgram -> (ECtx -> ENode -> m Expr) -> ECtx -> Atom -> m Atom
atomExprMapCtxM d fun ctx a = do
    v <- exprFoldCtxM fun ctx $ atomVal a
    return a{atomVal = v}

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

-- | Dependency graph among program relations.  An edge from Rel1 to
-- Rel2 means that there is a rule with Rel1 in the right-hand-side,
-- and Rel2 in the left-hand-side.  Edge label is equal to the
-- polarity with which Rel1 occurs in the rule.
--
-- Assumes that rules and relations have been validated before calling
-- this function.
progDependencyGraph :: DatalogProgram -> G.Gr String Bool
progDependencyGraph DatalogProgram{..} = G.insEdges edges g0
    where
    g0 = G.insNodes (zip [0..] $ M.keys progRelations) G.empty
    relidx rel = M.findIndex rel progRelations
    edges = concatMap (\Rule{..} ->
                        concatMap (\a ->
                                    mapMaybe (\case
                                               RHSLiteral pol a' -> Just (relidx $ atomRelation a', relidx $ atomRelation a, pol)
                                               _ -> Nothing)
                                             ruleRHS)
                                  ruleLHS)
                      progRules


