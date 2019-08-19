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
    progExprMapCtx,
    progTypeMapM,
    progTypeMap,
    progRHSMapM,
    progRHSMap,
    progAtomMapM,
    progAtomMap,
    DepGraphNode(..),
    depNodeIsRel,
    depNodeIsApply,
    DepGraph,
    progDependencyGraph,
    depGraphToDot
)
where

import qualified Data.Graph.Inductive              as G
import qualified Data.Map                          as M
import Data.Maybe
import Data.Char
import Control.Monad.Identity
import qualified Data.Text.Lazy                    as T
import qualified Data.GraphViz                     as GV
import qualified Data.GraphViz.Attributes.Complete as GV
import qualified Data.GraphViz.Printing            as GV

import Language.DifferentialDatalog.Util
import Language.DifferentialDatalog.Name
import Language.DifferentialDatalog.Syntax
import Language.DifferentialDatalog.Expr
import {-# SOURCE #-} Language.DifferentialDatalog.Rule
import Language.DifferentialDatalog.Type

-- | Map function 'fun' over all expressions in a program
progExprMapCtxM :: (Monad m) => DatalogProgram -> (ECtx -> ENode -> m Expr) -> m DatalogProgram
progExprMapCtxM d fun = do
    funcs' <- mapM (\f -> do e <- case funcDef f of
                                       Nothing -> return Nothing
                                       Just e  -> Just <$> exprFoldCtxM fun (CtxFunc f) e
                             return f{funcDef = e})
                   $ progFunctions d
    rules' <- mapM (\r -> do lhs <- mapIdxM (\a i -> atomExprMapCtxM fun (CtxRuleL r i) a) $ ruleLHS r
                             rhs <- mapIdxM (\x i -> rhsExprMapCtxM fun r i x) $ ruleRHS r
                             return r{ruleLHS = lhs, ruleRHS = rhs})
                   $ progRules d
    return d{ progFunctions = funcs'
            , progRules     = rules'}

atomExprMapCtxM :: (Monad m) => (ECtx -> ENode -> m Expr) -> ECtx -> Atom -> m Atom
atomExprMapCtxM fun ctx a = do
    v <- exprFoldCtxM fun ctx $ atomVal a
    return a{atomVal = v}

rhsExprMapCtxM :: (Monad m) => (ECtx -> ENode -> m Expr) -> Rule -> Int -> RuleRHS -> m RuleRHS
rhsExprMapCtxM fun r rhsidx l@RHSLiteral{}   = do
    a <- atomExprMapCtxM fun (CtxRuleRAtom r rhsidx) (rhsAtom l)
    return l{rhsAtom = a}
rhsExprMapCtxM fun r rhsidx c@RHSCondition{} = do
    e <- exprFoldCtxM fun (CtxRuleRCond r rhsidx) (rhsExpr c)
    return c{rhsExpr = e}
rhsExprMapCtxM fun r rhsidx a@RHSAggregate{} = do
    e <- exprFoldCtxM fun (CtxRuleRAggregate r rhsidx) (rhsAggExpr a)
    return a{rhsAggExpr = e}
rhsExprMapCtxM fun r rhsidx m@RHSFlatMap{}   = do
    e <- exprFoldCtxM fun (CtxRuleRFlatMap r rhsidx) (rhsMapExpr m)
    return m{rhsMapExpr = e}

progExprMapCtx :: DatalogProgram -> (ECtx -> ENode -> Expr) -> DatalogProgram
progExprMapCtx d fun = runIdentity $ progExprMapCtxM d  (\ctx e -> return $ fun ctx e)


-- | Apply function to all types referenced in the program
progTypeMapM :: (Monad m) => DatalogProgram -> (Type -> m Type) -> m DatalogProgram
progTypeMapM d@DatalogProgram{..} fun = do
    ts <- M.traverseWithKey (\_ (TypeDef p atrs n a t) -> TypeDef p atrs n a <$> mapM (typeMapM fun) t) progTypedefs
    fs <- M.traverseWithKey (\_ f -> do ret <- typeMapM fun $ funcType f
                                        as  <- mapM (\a -> setType a <$> (typeMapM fun $ typ a)) $ funcArgs f
                                        def <- mapM (exprTypeMapM fun) $ funcDef f
                                        return f{ funcType = ret, funcArgs = as, funcDef = def }) progFunctions
    trans <- M.traverseWithKey (\_ t -> do inputs  <- mapM (\i -> do t' <- hotypeTypeMapM (hofType i) fun
                                                                     return i{hofType = t'}) $ transInputs t
                                           outputs <- mapM (\o -> do t' <- hotypeTypeMapM (hofType o) fun
                                                                     return o{hofType = t'}) $ transOutputs t
                                           return t{ transInputs = inputs, transOutputs = outputs }) progTransformers
    rels <- M.traverseWithKey (\_ rel -> setType rel <$> (typeMapM fun $ typ rel)) progRelations
    rules <- mapM (ruleTypeMapM fun) progRules
    return d { progTypedefs     = ts
             , progFunctions    = fs
             , progTransformers = trans
             , progRelations    = rels
             , progRules        = rules
             }

hotypeTypeMapM :: (Monad m) => HOType -> (Type -> m Type) -> m HOType
hotypeTypeMapM hot@HOTypeRelation{..} fun = do
    t <- typeMapM fun hotType
    return hot { hotType = t }
hotypeTypeMapM hot@HOTypeFunction{..} fun = do
    ret <- typeMapM fun hotType
    as  <- mapM (\f -> setType f <$> (typeMapM fun $ typ f)) hotArgs
    return hot { hotArgs = as, hotType = ret }

progTypeMap :: DatalogProgram -> (Type -> Type) -> DatalogProgram
progTypeMap d fun = runIdentity $ progTypeMapM d (return . fun)

-- | Apply function to all rule RHS terms in the program
progRHSMapM :: (Monad m) => DatalogProgram -> (RuleRHS -> m RuleRHS) -> m DatalogProgram
progRHSMapM d fun = do
    rs <- mapM (\r -> do
                 rhs <- mapM fun $ ruleRHS r
                 return r { ruleRHS = rhs })
               $ progRules d
    return d { progRules = rs }

progRHSMap :: DatalogProgram -> (RuleRHS -> RuleRHS) -> DatalogProgram
progRHSMap d fun = runIdentity $ progRHSMapM d (return . fun)

-- | Apply function to all atoms in the program
progAtomMapM :: (Monad m) => DatalogProgram -> (Atom -> m Atom) -> m DatalogProgram
progAtomMapM d fun = do
    rs <- mapM (\r -> do
                 lhs <- mapM fun $ ruleLHS r
                 rhs <- mapM (\case
                               lit@RHSLiteral{} -> do a <- fun $ rhsAtom lit
                                                      return lit { rhsAtom = a }
                               rhs              -> return rhs) $ ruleRHS r
                 return r { ruleLHS = lhs, ruleRHS = rhs })
               $ progRules d
    return d { progRules = rs }

progAtomMap :: DatalogProgram -> (Atom -> Atom) -> DatalogProgram
progAtomMap d fun = runIdentity $ progAtomMapM d (return . fun)

data DepGraphNode = DepNodeRel   String
                  | DepNodeApply Apply

instance Show DepGraphNode where
    show (DepNodeRel rel) = rel
    show (DepNodeApply a) = show a

depNodeIsApply :: DepGraphNode -> Bool
depNodeIsApply (DepNodeApply _) = True
depNodeIsApply _                = False

depNodeIsRel :: DepGraphNode -> Bool
depNodeIsRel (DepNodeRel _) = True
depNodeIsRel _              = False

type DepGraph = G.Gr DepGraphNode Bool

-- | Dependency graph among program relations.  An edge from Rel1 to
-- Rel2 means that there is a rule with Rel1 in the right-hand-side,
-- and Rel2 in the left-hand-side.  Edge label is equal to the
-- polarity with which Rel1 occurs in the rule.
--
-- In addition, we conservatively add both a positive and a negative edge
-- from Rel1 to Rel2 if they appear respectively as input and output of a
-- transformer application (since we currently don't have a way of knowing
-- if the transformer is monotonic).
--
-- Assumes that rules and relations have been validated before calling
-- this function.
progDependencyGraph :: DatalogProgram -> DepGraph
progDependencyGraph DatalogProgram{..} = G.insEdges (edges ++ apply_edges) g1
    where
    g0 = G.insNodes (zip [0..] $ map DepNodeRel $ map name $ M.elems progRelations) G.empty
    indexed_applys = zip [M.size progRelations ..] progApplys
    g1 = G.insNodes (map (mapSnd DepNodeApply) indexed_applys) g0
    relidx rel = M.findIndex rel progRelations
    edges = concatMap (\Rule{..} ->
                        concatMap (\a ->
                                    mapMaybe (\case
                                               RHSLiteral pol a' -> Just (relidx $ atomRelation a', relidx $ atomRelation a, pol)
                                               _ -> Nothing)
                                             ruleRHS)
                                  ruleLHS)
                      progRules
    apply_edges = concatMap (\(idx, Apply{..}) ->
                             let inp_rels = filter (isUpper . head) applyInputs in
                             map (\i -> (relidx i, idx, True)) inp_rels ++ 
                             map (\o -> (idx, relidx o, True)) applyOutputs ++
                             map (\o -> (idx, relidx o, False)) applyOutputs)
                  indexed_applys

depGraphToDot :: DepGraph -> String
depGraphToDot gr =
  show $ GV.runDotCode $ GV.toDot $ GV.graphToDot params gr
  where
    params = GV.nonClusteredParams {
        GV.fmtNode = \(_, l) -> [GV.Label $ GV.StrLabel $ T.pack $ show l]
    }
