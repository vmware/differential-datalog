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

{-# LANGUAGE RecordWildCards, LambdaCase, FlexibleContexts #-}

module Language.DifferentialDatalog.Rule (
    ruleRHSVars,
    ruleVars,
    ruleRHSTermVars,
    ruleLHSVars,
    ruleTypeMapM,
    ruleHasJoins,
    ruleAggregateTypeParams
) where

import qualified Data.Set as S
import qualified Data.Map as M
import Data.List
import Control.Monad.Except
import Debug.Trace

import Language.DifferentialDatalog.Pos
import Language.DifferentialDatalog.Syntax
import {-# SOURCE #-} Language.DifferentialDatalog.Type
import {-# SOURCE #-} Language.DifferentialDatalog.Expr
import Language.DifferentialDatalog.ECtx
import Language.DifferentialDatalog.Util
import Language.DifferentialDatalog.NS
import Language.DifferentialDatalog.Validate

ruleRHSVars :: DatalogProgram -> Rule -> Int -> [Field]
ruleRHSVars d rl i = S.toList $ ruleRHSVarSet d rl i

-- | Variables visible in the 'i'th conjunct in the right-hand side of
-- a rule.  All conjuncts before 'i' must be validated before calling this
-- function.
ruleRHSVarSet :: DatalogProgram -> Rule -> Int -> S.Set Field
ruleRHSVarSet d rl i = ruleRHSVarSet' d rl (i-1)

-- Variables visible _after_ 'i'th conjunct.
ruleRHSVarSet' :: DatalogProgram -> Rule -> Int -> S.Set Field
ruleRHSVarSet' _ rl i | i < 0 = S.empty
ruleRHSVarSet' d rl i = 
    case ruleRHS rl !! i of
         RHSLiteral True  a            -> vs `S.union` (atomVarDecls d rl i)
         RHSLiteral False _            -> vs
         -- assignment introduces new variables
         RHSCondition (E e@(ESet _ l _)) -> vs `S.union` exprDecls d (CtxSetL e (CtxRuleRCond rl i)) l
         -- condition does not introduce new variables
         RHSCondition _                -> vs
         -- FlatMap introduces a variable
         RHSFlatMap v e                -> let t = case exprType' d (CtxRuleRFlatMap rl i) e of
                                                       TOpaque _ _         [t]     -> t
                                                       TOpaque _ "std.Map" [kt,vt] -> tTuple [kt,vt]
                                          in S.insert (Field nopos v t) vs
         -- Aggregation hides all variables except groupBy vars
         -- and the aggregate variable
         RHSAggregate gvars avar fname e -> let ctx = CtxRuleRAggregate rl i
                                                gvars' = map (getVar d ctx) gvars
                                                f = getFunc d fname
                                                tmap = ruleAggregateTypeParams d rl i
                                                atype = typeSubstTypeArgs tmap $ funcType f
                                                avar' = Field nopos avar atype
                                            in S.fromList $ avar':gvars'
    where
    vs = ruleRHSVarSet d rl i

ruleAggregateTypeParams :: DatalogProgram -> Rule -> Int -> M.Map String Type
ruleAggregateTypeParams d rl idx = 
    case ruleCheckAggregate d rl idx of
         Left e -> error $ "ruleAggregateTypeParams: " ++ e
         Right tmap -> tmap

exprDecls :: DatalogProgram -> ECtx -> Expr -> S.Set Field
exprDecls d ctx e = 
    S.fromList
        $ map (\(v, ctx') -> Field nopos v $ exprType' d ctx' (eVarDecl v)) 
        $ exprVarDecls ctx e

exprVarTypes :: DatalogProgram -> ECtx -> Expr -> [Field]
exprVarTypes d ctx e = 
    map (\(v, ctx) -> Field nopos v $ exprType d ctx (eVar v)) 
        $ exprVarOccurrences ctx e

atomVarDecls :: DatalogProgram -> Rule -> Int -> S.Set Field
atomVarDecls d rl i = 
    S.fromList
        $ exprVarTypes d (CtxRuleRAtom rl i) 
        $ atomVal $ rhsAtom $ ruleRHS rl !! i

-- | Variables used in a RHS term of a rule
ruleRHSTermVars :: Rule -> Int -> [String]
ruleRHSTermVars rl i = 
    case ruleRHS rl !! i of
         RHSLiteral{..}   -> exprVars $ atomVal rhsAtom
         RHSCondition{..} -> exprVars rhsExpr
         RHSFlatMap{..}   -> exprVars rhsMapExpr
         RHSAggregate{..} -> nub $ rhsGroupBy ++ exprVars rhsAggExpr
 
-- | All variables visible after the last RHS clause of the rule
ruleVars :: DatalogProgram -> Rule -> [Field]
ruleVars d rl@Rule{..} = ruleRHSVars d rl (length ruleRHS)

-- | Variables used in the head of the rule
ruleLHSVars :: DatalogProgram -> Rule -> [Field]
ruleLHSVars d rl = S.toList $ ruleLHSVarSet d rl

ruleLHSVarSet :: DatalogProgram -> Rule -> S.Set Field
ruleLHSVarSet d rl = S.fromList 
    $ concat 
    $ mapIdx (\a i -> exprVarTypes d (CtxRuleL rl i) $ atomVal a) 
    $ ruleLHS rl

-- | Map function over all types in a rule
ruleTypeMapM :: (Monad m) => (Type -> m Type) -> Rule -> m Rule
ruleTypeMapM fun rule@Rule{..} = do
    lhs <- mapM (\(Atom p r v) -> Atom p r <$> exprTypeMapM fun v) ruleLHS
    rhs <- mapM (\rhs -> case rhs of
                  RHSLiteral pol (Atom p r v) -> (RHSLiteral pol . Atom p r) <$> exprTypeMapM fun v
                  RHSCondition c              -> RHSCondition <$> exprTypeMapM fun c
                  RHSAggregate g v f e        -> RHSAggregate g v f <$> exprTypeMapM fun e
                  RHSFlatMap v e              -> RHSFlatMap v <$> exprTypeMapM fun e)
                ruleRHS
    return rule { ruleLHS = lhs, ruleRHS = rhs }

-- | true iff the rule contains at least one join or antijoin operation
ruleHasJoins :: Rule -> Bool
ruleHasJoins rule =
    any (\case
          RHSLiteral{} -> True
          _            -> False)
    $ tail $ ruleRHS rule
