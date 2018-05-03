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

{-# LANGUAGE RecordWildCards #-}

module Language.DifferentialDatalog.Rule (
    ruleRHSVars,
    ruleVars
) where

import qualified Data.Set as S

import Language.DifferentialDatalog.Pos
import Language.DifferentialDatalog.Syntax
import {-# SOURCE #-} Language.DifferentialDatalog.Type
import {-# SOURCE #-} Language.DifferentialDatalog.Expr
import Language.DifferentialDatalog.ECtx

import Debug.Trace

ruleRHSVars :: DatalogProgram -> Rule -> Int -> [Field]
ruleRHSVars d rl i = S.toList $ ruleRHSVarSet d rl i

-- | Variables visible in the 'i'th conjunct in the right-hand side of
-- a rule.  All conjuncts before 'i' be validated before calling this
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
         RHSFlatMap v e                -> let TOpaque _ _ [t] = exprType' d (CtxRuleRFlatMap rl i) e
                                          in S.insert (Field nopos v t) vs
         -- Aggregation hides all variables except groupBy vars
         -- and the aggregate variable
         RHSAggregate{}                -> error "ruleRHSVarSet' RHSAggregate: not implemented"
    where
    vs = ruleRHSVarSet d rl i


exprDecls :: DatalogProgram -> ECtx -> Expr -> S.Set Field
exprDecls d ctx e = 
    S.fromList
        $ map (\(v, ctx') -> Field nopos v $ exprType' d ctx' (eVarDecl v)) 
        $ exprVarDecls ctx e

exprVarTypes :: DatalogProgram -> ECtx -> Expr -> [Field]
exprVarTypes d ctx e = 
    map (\(v, ctx) -> Field nopos v $ exprType' d ctx (eVar v)) 
        $ exprVars ctx e

atomVarDecls :: DatalogProgram -> Rule -> Int -> S.Set Field
atomVarDecls d rl i = 
    S.fromList
        $ concatMap (\(f,v) -> exprVarTypes d (CtxRuleRAtom rl i f) v) 
        $ atomArgs $ rhsAtom $ ruleRHS rl !! i
 
-- | All variables defined in a rule
ruleVars :: DatalogProgram -> Rule -> [Field]
ruleVars d rl@Rule{..} = ruleRHSVars d rl (length ruleRHS)
