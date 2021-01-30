{-
Copyright (c) 2020-2021 VMware, Inc.
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
Module     : Debug
Description: Helper functions for adding debug hooks to a 'DatalogProgram'.
-}
module Language.DifferentialDatalog.Debug (
    debugUpdateRHSRules,
)
where

import Data.Maybe
import Data.Char
import Text.PrettyPrint

import {-# SOURCE #-} qualified Language.DifferentialDatalog.Compile as Compile
import Language.DifferentialDatalog.Syntax
import Language.DifferentialDatalog.Module
import Language.DifferentialDatalog.Var
import {-# SOURCE #-} Language.DifferentialDatalog.Type

-- For RHSLiteral, a binding to the expression is inserted if it's not bound to a variable.
-- For example, R(a, b, z, _) gets transformed into __r0 in R(a, b, z, _),
addBindingToRHSLiteral :: (RuleRHS, Int) -> RuleRHS
addBindingToRHSLiteral (r@(RHSLiteral True _), index) =
  let
    bindingName = "__" ++ (show index) ++ (render $ Compile.rnameFlat $ map toLower $ atomRelation $ rhsAtom r)
    expr = atomVal $ rhsAtom r
    exprNode = enode expr
    updatedAtomVal = case exprNode of
                     EBinding{} -> expr
                     _          -> eBinding bindingName expr
    updatedAtom = (rhsAtom r) { atomVal = updatedAtomVal }
  in r { rhsAtom = updatedAtom }
addBindingToRHSLiteral (rule, _) = rule

-- Transform RHSGroupBy to output a vector of input records in addition to the
-- user-defined group by rewriting:
-- 'var g = e.group_by(v1,v2)'
-- as
-- 'var __debug_g = (e, r).group_by(v1,v2),
--  (var input, var g) = debug_split_group(__debug_g),'
updateRHSGroupBy :: DatalogProgram -> Rule -> Int -> [(RuleRHS, Int)]
updateRHSGroupBy d rule rhsidx =
  let
     r = (ruleRHS rule) !! rhsidx
     aggVar = GroupVar rule rhsidx
     group_var = "__debug_" ++ rhsVar r
     input1 = head $ Compile.recordAfterPrefix d rule (rhsidx - 1)
     input = eTuple [ input1, rhsProject r ]
     rAgg = RHSGroupBy { rhsVar     = group_var
                       , rhsProject = input
                       , rhsGroupBy = rhsGroupBy r
                       }
     tinputs = tOpaque (mOD_STD ++ "::Vec") [exprType d (CtxRuleRProject rule rhsidx) input1]
     rCond = RHSCondition { rhsExpr = eSet (eTuple [eVarDecl "__inputs" tinputs, eVarDecl (rhsVar r) (varType d aggVar)])
                                           (eApplyFunc "debug::debug_split_group" [eVar group_var]) }
  in case r of
     RHSGroupBy{} -> [(rAgg, rhsidx), (rCond, rhsidx)]
     _ -> [(r, rhsidx)]

-- OperatorID is a tuple composed of rule index, rhs index and head index.
generateOperatorIdExpr :: Int -> Int -> Int -> Expr
generateOperatorIdExpr rlIdx rhsIdx headIdx =
  eTuple [eBit 32 $ toInteger rlIdx, eBit 32 $ toInteger rhsIdx, eBit 32 $ toInteger headIdx]

ddlogWeightExpr :: Expr
ddlogWeightExpr = eVar "ddlog_weight"

ddlogTimestampExpr :: Expr
ddlogTimestampExpr = eVar "ddlog_timestamp"

generateInspectDebugJoin :: DatalogProgram -> Int -> Rule -> Int -> Int -> [RuleRHS]
generateInspectDebugJoin d ruleIdx rule preRhsIdx index =
  let
    input1 = head $ Compile.recordAfterPrefix d rule (index - 1)
    input2 = eVar $ exprVar $ enode $ atomVal $ rhsAtom (ruleRHS rule !! index)
    outputs = Compile.recordAfterPrefix d rule index
  in map (\i -> RHSInspect {rhsInspectExpr = eApplyFunc "debug::debug_event_join"
                                             [generateOperatorIdExpr ruleIdx preRhsIdx i,
                                              ddlogWeightExpr,
                                              ddlogTimestampExpr,
                                              input1,
                                              input2,
                                              outputs !! i]}) [0..length outputs -1]

generateInspectDebug :: DatalogProgram -> Int -> Rule -> Int -> Int -> [RuleRHS]
generateInspectDebug d ruleIdx rule preRhsIdx index =
  let
    input1 = if index == 0
                then eVar $ exprVar $ enode $ atomVal $ rhsAtom $ head $ ruleRHS rule
                else head $ Compile.recordAfterPrefix d rule (index - 1)
    opType = if index == 0
                then "Map"
                else case (ruleRHS rule) !! index of
                     RHSLiteral{rhsPolarity=False} -> "Antijoin"
                     RHSInspect{} -> "Inspect"
                     RHSFlatMap{} -> "Flatmap"
                     RHSCondition{} -> "Condition"
                     rhs -> error $ "generateInspectDebug " ++ show rhs
    outputs = Compile.recordAfterPrefix d rule index
  in map (\i -> RHSInspect {rhsInspectExpr = eApplyFunc "debug::debug_event"
                                             [generateOperatorIdExpr ruleIdx preRhsIdx i,
                                              ddlogWeightExpr,
                                              ddlogTimestampExpr,
                                              eString opType,
                                              input1,
                                              outputs !! i]}) [0..length outputs - 1]


generateInspectDebugGroupBy :: DatalogProgram -> Int -> Rule -> Int -> Int -> [RuleRHS]
generateInspectDebugGroupBy d ruleIdx rule preRhsIdx index =
  let outputs = Compile.recordAfterPrefix d rule index
  in map (\i -> RHSInspect {rhsInspectExpr = eApplyFunc "debug::debug_event"
                                             [generateOperatorIdExpr ruleIdx preRhsIdx i,
                                              ddlogWeightExpr,
                                              ddlogTimestampExpr,
                                              eString "Aggregate",
                                              eVar "__inputs",
                                              outputs !! i]}) [0..length outputs -1]

mkInspect :: DatalogProgram -> Int -> Rule -> Int -> Int -> Maybe [RuleRHS]
mkInspect d ruleIdx rule preRhsIdx index =
  let rhsRule = ruleRHS rule
  in if index == 0 && index < length rhsRule - 1
        then Nothing
        else if rhsIsCondition (rhsRule !! index) && index /= length rhsRule - 1 && rhsIsCondition (rhsRule !! (index + 1))
                then Nothing
                else if index == 0
                     then Just $ generateInspectDebug d ruleIdx rule preRhsIdx index -- single term rule
                     else case (rhsRule !! (index - 1), rhsRule !! index) of
                          -- Insert Inspect after 'debug_split_group', not right after group_by.
                          (_, RHSGroupBy{}) -> Nothing
                          (RHSGroupBy{}, _) -> Just $ generateInspectDebugGroupBy d ruleIdx rule preRhsIdx index -- group_by
                          (_, RHSLiteral{rhsPolarity=True}) -> Just $ generateInspectDebugJoin d ruleIdx rule preRhsIdx index -- join
                          _ -> Just $ generateInspectDebug d ruleIdx rule preRhsIdx index -- antijoin, flatmap, filter/assignment, inspect

-- Insert inspect debug hook after each RHS term, except for the following:
-- 1. If a group of conditions appear consecutively, inspect debug hook is only
-- inserted after the last condition in the group.
-- 2. Inspect debug hook is not inserted after the first term, unless the rule
-- only contains one literal.
-- 3. If a rule has multiple heads, then multiple inspect is inserted after the last
-- term corresponding to each head.
insertRHSInspectDebugHooks :: DatalogProgram -> Int -> Rule -> [Int] -> [RuleRHS]
insertRHSInspectDebugHooks d rlIdx rule rhsIdxs =
  concatMap (\i -> let inspect = concat $ maybeToList $ mkInspect d rlIdx rule (rhsIdxs !! i) i in
                   (ruleRHS rule !! i) : inspect) [0..length (ruleRHS rule) - 1]

debugUpdateRHSRules :: DatalogProgram -> Int -> Rule -> [RuleRHS]
debugUpdateRHSRules d rlIdx rule =
  let
    -- First pass updates RHSLiteral without any binding with a binding.
    rhs = map addBindingToRHSLiteral $ zip (ruleRHS rule) [0..]
    -- Second pass updates RHSGroupBy to include inputs in the group.
    (rhs', preRhsIdxs) = unzip $ concatMap (updateRHSGroupBy d rule{ruleRHS = rhs}) [0..length rhs - 1]
  in insertRHSInspectDebugHooks d rlIdx rule {ruleRHS = rhs'} preRhsIdxs
