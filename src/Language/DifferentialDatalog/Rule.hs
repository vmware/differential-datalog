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

import Language.DifferentialDatalog.Syntax

-- | Variables visible in the 'i'th conjunct in the right-hand side of
-- a rule
ruleRHSMVars :: Rule -> Int -> [MField]
ruleRHSMVars rl i = S.toList $ ruleRHSVars' rl (i-1)

-- Variables visible _after_ 'i'th conjunct.
ruleRHSMVars' :: Rule -> Int -> S.Set Field
ruleRHSMVars' rl i | i < 0 = S.empty
ruleRHSMVars' rl i = 
    case ruleRHS rl !! i of
         RHSLiteral _ a               -> vs `S.union` (S.fromList $ atomVarDecls a)
         -- assignment introduces new variables
         RHSCondition (E ESet{_ l _}) -> vs `S.union` exprVars l
         -- condition does not introduce new variables
         RHSCondition _               -> vs
         -- FlatMap introduces a variable
         RHSFlatMap l _               -> vs `S.union` exprVars l
         -- Aggregation hides all variables except groupBy vars
         -- and the aggregate variable
         RHSAggregate gb v _          -> S.insert v $ S.fromList gb  
    where
    vs = ruleRHSMVars rl i
               
-- can return multiple occurrences to determine variables used and
-- declared in the same atom
atomVarDecls :: Atom -> [Field]
 
-- | All variables defined in a rule
ruleMVars :: Rule -> [MField]
ruleMVars rl@Rule{..} = ruleRHSMVars rl (length ruleRHS)
