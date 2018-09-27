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
Module     : Optimize
Description: Compiler optimization passes
-}
module Language.DifferentialDatalog.Optimize (
    optExpandMultiheadRules
) 
where

import Language.DifferentialDatalog.Pos
import Language.DifferentialDatalog.Name
import Language.DifferentialDatalog.Type
import Language.DifferentialDatalog.Syntax
import Language.DifferentialDatalog.Rule

-- | Replace multihead rules with several rules by introducing an
-- intermediate relation for the body of the rule.
optExpandMultiheadRules :: DatalogProgram -> DatalogProgram
optExpandMultiheadRules d = optExpandMultiheadRules' d 0

optExpandMultiheadRules' :: DatalogProgram -> Int -> DatalogProgram
optExpandMultiheadRules' d@DatalogProgram{progRules=[], ..} _ = d
optExpandMultiheadRules' d@DatalogProgram{progRules=r:rs, ..} i
    | length (ruleLHS r) == 1 = progAddRules [r] d'
    | otherwise               = progAddRules rules $ maybe d' (\rel -> progAddRel rel d') $ mrel
    where d' = optExpandMultiheadRules' d{progRules = rs} (i+1)
          (mrel, rules) = expandMultiheadRule d r i

-- Only introduce intermediate relations if the rule has joins or antijoins.
-- Other rules are heuristically considered cheap to compute vs the cost of maintaining
-- an extra arrangement.
expandMultiheadRule :: DatalogProgram -> Rule -> Int -> (Maybe Relation, [Rule])
expandMultiheadRule d rl ruleidx | ruleHasJoins rl = (Just rel, rule1 : rules)
    where
    -- variables used in the LHS of the rule
    lhsvars = ruleLHSVars d rl
    -- generate relation
    relname = "Rule_" ++ show ruleidx
    rel = Relation { relPos      = nopos
                   , relGround   = False
                   , relName     = relname
                   , relType     = tTuple $ map typ lhsvars
                   , relDistinct = False
                   }
    -- rule to compute the new relation
    rule1 = Rule { rulePos = nopos
                 , ruleLHS = [Atom nopos relname $ eTuple $ map (eVar . name) lhsvars]
                 , ruleRHS = ruleRHS rl
                 }
    -- rule per head of the original rule
    rules = map (\atom -> Rule { rulePos = pos rl
                               , ruleLHS = [atom]
                               , ruleRHS = [RHSLiteral True 
                                           $ Atom nopos relname 
                                           $ eTuple $ map (eVar . name) lhsvars]})
                $ ruleLHS rl
expandMultiheadRule d rl ruleidx = (Nothing, rules)
    where
    rules = map (\atom -> Rule { rulePos = pos rl
                               , ruleLHS = [atom]
                               , ruleRHS = ruleRHS rl})
                $ ruleLHS rl
