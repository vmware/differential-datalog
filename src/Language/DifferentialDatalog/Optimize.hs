{-
Copyright (c) 2018-2021 VMware, Inc.
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

{-# LANGUAGE TupleSections, LambdaCase, RecordWildCards, ImplicitParams #-}

{- |
Module     : Optimize
Description: Compiler optimization passes
-}
module Language.DifferentialDatalog.Optimize (
    optimize
)
where

import Data.List
import Data.Maybe
import Control.Monad.State
import qualified Data.Map as M
--import Debug.Trace

import Language.DifferentialDatalog.Config
import Language.DifferentialDatalog.Crate
import Language.DifferentialDatalog.Module
import Language.DifferentialDatalog.Pos
import Language.DifferentialDatalog.Name
import Language.DifferentialDatalog.Type
import Language.DifferentialDatalog.Syntax
import Language.DifferentialDatalog.Rule
import Language.DifferentialDatalog.Validate

optimize :: (?cfg::Config, ?crate_graph::CrateGraph) => DatalogProgram -> DatalogProgram
optimize d =
    let d' = optEliminateCommonPrefixes $ optExpandMultiheadRules d
    in if confReValidate ?cfg
       then case validate d' of
                 Left err -> error $ "could not validate optimized spec: " ++ err
                 Right _  -> d'
       else d'

-- | Replace multihead rules with several rules by introducing an
-- intermediate relation for the body of the rule.
optExpandMultiheadRules :: DatalogProgram -> DatalogProgram
optExpandMultiheadRules d = optExpandMultiheadRules' d 0

optExpandMultiheadRules' :: DatalogProgram -> Int -> DatalogProgram
optExpandMultiheadRules' d@DatalogProgram{progRules=[]} _ = d
optExpandMultiheadRules' d@DatalogProgram{progRules=r:rs} i
    | length (ruleLHS r) == 1 = progAddRules [r] d'
    | otherwise               = progAddRules rules $ maybe d' (\rel -> progAddRel rel d') $ mrel
    where d' = optExpandMultiheadRules' d{progRules = rs} (i+1)
          (mrel, rules) = expandMultiheadRule d r i

-- Only introduce intermediate relations if the rule has joins or antijoins.
-- Other rules are heuristically considered cheap to compute vs the cost of maintaining
-- an extra arrangement.
expandMultiheadRule :: DatalogProgram -> Rule -> Int -> (Maybe Relation, [Rule])
expandMultiheadRule d rl ruleidx = (Just rel, rule1 : rules)
    where
    lhs_pos = (fst $ pos $ head $ ruleLHS rl, snd $ pos $ last $ ruleLHS rl)
    -- variables used in the LHS of the rule
    lhsvars = ruleLHSVars d rl
    -- generate relation
    relname = "__MultiHead_" ++ show ruleidx
    rel = Relation { relPos        = nopos
                   , relAttrs      = []
                   , relRole       = RelInternal
                   , relSemantics  = RelSet
                   , relName       = relname
                   , relType       = tTuple $ map (varType d) lhsvars
                   , relPrimaryKey = Nothing
                   }
    -- rule to compute the new relation
    rule1 = Rule { rulePos = nopos
                 , ruleModule = ruleModule rl
                 , ruleLHS = [RuleLHS lhs_pos (Atom nopos relname delayZero False $ eTuple $ map (\v -> eTypedVar (name v) (varType d v)) lhsvars) Nothing]
                 , ruleRHS = ruleRHS rl
                 }
    -- rule per head of the original rule
    rules = map (\lhs -> Rule { rulePos = pos rl
                              , ruleModule = ruleModule rl
                              , ruleLHS = [lhs]
                              , ruleRHS = [RHSLiteral nopos True
                                          $ Atom nopos relname delayZero False
                                          $ eTuple $ map (\v -> eTypedVar (name v) (varType d v)) lhsvars]})
                $ ruleLHS rl

-- | Common prefix elimination.
-- Implements the following greedy algorithm:
--  collect all prefixes, order by length
--  find the longest prefix that occurs in multiple rules
--     if found:
--        factor it into a separate relation
--        repeat
--     else:
--        done
--
-- TODO: this optimization must be preceeded by rule normalization to detect different, but
-- equivalent prefixes.
--
optEliminateCommonPrefixes :: (?crate_graph::CrateGraph) => DatalogProgram -> DatalogProgram
optEliminateCommonPrefixes d = evalState (optEliminateCommonPrefixes' d) 0

type RulePrefix = [RuleRHS]

optEliminateCommonPrefixes' :: (?crate_graph::CrateGraph) => DatalogProgram -> State Int DatalogProgram
optEliminateCommonPrefixes' d = do
    let ordered_prefixes = reverse -- longest prefix first
                           $ sortOn (length . fst)
                           $ map fst
                           $ filter ((>1) . snd)
                           $ collectPrefixes d
    --trace ("ordered_prefixes:\n" ++ show ordered_prefixes) $
    if null ordered_prefixes
       then return d
       else do d' <- replacePrefix d $ head ordered_prefixes
               optEliminateCommonPrefixes' d'

-- Collect all prefixes in the program, only including prefixes of length >1
-- (to avoid infinite recursion replacing prefix of length 1)
-- counting the number of occurrences of each prefix in each crate.
-- We group prefixes per crate, as we currently don't have a way to generate
-- intermediate relations available from multiple crates.
collectPrefixes :: (?crate_graph::CrateGraph) => DatalogProgram -> [((RulePrefix, String), Int)]
collectPrefixes d =
    M.toList
    $ foldl' (\prefs rule -> foldl' (\prefs' pref -> addPrefix pref (crateName $ cgModuleCrate ?crate_graph $ ruleModule rule) prefs')
                                    prefs $ rulePrefixes rule)
             M.empty $ progRules d

addPrefix :: RulePrefix -> String -> M.Map (RulePrefix, String) Int -> M.Map (RulePrefix, String) Int
addPrefix p crate_name prefixes =
    M.alter (\case
              Nothing -> Just 1
              Just n  -> Just $ n + 1)
            (p, crate_name) prefixes

-- A prefix must contain >1 components, must be followed by at least one component.
-- The component following the prefix must be a join or an antijoin (we don't want to
-- break a sequence of flatmaps/filters/assignments/aggregations).
rulePrefixes :: Rule -> [RulePrefix]
rulePrefixes Rule{..} =
    filter (\pref -> rhsIsLiteral $ head $ drop (length pref) ruleRHS)
    $ filter (\pref -> length pref > 1 && length pref < length ruleRHS)
    $ inits ruleRHS

-- Replace prefix with a fresh relation
replacePrefix :: (?crate_graph::CrateGraph) => DatalogProgram -> (RulePrefix, String) -> State Int DatalogProgram
replacePrefix d (pref, crate_name) = {-trace ("replacePrefix " ++ show pref) $-} do
    let pref_pos = (fst $ pos $ head pref, snd $ pos $ last pref)
    let crate = fromJust $ cgLookupCrate ?crate_graph crate_name
    -- Module to place the new relation in.
    let mname = crateMainModule crate
    let pref_len = length pref
    -- allocate relation name
    idx <- get
    put $ idx + 1
    let relname = scoped mname $ "__Prefix_" ++ show idx
    -- variables visible in the rest of the rule
    -- (manufacture a bogus rule consisting only of the prefix to call ruleRHSVars on it)
    let vars = ruleRHSVars d (Rule nopos mname [] pref) pref_len
    -- relation
    let rel = Relation { relPos        = pref_pos
                       , relAttrs      = []
                       , relRole       = RelInternal
                       , relSemantics  = RelSet
                       , relName       = relname
                       , relType       = tTuple $ map (varType d) vars
                       , relPrimaryKey = Nothing
                       }
    -- rule
    let atom = Atom { atomPos      = pref_pos
                    , atomRelation = relname
                    , atomDelay    = delayZero
                    , atomDiff     = False
                    , atomVal      = eTuple $ map (\v -> eTypedVar (name v) (varType d v)) vars
                    }
    let rule = Rule { rulePos      = pref_pos
                    , ruleModule   = mname
                    , ruleLHS      = [RuleLHS pref_pos atom Nothing]
                    , ruleRHS      = pref
                    }
    -- replace prefix in all rules
    let rules' = map (\rule' -> if isPrefixOf pref $ ruleRHS rule'
                                  then rule' {ruleRHS = RHSLiteral pref_pos True atom : (drop pref_len $ ruleRHS rule')}
                                  else rule')
                 $ progRules d
    return d{ progRules = rule:rules'
            , progRelations = M.insert relname rel $ progRelations d}
