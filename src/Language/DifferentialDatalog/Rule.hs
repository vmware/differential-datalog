{-
Copyright (c) 2018-2020 VMware, Inc.
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
    rulePPPrefix,
    ruleRHSVars,
    ruleRHSNewVars,
    ruleVars,
    ruleRHSTermVars,
    ruleLHSVars,
    ruleTypeMapM,
    ruleHasJoins,
    ruleAggregateTypeParams,
    ruleIsDistinctByConstruction,
    ruleHeadIsRecursive,
    ruleIsRecursive
) where

import qualified Data.Set as S
import qualified Data.Map as M
import Data.List
--import Debug.Trace
import Text.PrettyPrint

import Language.DifferentialDatalog.Pos
import Language.DifferentialDatalog.PP
import Language.DifferentialDatalog.Syntax
import {-# SOURCE #-} Language.DifferentialDatalog.Expr
import Language.DifferentialDatalog.Util
import Language.DifferentialDatalog.NS
import Language.DifferentialDatalog.Var
import Language.DifferentialDatalog.Relation
import Language.DifferentialDatalog.Function
import Language.DifferentialDatalog.Type

-- | Pretty-print the first 'len' literals of a rule. 
rulePPPrefix :: Rule -> Int -> Doc
rulePPPrefix rl len = commaSep $ map pp $ take len $ ruleRHS rl

-- | New variables declared in the 'i'th conjunct in the right-hand
-- side of a rule.
ruleRHSNewVars :: DatalogProgram -> Rule -> Int -> [Var]
ruleRHSNewVars d rule idx =
    ruleRHSVars' d rule idx \\ ruleRHSVars' d rule (idx-1)

-- | Variables visible in the 'i'th conjunct in the right-hand side of
-- a rule.  All conjuncts before 'i' must be validated before calling this
-- function.
ruleRHSVars :: DatalogProgram -> Rule -> Int -> [Var]
ruleRHSVars d rl i = ruleRHSVars' d rl (i-1)

-- Variables visible _after_ 'i'th conjunct.
ruleRHSVars' :: DatalogProgram -> Rule -> Int -> [Var]
ruleRHSVars' _ _  i | i < 0 = []
ruleRHSVars' d rl i =
    case ruleRHS rl !! i of
         RHSLiteral True  a            -> nub $ vs ++ (exprVarDecls d (CtxRuleRAtom rl i) (atomVal a))
         RHSLiteral False _            -> vs
         -- assignment introduces new variables
         RHSCondition (E e@(ESet _ l _)) -> nub $ vs ++ (exprVarDecls d (CtxSetL e (CtxRuleRCond rl i)) l)
         -- condition does not introduce new variables
         RHSCondition _                -> vs
         -- FlatMap introduces a variable
         RHSFlatMap _ _                -> FlatMapVar rl i : vs
         -- Inspect does not introduce new variables
         RHSInspect _                  -> vs
         -- Aggregation hides all variables except groupBy vars
         -- and the aggregate variable
         RHSAggregate _ grpby _ _      -> let ctx = CtxRuleRGroupBy rl i
                                              gvars' = exprVars d ctx grpby
                                              avar' = AggregateVar rl i
                                          in nub $ avar':gvars'
    where
    vs = ruleRHSVars d rl i

-- Compute type argument map for the aggregate function.
-- e.g., given an aggregate function:
-- extern function group2map(g: Group<('K,'V)>): Map<'K,'V>
--
-- and its invocation:
-- Aggregate4(x, map) :- AggregateMe1(x,y), Aggregate((x), map = group2map((x,y)))
--
-- compute concrete types for 'K and 'V
ruleAggregateTypeParams :: DatalogProgram -> Rule -> Int -> M.Map String Type
ruleAggregateTypeParams d rl idx =
    case funcTypeArgSubsts d (pos e) f [tOpaque gROUP_TYPE [group_by_type, exprType d ctx e]] of
         Left er -> error $ "ruleAggregateTypeParams: " ++ er
         Right tmap -> tmap
    where
    RHSAggregate _ group_by fname e = ruleRHS rl !! idx
    ctx = CtxRuleRAggregate rl idx
    gctx = CtxRuleRGroupBy rl idx
    group_by_type = exprType d gctx group_by
    f = getFunc d fname

-- | Variables used in a RHS term of a rule.
ruleRHSTermVars :: DatalogProgram -> Rule -> Int -> [Var]
ruleRHSTermVars d rl i =
    case ruleRHS rl !! i of
         RHSLiteral{..}   -> exprFreeVars d (CtxRuleRAtom rl i) $ atomVal rhsAtom
         RHSCondition{..} -> exprFreeVars d (CtxRuleRCond rl i) rhsExpr
         RHSFlatMap{..}   -> exprFreeVars d (CtxRuleRFlatMap rl i) rhsMapExpr
         RHSInspect{..}   -> exprFreeVars d (CtxRuleRInspect rl i) rhsInspectExpr
         RHSAggregate{..} -> nub $ exprVars d (CtxRuleRGroupBy rl i) rhsGroupBy ++
                                   exprFreeVars d (CtxRuleRAggregate rl i) rhsAggExpr

-- | All variables visible after the last RHS clause of the rule
ruleVars :: DatalogProgram -> Rule -> [Var]
ruleVars d rl@Rule{..} = ruleRHSVars d rl (length ruleRHS)

-- | Variables used in the head of the rule
ruleLHSVars :: DatalogProgram -> Rule -> [Var]
ruleLHSVars d rl =
    nub
    $ concat
    $ mapIdx (\a i -> exprFreeVars d (CtxRuleL rl i) $ atomVal a)
    $ ruleLHS rl

-- | Map function over all types in a rule
ruleTypeMapM :: (Monad m) => (Type -> m Type) -> Rule -> m Rule
ruleTypeMapM fun rule@Rule{..} = do
    lhs <- mapM (\(Atom p r v) -> Atom p r <$> exprTypeMapM fun v) ruleLHS
    rhs <- mapM (\rhs -> case rhs of
                  RHSLiteral pol (Atom p r v) -> (RHSLiteral pol . Atom p r) <$> exprTypeMapM fun v
                  RHSCondition c              -> RHSCondition <$> exprTypeMapM fun c
                  RHSAggregate v g f e        -> RHSAggregate v g f <$> exprTypeMapM fun e
                  RHSFlatMap v e              -> RHSFlatMap v <$> exprTypeMapM fun e
                  RHSInspect e                -> RHSInspect <$> exprTypeMapM fun e)
                ruleRHS
    return rule { ruleLHS = lhs, ruleRHS = rhs }

-- | true iff the rule contains at least one join or antijoin operation
ruleHasJoins :: Rule -> Bool
ruleHasJoins rule =
    any (\case
          RHSLiteral{} -> True
          _            -> False)
    $ tail $ ruleRHS rule

-- | Checks if a rule (more precisely, the given head of the rule) yields a
-- relation with distinct elements.
ruleIsDistinctByConstruction :: DatalogProgram -> Rule -> Int -> Bool
ruleIsDistinctByConstruction d rl@Rule{..} head_idx = f True 0
    where
    head_atom = ruleLHS !! head_idx
    headrel = atomRelation head_atom
    -- Relation is distinct wrt 'headrel'.
    relIsDistinct' :: String -> Bool
    relIsDistinct' rel =
        -- ..and we _are_ using 'rel' from the top-level scope.
        not (relsAreMutuallyRecursive d rel headrel) &&
        -- 'rel' is distinct in the top-level scope..
        relIsDistinct d (getRelation d rel)

    -- Recurse over the body of the rule, checking if the output of each
    -- prefix is a distinct relation.
    --
    -- If the first argument is 'Just vs', then the prefix of the body generates
    -- a distincts relation over 'vs'; if it is 'Nothing' then the prefix of the
    -- rule outputs a non-distinct relation.
    f :: Bool -> Int -> Bool
    f False i | i == length ruleRHS         = False
    f True  i | i == length ruleRHS         =
        -- The head of the rule is an injective function of all the variables in its body
        exprIsInjective d (CtxRuleL rl head_idx) (S.fromList $ ruleVars d rl) (atomVal head_atom)
    f True i | rhsIsCondition (ruleRHS !! i)
                                            = f True (i + 1)
    -- Aggregate operator returns a distinct collection over group-by and aggregate
    -- variables even if the prefix before isn't distinct.
    f _    i | rhsIsAggregate (ruleRHS !! i)= f True (i + 1)
    f True i | rhsIsPositiveLiteral (ruleRHS !! i)
                                            =
        let a = rhsAtom $ ruleRHS !! i in
        -- 'a' is a distinct relation and does not contain wildcards
        if relIsDistinct' (atomRelation a) && (not $ exprContainsPHolders $ atomVal a)
           then f True (i+1)
           else f False (i+1)
    -- Antijoins preserve distinctness
    f True i | rhsIsNegativeLiteral (ruleRHS !! i)
                                            = f True (i + 1)
    f _ i                                   = f False (i + 1)

-- | Checks if a rule (more precisely, the given head of the rule) is part of a
-- recursive fragment of the program.
ruleHeadIsRecursive :: DatalogProgram -> Rule -> Int -> Bool
ruleHeadIsRecursive d Rule{..} head_idx =
    let head_atom = ruleLHS !! head_idx in
    any (relsAreMutuallyRecursive d (atomRelation head_atom))
        $ map (atomRelation . rhsAtom)
        $ filter rhsIsLiteral ruleRHS

-- | Checks if any head of the rule is part of a
-- recursive fragment of the program.
ruleIsRecursive :: DatalogProgram -> Rule -> Bool
ruleIsRecursive d rl@Rule{..} =
    any (ruleHeadIsRecursive d rl) [0 .. length ruleLHS - 1]
