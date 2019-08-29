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
    rulePPPrefix,
    ruleRHSVars,
    ruleRHSNewVars,
    ruleVars,
    ruleRHSTermVars,
    ruleLHSVars,
    ruleTypeMapM,
    ruleHasJoins,
    ruleAggregateTypeParams,
    atomVarOccurrences,
    atomVars,
    ruleIsDistinctByConstruction,
    ruleIsRecursive
) where

import qualified Data.Set as S
import qualified Data.Map as M
import Data.List
--import Debug.Trace
import Text.PrettyPrint

import Language.DifferentialDatalog.Pos
import Language.DifferentialDatalog.PP
import Language.DifferentialDatalog.Name
import Language.DifferentialDatalog.Syntax
import {-# SOURCE #-} Language.DifferentialDatalog.Type
import {-# SOURCE #-} Language.DifferentialDatalog.Expr
import Language.DifferentialDatalog.Util
import Language.DifferentialDatalog.NS
import Language.DifferentialDatalog.Validate
import Language.DifferentialDatalog.Relation

-- | Pretty-print the first 'len' literals of a rule. 
rulePPPrefix :: Rule -> Int -> Doc
rulePPPrefix rl len = commaSep $ map pp $ take len $ ruleRHS rl

-- | New variables declared in the 'i'th conjunct in the right-hand
-- side of a rule.
ruleRHSNewVars :: DatalogProgram -> Rule -> Int -> [Field]
ruleRHSNewVars d rule idx =
    S.toList $ ruleRHSVarSet' d rule idx S.\\ ruleRHSVarSet' d rule (idx-1)

ruleRHSVars :: DatalogProgram -> Rule -> Int -> [Field]
ruleRHSVars d rl i = S.toList $ ruleRHSVarSet d rl i

-- | Variables visible in the 'i'th conjunct in the right-hand side of
-- a rule.  All conjuncts before 'i' must be validated before calling this
-- function.
ruleRHSVarSet :: DatalogProgram -> Rule -> Int -> S.Set Field
ruleRHSVarSet d rl i = ruleRHSVarSet' d rl (i-1)

-- Variables visible _after_ 'i'th conjunct.
ruleRHSVarSet' :: DatalogProgram -> Rule -> Int -> S.Set Field
ruleRHSVarSet' _ _  i | i < 0 = S.empty
ruleRHSVarSet' d rl i =
    case ruleRHS rl !! i of
         RHSLiteral True  _            -> vs `S.union` (atomVarDecls d rl i)
         RHSLiteral False _            -> vs
         -- assignment introduces new variables
         RHSCondition (E e@(ESet _ l _)) -> vs `S.union` exprDecls d (CtxSetL e (CtxRuleRCond rl i)) l
         -- condition does not introduce new variables
         RHSCondition _                -> vs
         -- FlatMap introduces a variable
         RHSFlatMap v e                -> let t = case exprType' d (CtxRuleRFlatMap rl i) e of
                                                       TOpaque _ _         [t']    -> t'
                                                       TOpaque _ tname     [kt,vt] | tname == mAP_TYPE
                                                                                   -> tTuple [kt,vt]
                                                       t' -> error $ "Rule.ruleRHSVarSet': unexpected FlatMap type " ++ show t'
                                          in S.insert (Field nopos v t) vs
         -- Aggregation hides all variables except groupBy vars
         -- and the aggregate variable
         RHSAggregate avar gvars fname _ -> let ctx = CtxRuleRAggregate rl i
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
        $ map (\(v, ctx') -> Field nopos v $ exprType d ctx' (eVarDecl v))
        $ exprVarDecls ctx e

atomVarTypes :: DatalogProgram -> ECtx -> Expr -> [Field]
atomVarTypes d ctx e =
    map (\(v, ctx') -> Field nopos v $ exprType d ctx' (eVar v))
        $ atomVarOccurrences ctx e

atomVarOccurrences :: ECtx -> Expr -> [(String, ECtx)]
atomVarOccurrences ctx e =
    exprCollectCtx (\ctx' e' ->
                    case e' of
                         EVar _ v       -> [(v, ctx')]
                         EBinding _ v _ -> [(v, ctx')]
                         _              -> [])
                   (++) ctx e

atomVars :: Expr -> [String]
atomVars e =
    exprCollect (\case
                  EVar _ v       -> [v]
                  EBinding _ v _ -> [v]
                  _              -> [])
                (++) e

atomVarDecls :: DatalogProgram -> Rule -> Int -> S.Set Field
atomVarDecls d rl i = S.fromList $ atomVarTypes d (CtxRuleRAtom rl i) (atomVal $ rhsAtom $ ruleRHS rl !! i)

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
    $ mapIdx (\a i -> atomVarTypes d (CtxRuleL rl i) $ atomVal a)
    $ ruleLHS rl

-- | Map function over all types in a rule
ruleTypeMapM :: (Monad m) => (Type -> m Type) -> Rule -> m Rule
ruleTypeMapM fun rule@Rule{..} = do
    lhs <- mapM (\(Atom p r v) -> Atom p r <$> exprTypeMapM fun v) ruleLHS
    rhs <- mapM (\rhs -> case rhs of
                  RHSLiteral pol (Atom p r v) -> (RHSLiteral pol . Atom p r) <$> exprTypeMapM fun v
                  RHSCondition c              -> RHSCondition <$> exprTypeMapM fun c
                  RHSAggregate v g f e        -> RHSAggregate v g f <$> exprTypeMapM fun e
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
        exprIsInjective d (S.fromList $ map name $ ruleVars d rl) (atomVal head_atom)
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
ruleIsRecursive :: DatalogProgram -> Rule -> Int -> Bool
ruleIsRecursive d Rule{..} head_idx =
    let head_atom = ruleLHS !! head_idx in
    any (relsAreMutuallyRecursive d (atomRelation head_atom))
        $ map (atomRelation . rhsAtom)
        $ filter rhsIsLiteral ruleRHS
