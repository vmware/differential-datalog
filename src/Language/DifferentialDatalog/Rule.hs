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

{-# LANGUAGE RecordWildCards, LambdaCase, FlexibleContexts #-}

module Language.DifferentialDatalog.Rule (
    rulePPPrefix,
    rulePPStripped,
    ruleRHSVars,
    ruleRHSNewVars,
    ruleVars,
    ruleRHSTermVars,
    ruleLHSVars,
    ruleTypeMapM,
    ruleHasJoins,
    ruleGroupByKeyType,
    ruleGroupByValType,
    ruleIsDistinctByConstruction,
    ruleHeadIsRecursive,
    ruleIsRecursive,
    rulePrefixIsStream,
    ruleBodyIsStream
) where

import Prelude hiding ((<>))
import qualified Data.Set as S
import Data.List
import Data.Functor.Identity
--import Debug.Trace
import Text.PrettyPrint
import Text.Parsec (sourceName, sourceLine, sourceColumn)

import Language.DifferentialDatalog.PP
import Language.DifferentialDatalog.Syntax
import {-# SOURCE #-} Language.DifferentialDatalog.Expr
import Language.DifferentialDatalog.Util
import Language.DifferentialDatalog.NS
import {-# SOURCE #-} Language.DifferentialDatalog.Var
import Language.DifferentialDatalog.Relation
import Language.DifferentialDatalog.Type
import Language.DifferentialDatalog.DatalogProgram

-- | Remove all type annotations from the rule.  Used to remove the numerous
-- annotations injected by type inference to cleanup the rule before
-- printing it (but also removes all user-defined type annotations, so the
-- resulting rule may no longer type check and should not be used for
-- anything other then pretty-printing).
ruleStripTypeAnnotations :: Rule -> Rule
ruleStripTypeAnnotations rule@Rule{..} =
    runIdentity $ ruleExprMapCtxM (\ctx e -> return $ exprStripTypeAnnotationsRec (E e) ctx) rule

-- | Pretty-print the first 'len' literals of a rule. 
rulePPPrefix :: Rule -> Int -> Doc
rulePPPrefix rule len = commaSep (map pp $ take len $ ruleRHS $ ruleStripTypeAnnotations rule) <+> location rule

-- | Pretty-print rule without type annotations.
rulePPStripped :: Rule -> Doc
rulePPStripped rule = pp (ruleStripTypeAnnotations rule) <+> location rule

location :: Rule -> Doc
location rule = char '@' <+> file <> char ':' <> line <> char ':' <> column
    where
        (position, _) = rulePos rule
        file = text (sourceName position)
        line = int (sourceLine position)
        column = int (sourceColumn position)

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
         RHSLiteral _ True  a            -> exprVarDecls d (CtxRuleRAtom rl i) (atomVal a) ++ vs
         RHSLiteral _ False _            -> vs
         -- assignment introduces new variables
         RHSCondition _ (E e@(ESet _ l _)) -> exprVarDecls d (CtxSetL e (CtxRuleRCond rl i)) l ++ vs
         -- condition does not introduce new variables
         RHSCondition _ _                -> vs
         -- FlatMap introduces variables
         RHSFlatMap _ pat _              -> exprVarDecls d (CtxRuleRFlatMapVars rl i) pat ++ vs
         -- Inspect does not introduce new variables
         RHSInspect _ _                  -> vs
         -- group_by hides all variables except group-by vars
         -- and the group variable
         RHSGroupBy _ _  _ grpby       -> let ctx = CtxRuleRGroupBy rl i
                                              gvars' = exprVars d ctx grpby
                                              avar' = GroupVar rl i
                                          in nub $ avar':gvars'
    where
    vs = ruleRHSVars d rl i

ruleGroupByKeyType :: DatalogProgram -> Rule -> Int -> Type
ruleGroupByKeyType d rl idx =
    exprType d gctx $ rhsGroupBy $ ruleRHS rl !! idx
    where
    gctx = CtxRuleRGroupBy rl idx

ruleGroupByValType :: DatalogProgram -> Rule -> Int -> Type
ruleGroupByValType d rl idx =
    exprType d ctx $ rhsProject $ ruleRHS rl !! idx
    where
    ctx = CtxRuleRProject rl idx

-- | Variables used in a RHS term of a rule.
ruleRHSTermVars :: DatalogProgram -> Rule -> Int -> [Var]
ruleRHSTermVars d rl i =
    case ruleRHS rl !! i of
         RHSLiteral{..}   -> exprFreeVars d (CtxRuleRAtom rl i) $ atomVal rhsAtom
         RHSCondition{..} -> exprFreeVars d (CtxRuleRCond rl i) rhsExpr
         RHSFlatMap{..}   -> exprFreeVars d (CtxRuleRFlatMap rl i) rhsMapExpr
         RHSInspect{..}   -> exprFreeVars d (CtxRuleRInspect rl i) rhsInspectExpr
         RHSGroupBy{..}   -> nub $ exprVars d (CtxRuleRGroupBy rl i) rhsGroupBy ++
                                   exprFreeVars d (CtxRuleRProject rl i) rhsProject

-- | All variables visible after the last RHS clause of the rule
ruleVars :: DatalogProgram -> Rule -> [Var]
ruleVars d rl@Rule{..} = ruleRHSVars d rl (length ruleRHS)

-- | Variables used in the head of the rule
ruleLHSVars :: DatalogProgram -> Rule -> [Var]
ruleLHSVars d rl =
    nub
    $ concat
    $ mapIdx (\RuleLHS{..} i -> exprFreeVars d (CtxRuleLAtom rl i) (atomVal lhsAtom) ++
                                maybe [] (exprFreeVars d (CtxRuleLLocation rl i)) lhsLocation)
    $ ruleLHS rl

-- | Map function over all types in a rule
ruleTypeMapM :: (Monad m) => (Type -> m Type) -> Rule -> m Rule
ruleTypeMapM fun rule@Rule{..} = do
    lhs <- mapM (\lhs@RuleLHS{..} -> do
                    ea <- exprTypeMapM fun $ atomVal lhsAtom
                    el <- mapM (exprTypeMapM fun) lhsLocation
                    return $ lhs { lhsAtom = lhsAtom { atomVal = ea }
                                 , lhsLocation = el}) ruleLHS
    rhs <- mapM (\case
                  RHSLiteral rp pol (Atom p r del diff v) -> RHSLiteral rp pol . Atom p r del diff <$> exprTypeMapM fun v
                  RHSCondition rp c                       -> RHSCondition rp <$> exprTypeMapM fun c
                  RHSGroupBy rp v p g                     -> RHSGroupBy rp v <$> exprTypeMapM fun p <*> exprTypeMapM fun g
                  RHSFlatMap rp vs e                      -> RHSFlatMap rp <$> exprTypeMapM fun vs <*> exprTypeMapM fun e
                  RHSInspect rp e                         -> RHSInspect rp <$> exprTypeMapM fun e)
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
-- D3log annotations must be flattened before calling this function.
ruleIsDistinctByConstruction :: DatalogProgram -> Rule -> Int -> Bool
ruleIsDistinctByConstruction d rl@Rule{..} head_idx = f True 0
    where
    head_atom = lhsAtom $ ruleLHS !! head_idx
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
    -- If the first argument is 'True', then the prefix of the body generates
    -- a distinct relation; if it is 'False' then the prefix of the
    -- rule outputs a non-distinct relation.
    f :: Bool -> Int -> Bool
    f False i | i == length ruleRHS         = False
    f True  i | i == length ruleRHS         =
        -- The head of the rule is an injective function of all the variables in its body
        exprIsInjective d (CtxRuleLAtom rl head_idx) (S.fromList $ ruleVars d rl) (atomVal head_atom)
    f True i | rhsIsCondition (ruleRHS !! i)
                                            = f True (i + 1)
    -- group-by operator returns a distinct collection over group-by and group
    -- variables even if the prefix before isn't distinct.
    f _    i | rhsIsGroupBy (ruleRHS !! i)= f True (i + 1)
    f True i | rhsIsPositiveLiteral (ruleRHS !! i)
                                            =
        let a = rhsAtom $ ruleRHS !! i in
        -- 'a' is a distinct relation and does not contain wildcards or
        -- differentiation.
        if relIsDistinct' (atomRelation a) && not (exprContainsPHolders $ atomVal a) && not (atomDiff a)
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
    let head_atom = lhsAtom $ ruleLHS !! head_idx in
    any (relsAreMutuallyRecursive d (atomRelation head_atom) . atomRelation . rhsAtom)
        $ filter (not . atomIsDelayed . rhsAtom)
        $ filter rhsIsLiteral ruleRHS

-- | Checks if any head of the rule is part of a
-- recursive fragment of the program.
ruleIsRecursive :: DatalogProgram -> Rule -> Bool
ruleIsRecursive d rl@Rule{..} =
    any (ruleHeadIsRecursive d rl) [0 .. length ruleLHS - 1]

-- | True iff the prefix of the rule of length 'n' produces a stream.
rulePrefixIsStream :: DatalogProgram -> Rule -> Int -> Bool
rulePrefixIsStream d rl n =
    any (\rhs -> rhsIsLiteral rhs &&
                 relIsStream (getRelation d $ atomRelation (rhsAtom rhs)) &&
                 -- Differentiating a stream yields a relation that contains
                 -- the latest delta in the stream.
                 not (atomDiff $ rhsAtom rhs))
        $ take n $ ruleRHS rl

-- | True iff the body of the rule yields a stream.
ruleBodyIsStream :: DatalogProgram -> Rule -> Bool
ruleBodyIsStream d rl = rulePrefixIsStream d rl (length $ ruleRHS rl)
