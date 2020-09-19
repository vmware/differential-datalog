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

{-# LANGUAGE RecordWildCards, FlexibleContexts #-}

module Language.DifferentialDatalog.NS(
    lookupType, checkType, getType,
    lookupFuncs, checkFuncs, getFuncs,
    lookupFunc, checkFunc, getFunc,
    lookupTransformer, checkTransformer, getTransformer,
    lookupVar, checkVar, getVar,
    lookupConstructor, checkConstructor, getConstructor,
    lookupRelation, checkRelation, getRelation,
    ctxVars, ctxAllVars
    ) where

import qualified Data.Map as M
import Data.List
import Data.Maybe
import Control.Monad.Except
--import Debug.Trace

import {-# SOURCE #-} Language.DifferentialDatalog.Expr
import Language.DifferentialDatalog.Error
import Language.DifferentialDatalog.Name
import Language.DifferentialDatalog.Pos
import {-# SOURCE #-} Language.DifferentialDatalog.Rule
import Language.DifferentialDatalog.Syntax
import Language.DifferentialDatalog.Var
import {-# SOURCE #-} Language.DifferentialDatalog.TypeInference
import {-# SOURCE #-} Language.DifferentialDatalog.Type

lookupType :: DatalogProgram -> String -> Maybe TypeDef
lookupType DatalogProgram{..} n = M.lookup n progTypedefs

checkType :: (MonadError String me) => Pos -> DatalogProgram -> String -> me TypeDef
checkType p d n = case lookupType d n of
                       Nothing -> err d p $ "Unknown type: " ++ n
                       Just t  -> return t

getType :: DatalogProgram -> String -> TypeDef
getType d n = fromJust $ lookupType d n

-- Given only function name (and optionally the number of arguments), lookup can return
-- multiple functions.
lookupFuncs :: DatalogProgram -> String -> Maybe Int -> Maybe [Function]
lookupFuncs DatalogProgram{..} n Nothing =
    M.lookup n progFunctions
lookupFuncs DatalogProgram{..} n (Just nargs) =
    filter ((== nargs) . length . funcArgs) <$> M.lookup n progFunctions


checkFuncs :: (MonadError String me) => Pos -> DatalogProgram -> String -> Maybe Int -> me [Function]
checkFuncs p d n nargs = case lookupFuncs d n nargs of
                              Nothing -> err d p $ "Unknown function: '" ++ n ++ "'" ++
                                                   (maybe "" (\x -> " with " ++ show x ++ " arguments") nargs)
                              Just fs -> return fs

getFuncs :: DatalogProgram -> String -> Maybe Int -> [Function]
getFuncs d n nargs = fromJust $ lookupFuncs d n nargs

-- Find a function by its name, argument types, and return type.  This function should only be
-- called after type inference, at which point there should be exactly one such
-- function.
-- TODO: change ret_type from Maybe Type to Type once we've change aggregation
-- syntax to use expression in the LHS.
lookupFunc :: DatalogProgram -> String -> [Type] -> Maybe Type -> Maybe (Function, M.Map String Type)
lookupFunc d@DatalogProgram{..} n arg_types ret_type =
    listToMaybe $
    mapMaybe (\f@Function{..} -> case inferTypeArgs d nopos "" $ zip (map typ funcArgs ++ [funcType]) (arg_types ++ maybeToList ret_type) of
                                      Left _ -> Nothing
                                      Right tmap -> Just (f,tmap)
             ) candidates
    where
    candidates = maybe [] id $ lookupFuncs d n (Just $ length arg_types)

checkFunc :: (MonadError String me) => Pos -> DatalogProgram -> String -> [Type] -> Maybe Type -> me (Function, M.Map String Type)
checkFunc p d n arg_types ret_type =
    case lookupFunc d n arg_types ret_type of
         Nothing -> err d p $ "Unknown function: '" ++ n ++ "(" ++ (intercalate "," $ map show arg_types) ++ "): " <> (maybe "_" show ret_type) <> "'"
         Just f  -> return f

getFunc :: DatalogProgram -> String -> [Type] -> Maybe Type -> (Function, M.Map String Type)
getFunc d n arg_types ret_type = fromJust $ lookupFunc d n arg_types ret_type

lookupTransformer :: DatalogProgram -> String -> Maybe Transformer
lookupTransformer DatalogProgram{..} n = M.lookup n progTransformers

checkTransformer :: (MonadError String me) => Pos -> DatalogProgram -> String -> me Transformer
checkTransformer p d n = case lookupTransformer d n of
                              Nothing -> err d p $ "Unknown transformer: " ++ n
                              Just t  -> return t

getTransformer :: DatalogProgram -> String -> Transformer
getTransformer d n = fromJust $ lookupTransformer d n

lookupVar :: DatalogProgram -> ECtx -> String -> Maybe Var
lookupVar d ctx n = find ((==n) . name) $ ctxAllVars d ctx

checkVar :: (MonadError String me) => Pos -> DatalogProgram -> ECtx -> String -> me Var
checkVar p d c n = case lookupVar d c n of
                        Nothing -> err d p $ "Unknown variable: " ++ n -- ++ ". All known variables: " ++ (show $ (\(ls,vs) -> (map name ls, map name vs)) $ ctxVars d c)
                        Just v  -> return v

getVar :: DatalogProgram -> ECtx -> String -> Var
getVar d c n = 
    maybe (error $ "getVar: unknown variable '" ++ n ++ "' in\n" ++ show c ++ "\nVariables in scope:" ++ show (ctxAllVars d c)) id $ lookupVar d c n

lookupConstructor :: DatalogProgram -> String -> Maybe Constructor
lookupConstructor d c =
    find ((== c) . name) $ progConstructors d

checkConstructor :: (MonadError String me) => Pos -> DatalogProgram -> String -> me Constructor
checkConstructor p d c = case lookupConstructor d c of
                              Nothing   -> err d p $ "Unknown constructor: " ++ c
                              Just cons -> return cons

getConstructor :: DatalogProgram -> String -> Constructor
getConstructor d c = fromJust $ lookupConstructor d c

lookupRelation :: DatalogProgram -> String -> Maybe Relation
lookupRelation d n = M.lookup n $ progRelations d

checkRelation :: (MonadError String me) => Pos -> DatalogProgram -> String -> me Relation
checkRelation p d n = case lookupRelation d n of
                           Nothing  -> err d p $ "Unknown relation: " ++ n
                           Just rel -> return rel

getRelation :: DatalogProgram -> String -> Relation
getRelation d n = fromJust $ lookupRelation d n

arg2v :: Function -> FuncArg -> Var
arg2v f a = ArgVar f (name a)

-- All variables visible in 'ctx'.  This function is safe to call before
-- type checking.
ctxAllVars :: DatalogProgram -> ECtx -> [Var]
ctxAllVars d ctx = let (lvs, rvs) = ctxVars' d ctx False in lvs ++ rvs

-- All variables visible in 'ctx', classified into (writable, read-only)
-- variables.
-- This function is _unsafe_ to use before type inference.
ctxVars :: DatalogProgram -> ECtx -> ([Var], [Var])
ctxVars d ctx = ctxVars' d ctx True

-- All variables available in the scope: (l-vars, read-only vars).
--
-- The 'with_types' flag is true if 'd' contains enough type information
-- to determine variable types (i.e., it has been through type inference).
-- When false, `ctxVars` may misclassify variables, so it should only be
-- invoked this way in situations where variable mutability does not matter.
ctxVars' :: DatalogProgram -> ECtx -> Bool -> ([Var], [Var])
ctxVars' d ctx with_types =
    case ctx of
         CtxTop                   -> ([], [])
         CtxFunc f                -> (map (arg2v f) $ funcMutArgs f, map (arg2v f) $ funcImmutArgs f)
         CtxRuleL rl _            -> ([], ruleVars d rl)
         CtxRuleRAtom rl i        -> ([], ruleRHSVars d rl i)
         CtxRuleRCond rl i        -> ([], ruleRHSVars d rl i)
         CtxRuleRFlatMap rl i     -> ([], ruleRHSVars d rl i)
         CtxRuleRInspect rl i     -> let vars = (ruleRHSVars d rl i) ++ [WeightVar] in
                                     ([], TSVar rl : vars)
         CtxRuleRAggregate rl i   -> ([], ruleRHSVars d rl i)
         CtxRuleRGroupBy rl i     -> ([], ruleRHSVars d rl i)
         CtxKey rel@Relation{..}  -> ([], [KeyVar rel])
         CtxIndex idx@Index{..}   -> ([], map (\v -> (IdxVar idx $ name v)) idxVars)
         CtxApplyArg _ _ _        -> (plvars, prvars)
         CtxApplyFunc _ _         -> (plvars, prvars)
         CtxField _ _             -> (plvars, prvars)
         CtxTupField _ _          -> (plvars, prvars)
         CtxStruct _ _ _          -> (plvars, prvars)
         CtxTuple _ _ _           -> (plvars, prvars)
         CtxSlice  _ _            -> ([], plvars ++ prvars)
         CtxMatchExpr _ _         -> (plvars, prvars)
         CtxMatchPat _ _ _        -> ([], plvars ++ prvars)
         CtxMatchVal e pctx i     -> let patternVars = exprVarDecls d (CtxMatchPat e pctx i)
                                                       $ fst $ (exprCases e) !! i
                                         patternVarNames = map name patternVars
                                         plvars' = filter (\v -> notElem (name v) patternVarNames) plvars
                                         prvars' = filter (\v -> notElem (name v) patternVarNames) prvars in
                                     -- 'exprIsVarOrFieldLVal' needs to determine the type of 'e' and
                                     -- may crash if called before type inference.
                                     if with_types && (exprIsVarOrFieldLVal d pctx $ exprMatchExpr e)
                                        then (patternVars ++ plvars', prvars')
                                        else (plvars', patternVars ++ prvars')
         CtxSeq1 _ _              -> (plvars, prvars)
         CtxSeq2 e pctx           -> let seq1vars = exprVarDecls d (CtxSeq1 e pctx) $ exprLeft e
                                         varNames = map name seq1vars
                                         plvars' = filter (\v -> notElem (name v) varNames) plvars
                                         prvars' = filter (\v -> notElem (name v) varNames) prvars
                                     in (plvars' ++ seq1vars, prvars')
         CtxITEIf _ _             -> ([], plvars ++ prvars)
         CtxITEThen _ _           -> (plvars, prvars)
         CtxITEElse _ _           -> (plvars, prvars)
         CtxForIter _ _           -> (plvars, prvars)
         CtxForBody e@EFor{..} pctx -> let loopvar = ForVar pctx e
                                           prvars' = (filter ((/= name loopvar) . name) prvars)
                                           iterVarNames = map name $ exprVars d (CtxForIter e pctx) exprIter
                                           -- variables that occur in the iterator expression cannot
                                           -- be modified inside the loop
                                           plvars_not_iter = filter (\v -> name v /= (name loopvar) && notElem (name v) iterVarNames) plvars
                                           plvars_iter = filter (\v -> name v /= (name loopvar) && elem (name v) iterVarNames) plvars
                                       in (plvars_not_iter, prvars' ++ plvars_iter ++ [loopvar])
         CtxForBody _ _           -> error $ "NS.ctxMVars: invalid context " ++ show ctx
         CtxSetL _ _              -> (plvars, prvars)
         CtxSetR _ _              -> (plvars, prvars)
         CtxReturn _ _            -> (plvars, prvars)
         CtxBinOpL _ _            -> ([], plvars ++ prvars)
         CtxBinOpR _ _            -> ([], plvars ++ prvars)
         CtxUnOp _ _              -> ([], plvars ++ prvars)
         CtxBinding _ _           -> (plvars, prvars)
         CtxTyped _ _             -> (plvars, prvars)
         CtxAs _ _                -> (plvars, prvars)
         CtxRef _ _               -> (plvars, prvars)
         CtxTry _ _               -> (plvars, prvars)
         CtxClosure e pctx         -> -- Captured variables are immutable.
                                      -- Closure arguments can be mutable. We need to know the type of the closure to establish
                                      -- argument mutability.
                                      let EClosure{..} = e
                                          closure_vars = map (ClosureArgVar pctx e) [0 .. length exprClosureArgs - 1] in
                                      if with_types
                                      then let TFunction _ args _ = exprType' d pctx $ E e
                                               (closure_mut_vars, closure_imm_vars) = partition (atypeMut . snd) $ zip closure_vars args in
                                           (map fst closure_mut_vars, (map fst closure_imm_vars) ++ plvars ++ prvars)
                                      else ([], closure_vars ++ plvars ++ prvars)
    where (plvars, prvars) = ctxVars' d (ctxParent ctx) with_types
