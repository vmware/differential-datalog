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

{-# LANGUAGE RecordWildCards, FlexibleContexts #-}

module Language.DifferentialDatalog.NS(
    lookupType, checkType, getType,
    lookupFunc, checkFunc, getFunc,
    lookupTransformer, checkTransformer, getTransformer,
    lookupVar, checkVar, getVar,
    lookupConstructor, checkConstructor, getConstructor,
    lookupRelation, checkRelation, getRelation,
    ctxMVars, ctxVars,
    ctxAllVars,
    -- isLVar,
     ) where

import qualified Data.Map as M
import Data.List
import Control.Monad.Except
import Data.Maybe
--import Debug.Trace

import Language.DifferentialDatalog.Syntax
import Language.DifferentialDatalog.Name
import Language.DifferentialDatalog.Util
import Language.DifferentialDatalog.Pos
import {-# SOURCE #-} Language.DifferentialDatalog.Rule
--import {-# SOURCE #-} Relation
import {-# SOURCE #-} Language.DifferentialDatalog.Expr
import {-# SOURCE #-} Language.DifferentialDatalog.Type
--import {-# SOURCE #-} Builtins

lookupType :: DatalogProgram -> String -> Maybe TypeDef
lookupType DatalogProgram{..} n = M.lookup n progTypedefs

checkType :: (MonadError String me) => Pos -> DatalogProgram -> String -> me TypeDef
checkType p d n = case lookupType d n of
                       Nothing -> err p $ "Unknown type: " ++ n
                       Just t  -> return t

getType :: DatalogProgram -> String -> TypeDef
getType d n = fromJust $ lookupType d n


lookupFunc :: DatalogProgram -> String -> Maybe Function
lookupFunc DatalogProgram{..} n = M.lookup n progFunctions

checkFunc :: (MonadError String me) => Pos -> DatalogProgram -> String -> me Function
checkFunc p d n = case lookupFunc d n of
                       Nothing -> err p $ "Unknown function: " ++ n
                       Just f  -> return f

getFunc :: DatalogProgram -> String -> Function
getFunc d n = fromJust $ lookupFunc d n

lookupTransformer :: DatalogProgram -> String -> Maybe Transformer
lookupTransformer DatalogProgram{..} n = M.lookup n progTransformers

checkTransformer :: (MonadError String me) => Pos -> DatalogProgram -> String -> me Transformer
checkTransformer p d n = case lookupTransformer d n of
                              Nothing -> err p $ "Unknown transformer: " ++ n
                              Just t  -> return t

getTransformer :: DatalogProgram -> String -> Transformer
getTransformer d n = fromJust $ lookupTransformer d n

lookupVar :: DatalogProgram -> ECtx -> String -> Maybe Field
lookupVar d ctx n = find ((==n) . name) $ ctxAllVars d ctx

checkVar :: (MonadError String me) => Pos -> DatalogProgram -> ECtx -> String -> me Field
checkVar p d c n = case lookupVar d c n of
                        Nothing -> err p $ "Unknown variable: " ++ n -- ++ ". All known variables: " ++ (show $ (\(ls,vs) -> (map name ls, map name vs)) $ ctxVars d c)
                        Just v  -> return v

getVar :: DatalogProgram -> ECtx -> String -> Field
getVar d c n = fromJust $ lookupVar d c n

lookupConstructor :: DatalogProgram -> String -> Maybe Constructor
lookupConstructor d c =
    find ((== c) . name) $ progConstructors d

checkConstructor :: (MonadError String me) => Pos -> DatalogProgram -> String -> me Constructor
checkConstructor p d c = case lookupConstructor d c of
                              Nothing   -> err p $ "Unknown constructor: " ++ c
                              Just cons -> return cons

getConstructor :: DatalogProgram -> String -> Constructor
getConstructor d c = fromJust $ lookupConstructor d c

lookupRelation :: DatalogProgram -> String -> Maybe Relation
lookupRelation d n = M.lookup n $ progRelations d

checkRelation :: (MonadError String me) => Pos -> DatalogProgram -> String -> me Relation
checkRelation p d n = case lookupRelation d n of
                           Nothing  -> err p $ "Unknown relation: " ++ n
                           Just rel -> return rel

getRelation :: DatalogProgram -> String -> Relation
getRelation d n = fromJust $ lookupRelation d n

-- All variables available in the scope: (l-vars, read-only vars)
type MField = (String, Maybe Type)

f2mf :: Field -> MField
f2mf f = (name f, Just $ fieldType f)

arg2mf :: FuncArg -> MField
arg2mf a = (name a, Just $ argType a)

ctxAllVars :: DatalogProgram -> ECtx -> [Field]
ctxAllVars d ctx = let (lvs, rvs) = ctxVars d ctx in lvs ++ rvs

ctxVars :: DatalogProgram -> ECtx -> ([Field], [Field])
ctxVars d ctx = let (lvs, rvs) = ctxMVars d ctx in
                (map (\(n, mt) -> (Field nopos [] n $ maybe (error $ "variable " ++ n ++ " has unknown type") id mt)) lvs,
                 map (\(n, mt) -> (Field nopos [] n $ maybe (error $ "variable " ++ n ++ " has unknown type") id mt)) rvs)

ctxMVars :: DatalogProgram -> ECtx -> ([MField], [MField])
ctxMVars d ctx =
    case ctx of
         CtxTop                   -> ([], [])
         CtxFunc f                -> (map arg2mf $ funcMutArgs f, map arg2mf $ funcImmutArgs f)
         CtxRuleL rl _            -> ([], map f2mf $ ruleVars d rl)
         CtxRuleRAtom rl i        -> ([], map f2mf $ ruleRHSVars d rl i)
         CtxRuleRCond rl i        -> ([], map f2mf $ ruleRHSVars d rl i)
         CtxRuleRFlatMap rl i     -> ([], map f2mf $ ruleRHSVars d rl i)
         CtxRuleRAggregate rl i   -> ([], map f2mf $ ruleRHSVars d rl i)
         CtxKey Relation{..}      -> ([], [(keyVar $ fromJust relPrimaryKey, Just relType)])
         CtxIndex Index{..}       -> ([], map f2mf idxVars)
         CtxApply _ _ _           -> ([], plvars ++ prvars)
         CtxField _ _             -> (plvars, prvars)
         CtxTupField _ _          -> (plvars, prvars)
         CtxStruct _ _ _          -> (plvars, prvars)
         CtxTuple _ _ _           -> (plvars, prvars)
         CtxSlice  _ _            -> ([], plvars ++ prvars)
         CtxMatchExpr _ _         -> ([], plvars ++ prvars)
         CtxMatchPat _ _ _        -> ([], plvars ++ prvars)
         CtxMatchVal e pctx i     -> let patternVars = map (mapSnd $ ctxExpectType d) $ exprVarDecls (CtxMatchPat e pctx i) $ fst $ (exprCases e) !! i in
                                     if exprIsVarOrFieldLVal d pctx $ exprMatchExpr e
                                        then (plvars ++ patternVars, prvars)
                                        else (plvars, patternVars ++ prvars)
         CtxSeq1 _ _              -> (plvars, prvars)
         CtxSeq2 e pctx           -> let seq1vars = map (mapSnd $ ctxExpectType d) $ exprVarDecls (CtxSeq1 e pctx) $ exprLeft e
                                     in (plvars ++ seq1vars, prvars)
         CtxITEIf _ _             -> ([], plvars ++ prvars)
         CtxITEThen _ _           -> (plvars, prvars)
         CtxITEElse _ _           -> (plvars, prvars)
         CtxForIter _ _           -> (plvars, prvars)
         CtxForBody e@EFor{..} pctx -> let loopvar = (exprLoopVar, typeIterType d =<< exprTypeMaybe d (CtxForIter e pctx) exprIter)
                                           -- variables that occur in the iterator expression cannot
                                           -- be modified inside the loop
                                           plvars_not_iter = filter (\(v,_) -> notElem v $ exprVars exprIter) plvars
                                           plvars_iter = filter (\(v,_) -> elem v $ exprVars exprIter) plvars
                                       in (plvars_not_iter, prvars ++ plvars_iter ++ [loopvar])
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
    where (plvars, prvars) = ctxMVars d $ ctxParent ctx
