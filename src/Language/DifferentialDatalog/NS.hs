{-# LANGUAGE RecordWildCards, FlexibleContexts #-}

module Language.DifferentialDatalog.NS(
    lookupType, checkType, getType
--          lookupFunc, checkFunc, getFunc,
--          lookupVar, checkVar, getVar,
--          lookupSwitch, checkSwitch, getSwitch,
--          lookupPort, checkPort, getPort,
--          getPortDef,
--          lookupConstructor, checkConstructor, getConstructor,
--          lookupRelation, checkRelation, getRelation,
--          ctxMVars, ctxVars, ctxAllVars,
--          ctxRels,
--          isLVar,
--          lookupBuiltin, checkBuiltin, getBuiltin,
--          packetTypeName
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
--import {-# SOURCE #-} Relation
--import {-# SOURCE #-} Expr
--import {-# SOURCE #-} Type
--import {-# SOURCE #-} Builtins

lookupType :: DatalogProgram -> String -> Maybe TypeDef
lookupType DatalogProgram{..} n = M.lookup n progTypedefs

checkType :: (MonadError String me) => Pos -> DatalogProgram -> String -> me TypeDef
checkType p d n = case lookupType d n of
                       Nothing -> err p $ "Unknown type: " ++ n
                       Just t  -> return t

getType :: DatalogProgram -> String -> TypeDef
getType d n = fromJust $ lookupType d n


--lookupFunc :: Refine -> String -> Maybe Function
--lookupFunc Refine{..} n = find ((==n) . name) refineFuncs
--
--checkFunc :: (MonadError String me) => Pos -> Refine -> String -> me Function
--checkFunc p r n = case lookupFunc r n of
--                       Nothing -> errR r p $ "Unknown function: " ++ n
--                       Just f  -> return f
--
--getFunc :: Refine -> String -> Function
--getFunc r n = fromJust $ lookupFunc r n
--
--
--lookupSwitch :: Refine -> String -> Maybe Switch
--lookupSwitch Refine{..} n = find ((==n) . name) refineSwitches
--
--checkSwitch :: (MonadError String me) => Pos -> Refine -> String -> me Switch
--checkSwitch p r n = case lookupSwitch r n of
--                       Nothing -> errR r p $ "Unknown switch: " ++ n
--                       Just s  -> return s
--
--getSwitch :: Refine -> String -> Switch
--getSwitch r n = fromJust $ lookupSwitch r n
--
--lookupPort :: Refine -> String -> Maybe SwitchPort
--lookupPort Refine{..} n = find ((==n) . name) refinePorts
--
--checkPort :: (MonadError String me) => Pos -> Refine -> String -> me SwitchPort
--checkPort p r n = case lookupPort r n of
--                       Nothing -> errR r p $ "Unknown port: " ++ n
--                       Just pr -> return pr
--
--getPort :: Refine -> String -> SwitchPort
--getPort r n = fromJust $ lookupPort r n
--
--getPortDef :: Refine -> DirPort -> Maybe Function
--getPortDef r (DirPort n DirIn) = Just $ getFunc r $ portIn $ getPort r n
--getPortDef r (DirPort n DirOut) = fmap (getFunc r) $ portOut $ getPort r n
--
--lookupVar :: Refine -> ECtx -> String -> Maybe Field
--lookupVar r ctx n = find ((==n) . name) $ ctxAllVars r ctx
--
--checkVar :: (MonadError String me) => Pos -> Refine -> ECtx -> String -> me Field
--checkVar p r c n = case lookupVar r c n of
--                        Nothing -> err p $ "Unknown variable: " ++ n-- ++ ". All known variables: " ++ (show $ (\(ls,vs) -> (map fst ls, map fst vs)) $ ctxVars r c)
--                        Just v  -> return v
--
--getVar :: Refine -> ECtx -> String -> Field
--getVar r c n = fromJust $ lookupVar r c n
--
--isGlobalVar :: Refine -> String -> Bool
--isGlobalVar r v = isJust $ find ((==v) . name) $ refineState r
--
--lookupConstructor :: Refine -> String -> Maybe Constructor
--lookupConstructor r c =
--    find ((== c) . name)
--    $ concatMap (\td -> case tdefType td of
--                             Just (TStruct _ cs) -> cs
--                             _                   -> [])
--    $ refineTypes r
--
--checkConstructor :: (MonadError String me) => Pos -> Refine -> String -> me Constructor
--checkConstructor p r c = case lookupConstructor r c of
--                              Nothing   -> errR r p $ "Unknown constructor: " ++ c
--                              Just cons -> return cons
--
--getConstructor :: Refine -> String -> Constructor
--getConstructor r c = fromJust $ lookupConstructor r c
--
--lookupRelation :: Refine -> ECtx -> String -> Maybe Relation
--lookupRelation r ctx n = find ((==n) . name) $ (\(rw,ro) -> rw ++ ro) $ ctxRels r ctx
--
--checkRelation :: (MonadError String me) => Pos -> Refine -> ECtx -> String -> me Relation
--checkRelation p r ctx n = case lookupRelation r ctx n of
--                               Nothing  -> errR r p $ "Unknown relation: " ++ n -- ++ " in context " ++ show ctx
--                               Just rel -> return rel
--
--getRelation :: Refine -> String -> Relation
--getRelation r n = fromJust $ lookupRelation r CtxRefine n
--
---- All variables available in the scope: (l-vars, read-only vars)
--type MField = (String, Maybe Type)
--f2mf f = (name f, Just $ fieldType f)
--
--ctxAllVars :: Refine -> ECtx -> [Field]
--ctxAllVars r ctx = let (lvs, rvs) = ctxVars r ctx in lvs ++ rvs
--
--ctxVars :: Refine -> ECtx -> ([Field], [Field])
--ctxVars r ctx = let (lvs, rvs) = ctxMVars r ctx in
--                (map (\(n, mt) -> (Field nopos n $ maybe (error $ "variable " ++ n ++ " has unknown type") id mt)) lvs,
--                 map (\(n, mt) -> (Field nopos n $ maybe (error $ "variable " ++ n ++ " has unknown type") id mt)) rvs)
--
--ctxMVars :: Refine -> ECtx -> ([MField], [MField])
--ctxMVars r ctx =
--    case ctx of
--         CtxRefine                -> (map f2mf $ refineState r, [])
--         CtxCLI                   -> (plvars, prvars)
--         CtxFunc f _              -> let plvars' = filter (isGlobalVar r . fst) plvars
--                                         prvars' = filter (isGlobalVar r . fst) prvars in
--                                     if funcPure f
--                                        then ([], map f2mf $ funcArgs f)
--                                        else (plvars', (map f2mf $ funcArgs f) ++ prvars')
--         CtxAssume a              -> ([], vartypes $ exprVars ctx $ assExpr a)
--         CtxRelKey rel            -> ([], map f2mf $ relArgs rel)
--         CtxRelForeign _ con      -> let ForeignKey _ _ fname _ = con
--                                         frel = getRelation r fname in
--                                     ([], map f2mf $ relArgs frel)
--         CtxCheck rel             -> ([], map f2mf $ relArgs rel)
--         CtxRuleL rel rl _        -> ([], vartypes $ concatMap (exprVars (CtxRuleR rel rl)) $ filter exprIsRelPred $ ruleRHS rl)
--         CtxRuleR _ rl            -> ([], vartypes $ concatMap (exprVars ctx) $ filter exprIsRelPred $ ruleRHS rl)
--         CtxBuiltin _ _ _         -> ([], plvars ++ prvars) -- disallow assignments inside func & builtin args, cause we care about correctness
--         CtxApply _ _ _           -> ([], plvars ++ prvars)
--         CtxField _ _             -> (plvars, prvars)
--         CtxLocation _ _          -> ([], plvars ++ prvars)
--         CtxStruct _ _ _          -> (plvars, prvars)
--         CtxTuple _ _ _           -> (plvars, prvars)
--         CtxSlice  _ _            -> ([], plvars ++ prvars)
--         CtxMatchExpr _ _         -> ([], plvars ++ prvars)
--         CtxMatchPat _ _ _        -> ([], plvars ++ prvars)
--         CtxMatchVal e pctx i     -> let patternVars = map (mapSnd $ ctxExpectType r) $ exprVarDecls ctx $ fst $ (exprCases e) !! i in
--                                     if isLExpr r pctx $ exprMatchExpr e
--                                        then (plvars ++ patternVars, prvars)
--                                        else (plvars, patternVars ++ prvars)
--         CtxSeq1 _ _              -> (plvars, prvars)
--         CtxSeq2 e pctx           -> let seq1vars = map (mapSnd $ ctxExpectType r) $ exprVarDecls (CtxSeq1 e pctx) $ exprLeft e
--                                     in (plvars ++ seq1vars, prvars)
--         CtxPar1 _ _              -> ([], plvars ++ prvars)
--         CtxPar2 _ _              -> ([], plvars ++ prvars)
--         CtxITEIf _ _             -> ([], plvars ++ prvars)
--         CtxITEThen _ _           -> (plvars, prvars)
--         CtxITEElse _ _           -> (plvars, prvars)
--         CtxSetL _ _              -> (plvars, prvars)
--         CtxSetR _ _              -> ([], plvars ++ prvars)
--         CtxSend _ _              -> ([], plvars ++ prvars)
--         CtxBinOpL _ _            -> ([], plvars ++ prvars)
--         CtxBinOpR _ _            -> ([], plvars ++ prvars)
--         CtxUnOp _ _              -> ([], plvars ++ prvars)
--         CtxForCond e _           -> ([], (frkvar e) : (plvars ++ prvars))
--         CtxForBody e _           -> ((frkvar e) : plvars, prvars)
--         CtxForkCond e _          -> ([], (frkvar e) : (plvars ++ prvars))
--         CtxForkBody e _          -> ((frkvar e):plvars, prvars)
--         CtxWithCond e _          -> ([], (frkvar e) : (plvars ++ prvars))
--         CtxWithBody e _          -> ((frkvar e) : plvars, prvars)
--         CtxWithDef _ _           -> (plvars, prvars)
--         CtxAnyCond e _           -> ([], (frkvar e) : (plvars ++ prvars))
--         CtxAnyBody e _           -> ((frkvar e) : plvars, prvars)
--         CtxAnyDef _ _            -> (plvars, prvars)
--         CtxTyped _ _             -> (plvars, prvars)
--         CtxRelPred _ _ _         -> ([], plvars ++ prvars)
--         CtxPut _ _               -> ([], plvars ++ prvars)
--         CtxDelete _ _            -> ([], plvars ++ prvars)
--         CtxLambda e _            -> ([], map f2mf $ exprLambdaArgs e)
--         CtxApplyLambda _ _       -> ([], plvars ++ prvars)
--         CtxApplyLambdaArg _ _ _  -> ([], plvars ++ prvars)
--    where (plvars, prvars) = ctxMVars r $ ctxParent ctx
--          frkvar e = (exprFrkVar e, Just $ relRecordType $ getRelation r $ exprTable e)
--          vartypes :: [(String, ECtx)] -> [MField]
--          vartypes vs = map (\gr -> case filter (isJust . snd) $ map (mapSnd $ ctxExpectType r) gr of
--                                         []  -> (fst $ head gr, Nothing)
--                                         vs' -> head vs')
--                            $ sortAndGroup fst vs
--
---- Fork, with, any: relations become unavailable
---- Fork, Par: all relations become read-only
--ctxRels :: Refine -> ECtx -> ([Relation], [Relation])
--ctxRels r ctx =
--    case ctx of
--         CtxRefine         -> partition relMutable $ refineRels r
--         CtxRelKey _       -> ([],[])
--         CtxRelForeign _ _ -> ([],[])
--         CtxCheck _        -> ([],[])
--         CtxPar1 _ _       -> (plrels, prrels)
--         CtxPar2 _ _       -> (plrels, prrels)
--         CtxForkCond _ _   -> ([], [])
--         CtxForkBody _ _   -> ({-del (exprTable e)-} plrels, {-del (exprTable e)-} prrels)
--         CtxWithCond _ _   -> ([], [])
--         CtxWithBody _ _   -> ({-del (exprTable e)-} plrels, {-del (exprTable e)-} prrels)
--         CtxAnyCond _ _    -> ([], [])
--         CtxAnyBody _ _    -> ({-del (exprTable e)-} plrels, {-del (exprTable e)-} prrels)
--         _                 -> (plrels, prrels)
--    where (plrels, prrels) = ctxRels r $ ctxParent ctx
--
--
--lookupBuiltin :: String -> Maybe Builtin
--lookupBuiltin n = find ((==n) . name) builtins
--
--checkBuiltin :: (MonadError String me) => Pos -> String -> me Builtin
--checkBuiltin p n = case lookupBuiltin n of
--                        Nothing -> err p $ "Unknown builtin: " ++ n
--                        Just b  -> return b
--
--getBuiltin :: String -> Builtin
--getBuiltin n = fromJust $ lookupBuiltin n