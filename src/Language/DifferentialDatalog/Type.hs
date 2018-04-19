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

{-# LANGUAGE FlexibleContexts #-}

module Language.DifferentialDatalog.Type(
    WithType(..)
--    exprType, exprType', exprTypeMaybe, exprNodeType,
--    exprTypes,
--    exprTraverseTypeM,
--    typ', typ'',
--    isBool, isBit, isInt, isLocation, isStruct, isArray, isTuple, isLambda,
--    matchType, matchType',
--    ctxExpectType,
--    typeSubtypes,
--    typeSubtypesRec,
--    typeGraph,
--    typeSort
) where

import Data.Maybe
import Data.List
import Control.Monad.Except
import Control.Monad.State
import qualified Data.Graph.Inductive as G
--import Debug.Trace

import Language.DifferentialDatalog.Util
--import Expr
import Language.DifferentialDatalog.Syntax
--import NS
import Language.DifferentialDatalog.Pos
import Language.DifferentialDatalog.Name
--import {-# SOURCE #-} Relation

class WithType a where
    typ  :: a -> Type

instance WithType Type where
    typ = id

instance WithType Field where
    typ = fieldType
--
--exprTraverseTypeM :: (Monad m) => Refine -> (ECtx -> ExprNode (Maybe Type) -> m ()) -> ECtx -> Expr -> m ()
--exprTraverseTypeM r = exprTraverseCtxWithM (\ctx e -> return $ exprNodeType r ctx e)
--
--exprType :: Refine -> ECtx -> Expr -> Type
--exprType r ctx e = maybe (error $ "exprType: expression " ++ show e ++ " has unknown type") id $ exprTypeMaybe r ctx e
--
--exprType' :: Refine -> ECtx -> Expr -> Type
--exprType' r ctx e = typ' r $ exprType r ctx e
--
--exprTypeMaybe :: Refine -> ECtx -> Expr -> Maybe Type
--exprTypeMaybe r ctx e = exprFoldCtx (\ctx' e' -> fmap ((flip atPos) (pos e')) $ exprNodeType r ctx' e') ctx e
--
--exprNodeType :: Refine -> ECtx -> ExprNode (Maybe Type) -> Maybe Type
--exprNodeType r ctx e = fmap ((flip atPos) (pos e)) $ exprNodeType' r ctx e
--
--exprNodeType' :: Refine -> ECtx -> ExprNode (Maybe Type) -> Maybe Type
--exprNodeType' r ctx (EVar _ v)            = let (lvs, rvs) = ctxMVars r ctx in
--                                            fromJust $ lookup v $ lvs ++ rvs
--exprNodeType' _ _   (EPacket _)           = Just $ tUser packetTypeName
--exprNodeType' r ctx e@(EBuiltin _ f _)    = let bin = getBuiltin f
--                                            in Just $ (bfuncType bin) r ctx e
--exprNodeType' r _   (EApply _ f _)        = Just $ funcType $ getFunc r f
--exprNodeType' r _   (EField _ e f)        = case e of
--                                                 Nothing -> Nothing
--                                                 Just e' -> let TStruct _ cs = typ' r e' in
--                                                            fmap fieldType $ find ((==f) . name) $ concatMap consArgs cs
--exprNodeType' _ _   (ELocation _ _ _ _)   = Just tLocation
--exprNodeType' _ _   (EBool _ _)           = Just tBool
--exprNodeType' r ctx (EInt _ _)            = case fmap (typ' r) $ ctxExpectType r ctx of
--                                                 t@(Just (TBit _ _)) -> t
--                                                 t@(Just (TInt _))   -> t
--                                                 _                   -> Nothing
--exprNodeType' _ _   (EString _ _)         = Just tString
--exprNodeType' _ _   (EBit _ w _)          = Just $ tBit w
--exprNodeType' r _   (EStruct _ c _)       = Just $ tUser $ name $ consType r c
--exprNodeType' _ _   (ETuple _ fs)         = fmap tTuple $ sequence fs
--exprNodeType' _ _   (ESlice _ _ h l)      = Just $ tBit $ h - l + 1
--exprNodeType' r _   (EMatch _ _ cs)       = case find ((\t -> isJust t && (typ' r (fromJust t) /= tSink)) . snd) cs of
--                                                 Nothing    -> snd $ head cs
--                                                 Just (_,t) -> t
--exprNodeType' r ctx (EVarDecl _ _)        = ctxExpectType r ctx
--exprNodeType' _ _   (ESeq _ _ e2)         = e2
--exprNodeType' _ _   (EPar _ _ _)          = Just tSink
--exprNodeType' r _   (EITE _ _ t e)        = if maybe False (\t' -> typ' r t' == tSink) t
--                                               then maybe (Just $ tTuple []) id e
--                                               else t
--exprNodeType' _ _   (EDrop _)             = Just tSink
--exprNodeType' _ _   (ESet _ _ _)          = Just $ tTuple []
--exprNodeType' _ _   (ESend  _ _)          = Just tSink
--exprNodeType' r _   (EBinOp _ op (Just e1) (Just e2)) =
--                                  case op of
--                                       Eq     -> Just tBool
--                                       Neq    -> Just tBool
--                                       Lt     -> Just tBool
--                                       Gt     -> Just tBool
--                                       Lte    -> Just tBool
--                                       Gte    -> Just tBool
--                                       And    -> Just tBool
--                                       Or     -> Just tBool
--                                       Impl   -> Just tBool
--                                       Plus   -> Just $ if' (isBit r t1) (tBit (max (typeWidth t1) (typeWidth t2))) tInt
--                                       Minus  -> Just $ if' (isBit r t1) (tBit (max (typeWidth t1) (typeWidth t2))) tInt
--                                       ShiftR -> Just t1
--                                       ShiftL -> Just t1
--                                       Mod    -> Just t1
--                                       BAnd   -> Just t1
--                                       BOr    -> Just t1
--                                       Concat -> Just $ tBit (typeWidth t1 + typeWidth t2)
--    where t1 = typ' r e1
--          t2 = typ' r e2
--exprNodeType' _ _   (EBinOp _ ShiftR e1 _) = e1
--exprNodeType' _ _   (EBinOp _ ShiftL e1 _) = e1
--exprNodeType' _ _   (EBinOp _ _ _ _)       = Nothing
--exprNodeType' _ _   (EUnOp _ Not _)        = Just tBool
--exprNodeType' r _   (EUnOp _ BNeg (Just e))= Just $ typ' r e
--exprNodeType' _ _   (EUnOp _ _ _)          = Nothing
--exprNodeType' _ _   (EFork _ _ _ _ b)      = b
--exprNodeType' _ _   (EFor  _ _ _ _ _)      = Just $ tTuple []
--exprNodeType' _ _   (EWith _ _ _ _ b _)    = b
--exprNodeType' _ _   (EAny  _ _ _ _ b _)    = b
--exprNodeType' r ctx (EPHolder _)           = ctxExpectType r ctx
--exprNodeType' r ctx (EAnon _)              = case ctxInDelete ctx of
--                                                  Just (CtxDelete (EDelete _ rel _) _) -> Just $ relRecordType $ getRelation r rel
--                                                  _                                    -> error $ "exprNodeType ?: invalid context"
--exprNodeType' _ _   (ETyped _ _ t)         = Just t
--exprNodeType' _ _   (ERelPred _ _ _)       = Just tBool
--exprNodeType' _ _   (EPut _ _ _)           = Just $ tTuple []
--exprNodeType' _ _   (EDelete _ _ _)        = Just $ tTuple []
--exprNodeType' _ _   (ELambda _ as r _)     = Just $ tLambda as r
--exprNodeType' _ _   (EApplyLambda _ l _)   = case l of
--                                                  Just (TLambda _ _ t) -> Just t
--                                                  _                    -> Nothing
--
--
--exprTypes :: Refine -> ECtx -> Expr -> [Type]
--exprTypes r ctx e = nub $ execState (exprTraverseTypeM r (\ctx' e' -> modify ((fromJust $ exprNodeType r ctx' e'):)) ctx e) []
--
---- Unwrap typedef's down to actual type definition
--typ' :: (WithType a) => Refine -> a -> Type
--typ' r x = case typ x of
--                t@(TUser _ n) -> maybe t (typ' r) $ tdefType $ getType r n
--                t             -> t
--
---- Similar to typ', but does not unwrap the last typedef if it is a struct
--typ'' :: (WithType a) => Refine -> a -> Type
--typ'' r x = case typ x of
--                 t'@(TUser _ n) -> case tdefType $ getType r n of
--                                        (Just (TStruct _ _)) -> t'
--                                        Nothing              -> t'
--                                        Just t               -> typ'' r t
--                 t         -> t
--
--isBool :: (WithType a) => Refine -> a -> Bool
--isBool r a = case typ' r a of
--                  TBool _ -> True
--                  _       -> False
--
--isBit :: (WithType a) => Refine -> a -> Bool
--isBit r a = case typ' r a of
--                 TBit _ _ -> True
--                 _        -> False
--
--isInt :: (WithType a) => Refine -> a -> Bool
--isInt r a = case typ' r a of
--                 TInt _ -> True
--                 _      -> False
--
--isLocation :: (WithType a) => Refine -> a -> Bool
--isLocation r a = case typ' r a of
--                      TLocation _ -> True
--                      _           -> False
--
--isStruct :: (WithType a) => Refine -> a -> Bool
--isStruct r a = case typ' r a of
--                    TStruct _ _ -> True
--                    _           -> False
--
--isArray :: (WithType a) => Refine -> a -> Bool
--isArray r a = case typ' r a of
--                   TArray _ _ _ -> True
--                   _            -> False
--
--isTuple :: (WithType a) => Refine -> a -> Bool
--isTuple r a = case typ' r a of
--                   TTuple _ _ -> True
--                   _          -> False
--
--isLambda :: (WithType a) => Refine -> a -> Bool
--isLambda r a = case typ' r a of
--                    TLambda _ _ _ -> True
--                    _             -> False
--
--matchType :: (MonadError String me, WithType a, WithType b) => Pos -> Refine -> a -> b -> me ()
--matchType p r x y = assertR r (matchType' r x y) p $ "Incompatible types " ++ show (typ x) ++ " and " ++ show (typ y)
--
--
--matchType' :: (WithType a, WithType b) => Refine -> a -> b -> Bool
--matchType' r x y =
--    case (t1, t2) of
--         (TLocation _      , TLocation _)      -> True
--         (TBool _          , TBool _)          -> True
--         (TBit _ w1        , TBit _ w2)        -> w1==w2
--         (TInt _           , TInt _)           -> True
--         (TString _        , TString _)        -> True
--         (TSink _          , TSink _)          -> True
--         (TArray _ a1 l1   , TArray _ a2 l2)   -> matchType' r a1 a2 && l1 == l2
--         (TStruct _ cs1    , TStruct _ cs2)    -> (length cs1 == length cs2) &&
--                                                  (all (\(c1, c2) -> consName c1 == consName c2) $ zip cs1 cs2)
--         (TTuple _ fs1     , TTuple _ fs2)     -> (length fs1 == length fs2) &&
--                                                  (all (\(f1, f2) -> matchType' r f1 f2) $ zip fs1 fs2)
--         (TUser _ u1       , TUser _ u2)       -> u1 == u2
--         (TLambda _ as1 r1 , TLambda _ as2 r2) -> (length as1 == length as2) &&
--                                                  (all (\(a1, a2) -> matchType' r a1 a2) $ zip as1 as2) &&
--                                                  (matchType' r r1 r2)
--         _                                     -> False
--    where t1 = typ' r x
--          t2 = typ' r y
--
---- sub-types that immediately appear in the type expression
--typeSubtypes :: Refine -> Type -> [Type]
--typeSubtypes r = nub . typeSubtypes' r
--
--typeSubtypes' :: Refine -> Type -> [Type]
--typeSubtypes' _ t@TArray{}  = [typeElemType t]
--typeSubtypes' _ t@TStruct{} = (map typ $ structFields $ typeCons t)
--typeSubtypes' _ t@TTuple{}  = (map typ $ typeArgs t)
--typeSubtypes' r t@TUser{}   = (maybeToList $ tdefType $ getType r $ typeName t)
--typeSubtypes' _ _           = []
--
--typeSubtypesRec :: Refine -> Type -> [Type]
--typeSubtypesRec r t = typeSubtypesRec' r [] t
--
--typeSubtypesRec' :: Refine -> [Type] -> Type -> [Type]
--typeSubtypesRec' r acc t = let new = nub (t: typeSubtypes r t) \\ acc in
--                           new ++ foldl' (\acc' t' -> acc'++ typeSubtypesRec' r (acc++new++acc') t') [] new
--
---- The list of types must be closed under the typeSubtypes operation
--typeGraph :: Refine -> [Type] -> G.Gr Type ()
--typeGraph r ts = foldl' (\g t -> foldl' (\g' t' -> G.insEdge (typIdx t, typIdx t', ()) g') g
--                                 $ typeSubtypes r t) g0 ts
--    where g0 = G.insNodes (mapIdx (\t i -> (i, t)) ts) G.empty
--          typIdx t = fromJust $ elemIndex t ts
--
---- Sort list of types in dependency order; list must be closed under the typeSubtypes operation
--typeSort :: Refine -> [Type] -> [Type]
--typeSort r types  = reverse $ G.topsort' $ typeGraph r types
--
--
--{-
--typeDomainSize :: Refine -> Type -> Integer
--typeDomainSize r t =
--    case typ' r undefined t of
--         TBool _       -> 2
--         TUInt _ w     -> 2^w
--         TStruct _ fs  -> product $ map (typeDomainSize r . fieldType) fs
--         TArray _ t' l -> fromIntegral l * typeDomainSize r t'
--         TUser _ _     -> error "Type.typeDomainSize TUser"
--         TLocation _   -> error "Not implemented: Type.typeDomainSize TLocation"
--
--typeEnumerate :: Refine -> Type -> [Expr]
--typeEnumerate r t =
--    case typ' r undefined t of
--         TBool _      -> [EBool nopos False, EBool nopos True]
--         TUInt _ w    -> map (EInt nopos w) [0..2^w-1]
--         TStruct _ fs -> map (EStruct nopos sname) $ fieldsEnum fs
--         TArray _ _ _ -> error "Not implemented: Type.typeEnumerate TArray"
--         TUser _ _    -> error "Type.typeEnumerate TUser"
--         TLocation _  -> error "Not implemented: Type.typeEnumerate TLocation"
--    where TUser _ sname = typ'' r undefined t
--          fieldsEnum []     = [[]]
--          fieldsEnum (f:fs) = concatMap (\vs -> map (:vs) $ typeEnumerate r $ fieldType f) $ fieldsEnum fs
---}
--
---- Infer expected type from context
--ctxExpectType :: Refine -> ECtx -> Maybe Type
--ctxExpectType _ CtxRefine                            = Nothing
--ctxExpectType _ CtxCLI                               = Nothing
----ctxExpectType _ (CtxRoleGuard _)                     = Just tBool
----ctxExpectType _ (CtxPktGuard _)                      = Just tBool
----ctxExpectType _ (CtxRole _)                          = Just tSink
--ctxExpectType _ (CtxFunc f _)                        = Just $ funcType f
--ctxExpectType _ (CtxAssume _)                        = Just tBool
--ctxExpectType _ (CtxRelKey _)                        = Nothing
--ctxExpectType _ (CtxRelForeign _ _)                  = Nothing
--ctxExpectType _ (CtxCheck _)                         = Just tBool
--ctxExpectType _ (CtxRuleL rel _ i)                   = let args = relArgs rel in
--                                                       if' (i < length args) (Just $ fieldType $ args !! i) Nothing
--ctxExpectType _ (CtxRuleR _ _)                       = Just tBool
--ctxExpectType _ (CtxBuiltin _ _ _)                   = Nothing -- TODO: add expectType hook to Builtins
--ctxExpectType r (CtxApply (EApply _ f _) _ i)        = let args = funcArgs $ getFunc r f in
--                                                       if' (i < length args) (Just $ fieldType $ args !! i) Nothing
--ctxExpectType _ (CtxField (EField _ _ _) _)          = Nothing
--ctxExpectType r (CtxLocation (ELocation _ p _ _) _)  = Just $ relRecordType $ getRelation r $ portRel $ getPort r p
--ctxExpectType r (CtxStruct (EStruct _ c _) _ i)      = let args = consArgs $ getConstructor r c in
--                                                       if' (i < length args) (Just $ typ $ args !! i) Nothing
--ctxExpectType r (CtxTuple _ ctx i)                   = case ctxExpectType r ctx of
--                                                            Just t -> case typ' r t of
--                                                                           TTuple _ fs -> if' (i < length fs) (Just $ fs !! i) Nothing
--                                                                           _           -> Nothing
--                                                            Nothing -> Nothing
--ctxExpectType _ (CtxSlice _ _)                       = Nothing
--ctxExpectType _ (CtxMatchExpr _ _)                   = Nothing
--ctxExpectType r (CtxMatchPat e ctx _)                = exprTypeMaybe r (CtxMatchExpr e ctx) $ exprMatchExpr e
--ctxExpectType r (CtxMatchVal _ ctx _)                = ctxExpectType r ctx
--ctxExpectType _ (CtxSeq1 _ _)                        = Nothing
--ctxExpectType r (CtxSeq2 _ ctx)                      = ctxExpectType r ctx
--ctxExpectType _ (CtxPar1 _ _)                        = Just tSink
--ctxExpectType _ (CtxPar2 _ _)                        = Just tSink
--ctxExpectType _ (CtxITEIf _ _)                       = Just tBool
--ctxExpectType r (CtxITEThen _ ctx)                   = ctxExpectType r ctx
--ctxExpectType r (CtxITEElse _ ctx)                   = ctxExpectType r ctx
--ctxExpectType r (CtxSetL e@(ESet _ _ rhs) ctx)       = exprTypeMaybe r (CtxSetR e ctx) rhs
--ctxExpectType r (CtxSetR (ESet _ lhs _) ctx)         = exprTypeMaybe r ctx lhs -- avoid infinite recursion by evaluating lhs standalone
--ctxExpectType _ (CtxSend _ _)                        = Just $ tLocation
--ctxExpectType r (CtxBinOpL e ctx)                    = case exprBOp e of
--                                                            Eq     -> trhs
--                                                            Neq    -> trhs
--                                                            Lt     -> trhs
--                                                            Gt     -> trhs
--                                                            Lte    -> trhs
--                                                            Gte    -> trhs
--                                                            And    -> Just tBool
--                                                            Or     -> Just tBool
--                                                            Impl   -> Just tBool
--                                                            Plus   -> trhs
--                                                            Minus  -> trhs
--                                                            ShiftR -> Nothing
--                                                            ShiftL -> Nothing
--                                                            Mod    -> Nothing
--                                                            BAnd   -> trhs
--                                                            BOr    -> trhs
--                                                            Concat -> Nothing
--    where trhs = exprTypeMaybe r ctx $ exprRight e
--ctxExpectType r (CtxBinOpR e ctx)                    = case exprBOp e of
--                                                            Eq     -> tlhs
--                                                            Neq    -> tlhs
--                                                            Lt     -> tlhs
--                                                            Gt     -> tlhs
--                                                            Lte    -> tlhs
--                                                            Gte    -> tlhs
--                                                            And    -> Just tBool
--                                                            Or     -> Just tBool
--                                                            Impl   -> Just tBool
--                                                            Plus   -> tlhs
--                                                            Minus  -> tlhs
--                                                            ShiftR -> Nothing
--                                                            ShiftL -> Nothing
--                                                            Mod    -> Nothing
--                                                            BAnd   -> tlhs
--                                                            BOr    -> tlhs
--                                                            Concat -> Nothing
--    where tlhs = exprTypeMaybe r ctx $ exprLeft e
--ctxExpectType _ (CtxUnOp (EUnOp _ Not _) _)          = Just tBool
--ctxExpectType r (CtxUnOp (EUnOp _ BNeg _) ctx)       = ctxExpectType r ctx
--ctxExpectType _ (CtxForkCond _ _)                    = Just tBool
--ctxExpectType r (CtxForkBody _ ctx)                  = ctxExpectType r ctx
--ctxExpectType _ (CtxForCond _ _)                     = Just tBool
--ctxExpectType _ (CtxForBody _ _)                     = Just $ tTuple []
--ctxExpectType _ (CtxWithCond _ _)                    = Just tBool
--ctxExpectType r (CtxWithBody _ ctx)                  = ctxExpectType r ctx
--ctxExpectType r (CtxWithDef _ ctx)                   = ctxExpectType r ctx
--ctxExpectType _ (CtxAnyCond _ _)                     = Just tBool
--ctxExpectType r (CtxAnyBody _ ctx)                   = ctxExpectType r ctx
--ctxExpectType r (CtxAnyDef _ ctx)                    = ctxExpectType r ctx
--ctxExpectType _ (CtxTyped (ETyped _ _ t) _)          = Just t
--ctxExpectType r (CtxRelPred e _ i)                   = let args = relArgs $ getRelation r $ exprRel e in
--                                                       if' (i < length args) (Just $ fieldType $ args !! i) Nothing
--ctxExpectType r (CtxPut (EPut _ rel _) _)            = Just $ relRecordType $ getRelation r rel
--ctxExpectType _ (CtxDelete _ _)                      = Just tBool
--ctxExpectType _ (CtxLambda e _)                      = Just $ exprLambdaType e
--ctxExpectType _ (CtxApplyLambda _ _)                 = Nothing
--ctxExpectType r (CtxApplyLambdaArg e@(EApplyLambda _ l _) ctx i)
--                                                     = case exprTypeMaybe r (CtxApplyLambda e ctx) l of
--                                                            Just (TLambda _ ats _) -> Just (typ $ ats!!i)
--                                                            _                      -> Nothing
--ctxExpectType _ ctx                                  = error $ "ctxExpectType " ++ show ctx
