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

{-# LANGUAGE FlexibleContexts, RecordWildCards, TupleSections #-}

module Language.DifferentialDatalog.Type(
    WithType(..),
    typeUserTypes,
    typeIsPolymorphic,
    unifyTypes,
    exprType,
    exprType',
    exprType'',
    exprNodeType,
    relKeyType,
    typ', typ'',
    isBool, isBit, isInt, isString, isStruct, isTuple,
    checkTypesMatch,
    typesMatch,
    typeNormalize,
    typeSubstTypeArgs,
    ctxExpectType,
    ConsTree(..),
    consTreeEmpty,
    typeConsTree,
    consTreeAbduct,
    typeMapM,
    funcTypeArgSubsts
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
import qualified Data.Map as M
--import Debug.Trace

import Language.DifferentialDatalog.Util
import Language.DifferentialDatalog.Ops
import {-# SOURCE #-} Language.DifferentialDatalog.Expr
import Language.DifferentialDatalog.Syntax
import Language.DifferentialDatalog.NS
import Language.DifferentialDatalog.Pos
import Language.DifferentialDatalog.Name
import Language.DifferentialDatalog.ECtx
--import {-# SOURCE #-} Relation

-- | An object with type
class WithType a where
    typ     :: a -> Type
    setType :: a -> Type -> a

instance WithType Type where
    typ = id
    setType _ t = t

instance WithType Field where
    typ = fieldType
    setType f t = f { fieldType = t } 

instance WithType FuncArg where
    typ = argType
    setType a t = a { argType = t } 

instance WithType Relation where
    typ = relType
    setType r t = r { relType = t }

-- | True iff t is a polymorphic type, i.e., contains any type variables.
typeIsPolymorphic :: Type -> Bool
typeIsPolymorphic TBool{}     = False
typeIsPolymorphic TInt{}      = False
typeIsPolymorphic TString{}   = False
typeIsPolymorphic TBit{}      = False
typeIsPolymorphic TStruct{..} = any (any (typeIsPolymorphic . typ) . consArgs) typeCons
typeIsPolymorphic TTuple{..}  = any typeIsPolymorphic typeTupArgs
typeIsPolymorphic TUser{..}   = any typeIsPolymorphic typeArgs
typeIsPolymorphic TVar{}      = True
typeIsPolymorphic TOpaque{..} = any typeIsPolymorphic typeArgs


-- | Matches function parameter types against concrete argument types, e.g.,
-- given
-- > [(t<'A>, t<q<'B>>)]
-- derives
-- > 'A = q<'B>
-- 
-- Returns mapping from type variables to concrete types or 'Nothing'
-- if no such mapping was found due to a conflict, e.g.:
-- > [(t<'A>, t<q<'B>>), ('A, int)] // conflict
--
-- Note that concrete argument types can contain type variables.
-- Concrete and abstract type variables belong to different
-- namespaces (i.e., the same name represents different variables in
-- concrete and abstract types). 
unifyTypes :: (MonadError String me) => DatalogProgram -> Pos -> String -> [(Type, Type)] -> me (M.Map String Type)
unifyTypes d p ctx ts = do
    m0 <- M.unionsWith (++) <$> mapM ((M.map return <$>) . unifyTypes' d p ctx) ts
    M.traverseWithKey (\v ts -> checkConflicts d p ctx v ts) m0

checkConflicts :: (MonadError String me) => DatalogProgram -> Pos -> String -> String -> [Type] -> me Type
checkConflicts d p ctx v (t:ts) = do
    mapM (\t' -> when (not $ typesMatch d t t') 
                      $ err p $ "Conflicting bindings " ++ show t ++ " and " ++ show t' ++ " for type variable '" ++ v ++ " " ++ ctx) ts
    return t

unifyTypes' :: (MonadError String me) => DatalogProgram -> Pos -> String -> (Type, Type) -> me (M.Map String Type)
unifyTypes' d p ctx (a, c) =
    case (a',c') of
        (TBool{}         , TBool{})          -> return M.empty
        (TInt{}          , TInt{})           -> return M.empty
        (TString{}       , TString{})        -> return M.empty
        (TBit _ w1       , TBit _ w2)        | w1 == w2 
                                             -> return M.empty 
        (TTuple _ as1    , TTuple _ as2)     | length as1 == length as2 
                                             -> unifyTypes d p ctx $ zip as1 as2
        (TUser _ n1 as1  , TUser _ n2 as2)   | n1 == n2
                                             -> unifyTypes d p ctx $ zip as1 as2
        (TOpaque _ n1 as1, TOpaque _ n2 as2) | n1 == n2
                                             -> unifyTypes d p ctx $ zip as1 as2
        (TVar _ n1       , t)                -> return $ M.singleton n1 t
        _                                    -> err p $ "Cannot match expected type " ++ show a ++ " and actual type " ++ show c ++ " " ++ ctx  
    where a' = typ'' d a
          c' = typ'' d c

-- | Compute type of an expression.  The expression must be previously validated
-- to make sure that it has an unambiguous type.
exprType :: DatalogProgram -> ECtx -> Expr -> Type
exprType d ctx e = maybe (error $ "exprType: expression " ++ show e ++ " has unknown type in " ++ show ctx) id 
                         $ exprTypeMaybe d ctx e

-- | Like 'exprType', but also applies 'typ'' to result.
exprType' :: DatalogProgram -> ECtx -> Expr -> Type
exprType' d ctx e = typ' d $ exprType d ctx e

-- | Like 'exprType', but also applies 'typ''' to result.
exprType'' :: DatalogProgram -> ECtx -> Expr -> Type
exprType'' d ctx e = typ'' d $ exprType d ctx e

-- | Version of exprType that returns 'Nothing' if the type is not
-- known.  Can be used during validation.
exprTypeMaybe :: DatalogProgram -> ECtx -> Expr -> Maybe Type
exprTypeMaybe d ctx e = 
    exprFoldCtx (\ctx' e' -> 
                  case exprNodeType' d ctx' e' of
                       Left _  -> Nothing
                       Right t -> Just $ atPos t (pos e')) 
                ctx e

-- | Type of relation's primary key, if it has one
relKeyType :: DatalogProgram -> Relation -> Maybe Type
relKeyType d rel =
    fmap (\KeyExpr{..} -> exprType d (CtxKey rel) keyExpr) $ relPrimaryKey rel

-- | Compute expression node type; fail if type is undefined or there
-- is a conflict.
exprNodeType :: (MonadError String me) => DatalogProgram -> ECtx -> ExprNode Type -> me Type
exprNodeType d ctx e = fmap ((flip atPos) (pos e)) $ exprNodeType' d ctx (exprMap Just e)

funcTypeArgSubsts :: (MonadError String me) => DatalogProgram -> Pos -> Function -> [Type] -> me (M.Map String Type)
funcTypeArgSubsts d p f@Function{..} argtypes =
    unifyTypes d p ("in call to " ++ funcShowProto f) (zip (map typ funcArgs ++ [funcType]) argtypes)

structTypeArgs :: (MonadError String me) => DatalogProgram -> Pos -> ECtx -> String -> [(String, Type)] -> me [Type]
structTypeArgs d p ctx cname argtypes = do
    let TypeDef{..} = consType d cname
        Constructor{..} = getConstructor d cname
    let -- Try to extract type variable bindings from expected type;
        exp = case ctxExpectType'' d ctx of
                   Just (TUser _ n as) | n == tdefName 
                                       -> zip (map tVar tdefArgs) as
                   _                   -> []
    subst <- unifyTypes d p ("in type constructor " ++ cname) 
                        $ exp ++ mapMaybe (\a -> (typ a,) <$> lookup (name a) argtypes) consArgs
    mapM (\a -> case M.lookup a subst of
                     Nothing -> err p $ "Unable to bind type argument '" ++ a ++ " of type " ++ tdefName ++ 
                                        " to a concrete type in a call to type constructor " ++ cname
                     Just t  -> return t)
         tdefArgs

eunknown :: (MonadError String me) => Pos -> ECtx -> me Type
eunknown p ctx = err p $ "Expression has unknown type in " ++ show ctx

mtype2me :: (MonadError String me) => Pos -> ECtx -> Maybe Type -> me Type
mtype2me p ctx Nothing  = err p $ "Expression has unknown type in " ++ show ctx
mtype2me _ _   (Just t) = return t

exprNodeType' :: (MonadError String me) => DatalogProgram -> ECtx -> ExprNode (Maybe Type) -> me Type
exprNodeType' d ctx e@(EVar p v)            = 
    let (lvs, rvs) = ctxMVars d ctx in
    case lookup v $ lvs ++ rvs of
         Just mt -> mtype2me p ctx mt
         Nothing | ctxInRuleRHSPattern ctx -- handle implicit vardecls in rules
                 -> mtype2me p ctx $ ctxExpectType d ctx
         _       -> eunknown p ctx

exprNodeType' d ctx (EApply p f mas)      = do
    let func = getFunc d f
    let t = funcType func
    as <- mapM (mtype2me p ctx) mas
    subst <- funcTypeArgSubsts d p func $ as ++ maybeToList (ctxExpectType d ctx)
    return $ typeSubstTypeArgs subst t

exprNodeType' d ctx (EField p Nothing f)  = eunknown p ctx

exprNodeType' d _   (EField p (Just e) f) = do
    case typ' d e of
         t@TStruct{} -> 
             case find ((==f) . name) $ structFields t of
                  Nothing  -> err p  $ "Unknown field \"" ++ f ++ "\" in struct of type " ++ show t
                  Just fld -> do check (not $ structFieldGuarded (typ' d e) f) p
                                       $ "Access to guarded field \"" ++ f ++ "\""
                                 return $ fieldType fld
         _           -> err (pos e) $ "Expression is not a struct"

exprNodeType' _ _   (EBool _ _)           = return tBool

exprNodeType' d ctx (EInt p _)            = do
    case ctxExpectType' d ctx of
         Just t@(TBit _ _) -> return t
         Just t@(TInt _)   -> return t
         Nothing           -> return tInt
         _                 -> eunknown p ctx

exprNodeType' _ _   (EString _ _)         = return tString
exprNodeType' _ _   (EBit _ w _)          = return $ tBit w

exprNodeType' d ctx (EStruct p c mas)     = do
    let tdef = consType d c
    as <- mapM (\(f, mt) -> (f,) <$> mtype2me p ctx mt) mas
    targs <- structTypeArgs d p ctx c as
    return $ tUser (name tdef) targs

exprNodeType' _ ctx (ETuple p fs)         = fmap tTuple $ mapM (mtype2me p ctx) fs
exprNodeType' _ _   (ESlice _ _ h l)      = return $ tBit $ h - l + 1
exprNodeType' d ctx (EMatch p _ cs)       = mtype2me p ctx $ (fromJust . snd) <$> find (isJust . snd) cs
exprNodeType' d ctx (EVarDecl p _)        = mtype2me p ctx $ ctxExpectType d ctx
exprNodeType' _ ctx (ESeq p _ e2)         = mtype2me p ctx e2
exprNodeType' d ctx (EITE p _ Nothing e)  = mtype2me p ctx e
exprNodeType' d ctx (EITE _ _ (Just t) e) = return t
exprNodeType' _ _   (ESet _ _ _)          = return $ tTuple []

exprNodeType' d _   (EBinOp _ op (Just e1) (Just e2)) =
    case op of
         Eq     -> return tBool
         Neq    -> return tBool
         Lt     -> return tBool
         Gt     -> return tBool
         Lte    -> return tBool
         Gte    -> return tBool
         And    -> return tBool
         Or     -> return tBool
         Impl   -> return tBool
         Plus   -> return $ if isBit d t1 then tBit (max (typeWidth t1) (typeWidth t2)) else tInt
         Minus  -> return $ if isBit d t1 then tBit (max (typeWidth t1) (typeWidth t2)) else tInt
         Mod    -> return t1
         Times  -> return t1
         Div    -> return t1
         ShiftR -> return t1
         ShiftL -> return t1
         BAnd   -> return t1
         BOr    -> return t1
         Concat | isString d e1
                -> return tString
         Concat -> return $ tBit (typeWidth t1 + typeWidth t2)
    where t1 = typ' d e1
          t2 = typ' d e2

exprNodeType' _ ctx (EBinOp p ShiftR e1 _) = mtype2me p ctx e1
exprNodeType' _ ctx (EBinOp p ShiftL e1 _) = mtype2me p ctx e1
exprNodeType' _ ctx (EBinOp p _ _ _)       = eunknown p ctx
exprNodeType' _ _   (EUnOp _ Not _)        = return tBool
exprNodeType' d _   (EUnOp _ BNeg (Just e))= return $ typ' d e
exprNodeType' _ ctx (EUnOp p _ _)          = eunknown p ctx
exprNodeType' d ctx (EPHolder p)           = mtype2me p ctx $ ctxExpectType d ctx
exprNodeType' _ _   (ETyped _ _ t)         = return t

--exprTypes :: Refine -> ECtx -> Expr -> [Type]
--exprTypes r ctx e = nub $ execState (exprTraverseTypeM r (\ctx' e' -> modify ((fromJust $ exprNodeType r ctx' e'):)) ctx e) []

-- | Expand typedef's down to actual type definition, substituting
-- type arguments along the way
typ' :: (WithType a) => DatalogProgram -> a -> Type
typ' d x = _typ' d (typ x)

_typ' :: DatalogProgram -> Type -> Type
_typ' d (TUser _ n as) = 
    case tdefType tdef of
         Nothing -> tOpaque n as
         Just t  -> _typ' d $ typeSubstTypeArgs (M.fromList $ zip (tdefArgs tdef) as) t
    where tdef = getType d n
_typ' _ t = t 

-- | Similar to typ', but does not expand the last typedef if it is a struct
typ'' :: (WithType a) => DatalogProgram -> a -> Type
typ'' d x = _typ'' d (typ x)

_typ'' :: DatalogProgram -> Type -> Type
_typ'' d t'@(TUser _ n as) =
    case tdefType tdef of
         (Just (TStruct _ _)) -> t'
         Nothing              -> tOpaque n as
         Just t               -> _typ'' d $ typeSubstTypeArgs (M.fromList $ zip (tdefArgs tdef) as) t
    where tdef = getType d n
_typ'' _ t = t

typeSubstTypeArgs :: M.Map String Type -> Type -> Type
typeSubstTypeArgs subst (TUser _ n as)   = tUser n (map (typeSubstTypeArgs subst) as)
typeSubstTypeArgs subst (TOpaque _ n as) = tOpaque n (map (typeSubstTypeArgs subst) as)
typeSubstTypeArgs subst (TStruct _ cs)   = tStruct $ map (consSubstTypeArgs subst)  cs
typeSubstTypeArgs subst (TTuple _ ts)    = tTuple $ map (typeSubstTypeArgs subst)  ts
typeSubstTypeArgs subst (TVar _ tv)      = subst M.! tv
typeSubstTypeArgs _     t                = t

consSubstTypeArgs :: M.Map String Type -> Constructor -> Constructor
consSubstTypeArgs subst c = c{consArgs = args}
    where 
    args = map (\a -> a{fieldType = typeSubstTypeArgs subst $ fieldType a}) 
               $ consArgs c

isBool :: (WithType a) => DatalogProgram -> a -> Bool
isBool d a = case typ' d a of
                  TBool _ -> True
                  _       -> False

isBit :: (WithType a) => DatalogProgram -> a -> Bool
isBit d a = case typ' d a of
                 TBit _ _ -> True
                 _        -> False

isInt :: (WithType a) => DatalogProgram -> a -> Bool
isInt d a = case typ' d a of
                 TInt _ -> True
                 _      -> False

isString :: (WithType a) => DatalogProgram -> a -> Bool
isString d a = case typ' d a of
                    TString _ -> True
                    _      -> False

isStruct :: (WithType a) => DatalogProgram -> a -> Bool
isStruct d a = case typ' d a of
                    TStruct _ _ -> True
                    _           -> False

isTuple :: (WithType a) => DatalogProgram -> a -> Bool
isTuple d a = case typ' d a of
                   TTuple _ _ -> True
                   _          -> False

-- | Check if 'a' and 'b' have idential types up to type aliasing;
-- throw exception if they don't.
checkTypesMatch :: (MonadError String me, WithType a, WithType b) => Pos -> DatalogProgram -> a -> b -> me ()
checkTypesMatch p d x y = 
    check (typesMatch d x y) p 
          $ "Incompatible types " ++ show (typ x) ++ " and " ++ show (typ y)


-- | True iff 'a' and 'b' have idential types up to type aliasing.
typesMatch :: (WithType a, WithType b) => DatalogProgram -> a -> b -> Bool
typesMatch d x y = typeNormalize d x == typeNormalize d y

-- | Normalize type by applying typ'' to all its fields and type
-- arguments. 
typeNormalize :: (WithType a) => DatalogProgram -> a -> Type
typeNormalize d x = typeNormalize' d $ typ x

typeNormalize' :: DatalogProgram -> Type -> Type
typeNormalize' d t = 
    case t' of
         TBool{}            -> t'
         TBit{}             -> t'
         TInt{}             -> t'
         TString{}          -> t'
         TTuple{..}         -> t'{typeTupArgs = map (typeNormalize d) typeTupArgs}
         TUser{..}          -> t'{typeArgs = map (typeNormalize d) typeArgs}
         TOpaque{..}        -> t'{typeArgs = map (typeNormalize d) typeArgs}
         TVar{}             -> t'
    where t' = typ'' d t

-- User-defined types that appear in type expression
typeUserTypes :: Type -> [String]
typeUserTypes = nub . typeUserTypes'

typeUserTypes' :: Type -> [String]
typeUserTypes' TStruct{..} = concatMap (typeUserTypes . typ) 
                             $ concatMap consArgs typeCons
typeUserTypes' TTuple{..}  = concatMap (typeUserTypes . typ) typeTupArgs
typeUserTypes' TUser{..}   = typeName : concatMap typeUserTypes typeArgs
typeUserTypes' TOpaque{..} = typeName : concatMap typeUserTypes typeArgs
typeUserTypes' _           = []

-- sub-types that immediately appear in the type expression
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


-- | Rudimentary type inference engine. Infers expected type from context. 
ctxExpectType :: DatalogProgram -> ECtx -> Maybe Type
ctxExpectType _ CtxTop                               = Nothing
ctxExpectType _ (CtxFunc f)                          = Just $ funcType f
ctxExpectType d (CtxRuleL Rule{..} i)                = 
    Just $ relType $ getRelation d (atomRelation $ ruleLHS !! i)
ctxExpectType d (CtxRuleRAtom Rule{..} i)            =
    Just $ relType $ getRelation d (atomRelation $ rhsAtom $ ruleRHS !! i)
ctxExpectType _ (CtxRuleRCond Rule{..} i)            =
    case rhsExpr $ ruleRHS !! i of
         E ESet{} -> Just $ tTuple []
         _        -> Just tBool
ctxExpectType _ CtxRuleRFlatMap{}                    = Nothing
ctxExpectType _ CtxRuleRAggregate{}                  = Nothing
ctxExpectType _ CtxKey{}                             = Nothing
ctxExpectType d (CtxApply (EApply _ f _) _ i)        =
    let args = funcArgs $ getFunc d f
        t = argType $ args !! i in
    if i < length args
       -- Don't attempt to concretize polymorphic types here.
       -- Worry about this is if there is a use case.
       then if typeIsPolymorphic t
               then Nothing
               else Just t
       else Nothing
ctxExpectType _ (CtxField (EField _ _ _) _)          = Nothing
ctxExpectType d (CtxStruct (EStruct _ c _) ctx arg)  = do
    -- If struct type is known from context, e.g., it occurs in a match
    -- pattern or in a relational atom, propagate expected type info
    -- to fields to be able to detemine their type.  Otherwise, return
    -- Nothing.  We will validate field types later, when performing
    -- type unification in exprNodeType.
    expect <- ctxExpectType' d ctx
    case expect of
         TStruct _ cs -> do 
             cons <- find ((==c) . name) cs
             f <- find ((==arg) . name) $ consArgs cons
             return $ typ f
         _            -> Nothing
    {- let args = consArgs $ getConstructor d c in
    case find ((== arg) . name) args of
         Nothing -> Nothing
         Just a  -> let t = fieldType a in
                    if typeIsPolymorphic t
                       then Nothing
                       else Just t -}
         
ctxExpectType d (CtxTuple _ ctx i)                   = 
    case ctxExpectType d ctx of
         Just t -> case typ' d t of
                        TTuple _ fs -> if i < length fs then Just $ fs !! i else Nothing
                        _           -> Nothing
         Nothing -> Nothing
ctxExpectType _ (CtxSlice _ _)                       = Nothing
ctxExpectType _ (CtxMatchExpr _ _)                   = Nothing
ctxExpectType d (CtxMatchPat e ctx _)                = 
    exprTypeMaybe d (CtxMatchExpr e ctx) $ exprMatchExpr e
ctxExpectType d (CtxMatchVal _ ctx _)                = ctxExpectType d ctx
ctxExpectType _ (CtxSeq1 _ _)                        = Nothing
ctxExpectType d (CtxSeq2 _ ctx)                      = ctxExpectType d ctx
ctxExpectType _ (CtxITEIf _ _)                       = Just tBool
ctxExpectType d (CtxITEThen _ ctx)                   = ctxExpectType d ctx
ctxExpectType d (CtxITEElse _ ctx)                   = ctxExpectType d ctx
ctxExpectType d (CtxSetL e@(ESet _ _ rhs) ctx)       = exprTypeMaybe d (CtxSetR e ctx) rhs
ctxExpectType d (CtxSetR (ESet _ lhs _) ctx)         = -- avoid infinite recursion by evaluating lhs standalone
                                                       exprTypeMaybe d (CtxSeq1 (ESeq nopos lhs (error "ctxExpectType: should be unreachable")) ctx) lhs
ctxExpectType d (CtxBinOpL e ctx)                    = 
    case exprBOp e of
         Eq     -> trhs
         Neq    -> trhs
         Lt     -> trhs
         Gt     -> trhs
         Lte    -> trhs
         Gte    -> trhs
         And    -> Just tBool
         Or     -> Just tBool
         Impl   -> Just tBool
         Plus   -> trhs
         Minus  -> trhs
         ShiftR -> Nothing
         ShiftL -> Nothing
         Mod    -> Nothing
         Times  -> trhs
         Div    -> trhs
         BAnd   -> trhs
         BOr    -> trhs
         Concat -> Nothing
    where trhs = exprTypeMaybe d ctx $ exprRight e
ctxExpectType d (CtxBinOpR e ctx)                    = 
    case exprBOp e of
         Eq     -> tlhs
         Neq    -> tlhs
         Lt     -> tlhs
         Gt     -> tlhs
         Lte    -> tlhs
         Gte    -> tlhs
         And    -> Just tBool
         Or     -> Just tBool
         Impl   -> Just tBool
         Plus   -> tlhs
         Minus  -> tlhs
         ShiftR -> Nothing
         ShiftL -> Nothing
         Mod    -> Nothing
         Times  -> tlhs
         Div    -> tlhs
         BAnd   -> tlhs
         BOr    -> tlhs
         Concat -> Nothing
    where tlhs = exprTypeMaybe d ctx $ exprLeft e
ctxExpectType _ (CtxUnOp (EUnOp _ Not _) _)          = Just tBool
ctxExpectType d (CtxUnOp (EUnOp _ BNeg _) ctx)       = ctxExpectType d ctx
ctxExpectType _ (CtxTyped (ETyped _ _ t) _)          = Just t


ctxExpectType'' :: DatalogProgram -> ECtx -> Maybe Type
ctxExpectType'' d ctx = typ'' d <$> ctxExpectType d ctx

ctxExpectType' :: DatalogProgram -> ECtx -> Maybe Type
ctxExpectType' d ctx = typ' d <$> ctxExpectType d ctx


-- | Constructor tree represents the set of patterns that covers the
-- entire type.
--
-- The tree is constructed lazily, starting with a single wildcard
-- pattern (_), and then expanding it as parts of the tree are
-- covered by match patterns.
type CTreeNode = ExprNode ConsTree
data ConsTree = CT Type [CTreeNode] deriving Eq

-- | Check if the tree is empty
consTreeEmpty :: ConsTree -> Bool 
consTreeEmpty (CT _ []) = True
consTreeEmpty _         = False

-- | Build constructor tree of a type
typeConsTree :: DatalogProgram -> Type -> ConsTree
typeConsTree d t = CT t [EPHolder nopos]

consTreeNodeExpand :: DatalogProgram -> Type -> [CTreeNode]
consTreeNodeExpand d t = 
    case typ' d t of
         TStruct _ cs -> map (\c -> EStruct nopos (name c)
                                            $ map (\a -> (name a, CT (typ a) [EPHolder nopos]))
                                            $ consArgs c) cs
         TTuple _ fs  -> [ETuple nopos $ map (\f -> CT (typ f) [EPHolder nopos]) fs]
         TBool{}      -> [EBool nopos False, EBool nopos True]
         _            -> [EPHolder nopos]
    
-- | Abduct a pattern from constructor tree. Returns the remaining
-- tree and the abducted part.  
--
-- If the remaining tree is empty, this means that the type has been
-- fully covered by a sequence of patterns.
--
-- If the abducted part is empty, this means that the pattern is
-- redundant.
consTreeAbduct :: DatalogProgram -> ConsTree -> Expr -> (ConsTree, ConsTree)

-- wildcard (_), var decl abduct the entire tree
consTreeAbduct d (CT t cts) (E EPHolder{}) = (CT t [], CT t cts)
consTreeAbduct d (CT t cts) (E EVarDecl{}) = (CT t [], CT t cts)

-- expand the tree if necessary
consTreeAbduct d (CT t [EPHolder{}]) e =
    consTreeAbduct' d (CT t $ consTreeNodeExpand d t) e

consTreeAbduct d ct e = consTreeAbduct' d ct e


consTreeAbduct' :: DatalogProgram -> ConsTree -> Expr -> (ConsTree, ConsTree)
consTreeAbduct' d ct@(CT t nodes) (E e) = 
    case e of
         EBool p b      -> (CT t $ filter (/= EBool p b) nodes, CT t $ filter (== EBool p b) nodes)
         EInt p v       -> (ct, CT t [EInt p v])
         EString p s    -> (ct, CT t [EString p s])
         EBit p w v     -> (ct, CT t [EBit p w v])
         EStruct p c fs -> 
             let (leftover, abducted) = unzip $ map (\nd -> abductStruct d e nd) nodes
             in (CT t $ concat leftover, CT t $ concat abducted)
         ETuple p es    -> 
             let (leftover, abducted) = unzip $ map (\(ETuple _ ts) -> abductTuple d es ts) nodes
             in (CT t $ concat leftover, CT t $ concat abducted)
         ETyped p x _   -> consTreeAbduct d ct x


abductTuple :: DatalogProgram -> [Expr] -> [ConsTree] -> ([CTreeNode], [CTreeNode])
abductTuple d es ts = (map (ETuple nopos) leftover, map (ETuple nopos) abducted)
    where 
    (leftover, abducted) = abductMany d es ts

abductStruct :: DatalogProgram -> ENode -> CTreeNode -> ([CTreeNode], [CTreeNode])
abductStruct d (EStruct _ c fs) (EStruct _ c' ts) | c == c' =
    (map (EStruct nopos c . zip fnames) leftover, map (EStruct nopos c . zip fnames) abducted)
    where 
    (fnames, fvals) = unzip fs
    (leftover, abducted) = abductMany d fvals (map snd ts)
abductStruct d _ nd  = ([nd], [])

abductMany :: DatalogProgram -> [Expr] -> [ConsTree] -> ([[ConsTree]], [[ConsTree]])
abductMany d []     []       = ([], [[]])
abductMany d (e:es) (ct:cts) =
    let (CT t leftover, CT _ abducted) = consTreeAbduct d ct e
        (leftover', abducted') = abductMany d es cts
        leftover'' = concatMap (\l -> map ((CT t [l]) :) abducted') leftover ++
                     map (\l' -> ct : l') leftover'
        abducted'' = concatMap (\a -> map ((CT t [a]) :) abducted') abducted
    in (leftover'', abducted'')


-- | Visitor pattern for types
typeMapM :: (Monad m) => (Type -> m Type) -> Type -> m Type
typeMapM fun t@TBool{}     = fun t
typeMapM fun t@TInt{}      = fun t
typeMapM fun t@TString{}   = fun t
typeMapM fun t@TBit{}      = fun t
typeMapM fun t@TStruct{..} = do
    cons <- mapM (\c -> do 
                   cargs <- mapM (\a -> setType a <$> (typeMapM fun $ typ a)) $ consArgs c
                   return c{consArgs = cargs}) typeCons
    fun $ t { typeCons = cons }
typeMapM fun t@TTuple{..} = do
    args <- mapM (typeMapM fun) typeTupArgs
    fun $ t { typeTupArgs = args }
typeMapM fun t@TUser{..} = do
    args <- mapM (typeMapM fun) typeArgs
    fun $ t { typeArgs = args }
typeMapM fun t@TVar{}      = fun t
typeMapM fun t@TOpaque{..} = do
    args <- mapM (typeMapM fun) typeArgs
    fun $ t { typeArgs = args }
