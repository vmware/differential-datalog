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
    exprType, exprType', exprTypeMaybe, exprNodeType,
--    exprTypes,
    exprTraverseTypeM,
    typ', typ'',
    isBool, isBit, isInt, isStruct, isTuple,
    checkTypesMatch,
    typesMatch,
    ctxExpectType,
    ConsTree(..),
    consTreeEmpty,
    typeConsTree,
    consTreeAbduct
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
import Language.DifferentialDatalog.Expr
import Language.DifferentialDatalog.Syntax
import Language.DifferentialDatalog.NS
import Language.DifferentialDatalog.Pos
import Language.DifferentialDatalog.Name
--import {-# SOURCE #-} Relation

-- | An object with type
class WithType a where
    typ  :: a -> Type

instance WithType Type where
    typ = id

instance WithType Field where
    typ = fieldType

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
unifyTypes :: DatalogProgram -> [(Type, Type)] -> Maybe (M.Map String Type)
unifyTypes d ts = do
    m0 <- M.unionsWith (++) <$> mapM ((M.map return <$>) . unifyTypes' d) ts
    M.traverseWithKey (\_ ts -> checkConflicts d ts) m0

checkConflicts :: DatalogProgram -> [Type] -> Maybe Type
checkConflicts d (t:ts) = do
    mapM (\t' -> when (not $ typesMatch d t t') Nothing) ts
    return t

unifyTypes' :: DatalogProgram -> (Type, Type) -> Maybe (M.Map String Type)
unifyTypes' d (a, c) =
    case (a',c') of
        (TBool{}         , TBool{})          -> Just M.empty
        (TInt{}          , TInt{})           -> Just M.empty
        (TString{}       , TString{})        -> Just M.empty
        (TBit _ w1       , TBit _ w2)        | w1 == w2 
                                             -> Just M.empty 
        (TTuple _ as1    , TTuple _ as2)     | length as1 == length as2 
                                             -> unifyTypes d $ zip as1 as2
        (TUser _ n1 as1  , TUser _ n2 as2)   | n1 == n2
                                             -> unifyTypes d $ zip as1 as2
        (TOpaque _ n1 as1, TOpaque _ n2 as2) | n1 == n2
                                             -> unifyTypes d $ zip as1 as2
        (TVar _ n1       , t)                -> Just $ M.singleton n1 t
        _                                    -> Nothing
    where a' = typ'' d a
          c' = typ'' d c

-- | Visitor pattern implementation that carries type information
-- through the syntax tree.
exprTraverseTypeM :: (Monad m) => DatalogProgram -> (ECtx -> ExprNode (Maybe Type) -> m ()) -> ECtx -> Expr -> m ()
exprTraverseTypeM d = exprTraverseCtxWithM (\ctx e -> return $ exprNodeType d ctx e)

-- | Compute type of an expression.  The expression must be previously validated
-- to make sure that it has an unambiguous type.
exprType :: DatalogProgram -> ECtx -> Expr -> Type
exprType d ctx e = maybe (error $ "exprType: expression " ++ show e ++ " has unknown type") id 
                         $ exprTypeMaybe d ctx e

exprType' :: DatalogProgram -> ECtx -> Expr -> Type
exprType' d ctx e = typ' d $ exprType d ctx e

-- | Version of exprType that returns 'Nothing' if the type is not
-- known.  Can be used during validation.
exprTypeMaybe :: DatalogProgram -> ECtx -> Expr -> Maybe Type
exprTypeMaybe d ctx e = exprFoldCtx (\ctx' e' -> fmap ((flip atPos) (pos e')) $ exprNodeType d ctx' e') ctx e

exprNodeType :: DatalogProgram -> ECtx -> ExprNode (Maybe Type) -> Maybe Type
exprNodeType d ctx e = fmap ((flip atPos) (pos e)) $ exprNodeType' d ctx e


funcTypeArgSubsts :: DatalogProgram -> String -> [Type] -> Maybe (M.Map String Type)
funcTypeArgSubsts d fname argtypes = unifyTypes d (zip (map typ funcArgs) argtypes)
    where Function{..} = getFunc d fname

structTypeArgs :: DatalogProgram -> ECtx -> String -> [(String, Type)] -> Maybe [Type]
structTypeArgs d ctx cname argtypes = do
    let TypeDef{..} = consType d cname
        Constructor{..} = getConstructor d cname
    let -- Try to extract type variable bindings from expected type;
        exp = case ctxExpectType'' d ctx of
                   Just (TUser _ n as) | n == tdefName 
                                       -> zip (map tVar tdefArgs) as
                   _                   -> []
    subst <- unifyTypes d $ exp ++ mapMaybe (\a -> (typ a,) <$> lookup (name a) argtypes) consArgs
    mapM (\a -> M.lookup a subst) tdefArgs

exprNodeType' :: DatalogProgram -> ECtx -> ExprNode (Maybe Type) -> Maybe Type
exprNodeType' d ctx (EVar _ v)            = 
    let (lvs, rvs) = ctxMVars d ctx in
    fromJust $ lookup v $ lvs ++ rvs

exprNodeType' d _   (EApply _ f mas)      = do
    let t = funcType $ getFunc d f 
    as <- sequence mas
    subst <- funcTypeArgSubsts d f as
    return $ typeSubstTypeArgs subst t

exprNodeType' d _   (EField _ Nothing f)  = Nothing

exprNodeType' d _   (EField _ (Just e) f) = do
    let TStruct _ cs = typ' d e
    fieldType <$> find ((==f) . name) (concatMap consArgs cs)

exprNodeType' _ _   (EBool _ _)           = Just tBool

exprNodeType' d ctx (EInt _ _)            = do
    expect <- ctxExpectType' d ctx
    case expect of
         t@(TBit _ _) -> return t
         t@(TInt _)   -> return t
         _            -> Nothing

exprNodeType' _ _   (EString _ _)         = Just tString
exprNodeType' _ _   (EBit _ w _)          = Just $ tBit w

exprNodeType' d ctx (EStruct _ c mas)     = do
    let tdef = consType d c
    as <- mapM (\(f, mt) -> (f,) <$> mt) mas
    targs <- structTypeArgs d ctx c as
    return $ tUser (name tdef) targs

exprNodeType' _ _   (ETuple _ fs)         = fmap tTuple $ sequence fs
exprNodeType' _ _   (ESlice _ _ h l)      = Just $ tBit $ h - l + 1
exprNodeType' d _   (EMatch _ _ cs)       = (fromJust . snd) <$> find (isJust . snd) cs
exprNodeType' d ctx (EVarDecl _ _)        = ctxExpectType d ctx
exprNodeType' _ _   (ESeq _ _ e2)         = e2
exprNodeType' d _   (EITE _ _ Nothing e)  = e
exprNodeType' d _   (EITE _ _ (Just t) e) = Just t
exprNodeType' _ _   (ESet _ _ _)          = Just $ tTuple []

exprNodeType' d _   (EBinOp _ op (Just e1) (Just e2)) =
    case op of
         Eq     -> Just tBool
         Neq    -> Just tBool
         Lt     -> Just tBool
         Gt     -> Just tBool
         Lte    -> Just tBool
         Gte    -> Just tBool
         And    -> Just tBool
         Or     -> Just tBool
         Impl   -> Just tBool
         Plus   -> Just $ if isBit d t1 then tBit (max (typeWidth t1) (typeWidth t2)) else tInt
         Minus  -> Just $ if isBit d t1 then tBit (max (typeWidth t1) (typeWidth t2)) else tInt
         Mod    -> Just t1
         Times  -> Just t1
         Div    -> Just t1
         ShiftR -> Just t1
         ShiftL -> Just t1
         BAnd   -> Just t1
         BOr    -> Just t1
         Concat -> Just $ tBit (typeWidth t1 + typeWidth t2)
    where t1 = typ' d e1
          t2 = typ' d e2

exprNodeType' _ _   (EBinOp _ ShiftR e1 _) = e1
exprNodeType' _ _   (EBinOp _ ShiftL e1 _) = e1
exprNodeType' _ _   (EBinOp _ _ _ _)       = Nothing
exprNodeType' _ _   (EUnOp _ Not _)        = Just tBool
exprNodeType' d _   (EUnOp _ BNeg (Just e))= Just $ typ' d e
exprNodeType' _ _   (EUnOp _ _ _)          = Nothing
exprNodeType' d ctx (EPHolder _)           = ctxExpectType d ctx
exprNodeType' _ _   (ETyped _ _ t)         = Just t

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
    assert (typesMatch d x y) p 
           $ "Incompatible types " ++ show (typ x) ++ " and " ++ show (typ y)


-- | True iff 'a' and 'b' have idential types up to type aliasing.
typesMatch :: (WithType a, WithType b) => DatalogProgram -> a -> b -> Bool
typesMatch d x y =
    case (t1, t2) of
         (TBool _          , TBool _)          -> True
         (TBit _ w1        , TBit _ w2)        -> w1 == w2
         (TInt _           , TInt _)           -> True
         (TString _        , TString _)        -> True
         (TTuple _ fs1     , TTuple _ fs2)     -> (length fs1 == length fs2) &&
                                                  (all (\(f1, f2) -> typesMatch d f1 f2) $ zip fs1 fs2)
         (TUser _ t1 as1   , TUser _ t2 as2)   -> t1 == t2 &&
                                                  (all (\(a1, a2) -> typesMatch d a1 a2) $ zip as1 as2)
         (TOpaque _ t1 as1 , TOpaque _ t2 as2) -> t1 == t2 &&
                                                  (all (\(a1, a2) -> typesMatch d a1 a2) $ zip as1 as2)
         (TVar _ v1        , TVar _ v2)        -> v1 == v2
         _                                     -> False
    where t1 = typ'' d x
          t2 = typ'' d y


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
ctxExpectType d (CtxRuleL Rule{..} i fname)          = 
    let rel = getRelation d (atomRelation $ ruleLHS !! i)
    in fmap fieldType $ find ((== fname) . name) $ relArgs rel
ctxExpectType d (CtxRuleRAtom Rule{..} i fname)      =
    let rel = getRelation d (atomRelation $ rhsAtom $ ruleRHS !! i)
    in fmap fieldType $ find ((== fname) . name) $ relArgs rel
ctxExpectType _ (CtxRuleRCond Rule{..} i)            =
    case rhsExpr $ ruleRHS !! i of
         E ESet{} -> Just $ tTuple []
         _        -> Just tBool
ctxExpectType _ CtxRuleRFlatMap{}                    = Nothing
ctxExpectType _ CtxRuleRAggregate{}                  = Nothing
ctxExpectType d (CtxApply (EApply _ f _) _ i)        =
    let args = funcArgs $ getFunc d f
        t = fieldType $ args !! i in
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
ctxExpectType d (CtxSetR (ESet _ lhs _) ctx)         = exprTypeMaybe d ctx lhs -- avoid infinite recursion by evaluating lhs standalone
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
