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

{-# LANGUAGE FlexibleContexts, RecordWildCards, TupleSections #-}

module Language.DifferentialDatalog.Type(
    WithType(..),
    typeUserTypes,
    typeStaticMemberTypes,
    typeIsPolymorphic,
    unifyTypes,
    exprType,
    exprTypeMaybe,
    exprType',
    exprType'',
    exprNodeType,
    relKeyType,
    typ', typ'',
    typDeref',
    isBool, isBit, isSigned, isBigInt, isInteger, isFP, isString, isStruct, isTuple, isGroup, isMap, isTinySet, isRef, isDouble, isFloat,
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
    typeMap,
    funcTypeArgSubsts,
    funcGroupArgTypes,
    sET_TYPES,
    tINYSET_TYPE,
    mAP_TYPE,
    iNTERNED_TYPES,
    gROUP_TYPE,
    rEF_TYPE,
    ePOCH_TYPE,
    iTERATION_TYPE,
    wEIGHT_TYPE,
    checkIterable,
    typeIterType
) where

import Data.Maybe
import Data.List
import Control.Monad.Except
import Control.Monad.Identity
import qualified Data.Map as M
--import Debug.Trace

import Language.DifferentialDatalog.Ops
import {-# SOURCE #-} Language.DifferentialDatalog.Expr
import Language.DifferentialDatalog.Syntax
import Language.DifferentialDatalog.NS
import Language.DifferentialDatalog.Pos
import Language.DifferentialDatalog.Name
import Language.DifferentialDatalog.Function
import Language.DifferentialDatalog.ECtx
import Language.DifferentialDatalog.Error
--import {-# SOURCE #-} Relation

sET_TYPES :: [String]
sET_TYPES = ["std.Set", "std.Vec", tINYSET_TYPE]

tINYSET_TYPE :: String
tINYSET_TYPE = "tinyset.Set64"

-- Dynamically allocated types.
dYNAMIC_TYPES :: [String]
dYNAMIC_TYPES = rEF_TYPE : mAP_TYPE : sET_TYPES

gROUP_TYPE :: String
gROUP_TYPE = "std.Group"

rEF_TYPE :: String
rEF_TYPE = "std.Ref"

mAP_TYPE :: String
mAP_TYPE = "std.Map"

iNTERNED_TYPES :: [String]
iNTERNED_TYPES = ["intern.IObj", "internment.Intern"]

-- Special types used by Inspect operator.
ePOCH_TYPE :: String
ePOCH_TYPE = "std.DDEpoch"

iTERATION_TYPE :: String
iTERATION_TYPE = "std.DDIteration"

wEIGHT_TYPE :: String
wEIGHT_TYPE = "std.DDWeight"

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
typeIsPolymorphic TSigned{}   = False
typeIsPolymorphic TDouble{}   = False
typeIsPolymorphic TFloat{}    = False
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
-- Returns mapping from type variables to concrete types or an error
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
    M.traverseWithKey (\v ts' -> checkConflicts d p ctx v ts') m0

checkConflicts :: (MonadError String me) => DatalogProgram -> Pos -> String -> String -> [Type] -> me Type
checkConflicts d p ctx v (t:ts) = do
    mapM_ (\t' -> when (not $ typesMatch d t t')
                       $ err d p $ "Conflicting bindings " ++ show t ++ " and " ++ show t' ++ " for type variable '" ++ v ++ " " ++ ctx) ts
    return t
checkConflicts _ _ _ _ [] = error $ "Type.checkConflicts: invalid input"

unifyTypes' :: (MonadError String me) => DatalogProgram -> Pos -> String -> (Type, Type) -> me (M.Map String Type)
unifyTypes' d p ctx (a, c) =
    case (a',c') of
        (TBool{}         , TBool{})          -> return M.empty
        (TInt{}          , TInt{})           -> return M.empty
        (TString{}       , TString{})        -> return M.empty
        (TDouble{}       , TDouble{})        -> return M.empty
        (TFloat{}        , TFloat{})         -> return M.empty
        (TBit _ w1       , TBit _ w2)        | w1 == w2
                                             -> return M.empty
        (TSigned _ w1    , TSigned _ w2)     | w1 == w2
                                             -> return M.empty
        (TTuple _ as1    , TTuple _ as2)     | length as1 == length as2
                                             -> unifyTypes d p ctx $ zip as1 as2
        (TUser _ n1 as1  , TUser _ n2 as2)   | n1 == n2
                                             -> unifyTypes d p ctx $ zip as1 as2
        (TOpaque _ n1 as1, TOpaque _ n2 as2) | n1 == n2
                                             -> unifyTypes d p ctx $ zip as1 as2
        (TVar _ n1       , t)                -> return $ M.singleton n1 t
        _                                    -> err d p $ "Cannot match expected type " ++ show a ++ " and actual type " ++ show c ++ " " ++ ctx
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

structTypeArgs :: (MonadError String me) => DatalogProgram -> Pos -> ECtx -> String -> [(String, Type)] -> me [Type]
structTypeArgs d p ctx cname argtypes = do
    let TypeDef{..} = consType d cname
        Constructor{..} = getConstructor d cname
    let -- Try to extract type variable bindings from expected type;
        expect = case ctxExpectType'' d ctx of
                      Just (TUser _ n as) | n == tdefName
                                          -> zip (map tVar tdefArgs) as
                      _                   -> []
    subst <- unifyTypes d p ("in type constructor " ++ cname)
                        $ expect ++ mapMaybe (\a -> (typ a,) <$> lookup (name a) argtypes) consArgs
    mapM (\a -> case M.lookup a subst of
                     Nothing -> err d p $ "Unable to bind type argument '" ++ a ++ " of type " ++ tdefName ++
                                        " to a concrete type in a call to type constructor " ++ cname
                     Just t  -> return t)
         tdefArgs

eunknown :: (MonadError String me) => DatalogProgram -> Pos -> ECtx -> me Type
eunknown d p ctx = err d p $ "Expression has unknown type in " ++ show ctx

mtype2me :: (MonadError String me) => DatalogProgram -> Pos -> ECtx -> Maybe Type -> me Type
mtype2me d p ctx Nothing  = err d p $ "Expression has unknown type in " ++ show ctx
mtype2me _ _ _   (Just t) = return t

exprNodeType' :: (MonadError String me) => DatalogProgram -> ECtx -> ExprNode (Maybe Type) -> me Type
exprNodeType' d ctx (EVar p v)            =
    let (lvs, rvs) = ctxMVars d ctx in
    case lookup v $ lvs ++ rvs of
         Just mt -> mtype2me d p ctx mt
         Nothing | ctxInRuleRHSPositivePattern ctx -- handle implicit vardecls in rules
                 -> mtype2me d p ctx $ ctxExpectType d ctx
         _       -> eunknown d p ctx

exprNodeType' d ctx (EApply p f mas)      = do
    let func = getFunc d f
    let t = funcType func
    as <- mapM (mtype2me d p ctx) mas
    -- Infer types of type variables in 'f'.  Use information about types of concrete arguments.
    -- In addition, if expected return type is know from context, use that as well.
    subst <- funcTypeArgSubsts d p func $ as ++ maybeToList (ctxExpectType d ctx)
    -- 'funcTypeArgSubsts' may succeed without inferring all type variables if the return type
    -- uses type variables that do not occur among function arguments (e.g., `map_empty(): Map<'K,'V>`).
    -- We therefore check that type inference has succeeded.
    check d (M.size subst == length (funcTypeVars func)) p
          $ "Could not infer types of the following type variables (see the signature of function " ++ f ++ "): " ++
            (intercalate ", " $ map ("'" ++) $ funcTypeVars func \\ M.keys subst)
    return $ typeSubstTypeArgs subst t

exprNodeType' d ctx (EField p Nothing _)  = eunknown d p ctx
exprNodeType' d _   (EField p (Just e) f) = do
    case typDeref' d e of
         t@TStruct{} ->
             case find ((==f) . name) $ structFields t of
                  Nothing  -> err d p $ "Unknown field \"" ++ f ++ "\" in struct of type " ++ show t
                  Just fld -> do check d (not $ structFieldGuarded t f) p
                                       $ "Access to guarded field \"" ++ f ++ "\""
                                 return $ fieldType fld
         _           -> err d (pos e) $ "Expression is not a struct"


exprNodeType' d ctx (ETupField p Nothing _)  = eunknown d p ctx
exprNodeType' d _   (ETupField p (Just e) i) = do
    case typDeref' d e of
         t@TTuple{} -> do check d (((length $ typeTupArgs t) > i) && (0 <= i)) p
                            $ "Tuple \"" ++ show t ++ "\" does not have " ++ show i ++ " fields"
                          return $ (typeTupArgs t) !! i
         _           -> err d (pos e) $ "Expression is not a tuple"

exprNodeType' _ _   (EBool _ _)           = return tBool

exprNodeType' d ctx (EInt p _)            = do
    case ctxExpectType' d ctx of
         Just t@(TBit _ _)    -> return t
         Just t@(TSigned _ _) -> return t
         Just t@(TInt _)      -> return t
         Just t@(TDouble _)   -> return t
         Just t@(TFloat _)    -> return t
         Nothing              -> return tInt
         _                    -> eunknown d p ctx

exprNodeType' _ _   (EString _ _)         = return tString
exprNodeType' _ _   (EBit _ w _)          = return $ tBit w
exprNodeType' _ _   (ESigned _ w _)       = return $ tSigned w
exprNodeType' _ _   (EFloat _ _)          = return $ tFloat
exprNodeType' _ _   (EDouble _ _)         = return $ tDouble

exprNodeType' d ctx (EStruct p c mas)     = do
    let tdef = consType d c
    as <- mapM (\(f, mt) -> (f,) <$> mtype2me d p ctx mt) mas
    targs <- structTypeArgs d p ctx c as
    return $ tUser (name tdef) targs

exprNodeType' d ctx (ETuple p fs)         = fmap tTuple $ mapM (mtype2me d p ctx) fs
exprNodeType' _ _   (ESlice _ _ h l)      = return $ tBit $ h - l + 1
exprNodeType' d ctx (EMatch p _ cs)       = mtype2me d p ctx $ (fromJust . snd) <$> find (isJust . snd) cs
exprNodeType' d ctx (EVarDecl p _)        = mtype2me d p ctx $ ctxExpectType d ctx
exprNodeType' d ctx (ESeq p _ e2)         = mtype2me d p ctx e2
exprNodeType' d ctx (EITE p _ Nothing e)  = mtype2me d p ctx e
exprNodeType' _ _   (EITE _ _ (Just t) _) = return t
exprNodeType' _ _   (EFor _ _ _ _)        = return $ tTuple []
exprNodeType' _ _   (ESet _ _ _)          = return $ tTuple []
exprNodeType' d ctx (EContinue _)         = return $ maybe (tTuple []) id (ctxExpectType d ctx)
exprNodeType' d ctx (EBreak _)            = return $ maybe (tTuple []) id (ctxExpectType d ctx)
exprNodeType' d ctx (EReturn _ _)         = return $ maybe (tTuple []) id (ctxExpectType d ctx)

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
         Plus   -> return t1
         Minus  -> return t1
         Mod    -> return t1
         Times  -> return t1
         Div    -> return t1
         ShiftR -> return t1
         ShiftL -> return t1
         BAnd   -> return t1
         BOr    -> return t1
         BXor   -> return t1
         Concat | isString d e1
                -> return tString
         Concat -> return $ tBit (typeWidth t1 + typeWidth t2)
    where t1 = typ' d e1
          t2 = typ' d e2

exprNodeType' d ctx (EBinOp p ShiftR e1 _) = mtype2me d p ctx e1
exprNodeType' d ctx (EBinOp p ShiftL e1 _) = mtype2me d p ctx e1
exprNodeType' d ctx (EBinOp p _ _ _)       = eunknown d p ctx
exprNodeType' _ _   (EUnOp _ Not _)        = return tBool
exprNodeType' d _   (EUnOp _ BNeg (Just e))= return $ typ' d e
exprNodeType' d _   (EUnOp _ UMinus (Just e)) = return $ typ' d e
exprNodeType' d ctx (EUnOp p _ _)          = eunknown d p ctx
exprNodeType' d ctx (EPHolder p)           = mtype2me d p ctx $ ctxExpectType d ctx
exprNodeType' d ctx (EBinding p _ e)       = mtype2me d p ctx e
exprNodeType' _ _   (ETyped _ _ t)         = return t
exprNodeType' _ _   (EAs _ _ t)            = return t
exprNodeType' _ _   (ERef _ (Just t))      = return $ tOpaque rEF_TYPE [t]
exprNodeType' d ctx (ERef p _)             = eunknown d p ctx

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

-- | A variant of typ' to be used in contexts where 'Ref<>' should be
-- transparent.
--
-- Expand typedef's down to actual type definition, substituting
-- type arguments along the way. Additionally unwraps Ref types,
-- replacing 'Ref<t>' with 't'.
typDeref' :: (WithType a) => DatalogProgram -> a -> Type
typDeref' d x = _typDeref' d (typ x)

_typDeref' :: DatalogProgram -> Type -> Type
_typDeref' d (TUser _ n as) =
    case tdefType tdef of
         Nothing -> _typDeref' d $ tOpaque n as
         Just t  -> _typDeref' d $ typeSubstTypeArgs (M.fromList $ zip (tdefArgs tdef) as) t
    where tdef = getType d n
_typDeref' d (TOpaque _ tname [t]) | tname == rEF_TYPE = _typDeref' d t
_typDeref' _ t = t

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

isSigned :: (WithType a) => DatalogProgram -> a -> Bool
isSigned d a = case typ' d a of
                 TSigned _ _ -> True
                 _           -> False

isBigInt :: (WithType a) => DatalogProgram -> a -> Bool
isBigInt d a = case typ' d a of
                 TInt _ -> True
                 _      -> False

isInteger :: (WithType a) => DatalogProgram -> a -> Bool
isInteger d a = isBit d a || isSigned d a || isBigInt d a

isDouble :: (WithType a) => DatalogProgram -> a -> Bool
isDouble d a = case typ' d a of
                    TDouble _ -> True
                    _         -> False

isFloat :: (WithType a) => DatalogProgram -> a -> Bool
isFloat d a = case typ' d a of
                   TFloat _ -> True
                   _         -> False

isFP :: (WithType a) => DatalogProgram -> a -> Bool
isFP d a = isDouble d a || isFloat d a

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

isGroup :: (WithType a) => DatalogProgram -> a -> Bool
isGroup d a = case typ' d a of
                   TOpaque _ t _ | t == gROUP_TYPE -> True
                   _                               -> False

isMap :: (WithType a) => DatalogProgram -> a -> Bool
isMap d a = case typ' d a of
                 TOpaque _ t _ | t == mAP_TYPE -> True
                 _                             -> False

isTinySet :: (WithType a) => DatalogProgram -> a -> Bool
isTinySet d a = case typ' d a of
                 TOpaque _ t _ | t == tINYSET_TYPE -> True
                 _                                 -> False

isRef :: (WithType a) => DatalogProgram -> a -> Bool
isRef d a = case typ' d a of
                 TOpaque _ t _ | t == rEF_TYPE -> True
                 _                             -> False

-- | Check if 'a' and 'b' have identical types up to type aliasing;
-- throw exception if they don't.
checkTypesMatch :: (MonadError String me, WithType a, WithType b) => Pos -> DatalogProgram -> a -> b -> me ()
checkTypesMatch p d x y =
    check d (typesMatch d x y) p
          $ "Incompatible types " ++ show (typ x) ++ " and " ++ show (typ y)


-- | True iff 'a' and 'b' have identical types up to type aliasing.
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
         TSigned{}          -> t'
         TInt{}             -> t'
         TString{}          -> t'
         TDouble{}          -> t'
         TFloat{}           -> t'
         TTuple{..}         -> t'{typeTupArgs = map (typeNormalize d) typeTupArgs}
         TUser{..}          -> t'{typeArgs = map (typeNormalize d) typeArgs}
         TOpaque{..}        -> t'{typeArgs = map (typeNormalize d) typeArgs}
         TVar{}             -> t'
         _                  -> error $ "Type.typeNormalize': unexpected type " ++ show t'
    where t' = typ'' d t

-- User-defined types that appear in type expression
typeUserTypes :: Type -> [String]
typeUserTypes = nub . typeUserTypes'

typeUserTypes' :: Type -> [String]
typeUserTypes' TStruct{..} = concatMap (typeUserTypes . typ)
                             $ concatMap consArgs typeCons
typeUserTypes' TTuple{..}  = concatMap (typeUserTypes . typ) typeTupArgs
typeUserTypes' TUser{..}   = typeName : concatMap typeUserTypes typeArgs
typeUserTypes' TOpaque{..} = concatMap typeUserTypes typeArgs
typeUserTypes' _           = []

-- This function is used in validating recursive data types.
-- It computes the set of user-defined types that type 'T' contains as statically
-- allocated members.  The graph induced by this relation cannot be recursive, as
-- 'T' cannot contain an instance of itself, unless it is wrapped in a dynamic
-- allocation (via `Ref`, `Set`, or any other container type).
typeStaticMemberTypes :: Type -> [String]
typeStaticMemberTypes t = nub $ typeStaticMemberTypes' t

typeStaticMemberTypes' :: Type -> [String]
typeStaticMemberTypes' TStruct{..} = concatMap (typeStaticMemberTypes . typ)
                                         $ concatMap consArgs typeCons
typeStaticMemberTypes' TTuple{..}  = concatMap (typeStaticMemberTypes . typ) typeTupArgs
typeStaticMemberTypes' TUser{..}   | elem typeName dYNAMIC_TYPES = []
                                   | otherwise = typeName : concatMap typeStaticMemberTypes typeArgs
typeStaticMemberTypes' TOpaque{..} | elem typeName dYNAMIC_TYPES = []
                                   | otherwise = concatMap typeStaticMemberTypes typeArgs
typeStaticMemberTypes' _           = []


-- | Rudimentary type inference engine. Infers expected type from context.
ctxExpectType :: DatalogProgram -> ECtx -> Maybe Type
ctxExpectType _ CtxTop                               = Nothing
ctxExpectType _ (CtxFunc f)                          = Just $ funcType f
ctxExpectType d (CtxRuleL Rule{..} i)                =
    Just $ relType $ getRelation d (atomRelation $ ruleLHS !! i)
ctxExpectType d (CtxRuleRAtom Rule{..} i)            =
    Just $ relType $ getRelation d (atomRelation $ rhsAtom $ ruleRHS !! i)
ctxExpectType d (CtxIndex idx)                       =
    Just $ relType $ getRelation d (atomRelation $ idxAtom idx)
ctxExpectType _ (CtxRuleRCond Rule{..} i)            =
    case rhsExpr $ ruleRHS !! i of
         E ESet{} -> Just $ tTuple []
         _        -> Just tBool
ctxExpectType _ CtxRuleRFlatMap{}                    = Nothing
ctxExpectType _ CtxRuleRInspect{}                    = Nothing
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
ctxExpectType _ (CtxTupField (ETupField _ _ _) _)    = Nothing
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
ctxExpectType _ (CtxForIter _ _)                     = Nothing
ctxExpectType _ (CtxForBody _ _)                     = Just $ tTuple []
ctxExpectType d (CtxSetL e@(ESet _ _ rhs) ctx)       = exprTypeMaybe d (CtxSetR e ctx) rhs
ctxExpectType d (CtxSetR (ESet _ lhs _) ctx)         = -- avoid infinite recursion by evaluating lhs standalone
                                                       exprTypeMaybe d (CtxSeq1 (ESeq nopos lhs (error "ctxExpectType: should be unreachable")) ctx) lhs
ctxExpectType _ (CtxReturn _ ctx)                    = case ctxInFunc ctx of
                                                            Just f  -> Just $ funcType f
                                                            Nothing -> Nothing
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
         ShiftR -> ctxExpectType d ctx
         ShiftL -> ctxExpectType d ctx
         Mod    -> trhs
         Times  -> trhs
         Div    -> trhs
         BAnd   -> trhs
         BOr    -> trhs
         BXor   -> trhs
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
         ShiftR -> Just $ tBit 32
         ShiftL -> Just $ tBit 32
         Mod    -> tlhs
         Times  -> tlhs
         Div    -> tlhs
         BAnd   -> tlhs
         BOr    -> tlhs
         BXor   -> tlhs
         Concat -> Nothing
    where tlhs = exprTypeMaybe d ctx $ exprLeft e
ctxExpectType _ (CtxUnOp (EUnOp _ Not _) _)          = Just tBool
ctxExpectType d (CtxUnOp (EUnOp _ BNeg _) ctx)       = ctxExpectType d ctx
ctxExpectType d (CtxUnOp (EUnOp _ UMinus _) ctx)     = ctxExpectType d ctx
ctxExpectType d (CtxBinding _ ctx)                   = ctxExpectType d ctx
ctxExpectType _ (CtxTyped (ETyped _ _ t) _)          = Just t
ctxExpectType _ CtxAs{}                              = Nothing
ctxExpectType d (CtxRef ERef{} ctx)                  =
    case ctxExpectType' d ctx of
         Just (TOpaque _ rt [t]) | rt == rEF_TYPE -> Just t
         _ -> Nothing
ctxExpectType _ ctx                                  = error $ "Type.ctxExpectType: invalid context " ++ show ctx

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
typeConsTree :: Type -> ConsTree
typeConsTree t = CT t [EPHolder nopos]

consTreeNodeExpand :: DatalogProgram -> Type -> [CTreeNode]
consTreeNodeExpand d t =
    case typ' d t of
         TStruct _ cs               -> map (\c -> EStruct nopos (name c)
                                                  $ map (\a -> (name a, CT (typ a) [EPHolder nopos]))
                                                  $ consArgs c) cs
         TTuple _ fs                -> [ETuple nopos $ map (\f -> CT (typ f) [EPHolder nopos]) fs]
         TBool{}                    -> [EBool nopos False, EBool nopos True]
         TOpaque _ rt [t'] | rt == rEF_TYPE
                                    -> [ERef nopos $ CT t' [EPHolder nopos]]
         _                          -> [EPHolder nopos]

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
consTreeAbduct _ (CT t cts) (E EPHolder{}) = (CT t [], CT t cts)
consTreeAbduct _ (CT t cts) (E EVarDecl{}) = (CT t [], CT t cts)

-- expand the tree if necessary
consTreeAbduct d (CT t [EPHolder{}]) e =
    consTreeAbduct' d (CT t $ consTreeNodeExpand d t) e

consTreeAbduct d ct e = consTreeAbduct' d ct e


consTreeAbduct' :: DatalogProgram -> ConsTree -> Expr -> (ConsTree, ConsTree)
consTreeAbduct' d ct@(CT t nodes) (E e) =
    case e of
         EBool p b      -> (CT t $ filter (/= EBool p b) nodes, CT t $ filter (== EBool p b) nodes)
         EInt p v       -> (ct, CT t [EInt p v])
         EFloat p v     -> (ct, CT t [EFloat p v])
         EDouble p v    -> (ct, CT t [EDouble p v])
         EString p s    -> (ct, CT t [EString p s])
         EBit p w v     -> (ct, CT t [EBit p w v])
         ESigned p w v  -> (ct, CT t [ESigned p w v])
         EStruct _ _ _ ->
             let (leftover, abducted) = unzip $ map (\nd -> abductStruct d e nd) nodes
             in (CT t $ concat leftover, CT t $ concat abducted)
         ETuple _ es    ->
             let (leftover, abducted) = unzip $ map (\(ETuple _ ts) -> abductTuple d es ts) nodes
             in (CT t $ concat leftover, CT t $ concat abducted)
         ETyped _ x _   -> consTreeAbduct d ct x
         ERef _ x       ->
             let [ERef _ ct'] = nodes in
             let (leftover, abducted) = consTreeAbduct d ct' x in
             (CT t [ERef nopos leftover], CT t [ERef nopos abducted])
         _              -> error $ "Type.consTreeAbduct': invalid pattern " ++ show e


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
abductStruct _ _ nd  = ([nd], [])

abductMany :: DatalogProgram -> [Expr] -> [ConsTree] -> ([[ConsTree]], [[ConsTree]])
abductMany _ []     []       = ([], [[]])
abductMany d (e:es) (ct:cts) =
    let (CT t leftover, CT _ abducted) = consTreeAbduct d ct e
        (leftover', abducted') = abductMany d es cts
        leftover'' = concatMap (\l -> map ((CT t [l]) :) abducted') leftover ++
                     map (\l' -> ct : l') leftover'
        abducted'' = concatMap (\a -> map ((CT t [a]) :) abducted') abducted
    in (leftover'', abducted'')
abductMany _ _ _ = error "NS.abductMany: invalid arguments"

-- | Visitor pattern for types
typeMapM :: (Monad m) => (Type -> m Type) -> Type -> m Type
typeMapM fun t@TBool{}     = fun t
typeMapM fun t@TInt{}      = fun t
typeMapM fun t@TString{}   = fun t
typeMapM fun t@TBit{}      = fun t
typeMapM fun t@TSigned{}   = fun t
typeMapM fun t@TDouble{}   = fun t
typeMapM fun t@TFloat{}    = fun t
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

typeMap :: (Type -> Type) -> Type -> Type
typeMap f t = runIdentity $ typeMapM (return . f) t

-- Returns iterator type when iterating over a collection (element type for
-- sets, vectors, and groups, key-value pair for maps).  The Boolean flag in 
-- the returned tuple indicates whether the collection iterates by reference
-- (True) or by value (False) in Rust.
typeIterType :: DatalogProgram -> Type -> Maybe (Type, Bool)
typeIterType d t =
    case typ' d t of
         TOpaque _ tname [t']  | tname == tINYSET_TYPE -> Just (t', False)            -- Tinysets iterate by value.
         TOpaque _ tname [t']  | elem tname sET_TYPES  -> Just (t', True)             -- Other sets iterate by reference.
         TOpaque _ tname [k,v] | tname == mAP_TYPE     -> Just (tTuple [k,v], False)  -- Maps iterate by value.
         TOpaque _ tname [_,v] | tname == gROUP_TYPE   -> Just (v, False)             -- Groups iterate by value.
         _                                             -> Nothing

typeIsIterable :: (WithType a) =>  DatalogProgram -> a -> Bool
typeIsIterable d x =
    case typ' d x of
         TOpaque _ tname [_]   | elem tname sET_TYPES -> True
         TOpaque _ tname [_,_] | tname == mAP_TYPE    -> True
         TOpaque _ tname _     | tname == gROUP_TYPE  -> True
         _                                            -> False

checkIterable :: (MonadError String me, WithType a) => String -> Pos -> DatalogProgram -> a -> me ()
checkIterable prefix p d x =
    check d (typeIsIterable d x) p $
          prefix ++ " must have one of these types: " ++ intercalate ", " sET_TYPES ++ ", or " ++ mAP_TYPE ++ " but its type is " ++ show (typ x)
