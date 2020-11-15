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

{- |
Module     : Type
Description: Manipulate types in a DDlog program.  The main DDlog type
    inference engine is in TypeInference.hs.  It runs during validation and
    injects sufficient type annotations in the program so that the type of
    any expression can be determined by scanning the expression bottom-up,
    which is what functions in this module ('exprType', 'varType') do.
-}

module Language.DifferentialDatalog.Type(
    WithType(..),
    typeUserTypes,
    typeStaticMemberTypes,
    typeMemberTypes,
    typeIsPolymorphic,
    funcIsPolymorphic,
    exprType,
    exprType',
    exprType'',
    exprNodeType,
    relKeyType,
    typ', typ'',
    typDeref',
    typDeref'',
    isBool, isBit, isSigned, isBigInt, isInteger, isFP, isString, isStruct, isTuple, isGroup, isDouble, isFloat,
    isMap, isTinySet, isSharedRef,
    isOption, isResult,
    checkTypesMatch,
    typesMatch,
    typeNormalize,
    typeSubstTypeArgs,
    ConsTree(..),
    consTreeEmpty,
    typeConsTree,
    consTreeAbduct,
    typeMapM,
    typeMap,
    sET_TYPES,
    tINYSET_TYPE,
    mAP_TYPE,
    gROUP_TYPE,
    ePOCH_TYPE,
    iTERATION_TYPE,
    nESTED_TS_TYPE,
    wEIGHT_TYPE,
    oPTION_TYPE,
    sOME_CONSTRUCTOR,
    nONE_CONSTRUCTOR,
    rESULT_TYPE,
    eRR_CONSTRUCTOR,
    oK_CONSTRUCTOR,
    checkIterable,
    typeIterType,
    varType
) where

import Data.Maybe
import Data.List
import Control.Monad.Except
import Control.Monad.Identity
import qualified Data.Map as M
--import Debug.Trace

import Language.DifferentialDatalog.Attribute
import Language.DifferentialDatalog.Ops
import {-# SOURCE #-} Language.DifferentialDatalog.Expr
import Language.DifferentialDatalog.Syntax
import Language.DifferentialDatalog.NS
import Language.DifferentialDatalog.Pos
import Language.DifferentialDatalog.Name
import Language.DifferentialDatalog.ECtx
import Language.DifferentialDatalog.Error
import Language.DifferentialDatalog.Var
import Language.DifferentialDatalog.Module
import {-# SOURCE #-} Language.DifferentialDatalog.Rule

sET_TYPES :: [String]
sET_TYPES = [mOD_STD ++ "::Set", mOD_STD ++ "::Vec", tINYSET_TYPE]

tINYSET_TYPE :: String
tINYSET_TYPE = "tinyset::Set64"

-- Dynamically allocated types.
dYNAMIC_TYPES :: [String]
dYNAMIC_TYPES = mAP_TYPE : sET_TYPES

gROUP_TYPE :: String
gROUP_TYPE = mOD_STD ++ "::Group"

mAP_TYPE :: String
mAP_TYPE = mOD_STD ++ "::Map"

-- Special types used by Inspect operator.
ePOCH_TYPE :: String
ePOCH_TYPE = mOD_STD ++ "::DDEpoch"

iTERATION_TYPE :: String
iTERATION_TYPE = mOD_STD ++ "::DDIteration"

nESTED_TS_TYPE :: String
nESTED_TS_TYPE = mOD_STD ++ "::DDNestedTS"

wEIGHT_TYPE :: String
wEIGHT_TYPE = mOD_STD ++ "::DDWeight"

oPTION_TYPE :: String
oPTION_TYPE = mOD_STD ++ "::Option"

sOME_CONSTRUCTOR :: String
sOME_CONSTRUCTOR = mOD_STD ++ "::Some"

nONE_CONSTRUCTOR :: String
nONE_CONSTRUCTOR = mOD_STD ++ "::None"

rESULT_TYPE :: String
rESULT_TYPE = mOD_STD ++ "::Result"

eRR_CONSTRUCTOR :: String
eRR_CONSTRUCTOR = mOD_STD ++ "::Err"

oK_CONSTRUCTOR :: String
oK_CONSTRUCTOR = mOD_STD ++ "::Ok"

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
    typ = atypeType . argType
    setType a t = a { argType = (argType a){atypeType = t} }

instance WithType ArgType where
    typ = atypeType
    setType a t = a { atypeType = t }

instance WithType Relation where
    typ = relType
    setType r t = r { relType = t }

-- | True iff t is a polymorphic type, i.e., contains any type variables.
typeIsPolymorphic :: Type -> Bool
typeIsPolymorphic TBool{}       = False
typeIsPolymorphic TInt{}        = False
typeIsPolymorphic TString{}     = False
typeIsPolymorphic TBit{}        = False
typeIsPolymorphic TSigned{}     = False
typeIsPolymorphic TDouble{}     = False
typeIsPolymorphic TFloat{}      = False
typeIsPolymorphic TStruct{..}   = any (any (typeIsPolymorphic . typ) . consArgs) typeCons
typeIsPolymorphic TTuple{..}    = any typeIsPolymorphic typeTupArgs
typeIsPolymorphic TUser{..}     = any typeIsPolymorphic typeArgs
typeIsPolymorphic TVar{}        = True
typeIsPolymorphic TOpaque{..}   = any typeIsPolymorphic typeArgs
typeIsPolymorphic TFunction{..} = any (typeIsPolymorphic . typ) typeFuncArgs || typeIsPolymorphic typeRetType

-- | True iff f is a polymorphic function.
funcIsPolymorphic :: DatalogProgram -> String -> Bool
funcIsPolymorphic d fname = (length fs > 1) ||
                            (typeIsPolymorphic $ funcType f) ||
                            (any typeIsPolymorphic $ map typ $ funcArgs f)
    where
    fs = getFuncs d fname Nothing
    f = head fs

-- | Compute type of an expression.  The expression must be previously
-- validated.
exprType :: DatalogProgram -> ECtx -> Expr -> Type
exprType d ctx e = exprFoldCtx (exprNodeType' d) ctx e

-- | Like 'exprType', but also applies 'typ'' to result.
exprType' :: DatalogProgram -> ECtx -> Expr -> Type
exprType' d ctx e = typ' d $ exprType d ctx e

-- | Like 'exprType', but also applies 'typ''' to result.
exprType'' :: DatalogProgram -> ECtx -> Expr -> Type
exprType'' d ctx e = typ'' d $ exprType d ctx e

-- | Type of relation's primary key, if it has one
relKeyType :: DatalogProgram -> Relation -> Maybe Type
relKeyType d rel =
    fmap (\KeyExpr{..} -> exprType d (CtxKey rel) keyExpr) $ relPrimaryKey rel

-- | Compute expression node type; fail if type is undefined or there
-- is a conflict.
exprNodeType :: DatalogProgram -> ECtx -> ExprNode Type -> Type
exprNodeType d ctx e = ((flip atPos) (pos e)) $ exprNodeType' d ctx e

{-
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
-}

exprNodeType' :: DatalogProgram -> ECtx -> ExprNode Type -> Type
exprNodeType' d ctx (EVar p v)            =
    let vs = ctxAllVars d ctx in
    case find ((==v) . name) vs of
         Just var -> varType d var
         Nothing | ctxInRuleRHSPositivePattern ctx -- handle implicit vardecls in rules
                 -> varType d $ ExprVar ctx $ EVar p v
         _       -> error $ "exprNodeType': unknown variable " ++ v ++ " at " ++ show p

exprNodeType' d _   (EApply _ f _) = typeRetType $ typ' d f

exprNodeType' d ctx (EFunc _ [f]) | -- Type inference engine type-annotates polymorphic functions.
                                    funcIsPolymorphic d f = ctxExpectType ctx
                                  | otherwise            = t
    where [func] = getFuncs d f Nothing
          t = TFunction (pos func) (map argType $ funcArgs func) (funcType func)

exprNodeType' _ _   e@EFunc{} = error $ "exprNodeType' called with unresolved function name: " ++ show e

exprNodeType' d _   (EField _ e f) =
    let t@TStruct{} = typDeref' d e 
        fld = fromJust $ find ((==f) . name) $ structFields t
    in fieldType fld

exprNodeType' d _   (ETupField _ e i) =
    let t@TTuple{} = typDeref' d e
    in (typeTupArgs t) !! i

exprNodeType' _ _   (EBool _ _)           = tBool
exprNodeType' _ _   (EInt _ _)            = tInt
exprNodeType' _ _   (EString _ _)         = tString
exprNodeType' _ _   (EBit _ w _)          = tBit w
exprNodeType' _ _   (ESigned _ w _)       = tSigned w
exprNodeType' _ _   (EFloat _ _)          = tFloat
exprNodeType' _ _   (EDouble _ _)         = tDouble
exprNodeType' _ ctx EStruct{}             = ctxExpectType ctx
exprNodeType' _ _   (ETuple _ fs)         = tTuple fs
exprNodeType' _ _   (ESlice _ _ h l)      = tBit $ h - l + 1
exprNodeType' _ _   (EMatch _ _ cs)       = snd $ head cs
exprNodeType' _ ctx (EVarDecl _ _)        = ctxExpectType ctx
exprNodeType' _ _   (ESeq _ _ e2)         = e2
exprNodeType' _ _   (EITE _ _ t _)        = t
exprNodeType' _ _   (EFor _ _ _ _)        = tTuple []
exprNodeType' _ _   (ESet _ _ _)          = tTuple []
exprNodeType' _ ctx (EContinue _)         = ctxExpectType ctx
exprNodeType' _ ctx (EBreak _)            = ctxExpectType ctx
exprNodeType' _ ctx (EReturn _ _)         = ctxExpectType ctx

exprNodeType' d _   (EBinOp _ op e1 e2) =
    case op of
         Eq     -> tBool
         Neq    -> tBool
         Lt     -> tBool
         Gt     -> tBool
         Lte    -> tBool
         Gte    -> tBool
         And    -> tBool
         Or     -> tBool
         Impl   -> tBool
         Plus   -> t1
         Minus  -> t1
         Mod    -> t1
         Times  -> t1
         Div    -> t1
         ShiftR -> t1
         ShiftL -> t1
         BAnd   -> t1
         BOr    -> t1
         BXor   -> t1
         Concat | isString d e1
                -> tString
         Concat -> tBit (typeWidth t1 + typeWidth t2)
    where t1 = typ' d e1
          t2 = typ' d e2

exprNodeType' _ _   (EUnOp _ Not _)        = tBool
exprNodeType' d _   (EUnOp _ BNeg e)       = typ' d e
exprNodeType' d _   (EUnOp _ UMinus e)     = typ' d e
exprNodeType' _ ctx (EPHolder _)           = ctxExpectType ctx
exprNodeType' _ _   (EBinding _ _ e)       = e
exprNodeType' _ _   (ETyped _ _ t)         = t
exprNodeType' _ _   (EAs _ _ t)            = t
exprNodeType' _ ctx (ERef _ _)             = ctxExpectType ctx
exprNodeType' _ _   (ETry _ _)             = error "exprNodeType: ?-expressions should be eliminated during type inference."
exprNodeType' _ _   (EClosure _ args r _)  = tFunction (map (fromJust . ceargType) args) (fromJust r)
--exprNodeType' d ctx (ERef p _)             = eunknown d p ctx

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

-- | A variant of typ' to be used in contexts where shared references like
-- 'Ref<>' should be transparent.
--
-- Expand typedef's down to actual type definition, substituting
-- type arguments along the way. Additionally unwraps shared ref types,
-- replacing 'Ref<t>' with 't'.
typDeref' :: (WithType a) => DatalogProgram -> a -> Type
typDeref' d x = _typDeref' d (typ x)

_typDeref' :: DatalogProgram -> Type -> Type
_typDeref' d (TUser _ n as) =
    case tdefType tdef of
         Nothing -> _typDeref' d $ tOpaque n as
         Just t  -> _typDeref' d $ typeSubstTypeArgs (M.fromList $ zip (tdefArgs tdef) as) t
    where tdef = getType d n
_typDeref' d rt@(TOpaque _ _ [t]) | isSharedRef d rt = _typDeref' d t
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

-- | A variant of typ'' to be used in contexts where shared references like
-- 'Ref<>' should be transparent.
typDeref'' :: (WithType a) => DatalogProgram -> a -> Type
typDeref'' d x = _typDeref'' d (typ x)

_typDeref'' :: DatalogProgram -> Type -> Type
_typDeref'' d t'@(TUser _ n as) =
    case tdefType tdef of
         (Just (TStruct _ _)) -> t'
         Nothing              -> _typDeref'' d $ tOpaque n as
         Just t               -> _typDeref'' d $ typeSubstTypeArgs (M.fromList $ zip (tdefArgs tdef) as) t
    where tdef = getType d n
_typDeref'' d rt@(TOpaque _ _ [t]) | isSharedRef d rt = _typDeref'' d t
_typDeref'' _ t = t

typeSubstTypeArgs :: M.Map String Type -> Type -> Type
typeSubstTypeArgs subst (TUser _ n as)      = tUser n (map (typeSubstTypeArgs subst) as)
typeSubstTypeArgs subst (TOpaque _ n as)    = tOpaque n (map (typeSubstTypeArgs subst) as)
typeSubstTypeArgs subst (TStruct _ cs)      = tStruct $ map (consSubstTypeArgs subst)  cs
typeSubstTypeArgs subst (TTuple _ ts)       = tTuple $ map (typeSubstTypeArgs subst)  ts
typeSubstTypeArgs subst (TVar _ tv)         = subst M.! tv
typeSubstTypeArgs subst (TFunction _ as r)  = tFunction (map (\a -> a{atypeType = typeSubstTypeArgs subst $ typ a}) as)
                                                        (typeSubstTypeArgs subst r)
typeSubstTypeArgs _     t                   = t

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

isSharedRef :: (WithType a) => DatalogProgram -> a -> Bool
isSharedRef d a =
    case typ' d a of
         TOpaque _ t _ -> tdefGetSharedRefAttr d $ getType d t
         _             -> False

isOption :: (WithType a) => DatalogProgram -> a -> Bool
isOption d a = case typ'' d a of
                    TUser _ t _ | t == oPTION_TYPE -> True
                    _                              -> False

isResult :: (WithType a) => DatalogProgram -> a -> Bool
isResult d a = case typ'' d a of
                    TUser _ t _ | t == rESULT_TYPE -> True
                    _                              -> False

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
         TFunction{..}      -> t'{ typeFuncArgs = map (\a -> a{atypeType = typeNormalize d $ typ a}) typeFuncArgs
                                 , typeRetType = typeNormalize d typeRetType}
         _                  -> error $ "Type.typeNormalize': unexpected type " ++ show t'
    where t' = typ'' d t

-- User-defined types that appear in type expression
typeUserTypes :: Type -> [String]
typeUserTypes = nub . typeUserTypes'

typeUserTypes' :: Type -> [String]
typeUserTypes' TStruct{..}      = concatMap (typeUserTypes . typ)
                                  $ concatMap consArgs typeCons
typeUserTypes' TTuple{..}       = concatMap (typeUserTypes . typ) typeTupArgs
typeUserTypes' TUser{..}        = typeName : concatMap typeUserTypes typeArgs
typeUserTypes' TOpaque{..}      = concatMap typeUserTypes typeArgs
typeUserTypes' TFunction{..}    = concatMap (typeUserTypes . typ) typeFuncArgs ++ typeUserTypes typeRetType
typeUserTypes' _                = []

-- This function is used in validating recursive data types.
-- It computes the set of user-defined types that type 'T' contains as statically
-- allocated members.  The graph induced by this relation cannot be recursive, as
-- 'T' cannot contain an instance of itself, unless it is wrapped in a dynamic
-- allocation (via `Ref`, `Set`, or any other container type).
typeStaticMemberTypes :: DatalogProgram -> Type -> [String]
typeStaticMemberTypes d t = nub $ typeStaticMemberTypes' d t

typeStaticMemberTypes' :: DatalogProgram -> Type -> [String]
typeStaticMemberTypes' d TStruct{..}   = concatMap (typeStaticMemberTypes d . typ)
                                                   $ concatMap consArgs typeCons
typeStaticMemberTypes' d TTuple{..}    = concatMap (typeStaticMemberTypes d . typ) typeTupArgs
typeStaticMemberTypes' d t@TUser{..}   | elem typeName dYNAMIC_TYPES || isSharedRef d t = []
                                       | otherwise = typeName : concatMap (typeStaticMemberTypes d) typeArgs
typeStaticMemberTypes' d t@TOpaque{..} | elem typeName dYNAMIC_TYPES || isSharedRef d t = []
                                       | otherwise = concatMap (typeStaticMemberTypes d) typeArgs
typeStaticMemberTypes' _ _             = []

-- This function is used in validating recursive data types.
-- It computes the set of user-defined types that type 'T' contains as statically
-- or dynamically allocated members.
typeMemberTypes :: DatalogProgram -> Type -> [String]
typeMemberTypes d t = nub $ typeMemberTypes' d t

typeMemberTypes' :: DatalogProgram -> Type -> [String]
typeMemberTypes' d TStruct{..} = concatMap (typeMemberTypes d . typ) $ concatMap consArgs typeCons
typeMemberTypes' d TTuple{..}  = concatMap (typeMemberTypes d . typ) typeTupArgs
typeMemberTypes' d TUser{..}   = typeName : concatMap (typeMemberTypes d) typeArgs
typeMemberTypes' d TOpaque{..} = concatMap (typeMemberTypes d) typeArgs
typeMemberTypes' _ _           = []

-- Expressions whose type cannot be inferred in a bottom up manner must have
-- explicit type annotations.
ctxExpectType :: ECtx -> Type
ctxExpectType (CtxTyped (ETyped _ _ t) _) = t
ctxExpectType ctx                         = error $ "ctxExpectType: Unknown type in context:\n" ++ show ctx

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
         rt@(TOpaque _ _ [t']) | isSharedRef d rt
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
typeMapM fun t@TFunction{..} = do
    args <- mapM (\a -> setType a <$> typeMapM fun (atypeType a)) typeFuncArgs
    ret <- typeMapM fun typeRetType
    return t{typeFuncArgs = args, typeRetType = ret}

typeMap :: (Type -> Type) -> Type -> Type
typeMap f t = runIdentity $ typeMapM (return . f) t

-- Returns iterator type when iterating over a collection (element type for
-- sets, vectors, and groups, key-value pair for maps).  The Boolean flag in 
-- the returned tuple indicates whether the collection iterates by reference
-- (True) or by value (False) in Rust.
typeIterType :: DatalogProgram -> Type -> (Type, Bool)
typeIterType d t =
    case typ' d t of
         TOpaque _ tname [t']  | tname == tINYSET_TYPE -> (t', False)            -- Tinysets iterate by value.
         TOpaque _ tname [t']  | elem tname sET_TYPES  -> (t', True)             -- Other sets iterate by reference.
         TOpaque _ tname [k,v] | tname == mAP_TYPE     -> (tTuple [k,v], False)  -- Maps iterate by value.
         TOpaque _ tname [_,v] | tname == gROUP_TYPE   -> (v, False)             -- Groups iterate by value.
         _                                             -> error $ "typeIterType " ++ show t

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

varType :: DatalogProgram -> Var -> Type
varType _ (ExprVar ctx EVarDecl{})         = ctxExpectType ctx
varType _ (ExprVar ctx EVar{})             = ctxExpectType ctx
varType _ v@ExprVar{}                      = error $ "varType " ++ show v
varType d (ForVar ctx e@EFor{..})          = fst $ typeIterType d $ exprType d (CtxForIter e ctx) exprIter
varType _ v@ForVar{}                       = error $ "varType " ++ show v
varType d (BindingVar ctx e@EBinding{..})  = exprType d (CtxBinding e ctx) exprPattern
varType _ v@BindingVar{}                   = error $ "varType " ++ show v
varType _ ArgVar{..}                       = typ $ fromJust $ find ((== varName) . name) $ funcArgs varFunc
varType d ClosureArgVar{..}                = let TFunction{..} = exprType' d varCtx $ E varExpr in
                                             typ $ typeFuncArgs !! varArgIdx
varType _ KeyVar{..}                       = typ varRel
varType _ IdxVar{..}                       = typ $ fromJust $ find ((== varName) . name) $ idxVars varIndex
varType d (FlatMapVar rl i)                = case typ' d $ exprType d (CtxRuleRFlatMap rl i) (rhsMapExpr $ ruleRHS rl !! i) of
                                                  TOpaque _ _ [t']     -> t'
                                                  TOpaque _ tname [kt,vt] | tname == mAP_TYPE
                                                                       -> tTuple [kt,vt]
                                                  _                    -> error $ "varType FlatMapVar " ++ show rl ++ " " ++ show i 
varType d (GroupVar rl i)                  = let ktype = ruleGroupByKeyType d rl i
                                                 vtype = ruleGroupByValType d rl i
                                             in tOpaque gROUP_TYPE [ktype, vtype]
varType _ WeightVar                        = tUser wEIGHT_TYPE []
varType d (TSVar rl)                       = if ruleIsRecursive d rl
                                             then tUser nESTED_TS_TYPE []
                                             else tUser ePOCH_TYPE []
