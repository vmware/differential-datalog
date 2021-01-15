{-
Copyright (c) 2020 VMware, Inc.
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

{-# LANGUAGE FlexibleContexts, RecordWildCards, TupleSections, ImplicitParams, RankNTypes #-}

{-|
Module: TypeInference
Description: Type inference engine: generates type constraints and solves them using
  the algorithm in Unification.hs.
-}

module Language.DifferentialDatalog.TypeInference(
    inferTypes,
    inferTypeArgs,
    unifyTypes,
    typeToTExpr
)
where

import qualified Data.Map as M
--import Data.List.Utils
import Control.Monad.Except
import Control.Monad.State.Lazy
import Data.Either
import Data.List
import Data.Maybe
import GHC.Float
--import Debug.Trace

import Language.DifferentialDatalog.Attribute
import Language.DifferentialDatalog.ECtx
import Language.DifferentialDatalog.Error
import Language.DifferentialDatalog.Expr
import Language.DifferentialDatalog.Module
import Language.DifferentialDatalog.Name
import Language.DifferentialDatalog.NS
import Language.DifferentialDatalog.Ops
import Language.DifferentialDatalog.Pos
import {-# SOURCE #-} Language.DifferentialDatalog.Rule
import Language.DifferentialDatalog.Syntax
import Language.DifferentialDatalog.Type
import Language.DifferentialDatalog.Util
import Language.DifferentialDatalog.Unification
import Language.DifferentialDatalog.Var


-- Constraint generator state.
data GeneratorState = GeneratorState {
    -- Constraints generated so far.
    genConstraints :: [Constraint],
    -- TypeVar-to-id map.  We assign unique ids to type variables to speed up
    -- comparisons.
    genVars        :: M.Map (Maybe ELocator, Maybe VLocator, Maybe String) Int,
    -- The number of type variables in 'genVars'. Used to assign ids to new variables.
    genNumVars     :: Int,
    -- Unique ids of integer variables.
    genIVars       :: M.Map (ELocator, Int) Int,
    -- The number of variables in 'genIVars'. Used to assign ids to new variables.
    genNumIVars     :: Int
}

emptyGenerator :: GeneratorState
emptyGenerator = GeneratorState {
    genConstraints = [],
    genVars        = M.empty,
    genNumVars     = 0,
    genIVars       = M.empty,
    genNumIVars    = 0
}

type GeneratorMonad t = State GeneratorState t

addConstraint :: Constraint -> GeneratorMonad ()
addConstraint c = modify $ \gen@GeneratorState{..} -> gen{genConstraints = c : genConstraints}

addConstraints :: [Constraint] -> GeneratorMonad ()
addConstraints cs = modify $ \gen@GeneratorState{..} -> gen{genConstraints = genConstraints ++ cs}

tvarTypeOfVar :: Var -> GeneratorMonad TypeVar
tvarTypeOfVar v = do
    let locator = varLocator v
    gen@GeneratorState{..} <- get
    case M.lookup (Nothing, Just locator, Nothing) genVars of
         Nothing -> do let tv = TVarTypeOfVar genNumVars v
                       put $ gen {genVars = M.insert (Nothing, Just locator, Nothing) genNumVars genVars, genNumVars = genNumVars + 1}
                       return tv
         Just i  -> return $ TVarTypeOfVar i v

tvarTypeOfExpr :: DDExpr -> GeneratorMonad TypeVar
tvarTypeOfExpr de = do
    let locator = ctxToELocator (ddexprCtx de)
    gen@GeneratorState{..} <- get
    case M.lookup (Just locator, Nothing, Nothing) genVars of
         Nothing -> do let tv = TVarTypeOfExpr genNumVars de
                       put $ gen {genVars = M.insert (Just locator, Nothing, Nothing) genNumVars genVars, genNumVars = genNumVars + 1}
                       return tv
         Just i  -> return $ TVarTypeOfExpr i de

tvarAux :: DDExpr -> String -> GeneratorMonad TypeVar
tvarAux de n = do
    let locator = ctxToELocator (ddexprCtx de)
    gen@GeneratorState{..} <- get
    case M.lookup (Just locator, Nothing, Just n) genVars of
         Nothing -> do let tv = TVarAux genNumVars de n
                       put $ gen {genVars = M.insert (Just locator, Nothing, Just n) genNumVars genVars, genNumVars = genNumVars + 1}
                       return tv
         Just i -> return $ TVarAux i de n

ivarWidthOfExpr :: DDExpr -> GeneratorMonad IntVar
ivarWidthOfExpr de = do
    let locator = ctxToELocator (ddexprCtx de)
    gen@GeneratorState{..} <- get
    case M.lookup (locator, 0) genIVars of
         Nothing -> do let iv = WidthOfExpr genNumIVars de
                       put $ gen {genIVars = M.insert (locator, 0) genNumIVars genIVars, genNumIVars = genNumIVars + 1}
                       return iv
         Just i  -> return $ WidthOfExpr i de

ivarMutabilityOfArg :: DDExpr -> Int -> GeneratorMonad IntVar
ivarMutabilityOfArg de arg = do
    let locator = ctxToELocator (ddexprCtx de)
    gen@GeneratorState{..} <- get
    case M.lookup (locator, arg) genIVars of
         Nothing -> do let iv = MutabilityOfArg genNumIVars de arg
                       put $ gen {genIVars = M.insert (locator, arg) genNumIVars genIVars, genNumIVars = genNumIVars + 1}
                       return iv
         Just i  -> return $ MutabilityOfArg i de arg

teTypeOfExpr :: DDExpr -> GeneratorMonad TExpr
teTypeOfExpr de = TETVar <$> tvarTypeOfExpr de

teTypeOfVar :: Var -> GeneratorMonad TExpr
teTypeOfVar v = TETVar <$> tvarTypeOfVar v

teTVarAux :: DDExpr -> String -> GeneratorMonad TExpr
teTVarAux de v = TETVar <$> tvarAux de v

-- The actual type of 'tv' is 'te'.
(===) :: TypeVar -> TExpr -> Predicate
(===) tv te =
    -- Assuming 't1' is a type expression of the for <type of XXX>, the
    -- meaning of the predicate is 'XXX' must have type 't2'.  We record 't1'
    -- as the explanation of the predicate.  As 'predLHS' and 'predRHS' get
    -- transformed, the explanation will keep track of the program object that
    -- the predicate constrains.
    PEq {
        predLHS = TETVar tv,
        predRHS = te,
        predExplanation = ExplanationTVar tv Actual
    }

(====) :: TypeVar -> TExpr -> Constraint
(====) tv te = CPredicate $ tv === te

(<===) :: GeneratorMonad TypeVar -> TExpr -> GeneratorMonad Predicate
(<===) tv te = do
    tv' <- tv
    return $ tv' === te

(<====) :: GeneratorMonad TypeVar -> TExpr -> GeneratorMonad Constraint
(<====) tv te = CPredicate <$> tv <=== te

(<===>) :: GeneratorMonad TypeVar -> GeneratorMonad TExpr -> GeneratorMonad Predicate
(<===>) tv te = do
    tv' <- tv
    te' <- te
    return $ tv' === te'

(<====>) :: GeneratorMonad TypeVar -> GeneratorMonad TExpr -> GeneratorMonad Constraint
(<====>) tv te = CPredicate <$> tv <===> te

-- The expected type of 'tv' is 'te'.
(~~~) :: TypeVar -> TExpr -> Predicate
(~~~) tv te =
    PEq {
        predLHS = TETVar tv,
        predRHS = te,
        predExplanation = ExplanationTVar tv Expected
    }

(~~~~) :: TypeVar -> TExpr -> Constraint
(~~~~) tv te = CPredicate $ tv ~~~ te

(<~~~) :: GeneratorMonad TypeVar -> TExpr -> GeneratorMonad Predicate
(<~~~) tv te = do
    tv' <- tv
    return $ tv' ~~~ te

(<~~~~) :: GeneratorMonad TypeVar -> TExpr -> GeneratorMonad Constraint
(<~~~~) tv te = CPredicate <$> tv <~~~ te

(<~~~>) :: GeneratorMonad TypeVar -> GeneratorMonad TExpr -> GeneratorMonad Predicate
(<~~~>) tv te = do
    tv' <- tv
    te' <- te
    return $ tv' ~~~ te'

(<~~~~>) :: GeneratorMonad TypeVar -> GeneratorMonad TExpr -> GeneratorMonad Constraint
(<~~~~>) tv te = CPredicate <$> tv <~~~> te


teTuple :: [TExpr] -> TExpr
teTuple [t] = t
teTuple ts  = TETuple ts

-- Convert type to a type expression, replacing type arguments with fresh
-- variables.  The 'de' argument is used to generate unique type variable
-- identifiers.  For example, when doing type inference for a function call
-- expression, we introduce fresh type variables for type arguments of the
-- function.
typeToTExpr' :: (?d::DatalogProgram) => DDExpr -> Type -> GeneratorMonad TExpr
typeToTExpr' _  TBool{}         = return TEBool
typeToTExpr' _  TInt{}          = return TEBigInt
typeToTExpr' _  TString{}       = return TEString
typeToTExpr' _  TBit{..}        = return $ TEBit $ IConst typeWidth
typeToTExpr' _  TSigned{..}     = return $ TESigned $ IConst typeWidth
typeToTExpr' _  TFloat{}        = return TEFloat
typeToTExpr' _  TDouble{}       = return TEDouble
typeToTExpr' de TTuple{..}      = teTuple <$> mapM (typeToTExpr' de . typ) typeTupArgs
typeToTExpr' de TUser{..}       = TEUser typeName <$> mapM (typeToTExpr' de) typeArgs
typeToTExpr' de TVar{..}        = teTVarAux de tvarName
typeToTExpr' de TOpaque{..}     = TEExtern typeName <$> mapM (typeToTExpr' de) typeArgs
typeToTExpr' _  t@TStruct{}     = error $ "typeToTExpr': unexpected '" ++ show t ++ "'"
typeToTExpr' de TFunction{..}   = TEFunc <$> mapM (\arg -> (if atypeMut arg then IConst 1 else IConst 0,) <$> typeToTExpr' de (typ arg)) typeFuncArgs
                                         <*> typeToTExpr' de typeRetType

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
inferTypeArgs :: (MonadError String me) => DatalogProgram -> Pos -> String -> [(Type, Type)] -> me (M.Map String Type)
inferTypeArgs d p ctx ts = do
    let ?d = d
    let constraints = -- Manufacture a bogus expression for typeToTExpr'.
                     evalState (mapM (\(t1,t2) -> do te1 <- typeToTExpr' (DDExpr CtxTop eTrue) t1
                                                     let te2 = typeToTExpr t2
                                                     return $ CPredicate $ PEq te1 te2 $ ExplanationString p ctx)
                                     ts)
                               emptyGenerator
    typing <- solve d constraints True
    return $ M.fromList $ map (\(TVarAux{..}, t) -> (tvarName, t)) $ M.toList typing

-- Check if two types are compatible (i.e., both can be concretized to the same type).
unifyTypes :: DatalogProgram -> Type -> Type -> Bool
unifyTypes d t1 t2 =
    let ?d = d in
    let constraints = -- Manufacture bogus expressions for typeToTExpr'.
                     evalState (do te1 <- typeToTExpr' (DDExpr CtxTop eTrue) t1
                                   te2 <- typeToTExpr' (DDExpr CtxTop eFalse) t2
                                   return [CPredicate $ PEq te1 te2 $ ExplanationString nopos ""])
                               emptyGenerator
    in isRight $ solve d constraints False

-- Check if to type expressions are compatible (i.e., both can be concretized to the same type).
unifyTExprs :: (?d::DatalogProgram) => TExpr -> TExpr -> Bool
unifyTExprs te1 te2 =
    isRight $ solve ?d [CPredicate $ PEq te1 te2 $ ExplanationString nopos ""] False

-- Type expression is a struct of the specified user-defined type: 'is_MyStruct |e|'.
deIsStruct :: (?d::DatalogProgram) => DDExpr -> String -> GeneratorMonad Constraint
deIsStruct de n = CPredicate <$> deIsStruct_ de n

deIsStruct_ :: (?d::DatalogProgram) => DDExpr -> String -> GeneratorMonad Predicate
deIsStruct_ de n =
    tvarTypeOfExpr de <===> (TEUser n <$> (mapM (teTVarAux de . name) tdefArgs))
    where
    TypeDef{..} = getType ?d n

deIsBit :: DDExpr -> GeneratorMonad Constraint
deIsBit de = CPredicate <$> deIsBit_ de

deIsBit_ :: DDExpr -> GeneratorMonad Predicate
deIsBit_ de = tvarTypeOfExpr de <~~~> ((TEBit . IVar) <$> ivarWidthOfExpr de)

deIsFP :: (?d::DatalogProgram) => DDExpr -> GeneratorMonad Constraint
deIsFP de = do
    isdouble <- tvarTypeOfExpr de <==== TEDouble
    ce <- teTypeOfExpr de
    let expand TETVar{} = return Nothing
        expand TEFloat = return $ Just []
        expand TEDouble = return $ Just []
        expand te = err ?d (pos de)
                    $ "floating point expression '" ++ show de ++ "' is used in a context where type '" ++ show te ++ "' is expected"
    return $ CLazy ce expand (Just [isdouble]) de
           $ "expression '" ++ show de ++ "' must be of a floating point type ('float' or 'double')"

-- 'is_bits t = is_Bit t || is_Signed t'.
deIsBits :: (?d::DatalogProgram) => DDExpr -> GeneratorMonad Constraint
deIsBits de = do
    ce <- teTypeOfExpr de
    let expand TETVar{}   = return Nothing
        expand TEBit{}    = return $ Just []
        expand TESigned{} = return $ Just []
        expand te = err ?d (pos de)
                    $ "expression '" ++ show de ++ "' must be of a fixed-width integer type ('bit<>' or 'signed<>'), but its type is " ++ show te
    return $ CLazy ce expand Nothing de
           $ "expression '" ++ show de ++ "' must be of a fixed-width integer type ('bit<>' or 'signed<>')"

-- 'is_int t = is_bits t || is_BigInt t'.
deIsInt :: (?d::DatalogProgram) => DDExpr -> GeneratorMonad Constraint
deIsInt de = do
    let expand TETVar{}   = return Nothing
        expand TEBit{}    = return $ Just []
        expand TESigned{} = return $ Just []
        expand TEBigInt{} = return $ Just []
        expand te = err ?d (pos de)
                    $ "expression '" ++ show de ++ "' must be of an integer type ('bit<>', 'signed<>', or 'bigint'), but its type is " ++ show te
    ce <- teTypeOfExpr de
    return $ CLazy ce expand Nothing de
           $ "expression '" ++ show de ++ "' must be of an integer type ('bit<>', 'signed<>', or 'bigint')"

-- 'is_num t = is_int t || is_fp t'.
deIsNum :: (?d::DatalogProgram) => DDExpr -> Maybe [Constraint] -> (forall me . (MonadError String me) => TExpr -> me (Maybe [Constraint])) -> GeneratorMonad Constraint
deIsNum de def ferr = do
    let expand TETVar{}   = return Nothing
        expand TEBit{}    = return $ Just []
        expand TESigned{} = return $ Just []
        expand TEBigInt{} = return $ Just []
        expand TEFloat    = return $ Just []
        expand TEDouble   = return $ Just []
        expand te = ferr te
    ce <- teTypeOfExpr de
    return $ CLazy ce expand def de
           $ "expression '" ++ show de ++ "' must be of a numeric type"

-- Type expression is a shared reference type with specified inner type.
deIsSharedRef :: (?d::DatalogProgram) => DDExpr -> TypeVar -> GeneratorMonad Constraint
deIsSharedRef de tv = do
    ce <- teTypeOfExpr de
    let expand TETVar{} = return Nothing
        expand (TEExtern n [t']) | elem n sref_types = return $ Just [tv ~~~~ t']
        expand te = err ?d (pos de)
                        $ "expression '" ++ show de ++ "' must be of a shared reference type, e.g., 'Intern<>' or 'Ref<>', but its type is " ++ show te
    return $ CLazy ce expand Nothing de
           $ "expression '" ++ show de ++ "' must be a shared reference"
    where
    -- All shared reference types.
    sref_types = map name
                 $ filter (tdefGetSharedRefAttr ?d)
                 $ M.elems $ progTypedefs ?d

-- Expression is a collection with specified element type (when iterating using
-- for-loop).
deIsIterable :: (?d::DatalogProgram) => DDExpr -> TypeVar -> GeneratorMonad Constraint
deIsIterable de tv = do
    let expand TETVar{} = return Nothing
        expand (TEExtern n [t'])    | elem n sET_TYPES = return $ Just [tv ==== t']
        expand (TEExtern n [_, t']) | n == gROUP_TYPE  = return $ Just [tv ==== t']
        expand (TEExtern n [k,v])   | n == mAP_TYPE    = return $ Just [tv ==== teTuple [k,v]]
        expand te = err ?d (pos de)
                    $ "expression '" ++ show de ++ "' must be of an iterable type, e.g., 'Set<>', 'Map<>', 'Vec<>', or 'Group<>', but its type is " ++ show te
    ce <- teTypeOfExpr de
    return $ CLazy ce expand Nothing de
           $ "expression '" ++ show de ++ "' must be of an iterable type, e.g., 'Set<>', 'Map<>', 'Vec<>', or 'Group<>'"

-- Main type inference function.  Takes one or more expressions and tries to
-- infer types for all variables and subexpressions.
--
-- The input argument to this function is one of:
-- * The body of a function.
-- * Index expression.
-- * Primary key expression.
-- * All expressions in a rule, including RHS and LHS literals.
--
-- Returns transformed input expressions with
-- * type annotations attached to some of the subexpressions
-- * integer and floating point literals converted to their inferred types
-- * string conversions injected.
--
-- The transformed expression contains sufficient type information embedded in
-- it to allow the type of each subexpression to be determined by scanning the
-- expression bottom-up without running this type inference engine again.
--
-- This invariant must be maintained by all subsequent program transformations,
-- e.g., if a new type declaration is introduced, it must be annotated with
-- variable type.

--short :: String -> String
--short = (\x -> if length x < 100 then x else take (100 - 3) x ++ "...") . replace "\n" " "

inferTypes :: (MonadError String me) => DatalogProgram -> [(ECtx, Expr)] -> me [Expr]
inferTypes d es = do
    let ?d = d
    let es' = map (\(ctx, e) -> DDExpr ctx e) es
    let GeneratorState{..} = execState (do mapM_ contextConstraints es'
                                           mapM_ exprConstraints es')
                                       emptyGenerator
    typing <- --trace ("inferTypes " ++ show es ++ "\nConstraints:\n" ++ constraintsShow genConstraints) $
              solve d genConstraints True
    -- Extract ECtx -> Type mapping from 'typing'.
    let ctxtypes = foldl' (\m (tv, t) ->
                            case tv of
                                 TVarTypeOfExpr _ (DDExpr ctx _) -> M.insert (ctxToELocator ctx) t m
                                 _                               -> m) M.empty
                          $ M.toList typing
    let add_types :: (MonadError String me) => ECtx -> ExprNode (Expr, Type) -> me (Expr, Type)
        add_types ctx e =
            -- String conversion.
            case ctx of
                CtxBinOpR _ ctx' | ctxtypes M.! (ctxToELocator ctx') == tString && t /= tString
                                 -> do e'' <- exprInjectStringConversion d (enode e') t
                                       return (e'', t)
                _ -> return (e', t)
            where
            t = case M.lookup (ctxToELocator ctx) ctxtypes of
                     Just t' -> t'
                     Nothing -> case e of
                                     -- Relations that interrupt control flow sometimes have
                                     -- type unrestricted by context, e.g., in '{return; ()}',
                                     -- the type of `return` can be anything.
                                     -- We default them to empty tuples.
                                     EContinue{} -> tTuple []
                                     EBreak{}    -> tTuple []
                                     EReturn{}   -> tTuple []
                                     _           -> error $ "inferTypes: context has not been assigned a type:\n" ++ show ctx
            expr = exprMap fst e
            annotated = if ctxIsTyped ctx
                           then E expr
                           else E $ ETyped (pos e) (E expr) t
            e' = case e of
                 -- Convert integer literals to bit vectors if necessary.
                 EInt{..}     -> case t of
                                      TBit{..}    -> E $ EBit    (pos e) typeWidth exprIVal
                                      TSigned{..} -> E $ ESigned (pos e) typeWidth exprIVal
                                      TFloat{}    -> E $ EFloat  (pos e) $ fromInteger exprIVal
                                      TDouble{}   -> E $ EDouble (pos e) $ fromInteger exprIVal
                                      TInt{}      -> E expr
                                      _           -> error $ "inferTypes: unexpected integer type '" ++ show t ++ "'"
                 -- Convert doubles to floats if necessary.
                 EDouble{..} -> case t of
                                     TFloat{}    -> E $ EFloat  (pos e) $ double2Float exprDVal
                                     TDouble{}   -> E expr
                                     _           -> error $ "inferTypes: unexpected floating point type '" ++ show t ++ "'"
                 -- Annotate all expressions whose type cannot be derived in a bottom-up manner:
                 -- var decls, placeholders, polymorphic functions, structs, etc.
                 -- This should be enough to determine the type of any
                 -- expression without type inference, by scanning it bottom up.
                 EVarDecl{}  -> annotated
                 EVar{} | ctxInRuleRHSPattern ctx
                             -> annotated
                 EPHolder{}  -> annotated
                 EFunc{..}  -> do
                    let t' = typ' ?d t
                    let arg_types = map typ $ typeFuncArgs t'
                    let fs = mapMaybe (\fname -> lookupFunc ?d fname arg_types (typeRetType t')) exprFuncName
                    let (f, _) = case fs of
                                      [f'] -> f'
                                      _ -> error $ "TypeInference.add_types: e=" ++ show expr ++ "\nfs = " ++ show (map (name . fst) fs)
                    if funcIsPolymorphic ?d $ name f
                    then if ctxIsTyped ctx
                         then E $ expr{exprFuncName = [name f]}
                         else E $ ETyped (pos e) (E $ expr{exprFuncName = [name f]}) t
                    else E $ expr{exprFuncName = [name f]}
                 -- Make sure that closures are annotated with argument and
                 -- return types.
                 EClosure{..} ->
                    let TFunction{..} = typ' ?d t in
                    E $ EClosure (pos e)
                                 (map (\(carg, arg) -> carg{ceargType = Just arg}) $ zip exprClosureArgs typeFuncArgs)
                                 (Just typeRetType)
                                 (fst exprExpr)
                 EStruct{}    -> annotated
                 EContinue{}  -> annotated
                 EBreak{}     -> annotated
                 EReturn{}    -> annotated
                 ERef{}       -> annotated
                 ETry{..} | isOption ?d inner_type && isOption ?d ret_type
                             -> E $ EMatch (pos e) inner_expr
                                    [(eStruct nONE_CONSTRUCTOR [] inner_type, eReturn (eStruct nONE_CONSTRUCTOR [] ret_type) t),
                                     (eStruct sOME_CONSTRUCTOR [(makeIdentifierWithPos "x", eVarDecl "__x" t)] inner_type, eVar "__x")]
                          | isResult ?d inner_type && isOption ?d ret_type
                             -> E $ EMatch (pos e) inner_expr
                                    [(eStruct eRR_CONSTRUCTOR [(makeIdentifierWithPos "err", eTyped ePHolder inner_etype)] inner_type, eReturn (eStruct nONE_CONSTRUCTOR [] ret_type) t),
                                     (eStruct oK_CONSTRUCTOR [(makeIdentifierWithPos "res", eVarDecl "__x" t)] inner_type, eVar "__x")]
                          | isResult ?d inner_type && isResult ?d ret_type
                             -> E $ EMatch (pos e) inner_expr
                                    [(eStruct eRR_CONSTRUCTOR [(makeIdentifierWithPos "err", eVarDecl "__e" etype)] inner_type, eReturn (eStruct eRR_CONSTRUCTOR [(makeIdentifierWithPos "err", eVar "__e")] ret_type) t),
                                     (eStruct oK_CONSTRUCTOR [(makeIdentifierWithPos "res", eVarDecl "__x" t)] inner_type, eVar "__x")]
                          | otherwise -> error $ "TypeInference.add_types: e=" ++ show expr ++ " type=" ++ show inner_type ++ " function or closure: " ++ show ctx'
                    where
                    ctx' = fromMaybe (error $ "inferTypes '" ++ show expr ++ "': '?' not inside function or closure")
                                     $ ctxInFuncOrClosure ctx
                    ret_type = case ctx' of
                                    CtxFunc{..} -> funcType ctxFunc
                                    ctx''@CtxClosure{} ->
                                        case M.lookup (ctxToELocator ctx'') ctxtypes of
                                             Just t' -> t'
                                             _ -> error $ "inferTypes '" ++ show expr ++ "': unknown closure type"
                                    _ -> error $ "inferTypes '" ++ show expr ++ "': ctx' = " ++ show ctx'
                    TUser _ _ [_, etype] = typ'' ?d ret_type
                    (inner_expr, inner_type) = exprExpr
                    TUser _ _ [_, inner_etype] = typ'' ?d inner_type
                 _           -> E expr
    --trace ("\nctxtypes:\n" ++ (intercalate "\n" $ map (\(ctx, t) -> show ctx ++ ": " ++ show t) $ M.toList ctxtypes)) $
    mapM (\(DDExpr ctx e) -> fst <$> exprFoldCtxM add_types ctx e) es'
    {-trace ("inferTypes " ++ (intercalate "\n\n" $ map show es) ++ "\nconstraints:\n" ++ (constraintsShow genConstraints))$-}

{- Context-specific constraints. -}

-- When evaluating the body of a function:
-- 'function f(arg1: t1, ..., argn: tn): t0 { e }'
-- the following type constraints are added:
-- '|e| = t0, |argi|=ti'.
contextConstraints :: (?d::DatalogProgram) => DDExpr -> GeneratorMonad ()
contextConstraints de@(DDExpr (CtxFunc f@Function{..}) _) = do
    addConstraint =<< tvarTypeOfExpr de <~~~~ typeToTExpr funcType
    addConstraints =<< mapIdxM (\a i -> tvarTypeOfVar (ArgVar f i (name a)) <==== typeToTExpr (typ a)) funcArgs

-- When evaluating index expression:
-- 'index I(v1: t1, .., vn: tn) on R[e]',
-- the following type constraints are added:
-- '|vi| = |ti|, |e| = reltype_R'.
contextConstraints de@(DDExpr (CtxIndex idx@Index{..}) _) = do
    addConstraint =<< tvarTypeOfExpr de <~~~~ typeToTExpr (typ $ getRelation ?d $ atomRelation idxAtom)
    addConstraints =<< mapIdxM (\v i -> tvarTypeOfVar (IdxVar idx i (name v)) <==== typeToTExpr (typ v)) idxVars

-- When evaluating primary key expression:
-- 'relation R(..) primary key (x) e',
-- the following type constraints are added:
-- '|x| = reltype_R.
contextConstraints (DDExpr (CtxKey rel) _) =
    addConstraint =<< tvarTypeOfVar (KeyVar rel) <==== typeToTExpr (typ rel)

-- When evaluating expression e in a rule literal or the head of a rule:
-- R[e], the following type constraint is added:
-- '|e| = reltype_R'.
contextConstraints de@(DDExpr (CtxRuleRAtom rl i) _) =
    addConstraint =<< tvarTypeOfExpr de <~~~~ typeToTExpr (typ $ getRelation ?d $ atomRelation a)
    where a = rhsAtom $ ruleRHS rl !! i

contextConstraints de@(DDExpr (CtxRuleL rl i) _) =
    addConstraint =<< tvarTypeOfExpr de <~~~~ typeToTExpr (typ $ getRelation ?d $ atomRelation a)
    where a = ruleLHS rl !! i

-- When evaluating expression e in a filter clause of a rule: |e|=Bool.
contextConstraints de@(DDExpr (CtxRuleRCond rl i) _) | rhsIsFilterCondition $ ruleRHS rl !! i =
    addConstraint =<< tvarTypeOfExpr de <~~~~ TEBool
                                                     | otherwise = return ()

-- When evaluating aggregate: 'var g = e.group_by(v1,..,vn)'
-- (|v1|,...,|vn|)=K,
-- |e| = V,
-- |v| = Group<K,V>
contextConstraints de@(DDExpr (CtxRuleRProject rl i) _) = do
    tval <- teTypeOfExpr de
    tkey <- teTypeOfExpr (DDExpr (CtxRuleRGroupBy rl i) rhsGroupBy)
    addConstraint =<< tvarTypeOfVar (GroupVar rl i) <==== TEExtern gROUP_TYPE [tkey, tval]
    where
    RHSGroupBy{..} = ruleRHS rl !! i

contextConstraints (DDExpr CtxRuleRGroupBy{} _) = return ()

-- When evaluating 'var v = FlatMap(e)'
-- the following type constraints are added:
-- |v| = iterator_type(|e|)
contextConstraints de@(DDExpr (CtxRuleRFlatMap rl i) _) =
    addConstraint =<< deIsIterable de =<< tvarTypeOfVar (FlatMapVar rl i)

-- When evaluating expression e in an Inspect clause of a rule:
-- |e| = Tuple0, |var ddlog_weight| = Bit 64, |var ddlog_iter| = ....
contextConstraints de@(DDExpr (CtxRuleRInspect rl _) _) = do
    addConstraint =<< tvarTypeOfExpr de <~~~~ teTuple []
    addConstraint =<< tvarTypeOfVar WeightVar <==== typeToTExpr (tUser wEIGHT_TYPE [])
    if ruleIsRecursive ?d rl
       then addConstraint =<< tvarTypeOfVar (TSVar rl) <==== typeToTExpr (tUser nESTED_TS_TYPE [])
       else addConstraint =<< tvarTypeOfVar (TSVar rl) <==== typeToTExpr (tUser ePOCH_TYPE [])

contextConstraints (DDExpr ctx _) =
    error $ "contextConstraints called in unexpected context " ++ show ctx

{- Encode type constraints for an expression. -}

exprConstraints :: (?d::DatalogProgram) => DDExpr -> GeneratorMonad ()
exprConstraints (DDExpr ctx e) =
    exprTraverseCtxM exprConstraints' ctx e

exprConstraints' :: (?d::DatalogProgram) => ECtx -> ENode -> GeneratorMonad ()
exprConstraints' ctx e = do
    let ddexpr = DDExpr ctx $ E e
    exprConstraints_ ddexpr

-- Variable reference expression has the same type as the variable.
exprConstraints_ :: (?d::DatalogProgram) => DDExpr -> GeneratorMonad ()
exprConstraints_ de@(DDExpr ctx e@(E (EVar _ v))) = do
    let var = case lookupVar ?d ctx v of
                   Nothing -> case exprVarDecls ?d ctx e of
                                   [var'] -> var'
                                   _      -> error $ "Unknown variable '" ++ v ++ "' in " ++ show ctx
                   Just var' -> var'
    addConstraint =<< tvarTypeOfExpr de <====> teTypeOfVar var

-- Boolean literal.
exprConstraints_ de@(DDExpr _ (E EBool{})) =
    addConstraint =<< tvarTypeOfExpr de <==== TEBool

-- String literal.
exprConstraints_ de@(DDExpr _ (E EString{})) =
    addConstraint =<< tvarTypeOfExpr de <==== TEString

-- Unsigned bitvector literal.
exprConstraints_ de@(DDExpr _ (E EBit{..})) =
    addConstraint =<< tvarTypeOfExpr de <==== TEBit (IConst exprWidth)

-- Signed bitvector literal.
exprConstraints_ de@(DDExpr _ (E ESigned{..})) =
    addConstraint =<< tvarTypeOfExpr de <==== TESigned (IConst exprWidth)

-- Integer expression with unspecified width can be a signed or unsigned
-- bitvector or a bigint.  All of these cases are captured by 'deIsInt'.
exprConstraints_ de@(DDExpr _ (E EInt{..})) = do
    dev <- tvarTypeOfExpr de
    -- Default to bit<64/128>, signed<64/128>, or bigint based on the sign and value of
    -- the integer literal.
    let def = if exprIVal >= 0
              then if bitWidth exprIVal <= 64
                   then Just [dev ==== TEBit (IConst 64)]
                   else if bitWidth exprIVal <= 128
                        then Just [dev ==== TEBit (IConst 128)]
                        else Just [dev ==== TEBigInt]
              else if bitWidth (-exprIVal) <= 63
                   then Just [dev ==== TESigned (IConst 64)]
                   else if bitWidth (-exprIVal) <= 127
                        then Just [dev ==== TESigned (IConst 128)]
                        else Just [dev ==== TEBigInt]
    addConstraint =<< deIsNum de def
                              (\te -> err ?d (pos de) $ "integer value '" ++ show de ++ "' is used in a context where type '" ++ show te ++ "' is expected")

-- 32-bit FP literal.
exprConstraints_ de@(DDExpr _ (E EFloat{})) =
    addConstraint =<< tvarTypeOfExpr de <==== TEFloat

-- 64-bit or unknown width FP literal.
exprConstraints_ de@(DDExpr _ (E EDouble{})) =
    addConstraint =<< deIsFP de

exprConstraints_ de@(DDExpr _ (E e@EFunc{..})) = do
    -- Generate type constraints for all candidate functions.
    candidate_types <- mapM (\Function{..} -> do
                              ret_t <- typeToTExpr' de funcType
                              arg_ts <- mapM (\arg -> (if argMut arg then IConst 1 else IConst 0,) <$> typeToTExpr' de (typ arg)) funcArgs
                              return $ TEFunc arg_ts ret_t)
                            candidate_funcs
    case candidate_types of
         [te] -> addConstraint =<< tvarTypeOfExpr de <==== te
         _    -> do
           dev <- tvarTypeOfExpr de
           ce <- teTypeOfExpr de
           let -- Match function type against each candidate function.  If a unique match is found, we've found
               -- our candidate and can expand the lazy constraint.  If multiple potential matches remain, return
               -- Nothing and try again later, when the argument type has been further concretized, but if the
               -- argument is already fully concretized, complain.  If there are no matches, signal type conflict.
               expand te = case findIndices (\candidate_te -> unifyTExprs te candidate_te) $ candidate_types of
                                []  -> case te of
                                            TEFunc{} -> err ?d (pos e) $ "Unknown function '" ++ fname ++ "' of type '" ++ show te ++ "'"
                                            _ ->  err ?d (pos e) $ "Expected expression of type '" ++ show te ++ "', but found function '" ++ fname ++ "'"
                                [i] -> return $ Just [dev ==== (candidate_types !! i)]
                                is | teIsConstant te -> err ?d (pos e)
                                                        $ "Ambiguous reference to function '" ++ fname ++ "' of type '" ++ show te ++ "'" ++
                                                          " may refer to:\n  " ++
                                                          (intercalate "\n  " $ map (\i -> (name $ candidate_funcs !! i) ++ " at " ++ (spos $ candidate_funcs !! i)) is)
                                   | otherwise -> return Nothing
           addConstraint $ CLazy ce expand Nothing de $ "Function '" ++ fname ++ "' at " ++ (show $ pos e)
    where
    -- All functions that 'exprFuncName' may be referring to.
    candidate_funcs = concatMap (\f -> getFuncs ?d f Nothing) exprFuncName
    fname = nameLocalStr $ head exprFuncName


-- f(e1,...,en)
-- |f| = function(|e1|,..,|en|): |e|
exprConstraints_ de@(DDExpr ctx (E e@EApply{..})) = do
    let defunc = DDExpr (CtxApplyFunc e ctx) exprFunc
    ftype <- TEFunc <$> (mapIdxM (\a i -> (,) <$> (IVar <$> ivarMutabilityOfArg defunc i) <*> teTypeOfExpr (DDExpr (CtxApplyArg e ctx i) a)) exprArgs) <*> teTypeOfExpr de
    addConstraint =<< tvarTypeOfExpr defunc <==== ftype

-- Struct field access: 'e1.f', where field 'f' is present in user-defined
-- structs in 'S1',...,'Sn'.
--
-- 'is_S1 |e1| and |e| = |e1.f|
--  or .. or
--  is_Sn |e1| and |e| = |e1.f|'
--
-- In addition, 'e1' can be a shared reference to a struct:
-- for each shared reference type 'Ref_i' and each struct 'S_j', add a disjunct:
-- '|e1| = Ref_i<S_j> and |e| = |S_j.f|'

exprConstraints_ de@(DDExpr ctx (E e@EField{..})) = do
    dv <- tvarTypeOfExpr de
    let expand t' = case teDeref t' of
                         -- Type has not been sufficiently expanded yet.
                         TETVar{} -> return Nothing
                         te@(TEUser n _) | elem n (map name candidates) -> do
                            let t'' = fromJust $ tdefType $ getType ?d n
                            let guarded = structFieldGuarded t'' exprField
                            check ?d (not guarded) (pos e) $ "Access to guarded field \'" ++ exprField ++ "\' (not all constructors of type '" ++ n ++ "' have this field)."
                            let fld_type = typeToTExpr $ typ $ fromJust $ find ((==exprField) . name) $ structFields $ typ' ?d $ teToType te
                            return $ Just [dv ==== fld_type]
                         _ -> err ?d (pos estruct)
                                  $ "expression '" ++ show estruct ++ "' must have a field named '" ++ exprField ++ "', but its type '" ++ show t' ++ "' doesn't"
    ce <- teTypeOfExpr estruct
    addConstraint $ CLazy ce expand Nothing estruct
                  $ "expression '" ++ show estruct ++ "' must have a field named '" ++ exprField ++ "'"
    where
    ctx' = CtxField e ctx
    estruct = DDExpr ctx' exprStruct
    -- All structs that have the field.
    candidates = filter (\tdef -> isJust $ structLookupField (fromJust $ tdefType tdef) exprField)
               $ filter (isJust . tdefType)
               $ M.elems
               $ progTypedefs ?d

-- Tuple field access: 'e1.N'.
--
-- '|e1|=(...,|e|/*N'th position*/,...)'
--
-- In addition, 'e1' can be a shared reference to a tuple:
-- for each shared reference type 'Ref_i', add a disjunct:
-- '|e1|=Ref_i<(...,|e|/*N'th position*/,...)>'
exprConstraints_ de@(DDExpr ctx (E e@ETupField{..})) = do
    dvar <- tvarTypeOfExpr de
    let expand te = case teDeref te of
                         -- Type has not been sufficiently expanded yet.
                         TETVar{} -> return Nothing
                         TETuple as -> do
                            check ?d (length as >= (exprTupField + 1)) (pos e)
                                  $ "Expected tuple with at least " ++ show (exprTupField+1) ++ " fields, but expression '" ++ show e ++ "' of type '" ++ show te ++ "' only has " ++ show (length as)
                            return $ Just [dvar ==== (as !! exprTupField)]
                         _ -> err ?d (pos de)
                                  $ "expression '" ++ show etuple ++ "' must be a tuple, but its type is '" ++ show te ++ "'"
    ce <- teTypeOfExpr etuple
    addConstraint $ CLazy ce expand Nothing etuple
                  $ "expression '" ++ show de ++ "' must be a tuple with at least " ++ show (exprTupField+1) ++ " fields"
    where
    etuple = DDExpr (CtxTupField e ctx) exprTuple

-- Struct expression: 'Cons{.f1=e1,..,.fn=en}', where 'Cons' is a constructor of
-- type 'MyStruct'
--
-- 'is_MyStruct |de| and |e1|=MyStruct_f1 |e| and ... and |en| = MyStruct_fn |e|'
exprConstraints_ de@(DDExpr ctx (E e@EStruct{..})) = do
    addConstraint =<< deIsStruct de tdefName
    addConstraints =<< mapIdxM (\(arg, efield) i -> tvarTypeOfExpr (DDExpr (CtxStruct e ctx (i, IdentifierWithPos (pos arg) $ name arg)) efield) <~~~~> typeToTExpr' de (typ arg))
                            (zip consArgs $ map snd exprStructFields)
    where
    Constructor{..} = getConstructor ?d exprConstructor
    TypeDef{..} = consType ?d exprConstructor

-- Tuple expression '(e1,..,en)'.
--
-- '|e| = (|e1|,...,|en|)'.
exprConstraints_ de@(DDExpr ctx (E e@ETuple{..})) =
    addConstraint =<< tvarTypeOfExpr de <====>
                      (teTuple <$> mapIdxM (\e' i -> teTypeOfExpr (DDExpr (CtxTuple e ctx i) e')) exprTupleFields)

-- Bit slice 'e1[h:l]'
--
-- 'is_Bit(|e1|) and |e|=Bit (h-l+1)'
--
-- Additional constraints 'bitWidth |e1| >= h-l+1', 'h>=l' are enforced
-- in Validate.hs.
exprConstraints_ de@(DDExpr ctx (E e@ESlice{..})) = do
    addConstraint =<< tvarTypeOfExpr de <==== TEBit (IConst w)
    addConstraint =<< deIsBit ebits
    where
    ebits = DDExpr (CtxSlice e ctx) exprOp
    w = exprH - exprL + 1

-- Match 'match(e0) {e1->g1, .., en->gn}'
--
-- '|e0| =|e1|=..=|en| and |g1|=...=|gn|=|e|'
exprConstraints_ de@(DDExpr ctx (E e@EMatch{..})) =
    mapIdxM_ (\(ei, gi) i -> do
               addConstraint =<< tvarTypeOfExpr (DDExpr (CtxMatchPat e ctx i) ei) <~~~~> teTypeOfExpr mexpr
               addConstraint =<< tvarTypeOfExpr de <~~~~> teTypeOfExpr (DDExpr (CtxMatchVal e ctx i) gi))
             exprCases
    where
    mexpr = DDExpr (CtxMatchExpr e ctx) exprMatchExpr

-- Variable declaration: 'var v'
--
-- '|e|=|var v|'
exprConstraints_ de@(DDExpr ctx (E e@EVarDecl{})) =
    addConstraint =<< tvarTypeOfExpr de <====> teTypeOfVar (ExprVar ctx e)

-- Sequence 'e1;e2'
--
-- '|e|=|e2|'
exprConstraints_ de@(DDExpr ctx (E e@ESeq{..})) =
    addConstraint =<< tvarTypeOfExpr de <====> teTypeOfExpr (DDExpr (CtxSeq2 e ctx) exprRight)

--- 'if (e1){e2}else{e3}'
--
-- '|e1|=Bool and |e2|=|e3|=|e|'
exprConstraints_ de@(DDExpr ctx (E e@EITE{..})) = do
    addConstraint =<< tvarTypeOfExpr (DDExpr (CtxITEIf e ctx) exprCond) <~~~~ TEBool
    addConstraint =<< tvarTypeOfExpr (DDExpr (CtxITEThen e ctx) exprThen) <~~~~> teTypeOfExpr (DDExpr (CtxITEElse e ctx) exprElse)
    addConstraint =<< tvarTypeOfExpr de <====> teTypeOfExpr (DDExpr (CtxITEThen e ctx) exprThen)

-- 'for (v in e1) {e2}'
--
-- 'is_iterable |e1| and |var v|=iterator_type |e1| and |e| = () and |e2| = () '
exprConstraints_ de@(DDExpr ctx (E e@EFor{..})) = do
    addConstraint =<< deIsIterable (DDExpr (CtxForIter e ctx) exprIter) =<< (tvarTypeOfVar $ ForVar ctx e)
    addConstraint =<< tvarTypeOfExpr de <==== teTuple []
    addConstraint =<< tvarTypeOfExpr (DDExpr (CtxForBody e ctx) exprBody) <~~~~ teTuple []

-- Assignment: 'e1=e2'.
--
-- '|e|=Tuple0 and |e1|=|e2|'.
exprConstraints_ de@(DDExpr ctx (E e@ESet{..})) = do
    addConstraint =<< tvarTypeOfExpr de <==== teTuple []
    addConstraint =<< tvarTypeOfExpr (DDExpr (CtxSetL e ctx) exprLVal) <====> teTypeOfExpr (DDExpr (CtxSetR e ctx) exprRVal)

-- 'return e1'
--
-- Inside function:
-- '|e1|=ret_type', where 'ret_type' is the return type of the function;
--
-- Inside closure:
-- '|e1|=|eclosure|', where 'eclosure'
exprConstraints_ (DDExpr ctx (E e@EReturn{..})) = do
    ret_type <- case ctxInFuncOrClosure ctx of
                     Just CtxFunc{..} -> return $ typeToTExpr $ funcType ctxFunc
                     Just ctx'@CtxClosure{..} -> teTypeOfExpr (DDExpr ctx' $ exprExpr ctxParExpr)
                     _ -> -- Validate.hs should make sure this does not happen.
                          error $ "exprConstraints_ " ++ show e ++ " return not inside function or closure"
    addConstraint =<< tvarTypeOfExpr (DDExpr (CtxReturn e ctx) exprRetVal) <~~~~ ret_type


-- Binary operator 'e1 op e2', where `op` is one of '==, !=, <, >, <=, >='.
--
-- '|e1| = |e2| ans |e| = Bool'
exprConstraints_ de@(DDExpr ctx (E e@EBinOp{..})) | elem exprBOp [Eq, Neq, Lt, Lte, Gt, Gte] = do
    addConstraint =<< tvarTypeOfExpr l <~~~~> teTypeOfExpr r
    addConstraint =<< tvarTypeOfExpr de <==== TEBool

-- Binary operator 'e1 op e2', where `op` is one of '&&, ||, =>'.
--
-- '|e1| = |e2| = Bool and |e| == Bool'
                                                | elem exprBOp [And, Or, Impl] = do
    addConstraint =<< tvarTypeOfExpr l <~~~~> teTypeOfExpr r
    addConstraint =<< tvarTypeOfExpr l <~~~~ TEBool
    addConstraint =<< tvarTypeOfExpr de <==== TEBool

-- Binary operator 'e1 op e2', where `op` is one of '+, -, *. /'.
--
-- '|e| = |e1| = |e2| and is_num|e1|'
                                                | elem exprBOp [Plus, Minus, Times, Div] = do
    addConstraint =<< tvarTypeOfExpr l <~~~~> teTypeOfExpr r
    addConstraint =<< tvarTypeOfExpr de <====> teTypeOfExpr l
    addConstraint =<< (deIsNum de Nothing
                               (\te -> err ?d (pos de) $ "expression '" ++ show de ++ "' of a numeric type appears in a context where type '" ++ show te ++ "' is expected"))

-- Binary operator 'e1 % e2'.
--
-- '|e1| = |e2| and is_int(|e1|) and |e| = |e1|'
                                                | exprBOp == Mod = do
    addConstraint =<< tvarTypeOfExpr l <~~~~> teTypeOfExpr r
    addConstraint =<< deIsInt l
    addConstraint =<< tvarTypeOfExpr de <====> teTypeOfExpr l

-- Binary operator 'e1 op e2', where `op` is one of '<<, >>'.
--
-- 'is_int |e1| and is_int |e2| and |e| = |e1|'
                                                | elem exprBOp [ShiftL, ShiftR] = do
    addConstraint =<< deIsInt l
    -- If the type of 'r' cannot be inferred, default to 'u32'
    --isbits <- deIsBits r
    addConstraint =<< tvarTypeOfExpr r <==== TEBit (IConst 32)
    --addConstraint $ isbits {cDefault = Just [def]}
    addConstraint =<< tvarTypeOfExpr de <====> teTypeOfExpr l

-- Binary operator 'e1 op e2', where `op` is one of '|,&,^'.
--
-- '|e| = |e1| = |e2| and is_bits|e1|'
                                                | elem exprBOp [BOr, BAnd, BXor] = do
    addConstraint =<< tvarTypeOfExpr l <~~~~> teTypeOfExpr r
    addConstraint =<< tvarTypeOfExpr de <====> teTypeOfExpr l
    addConstraint =<< deIsBits l

-- Binary operator 'e1 ++ e2'.
--
-- is_String(|e1|) or is_Bit |e1| and is_Bit |e2| and bitWidth |e| = bitWidth |e1| + bitWidth |e2|
-- Note: we do not include the is_String(|e2|) constraint, as the compiler
-- automatically converts the expression in the RHS of concatenation operator to
-- string.
                                                | exprBOp == Concat = do
    isbit_r <- deIsBit r
    ivar_r <- ivarWidthOfExpr r
    isstring <- tvarTypeOfExpr de <==== TEString
    dte <- tvarTypeOfExpr de
    ce <- teTypeOfExpr l
    let expand TETVar{} = return Nothing
        expand TEString = return $ Just [isstring]
        expand (TEBit w) = return $ Just [isbit_r, dte ==== (TEBit $ IPlus w $ IVar ivar_r)]
        expand te = err ?d (pos l)
                        $ "expression '" ++ show l ++ "' must be of type that supports concatenation operator (++), i.e., 'bit<>' or 'string', but its type '" ++ show te ++ "' doesn't"
    addConstraint $ CLazy ce expand Nothing l
                  $ "expression '" ++ show l ++ "' must be of type that supports concatenation operator (++), i.e., 'bit<>' or 'string'"
                                                | otherwise =
    error $ "exprConstraints_: unknown binary operator " ++ show exprBOp
    where
    l = DDExpr (CtxBinOpL e ctx) exprLeft
    r = DDExpr (CtxBinOpR e ctx) exprRight

-- Boolean negation 'not e1'.
--
-- '|e| = Bool and |e1| = Bool'
exprConstraints_ de@(DDExpr ctx (E e@EUnOp{..}))  | exprUOp == Not = do
    addConstraint =<< tvarTypeOfExpr op <~~~~ TEBool
    addConstraint =<< tvarTypeOfExpr de <==== TEBool

-- Bit-wise negation '~e1'.
--
-- 'is_bits |e1| and |e| = |e1|'
                                                | exprUOp == BNeg = do
    addConstraint =<< deIsBits op
    addConstraint =<< tvarTypeOfExpr de <====> teTypeOfExpr op

                                                | exprUOp == UMinus = do
-- Unary minus '-e1'.
--
-- 'is_num |e1| and |e| = |e1|'
--
    addConstraint =<< deIsNum op Nothing
                              (\te -> err ?d (pos de) $ "expression '" ++ show op ++ "' must be of a numeric type, but its type is " ++ show te)
    addConstraint =<< tvarTypeOfExpr de <====> teTypeOfExpr op
                                                | otherwise =
    error $ "exprConstraints_: unknown unary operator " ++ show exprUOp
    where
    op = DDExpr (CtxUnOp e ctx) exprOp

-- Variable binding 'v @ e1'.
--
-- '|e|=|e1|=|var v|'
exprConstraints_ de@(DDExpr ctx (E e@EBinding{..})) = do
    addConstraint =<< tvarTypeOfVar (BindingVar ctx e) <====> teTypeOfExpr de
    addConstraint =<< tvarTypeOfExpr de <====> teTypeOfExpr (DDExpr (CtxBinding e ctx) exprPattern)

-- Explicit type annotation 'e1: t'
--
-- '|e|=|e1|=t'.
exprConstraints_ de@(DDExpr ctx (E e@ETyped{..})) = do
    addConstraint =<< tvarTypeOfExpr de <==== typeToTExpr exprTSpec
    addConstraint =<< tvarTypeOfExpr (DDExpr (CtxTyped e ctx) exprExpr) <~~~~ typeToTExpr exprTSpec

-- &-pattern: '&e1'
--
-- 'is_sharef_ref |e| and |e1|=ref_deref |e|'
exprConstraints_ de@(DDExpr ctx (E e@ERef{..})) =
    addConstraint =<< deIsSharedRef de =<< tvarTypeOfExpr eref
    where
    eref = DDExpr (CtxRef e ctx) exprPattern

-- Type coercion: 'e1 as t'
--
-- '|e| = t'.  Other constraints on valid source/destination pairs are enforced
-- outside of the type inference engine.
exprConstraints_ de@(DDExpr _ (E EAs{..})) =
    addConstraint =<< tvarTypeOfExpr de <==== typeToTExpr exprTSpec

-- ?-expression: 'e1?'
--
-- * In a function or closure whose return type is 'Option<_>'.
--   |e1| = Option<T> ==> |e| = T
--   |e1| = Result<T,E> ==> |e| = T
-- * In a function or closure whose return type is 'Result<_,E>'
--   |e1| = Result<T,E1> ==> |e| = T, E1 == E
exprConstraints_ de@(DDExpr ctx (E e@ETry{})) = do
    dtv <- tvarTypeOfExpr de
    let de1 = DDExpr (CtxTry e ctx) $ exprExpr e
    dtv1 <- tvarTypeOfExpr de1
    dte1 <- teTypeOfExpr de1
    -- In a function return type is the return type of the function.
    -- In a closure, return type is the type of the body of the closure.
    ret_type <- case ctxInFuncOrClosure ctx of
                     Just CtxFunc{..} -> return $ typeToTExpr $ funcType ctxFunc
                     Just ctx'@CtxClosure{..} -> teTypeOfExpr (DDExpr ctx' $ exprExpr ctxParExpr)
                     _ -> -- Validate.hs should make sure this does not happen.
                          error $ "exprConstraints_ " ++ show e ++ ": '?' not inside function or closure"
    -- Create a lazy constraint that triggers once we know the return type and
    -- the type of 'de1'.  Valid cobinations are:
    -- (Result<>, Result<>), (Option<>, Result<>), (Option<>, Option<>)
    let expand (TETuple [rt, et]) =
            case teExpandAliases rt of
                 TETVar{} -> return Nothing
                 TEUser rtname [_] | rtname == oPTION_TYPE ->
                    case teExpandAliases et of
                         TETVar{} -> return Nothing
                         TEUser etname [etarg] | etname == oPTION_TYPE ->
                             return $ Just [dtv ==== etarg]
                         TEUser etname [etok, _] | etname == rESULT_TYPE ->
                             return $ Just [dtv ==== etok]
                         _ -> err ?d (pos e)
                                  $ "expression '" ++ show de1 ++ "' must be of type 'Option<>' or 'Result<>', but its type is '" ++ show et ++ "'"
                 TEUser rtname [_, rterr] | rtname == rESULT_TYPE ->
                    case teExpandAliases et of
                         TETVar{} -> return Nothing
                         TEUser etname [etok, _] | etname == rESULT_TYPE ->
                             return $ Just [dtv ==== etok,
                                            dtv1 ~~~~ (TEUser rESULT_TYPE [etok, rterr])]
                         _ -> err ?d (pos e)
                                  $ "expression '" ++ show de1 ++ "' must be of type 'Result<>', but its type is '" ++ show et ++ "'"
                 _ -> err ?d (pos e)
                          $ "'?' can only be used inside functions or closures that return 'Option<>' or 'Result<>', not '" ++ show rt ++ "'"
        expand x = error $ "exprConstraints_ '" ++ show de ++ "': unexpected type in expand: '" ++ show x ++ "'"
    addConstraint $ CLazy (TETuple [ret_type, dte1]) expand Nothing de
                  $ "'?' operator must be applicable to expression '" ++ show de1 ++ "'"

-- 'break', 'continue', '_' expressions are happy to take any type required by
-- their context and so generate no type constraints.
exprConstraints_ (DDExpr _ (E EBreak{})) = return ()
exprConstraints_ (DDExpr _ (E EContinue{})) = return ()
exprConstraints_ (DDExpr _ (E EPHolder{})) = return ()

-- e = function(v1,...,vn) e0
-- |e| = function(|v1|,..,|vn|): |e0|.
--
-- In addition, if types of some of v1,.., vn are specified, add constraints
-- |vi| = ti for each specified type.
-- If the return type is specified, add constraint |e0| = |t0|.
exprConstraints_ de@(DDExpr ctx (E e@EClosure{..})) = do
    addConstraint =<< tvarTypeOfExpr de <====> (TEFunc <$> (mapIdxM (\_ i -> (,) <$> (IVar <$> ivarMutabilityOfArg de i) <*> teTypeOfVar (ClosureArgVar ctx e i))
                                                                    exprClosureArgs)
                                                       <*> teTypeOfExpr (DDExpr (CtxClosure e ctx) exprExpr))
    mapIdxM_ (\a i -> case ceargType a of
                           Just t -> addConstraint =<< tvarTypeOfVar (ClosureArgVar ctx e i) <==== typeToTExpr (typ t)
                           Nothing -> return ())
             exprClosureArgs
    case exprClosureType of
         Just t -> addConstraint =<< tvarTypeOfExpr (DDExpr (CtxClosure e ctx) exprExpr) <~~~~ typeToTExpr t
         Nothing -> return ()
