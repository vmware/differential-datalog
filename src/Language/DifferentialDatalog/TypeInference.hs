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

module Language.DifferentialDatalog.TypeInference(
    DDExpr(..),
    tvObject,
    inferTypes
)
where

import qualified Data.Map as M
import Data.List
--import Data.List.Utils
import Control.Monad.Except
import Control.Monad.State.Lazy
import Data.Maybe
import GHC.Float
--import Debug.Trace

import Language.DifferentialDatalog.Attribute
import Language.DifferentialDatalog.ECtx
import Language.DifferentialDatalog.Error
import Language.DifferentialDatalog.Expr
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
    genVars        :: M.Map (Maybe DDExpr, Maybe Var, Maybe String) Int,
    -- The number of type variables in 'genVars'. Used to assign ids to new variables.
    genNumVars     :: Int
}

emptyGenerator :: GeneratorState
emptyGenerator = GeneratorState {
    genConstraints = [],
    genVars        = M.empty,
    genNumVars     = 0
}

type GeneratorMonad t = State GeneratorState t

addConstraint :: Constraint -> GeneratorMonad ()
addConstraint c = modify $ \gen@GeneratorState{..} -> gen{genConstraints = c : genConstraints}

addConstraints :: [Constraint] -> GeneratorMonad ()
addConstraints cs = modify $ \gen@GeneratorState{..} -> gen{genConstraints = cs ++ genConstraints}

tvarTypeOfVar :: Var -> GeneratorMonad TypeVar
tvarTypeOfVar v = do
    gen@GeneratorState{..} <- get
    case M.lookup (Nothing, Just v, Nothing) genVars of
         Nothing -> do let tv = TVarTypeOfVar genNumVars v
                       put $ gen {genVars = M.insert (Nothing, Just v, Nothing) genNumVars genVars, genNumVars = genNumVars + 1}
                       return tv
         Just i  -> return $ TVarTypeOfVar i v

tvarTypeOfExpr :: DDExpr -> GeneratorMonad TypeVar
tvarTypeOfExpr de = do
    gen@GeneratorState{..} <- get
    case M.lookup (Just de, Nothing, Nothing) genVars of
         Nothing -> do let tv = TVarTypeOfExpr genNumVars de
                       put $ gen {genVars = M.insert (Just de, Nothing, Nothing) genNumVars genVars, genNumVars = genNumVars + 1}
                       return tv
         Just i  -> return $ TVarTypeOfExpr i de

tvarAux :: DDExpr -> String -> GeneratorMonad TypeVar
tvarAux de n = do
    gen@GeneratorState{..} <- get
    case M.lookup (Just de, Nothing, Just n) genVars of
         Nothing -> do let tv = TVarAux genNumVars de n
                       put $ gen {genVars = M.insert (Just de, Nothing, Just n) genNumVars genVars, genNumVars = genNumVars + 1}
                       return tv
         Just i -> return $ TVarAux i de n

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

-- Convert type to a type expression, replacing type arguments ('A, 'B, ...)
-- with type constants 'TETArg "A", TETArg "B", ...'.  For example, when
-- generating type inference in the body of a function, we treat its type
-- arguments as constants.  Inferred types for variables and expressions inside
-- the body of the function may depend on these constants.
typeToTExpr :: (?d::DatalogProgram) => Type -> TExpr
typeToTExpr t = evalState (typeToTExpr_ Nothing t) emptyGenerator

-- Convert type to a type expression, replacing type arguments with fresh
-- variables.  The 'de' argument is used to generate unique type variable
-- identifiers.  For example, when doing type inference for a function call
-- expression, we introduce fresh type variables for type arguments of the
-- function.
typeToTExpr' :: (?d::DatalogProgram) => DDExpr -> Type -> GeneratorMonad TExpr
typeToTExpr' de t = typeToTExpr_ (Just de) t

typeToTExpr_ :: (?d::DatalogProgram) => Maybe DDExpr -> Type -> GeneratorMonad TExpr
typeToTExpr_ mde t = typeToTExpr__ mde (typ'' ?d t)

typeToTExpr__ :: (?d::DatalogProgram) => Maybe DDExpr -> Type -> GeneratorMonad TExpr
typeToTExpr__ _         TBool{}      = return TEBool
typeToTExpr__ _         TInt{}       = return TEBigInt
typeToTExpr__ _         TString{}    = return TEString
typeToTExpr__ _         TBit{..}     = return $ TEBit $ IConst typeWidth
typeToTExpr__ _         TSigned{..}  = return $ TESigned $ IConst typeWidth
typeToTExpr__ _         TFloat{}     = return TEFloat
typeToTExpr__ _         TDouble{}    = return TEDouble
typeToTExpr__ mde       TTuple{..}   = teTuple <$> mapM (typeToTExpr_ mde . typ) typeTupArgs
typeToTExpr__ mde       TUser{..}    = TEUser typeName <$> mapM (typeToTExpr_ mde) typeArgs
typeToTExpr__ Nothing   TVar{..}     = return $ TETArg tvarName
typeToTExpr__ (Just de) TVar{..}     = teTVarAux de tvarName
typeToTExpr__ mde       TOpaque{..}  = TEExtern typeName <$> mapM (typeToTExpr_ mde) typeArgs
typeToTExpr__ _         t@TStruct{}  = error $ "typeToTExpr__: unexpected '" ++ show t ++ "'"

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
deIsBit_ de = tvarTypeOfExpr de <~~~ (TEBit (IVar $ WidthOfExpr de))

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
        expand (TEExtern n [t'])    | elem n sET_TYPES = return $ Just [tv ~~~~ t']
        expand (TEExtern n [_, t']) | n == gROUP_TYPE  = return $ Just [tv ~~~~ t']
        expand (TEExtern n [k,v])   | n == mAP_TYPE    = return $ Just [tv ~~~~ teTuple [k,v]]
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

inferTypes :: (MonadError String me) => DatalogProgram -> [DDExpr] -> me [Expr]
inferTypes d es = do
    let ?d = d
    let GeneratorState{..} = execState (do mapM_ contextConstraints es
                                           mapM_ exprConstraints es)
                                       emptyGenerator
    typing <- --trace ("inferTypes " ++ show es ++ "\nConstraints:\n" ++ constraintsShow genConstraints) $
              solve d genConstraints
    -- Extract ECtx -> Type mapping from 'typing'.
    let ctxtypes = foldl' (\m (tv, t) ->
                            case tv of
                                 TVarTypeOfExpr _ (DDExpr ctx _) -> M.insert ctx t m
                                 _                               -> m) M.empty
                          $ M.toList typing
    let add_types :: (MonadError String me) => ECtx -> ENode -> me Expr
        add_types ctx e =
            -- String conversion.
            case ctx of
                CtxBinOpR _ ctx' | ctxtypes M.! ctx' == tString && t /= tString
                                 -> do e'' <- exprInjectStringConversion d (enode e') t
                                       return $ E $ ETyped (pos e') e'' tString
                _ -> return e'
            where
            t = case M.lookup ctx ctxtypes of
                     Just t' -> t'
                     Nothing -> error $ "inferTypes: context has not been assigned a type:\n" ++ show ctx
            annotated = if ctxIsTyped ctx
                           then E e
                           else E $ ETyped (pos e) (E e) t
            e' = case e of
                 -- Convert integer literals to bit vectors if necessary.
                 EInt{..}     -> case t of
                                      TBit{..}    -> E $ EBit    (pos e) typeWidth exprIVal
                                      TSigned{..} -> E $ ESigned (pos e) typeWidth exprIVal
                                      TFloat{}    -> E $ EFloat  (pos e) $ fromInteger exprIVal
                                      TDouble{}   -> E $ EDouble (pos e) $ fromInteger exprIVal
                                      TInt{}      -> E e
                                      _           -> error $ "inferTypes: unexpected integer type '" ++ show t ++ "'"
                 -- Convert doubles to floats if necessary.
                 EDouble{..} -> case t of
                                     TFloat{..}  -> E $ EFloat  (pos e) $ double2Float exprDVal
                                     TDouble{}   -> E e
                                     _           -> error $ "inferTypes: unexpected floating point type '" ++ show t ++ "'"
                 -- Annotate all expressions whose type cannot be derived in a bottom-up manner:
                 -- var decls, placeholders, function calls, structs, etc.
                 -- This should be enough to determine the type of any
                 -- expression without type inference, by scanning it bottom up.
                 EVarDecl{}  -> annotated
                 EVar{} | ctxInRuleRHSPattern ctx 
                             -> annotated
                 EPHolder{}  -> annotated
                 EApply{..} | typeIsPolymorphic (funcType $ getFunc ?d exprFunc)
                             -> annotated
                 EStruct{}   -> annotated
                 EContinue{} -> annotated
                 EBreak{}    -> annotated
                 EReturn{}   -> annotated
                 ERef{}      -> annotated
                 _           -> E e
    mapM (\(DDExpr ctx e) -> exprFoldCtxM add_types ctx e) es
    {-trace ("inferTypes " ++ (intercalate "\n\n" $ map show es) ++ "\nconstraints:\n" ++ (constraintsShow genConstraints))$-}

{- Context-specific constraints. -}

-- When evaluating the body of a function:
-- 'function f(arg1: t1, ..., argn: tn): t0 { e }'
-- the following type constraints are added:
-- '|e| = t0, |argi|=ti'.
contextConstraints :: (?d::DatalogProgram) => DDExpr -> GeneratorMonad ()
contextConstraints de@(DDExpr (CtxFunc f@Function{..}) _) = do
    addConstraint =<< tvarTypeOfExpr de <~~~~ typeToTExpr funcType
    addConstraints =<< mapM (\a -> tvarTypeOfVar (ArgVar f (name a)) <==== typeToTExpr (typ a)) funcArgs

-- When evaluating index expression:
-- 'index I(v1: t1, .., vn: tn) on R[e]',
-- the following type constraints are added:
-- '|vi| = |ti|, |e| = reltype_R'.
contextConstraints de@(DDExpr (CtxIndex idx@Index{..}) _) = do
    addConstraint =<< tvarTypeOfExpr de <~~~~ typeToTExpr (typ $ getRelation ?d $ atomRelation idxAtom)
    addConstraints =<< mapM (\v -> tvarTypeOfVar (IdxVar idx (name v)) <==== typeToTExpr (typ v)) idxVars

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

-- When evaluating aggregate: 'var v = Aggregate((v1,..,vn), f(e))'
-- where 'function f<'A1,...,'Am>(g: Group<K,V>): T', where K,V,T may
-- depend on type arguments 'Ai', the following type constraints are
-- added:
-- (|v1|,...,|vn|)=K,
-- |e| = V,
-- |v| = T
contextConstraints de@(DDExpr (CtxRuleRAggregate rl i) _) = do
    addConstraint =<< tvarTypeOfExpr de <~~~~> typeToTExpr' de vtype
    addConstraint =<< tvarTypeOfVar (AggregateVar rl i) <====> typeToTExpr' de ret_type
    where
    RHSAggregate{..} = ruleRHS rl !! i
    Function{funcArgs = [grp_type], funcType = ret_type} = getFunc ?d rhsAggFunc
    TOpaque{typeArgs = [_, vtype]} = typ' ?d grp_type

contextConstraints de@(DDExpr ctx@(CtxRuleRGroupBy rl i) _) =
    addConstraint =<< tvarTypeOfExpr (DDExpr ctx rhsGroupBy) <~~~~> typeToTExpr' de ktype
    where
    RHSAggregate{..} = ruleRHS rl !! i
    Function{funcArgs = [grp_type]} = getFunc ?d rhsAggFunc
    TOpaque{typeArgs = [ktype, _]} = typ' ?d grp_type


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

-- f(e1,...,en)
--
-- |de|=f_ret |a1|...|aj| and
-- |e1|=f_arg_1 |a1|...|aj| and ... and |en|=f_arg_n |a1|...|aj|, where |a1|...|aj| are fresh type variables
exprConstraints_ de@(DDExpr ctx (E e@EApply{..})) = do
    addConstraint =<< tvarTypeOfExpr de <====> typeToTExpr' de funcType
    addConstraints =<< (mapIdxM (\(farg, earg) i -> tvarTypeOfExpr (DDExpr (CtxApply e ctx i) earg) <~~~~> typeToTExpr' de (typ farg))
                                $ zip funcArgs exprArgs)
    where
    Function{..} = getFunc ?d exprFunc

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
    addConstraints =<< mapM (\(arg, efield) -> tvarTypeOfExpr (DDExpr (CtxStruct e ctx (name arg)) efield) <~~~~> typeToTExpr' de (typ arg))
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
-- '|e1|=retType'
exprConstraints_ (DDExpr ctx (E e@EReturn{..})) =
    addConstraint =<< tvarTypeOfExpr (DDExpr (CtxReturn e ctx) exprRetVal) <~~~~ typeToTExpr funcType
    where
    Just Function{..} = ctxInFunc ctx

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
    isbits <- deIsBits r
    def <- tvarTypeOfExpr r <==== TEBit (IConst 32)
    addConstraint $ isbits {cDefault = Just [def]}
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
    isstring <- tvarTypeOfExpr de <==== TEString
    dte <- tvarTypeOfExpr de
    ce <- teTypeOfExpr l
    let expand TETVar{} = return Nothing
        expand TEString = return $ Just [isstring]
        expand (TEBit w) = return $ Just [isbit_r, dte ==== TEBit (IPlus w (IVar $ WidthOfExpr r))] 
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

-- 'break', 'continue', '_' expressions are happy to take any type required by
-- their context and so generate no type constraints.
exprConstraints_ (DDExpr _ (E EBreak{})) = return ()
exprConstraints_ (DDExpr _ (E EContinue{})) = return ()
exprConstraints_ (DDExpr _ (E EPHolder{})) = return ()
