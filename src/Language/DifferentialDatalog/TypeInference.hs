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

{-# LANGUAGE FlexibleContexts, RecordWildCards, TupleSections, ImplicitParams #-}

module Language.DifferentialDatalog.TypeInference(
    TypeInferenceResult(..),
    inferTypes
)
where

import qualified Data.Map as M
import Control.Monad.State.Lazy
import Data.Maybe

import Language.DifferentialDatalog.Attribute
import Language.DifferentialDatalog.ECtx
import Language.DifferentialDatalog.Expr
import Language.DifferentialDatalog.Name
import Language.DifferentialDatalog.NS
import Language.DifferentialDatalog.Ops
import Language.DifferentialDatalog.Rule
import Language.DifferentialDatalog.Syntax
import Language.DifferentialDatalog.Type
import Language.DifferentialDatalog.Util
import Language.DifferentialDatalog.Var

data TypeInferenceResult = TypeInferenceOk (M.Map Var Type) (M.Map ECtx Type)
                         | TypeInferenceConflict
                         | TypeInferenceAmbiguity

-- Uniquely identifies an expression in a DDlog program.
data DDExpr = DDExpr {ddexprCtx::ECtx, ddexprExpr::Expr}

-- A type constraint is an atomic predicate or a disjuncion of conjuntions of
-- predicates.  We use a unification-based solver to resolv type constraints,
-- which does not handle arbitrary Boolean combination constraints, hence this
-- restricted structure.
data Constraint = CPredicate Predicate
                  -- Disjunction of conjunctions.  The first condition in each
                  -- conjunction must not overlap with others, e.g.,
                  -- 'c11 && c12 && ... ||
                  --  ...
                  --  cn1 && cn2 && ...'
                  --  where 'c1i' are all mutually exclusive.
                | CDisj [[Predicate]]

cdisj :: [[Predicate]] -> [Constraint]
cdisj [ps] = map CPredicate ps
cdisj pss  = [CDisj pss]

-- Integer expressions represent widths of bit and signed types.
data IExpr = -- Integer constant.
             IConst Int
             -- Integer variable equal to the width of a DDExpr.
           | IWidthOfExpr DDExpr
             -- Sum of two widths.
           | IPlus IExpr IExpr

-- Type expression: an expression that returns a value of 'Type' sort.
data TExpr = TEBool
           | TEString
           | TEBigInt
           | TEBit IExpr
           | TESigned IExpr
           | TEFloat
           | TEDouble
             -- Tuple type whose size and field types may be only partially known.
           | TETuple (Maybe Int) (M.Map Int TExpr)
             -- User-defined type.
           | TEUser String [TExpr]
             -- Extern type.
           | TEExtern String [TExpr]
             -- In a function context only, a type argument of the function (e.g., 'A).
           | TETArg String
             -- Type of a DDlog expression.
           | TETypeOfExpr DDExpr
             -- Type of a DDlog variable.
           | TETypeOfVar Var
             -- We sometimes need to introduce extra type variables to model
             -- unknown type arguments of function calls.  We could use
             -- arbitrary auto-generated names for them, but to avoid a state
             -- monad, we use context and type argument name to uniquely
             -- identify the variable.
           | TETVar ECtx String

teTuple :: [TExpr] -> TExpr
teTuple [t] = t
teTuple ts  = TETuple (Just $ length ts) (M.fromList $ mapIdx (\t i -> (i, t)) ts)

-- Type congruence, e.g., '|e| = BigInt'.
data Predicate = PEq TExpr TExpr

(===) :: TExpr -> TExpr -> Constraint
(===) t1 t2 = CPredicate $ PEq t1 t2

-- Type expression is a struct of the specified user-defined type: 'is_MyStruct |e|'.
deIsStruct :: (?d::DatalogProgram) => DDExpr -> String -> Constraint
deIsStruct de n = CPredicate $ deIsStruct_ de n

deIsStruct_ :: (?d::DatalogProgram) => DDExpr -> String -> Predicate
deIsStruct_ de@(DDExpr ctx _) n =
    PEq (TETypeOfExpr de) (TEUser n (map (\a -> TETVar ctx $ name a) tdefArgs))
    where
    TypeDef{..} = getType ?d n

deIsBool :: DDExpr -> Constraint
deIsBool de = TETypeOfExpr de === TEBool

deIsString :: DDExpr -> Constraint
deIsString de = CPredicate $ deIsString_ de

deIsString_ :: DDExpr -> Predicate
deIsString_ de = PEq (TETypeOfExpr de) TEString

deIsBit :: DDExpr -> Constraint
deIsBit de = CPredicate $ deIsBit_ de

deIsBit_ :: DDExpr -> Predicate
deIsBit_ de = PEq (TETypeOfExpr de) (TEBit (IWidthOfExpr de))

deIsSigned_ :: DDExpr -> Predicate
deIsSigned_ de = PEq (TETypeOfExpr de) (TESigned (IWidthOfExpr de))

deIsBigInt_ :: DDExpr -> Predicate
deIsBigInt_ de = PEq (TETypeOfExpr de) TEBigInt

deIsFloat :: DDExpr -> Constraint
deIsFloat de = CPredicate $ deIsFloat_ de

deIsFloat_ :: DDExpr -> Predicate
deIsFloat_ de = PEq (TETypeOfExpr de) TEFloat

deIsDouble_ :: DDExpr -> Predicate
deIsDouble_ de = PEq (TETypeOfExpr de) TEDouble

deIsFP :: DDExpr -> Constraint
deIsFP de = CDisj $ deIsFP_ de

deIsFP_ :: DDExpr -> [[Predicate]]
deIsFP_ de = [[deIsFloat_ de], [deIsDouble_ de]]

-- 'is_bits t = is_Bit t || is_Signed t'.
deIsBits :: DDExpr -> Constraint
deIsBits de = CDisj $ deIsBits_ de

deIsBits_ :: DDExpr -> [[Predicate]]
deIsBits_ de = [[deIsBit_ de], [deIsSigned_ de]]

-- 'is_int t = is_bits t || is_BigInt t'.
deIsInt :: DDExpr -> Constraint
deIsInt de = CDisj $ deIsInt_ de

deIsInt_ :: DDExpr -> [[Predicate]]
deIsInt_ de = deIsBits_ de ++ [[deIsBigInt_ de]]

-- 'is_num t = is_int t || is_fp t'.
deIsNum :: DDExpr -> Constraint
deIsNum de = CDisj $ deIsInt_ de ++ deIsFP_ de

-- Type expression is a shared reference type with specified inner type.
deIsSharedRef :: (?d::DatalogProgram) => DDExpr -> TExpr -> [Constraint]
deIsSharedRef de t =
    cdisj $ map (\tdef -> [PEq (TETypeOfExpr de) (TEExtern (name tdef) [t])]) sref_tdefs
    where
    -- All shared reference types.
    sref_tdefs = filter (tdefGetSharedRefAttr ?d)
                 $ M.elems $ progTypedefs ?d

-- Expression is a set (Set, Tinyset, Vec, ..) with specified element type.
deIsSet :: (?d::DatalogProgram) => DDExpr -> TExpr -> Constraint
deIsSet de t = CDisj $ map (deIsSet_ de t) sET_TYPES

-- Expression is a collection with specified element type (when iterating using
-- for-loop).
deIsIterable :: (?d::DatalogProgram) => DDExpr -> TExpr -> Constraint
deIsIterable de t =
    CDisj $
    deIsGroup_ de t :
    deIsMap_ de t :
    map (deIsSet_ de t) sET_TYPES

deIsSet_ :: (?d::DatalogProgram) => DDExpr -> TExpr -> String -> [Predicate]
deIsSet_ de t set_type =
    [PEq (TETypeOfExpr de) (TEExtern set_type [t])]
    where
    TypeDef{tdefArgs=[_],..} = getType ?d set_type

deIsGroup_ :: (?d::DatalogProgram) => DDExpr -> TExpr -> [Predicate]
deIsGroup_ de@DDExpr{..} t =
    [PEq (TETypeOfExpr de) (TEExtern gROUP_TYPE [TETVar ddexprCtx (name k), t])]
    where
    TypeDef{tdefArgs=[k,_],..} = getType ?d gROUP_TYPE

deIsMap_ :: (?d::DatalogProgram) => DDExpr -> TExpr -> [Predicate]
deIsMap_ de@DDExpr{..} t =
    [ PEq (TETypeOfExpr de) (TEExtern mAP_TYPE [TETVar ddexprCtx (name k), TETVar ddexprCtx (name v)])
    , PEq t (TETuple (Just 2) $ M.fromList [(0, TETVar ddexprCtx (name k)), (1, TETVar ddexprCtx (name v))])]
    where
    TypeDef{tdefArgs=[k,v],..} = getType ?d mAP_TYPE

-- Convert type to a type expression, replacing type arguments ('A, 'B, ...)
-- with type constants 'TETArg "A", TETArg "B", ...'.  For example, when
-- generating type inference in the body of a function, we treat its type
-- arguments as constants.  Inferred types for variables and expressions inside
-- the body of the function may depend on this constants.
typeToTExpr :: (?d::DatalogProgram) => Type -> TExpr
typeToTExpr t = typeToTExpr_ Nothing t

-- Convert type to a type expression, replacing type arguments with fresh
-- variables.  The 'ctx' argument is used to generate unique type variable
-- identifiers.  For example, when doing type inference for a function call
-- expression, we introduce fresh type variables for type arguments of the
-- function.
typeToTExpr' :: (?d::DatalogProgram) => ECtx -> Type -> TExpr
typeToTExpr' ctx t = typeToTExpr_ (Just ctx) t

typeToTExpr_ :: (?d::DatalogProgram) => Maybe ECtx -> Type -> TExpr
typeToTExpr_ mctx t = typeToTExpr__ mctx (typ'' ?d t)

typeToTExpr__ :: (?d::DatalogProgram) => Maybe ECtx -> Type -> TExpr
typeToTExpr__ _         TBool{}      = TEBool
typeToTExpr__ _         TInt{}       = TEBigInt
typeToTExpr__ _         TString{}    = TEString
typeToTExpr__ _         TBit{..}     = TEBit $ IConst typeWidth
typeToTExpr__ _         TSigned{..}  = TESigned $ IConst typeWidth
typeToTExpr__ _         TFloat{}     = TEFloat
typeToTExpr__ _         TDouble{}    = TEDouble
typeToTExpr__ mctx      TTuple{..}   = TETuple (Just $ length typeTupArgs) (M.fromList $ mapIdx (\a i -> (i, typeToTExpr_ mctx (typ a))) typeTupArgs)
typeToTExpr__ mctx      TUser{..}    = TEUser typeName $ map (typeToTExpr_ mctx) typeArgs
typeToTExpr__ Nothing   TVar{..}     = TETArg tvarName
typeToTExpr__ (Just c)  TVar{..}     = TETVar c tvarName
typeToTExpr__ mctx      TOpaque{..}  = TEExtern typeName $ map (typeToTExpr_ mctx) typeArgs
typeToTExpr__ _         t@TStruct{}  = error $ "typeToTExpr__: unexpected '" ++ show t ++ "'"

-- Main type inference function.  Takes one or more expressions an tries to
-- infer types for all variables and subexpressions.
--
-- The input argument to this function is one of:
-- * The body of a function.
-- * Index expression.
-- * Primary key expression.
-- * All expressions in a rule, including RHS and LHS literals.
inferTypes :: DatalogProgram -> [DDExpr] -> TypeInferenceResult
inferTypes d es =
    let ?d = d in
    let ctx_constr = concatMap contextConstraints es
        expr_constr = concatMap exprConstraints es
        all_constr = ctx_constr ++ expr_constr
    in solveTypeConstraints all_constr

solveTypeConstraints :: [Constraint] -> TypeInferenceResult
solveTypeConstraints _ = error "solveTypeConstraints: not implemented"

{- Context-specific constraints. -}

-- When evaluating the body of a function:
-- 'function f(arg1: t1, ..., argn: tn): t0 { e }'
-- the following type constraints are added:
-- '|e| = t0, |argi|=ti'.
contextConstraints :: (?d::DatalogProgram) => DDExpr -> [Constraint]
contextConstraints de@(DDExpr (CtxFunc f@Function{..}) _) =
    (TETypeOfExpr de === typeToTExpr funcType) :
    (map (\a -> TETypeOfVar (ArgVar f (name a)) === typeToTExpr (typ a)) funcArgs)

-- When evaluating index expression:
-- 'index I(v1: t1, .., vn: tn) on R[e]',
-- the following type constraints are added:
-- '|vi| = |ti|, |e| = reltype_R'.
contextConstraints de@(DDExpr (CtxIndex idx@Index{..}) _) =
    (TETypeOfExpr de === typeToTExpr (typ $ getRelation ?d $ atomRelation idxAtom)) :
    (map (\v -> TETypeOfVar (IdxVar idx (name v)) === typeToTExpr (typ v)) idxVars)

-- When evaluating primary key expression:
-- 'relation R(..) primary key (x) e',
-- the following type constraints are added:
-- '|x| = reltype_R.
contextConstraints (DDExpr (CtxKey rel) _) =
    [TETypeOfVar (KeyVar rel) === typeToTExpr (typ rel)]

-- When evaluating expression e in a rule literal or the head of a rule:
-- R[e], the following type constraint is added:
-- '|e| = reltype_R'.
contextConstraints de@(DDExpr (CtxRuleRAtom rl i) _) =
    [TETypeOfExpr de === typeToTExpr (typ $ getRelation ?d $ atomRelation a)]
    where a = rhsAtom $ ruleRHS rl !! i

contextConstraints de@(DDExpr (CtxRuleL rl i) _) =
    [TETypeOfExpr de === typeToTExpr (typ $ getRelation ?d $ atomRelation a)]
    where a = ruleLHS rl !! i

-- When evaluating expression e in a filter clause of a rule: |e|=Bool.
contextConstraints de@(DDExpr (CtxRuleRCond rl i) _) | rhsIsFilterCondition $ ruleRHS rl !! i =
    [deIsBool de]
                                                     | otherwise = []

-- When evaluating aggregate: 'var v = Aggregate((v1,..,vn), f(e))'
-- where 'function f<'A1,...,'Am>(g: Group<K,V>): T', where K,V,T may
-- depend on type arguments 'Ai', the following type constraints are
-- added:
-- (|v1|,...,|vn|)=K,
-- |e| = V,
-- |v| = T
contextConstraints de@(DDExpr (CtxRuleRAggregate rl i) _) =
    [ TVarTypeOfExpr de ==== typeToTExpr' de vtype
    , TVarTypeOfVar (AggregateVar rl i) ==== typeToTExpr' de ret_type]
    where
    RHSAggregate{..} = ruleRHS rl !! i
    Function{funcArgs = [grp_type], funcType = ret_type} = getFunc ?d rhsAggFunc
    TOpaque{typeArgs = [_, vtype]} = typ' ?d grp_type

contextConstraints de@(DDExpr ctx@(CtxRuleRGroupBy rl i) _) =
    [ TVarTypeOfExpr (DDExpr ctx rhsGroupBy) ==== typeToTExpr' de ktype]
    where
    RHSAggregate{..} = ruleRHS rl !! i
    Function{funcArgs = [grp_type]} = getFunc ?d rhsAggFunc
    TOpaque{typeArgs = [ktype, _]} = typ' ?d grp_type


-- When evaluating 'var v = FlatMap(e)'
-- the following type constraints are added:
-- |v| = iterator_type(|e|)
contextConstraints de@(DDExpr (CtxRuleRFlatMap rl i) _) =
    [deIsSet de $ TETypeOfVar (FlatMapVar rl i)]

-- When evaluating expression e in an Inspect clause of a rule:
-- |e| = Tuple0, |var ddlog_weight| = Bit 64, |var ddlog_iter| = ....
contextConstraints de@(DDExpr (CtxRuleRInspect rl _) _) =
    [ TETypeOfExpr de === TETuple (Just 0) M.empty
    , TETypeOfVar WeightVar === typeToTExpr (tUser wEIGHT_TYPE [])
    , if ruleIsRecursive ?d rl
         then TETypeOfVar (TSVar rl) === typeToTExpr (tUser nESTED_TS_TYPE [])
         else TETypeOfVar (TSVar rl) === typeToTExpr (tUser ePOCH_TYPE []) ]

contextConstraints (DDExpr ctx _) =
    error $ "contextConstraints called in unexpected context " ++ show ctx

{- Encode type constraints for an expression. -}

exprConstraints :: (?d::DatalogProgram) => DDExpr -> [Constraint]
exprConstraints (DDExpr ctx e) =
    execState (exprTraverseCtxM exprConstraints' ctx e) []

exprConstraints' :: (?d::DatalogProgram) => ECtx -> ENode -> State [Constraint] ()
exprConstraints' ctx e = do
    let ddexpr = DDExpr ctx $ E e
    let constr = exprConstraints_ ddexpr
    modify (++ constr)

-- Variable reference expression has the same type as the variable.
exprConstraints_ :: (?d::DatalogProgram) => DDExpr -> [Constraint]
exprConstraints_ de@(DDExpr ctx (E (EVar _ v))) =
    [TETypeOfExpr de === TETypeOfVar (getVar ?d ctx v)]

-- Boolean literal.
exprConstraints_ de@(DDExpr _ (E EBool{})) =
    [deIsBool de]

-- String literal.
exprConstraints_ de@(DDExpr _ (E EString{})) =
    [deIsString de]

-- Unsigned bitvector literal.
exprConstraints_ de@(DDExpr _ (E EBit{..})) =
    [TETypeOfExpr de === TEBit (IConst exprWidth)]

-- Signed bitvector literal.
exprConstraints_ de@(DDExpr _ (E ESigned{..})) =
    [TETypeOfExpr de === TESigned (IConst exprWidth)]

-- Integer expression with unspecified width can be a signed or unsigned
-- bitvector or a bigint.  All of these cases are captured by 'deIsInt'.
exprConstraints_ de@(DDExpr _ (E EInt{})) =
    [deIsInt de]

-- 32-bit FP literal.
exprConstraints_ de@(DDExpr _ (E EFloat{})) =
    [deIsFloat de]

-- 64-bit or unknown width FP literal.
exprConstraints_ de@(DDExpr _ (E EDouble{})) =
    [deIsFP de]

-- f(e1,...,en)
--
-- |de|=f_ret |a1|...|aj| and
-- |e1|=f_arg_1 |a1|...|aj| and ... and |en|=f_arg_n |a1|...|aj|, where |a1|...|aj| are fresh type variables
exprConstraints_ de@(DDExpr ctx (E e@EApply{..})) =
    (TETypeOfExpr de === typeToTExpr' ctx funcType) :
    (mapIdx (\(farg, earg) i -> TETypeOfExpr (DDExpr (CtxApply e ctx i) earg) === typeToTExpr' ctx (typ farg))
            $ zip funcArgs exprArgs)
    where
    Function{..} = getFunc ?d exprFunc

-- Struct field access: 'e1.f', where field 'f' is present in user-defined
-- structs in 'S1',...,'Sn'.
--
-- 'is_S1 |e1| and |e| = |e1.f|
--  or .. or
--  is_Sn |e1| and |e| = |e1.f|'
exprConstraints_ de@(DDExpr ctx (E e@EField{..})) =
    cdisj
    $ map (\TypeDef{tdefType = Just t,..} ->
            deIsStruct_ estruct tdefName : [PEq (TETypeOfExpr de) (typeToTExpr' ctx' (typ $ structGetField t exprField))])
      candidates
    where
    ctx' = CtxField e ctx
    estruct = DDExpr ctx' exprStruct
    -- All structs that have the field.
    candidates = filter (\tdef -> isJust $ structLookupField (fromJust $ tdefType tdef) exprField)
               $ filter (isJust . tdefType)
               $ M.elems
               $ progTypedefs ?d

-- Tuple field access: 'e.N'.
--
-- 'tuple_has_field_<i+1> |e1| and |e|=tupleFieldi |e1|'
exprConstraints_ de@(DDExpr ctx (E e@ETupField{..})) =
    [TETypeOfExpr etuple === TETuple Nothing (M.singleton exprTupField $ TETypeOfExpr de)]
    where
    etuple = DDExpr (CtxTupField e ctx) exprTuple

-- Struct expression: 'Cons{.f1=e1,..,.fn=en}', where 'Cons' is a constructor of
-- type 'MyStruct'
--
-- 'is_MyStruct |de| and |e1|=MyStruct_f1 |e| and ... and |en| = MyStruct_fn |e|'
exprConstraints_ de@(DDExpr ctx (E e@EStruct{..})) =
    deIsStruct de tdefName :
    (map (\(arg, efield) -> TETypeOfExpr (DDExpr (CtxStruct e ctx (name arg)) efield) === typeToTExpr' ctx (typ arg))
         $ zip consArgs $ map snd exprStructFields)
    where
    Constructor{..} = getConstructor ?d exprConstructor
    TypeDef{..} = consType ?d exprConstructor

-- Tuple expression '(e1,..,en)'.
--
-- '|e| = (|e1|,...,|en|)'.
exprConstraints_ de@(DDExpr ctx (E e@ETuple{..})) =
    [TETypeOfExpr de === TETuple (Just $ length exprTupleFields) (M.fromList $ mapIdx (\e' i -> (i, TETypeOfExpr (DDExpr (CtxTuple e ctx i) e'))) exprTupleFields)]

-- Bit slice 'e1[h:l]'
--
-- 'is_Bit(|e1|) and |e|=Bit (h-l+1)'
--
-- TODO: additional constraint 'bitWidth |e1| >= h-l+1 and'
-- should be enforced outside of the type inference engine.
exprConstraints_ de@(DDExpr ctx (E e@ESlice{..})) =
    [ TETypeOfExpr de === TEBit (IConst w)
    , deIsBit ebits]
    where
    ebits = DDExpr (CtxSlice e ctx) exprOp
    w = exprH - exprL + 1

-- Match 'match(e0) {e1->g1, .., en->gn}'
--
-- '|e0| =|e1|=..=|en| and |g1|=...=|gn|=|e|'
exprConstraints_ de@(DDExpr ctx (E e@EMatch{..})) =
    concat $ mapIdx (\(ei, gi) i ->
                      [ TETypeOfExpr mexpr === TETypeOfExpr (DDExpr (CtxMatchPat e ctx i) ei)
                      , TETypeOfExpr de === TETypeOfExpr (DDExpr (CtxMatchVal e ctx i) gi) ])
                    exprCases
    where
    mexpr = DDExpr (CtxMatchExpr e ctx) exprMatchExpr

-- Variable declaration: 'var v'
--
-- '|e|=|var v|'
exprConstraints_ de@(DDExpr ctx (E e@EVarDecl{})) =
    [TETypeOfExpr de === TETypeOfVar (ExprVar ctx e)]

-- Sequence 'e1;e2'
--
-- '|e|=|e2|'
exprConstraints_ de@(DDExpr ctx (E e@ESeq{..})) =
    [TETypeOfExpr de === TETypeOfExpr (DDExpr (CtxSeq2 e ctx) exprRight) ]

--- 'if (e1){e2}else{e3}'
--
-- '|e1|=Bool and |e2|=|e3|'
exprConstraints_ de@(DDExpr ctx (E e@EITE{..})) =
    [ TETypeOfExpr de === TEBool
    , TETypeOfExpr (DDExpr (CtxITEThen e ctx) exprThen) === TETypeOfExpr (DDExpr (CtxITEElse e ctx) exprElse) ]

-- 'for (v in e1) {e2}'
--
-- 'is_iterable |e1| and |var v|=iterator_type |e1| and |e| = ()'
exprConstraints_ de@(DDExpr ctx (E e@EFor{..})) =
    [ deIsIterable (DDExpr (CtxForIter e ctx) exprIter) (TETypeOfVar $ ForVar ctx e)
    , TETypeOfExpr de === TETuple (Just 0) M.empty ]

-- Assignment: 'e1=e2'.
--
-- '|e|=Tuple0 and |e1|=|e2|'.
exprConstraints_ de@(DDExpr ctx (E e@ESet{..})) =
    [ TETypeOfExpr de === TETuple (Just 0) M.empty
    , TETypeOfExpr (DDExpr (CtxSetL e ctx) exprLVal) === TETypeOfExpr (DDExpr (CtxSetR e ctx) exprRVal) ]

-- 'return e1'
--
-- '|e1|=retType'
exprConstraints_ (DDExpr ctx (E e@EReturn{..})) =
    [TETypeOfExpr (DDExpr (CtxReturn e ctx) exprRetVal) === typeToTExpr funcType]
    where
    Just Function{..} = ctxInFunc ctx

-- Binary operator 'e1 op e2', where `op` is one of '==, !=, <, >, <=, >='.
--
-- '|e1| = |e2| ans |e| = Bool'
exprConstraints_ de@(DDExpr ctx (E e@EBinOp{..})) | elem exprBOp [Eq, Neq, Lt, Lte, Gt, Gte] =
    [ TETypeOfExpr l === TETypeOfExpr r
    , TETypeOfExpr de === TEBool ]

-- Binary operator 'e1 op e2', where `op` is one of '&&, ||, =>'.
--
-- '|e1| = |e2| = Bool and |e| == Bool'
                                                | elem exprBOp [And, Or, Impl] =
    [ TETypeOfExpr l === TETypeOfExpr (DDExpr (CtxBinOpR e ctx) exprRight)
    , TETypeOfExpr l === TEBool
    , TETypeOfExpr de === TEBool ]

-- Binary operator 'e1 op e2', where `op` is one of '+, -, *. /'.
--
-- '|e| = |e1| = |e2| and is_num|e1|'
                                                | elem exprBOp [Plus, Minus, Times, Div] =
    [ TETypeOfExpr l === TETypeOfExpr r
    , TETypeOfExpr de === TETypeOfExpr l
    , deIsNum $ DDExpr (CtxBinOpL e ctx) exprLeft ]

-- Binary operator 'e1 % e2'.
--
-- '|e1| = |e2| and is_int(|e1|) and |e| = |e1|'
                                                | exprBOp == Mod =
    [ TETypeOfExpr l === TETypeOfExpr r
    , deIsInt l
    , TETypeOfExpr de === TETypeOfExpr l ]

-- Binary operator 'e1 op e2', where `op` is one of '<<, >>'.
--
-- 'is_int |e1| and is_int |e2| and |e| = |e1|'
                                                | elem exprBOp [Plus, Minus] =
    [ deIsInt l
    , deIsInt r
    , TETypeOfExpr de === TETypeOfExpr l ]

-- Binary operator 'e1 op e2', where `op` is one of '|,&,^'.
--
-- '|e| = |e1| = |e2| and is_bits|e1|'
                                                | elem exprBOp [BOr, BAnd, BXor] =
    [ TETypeOfExpr l === TETypeOfExpr r
    , TETypeOfExpr de === TETypeOfExpr l
    , deIsBits l ]

-- Binary operator 'e1 ++ e2'.
--
-- is_String(|e1|) and is_String(|e2|) or is_Bit |e1| and is_Bit |e2|
                                                | exprBOp == Concat =
    [CDisj [ [deIsString_ l, deIsString_ r, deIsString_ de]
           , [deIsBit_ l, deIsBit_ r, PEq (TETypeOfExpr de) (TEBit (IPlus (IWidthOfExpr l) (IWidthOfExpr r)))] ]]
                                                | otherwise =
    error $ "exprConstraints_: unknown binary operator " ++ show exprBOp
    where
    l = DDExpr (CtxBinOpL e ctx) exprLeft
    r = DDExpr (CtxBinOpR e ctx) exprRight

-- Boolean negation 'not e1'.
--
-- '|e| = Bool and |e1| = Bool'
exprConstraints_ de@(DDExpr ctx (E e@EUnOp{..}))  | exprUOp == Not =
    [ deIsBool op
    , deIsBool de ]

-- Bit-wise negation '~e1'.
--
-- 'is_bits |e1| and |e| = |e1|'
                                                | exprUOp == BNeg =
    [ deIsBits op
    , TETypeOfExpr de === TETypeOfExpr op ]

                                                | exprUOp == UMinus =
-- Unary minus '-e1'.
--
-- 'is_num |e1| and |e| = |e1|'
--
    [ deIsNum op
    , TETypeOfExpr de === TETypeOfExpr op ]
                                                | otherwise =
    error $ "exprConstraints_: unknown unary operator " ++ show exprUOp
    where
    op = DDExpr (CtxUnOp e ctx) exprOp

-- Variable binding 'v @ e1'.
--
-- '|e|=|e1|=|var v|'
exprConstraints_ de@(DDExpr ctx (E e@EBinding{..})) =
    [ TETypeOfVar (BindingVar ctx e) === TETypeOfExpr de
    , TETypeOfExpr de === TETypeOfExpr (DDExpr (CtxBinding e ctx) exprPattern) ]

-- Explicit type annotation 'e1: t'
--
-- '|e|=|e1|=t'.
exprConstraints_ de@(DDExpr ctx (E e@ETyped{..})) =
    [ typeToTExpr exprTSpec === TETypeOfExpr de
    , TETypeOfExpr de === TETypeOfExpr (DDExpr (CtxTyped e ctx) exprExpr) ]

-- &-pattern: '&e1'
--
-- 'is_sharef_ref |e| and |e1|=ref_deref |e|'
exprConstraints_ de@(DDExpr ctx (E e@ERef{..})) =
    deIsSharedRef de (TETypeOfExpr eref)
    where
    eref = DDExpr (CtxRef e ctx) exprPattern

-- Type coercion: 'e1 as t'
--
-- '|e| = t'.  Other constraints on valid source/destination pairs are enforced
-- outside of the type inference engine.
exprConstraints_ de@(DDExpr _ (E EAs{..})) =
    [TETypeOfExpr de === typeToTExpr exprTSpec]

-- 'break', 'continue', '_' expressions are happy to take any type required by
-- their context and so generate no type constraints.
exprConstraints_ (DDExpr _ (E EBreak{})) = []
exprConstraints_ (DDExpr _ (E EContinue{})) = []
exprConstraints_ (DDExpr _ (E EPHolder{})) = []
