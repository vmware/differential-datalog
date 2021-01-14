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

{-|
Module: Unification
Description: Unification-based type inference solver.
-}

{-# LANGUAGE FlexibleContexts, RecordWildCards, TupleSections, ImplicitParams, RankNTypes #-}

module Language.DifferentialDatalog.Unification(
    Typing,
    DDExpr(..),
    PredicateExplanation(..),
    ExplanationKind(..),
    Constraint(..),
    constraintsShow,
    IntVar(..),
    IExpr(..),
    TypeVar(..),
    tvObject,
    TExpr(..),
    teToType,
    teDeref,
    teIsConstant,
    teExpandAliases,
    typeToTExpr,
    Predicate(..),
    solve)
where

import Control.Monad.Except
import Data.List
import qualified Data.Map as M
import Data.Maybe
-- import Debug.Trace

import Language.DifferentialDatalog.Attribute
import Language.DifferentialDatalog.Error
import Language.DifferentialDatalog.Name
import Language.DifferentialDatalog.NS
import Language.DifferentialDatalog.Pos
import Language.DifferentialDatalog.Syntax
import Language.DifferentialDatalog.Type
import Language.DifferentialDatalog.Util
import Language.DifferentialDatalog.Var

-- The unification algorithm may not converge on resursive datatypes.  This
-- constant bounds maximum recursion depth.
--
-- Example: typedef S<x> = Either<T<x>, x>
--          typedef T<x> = Either<S<x>, x>
--
-- 'unify S<'a> T<'a>' -->
-- 'unify Either<T<'a>, 'a> Either<S<'a>, 'a>' -->
-- 'unify T<'a> S<'a>' etc. (infinite recursion)
--
-- The reason is that the unification solver fails to recognize that 'S' and 'T'
-- are really the same type.  A proper way to get around this is to extend the
-- type checking algorithm to identify such congruences (probably using
-- automata equivalence checking).
mAX_UNIFICATION_DEPTH :: Int
mAX_UNIFICATION_DEPTH = 10

type Typing = M.Map TypeVar Type

-- Uniquely identifies an expression in a DDlog program.
data DDExpr = DDExpr {ddexprCtx::ECtx, ddexprExpr::Expr} deriving (Eq, Ord)

instance Show DDExpr where
    show e = show $ ddexprExpr e

instance WithPos DDExpr where
    pos e = pos $ ddexprExpr e
    atPos _ _ = error "DDExpr.atPos: not impleemnted"

-- An integer constraint can encode that two expressions have the same bit-width
-- or the same mutability.
data IntEqKind = WidthEq | MutEq
                 deriving (Eq, Ord)

-- A type constraint is an atomic predicate, that requires two type expressions
-- to be equivalent, a width constraint, that requires two integer expressions
-- representing bit widths to be equivalent, or a lazy constraint.  We use a
-- unification-based solver to solve type constraints, which does not handle
-- arbitrary Boolean combinations of constraints, hence this restricted structure.
data Constraint = CPredicate {cPred::Predicate}
                  -- Integer constraint of the form 'expr1 == expr2', where
                  -- 'expr1' and 'expr2' are integer expressions.
                | CIntEq {cLHS::IExpr, cRHS::IExpr, cIntEqKind::IntEqKind, cIntExplanation::IntExplanation}
                  -- A lazy constraint gets activated once the type of a
                  -- specified "trigger" expression ('cExpr') is fully or partially resolved.
                  -- It is used to represent disjunctive constraints, where the set
                  -- of predicates depends on the type of the trigger.  E.g.,
                  -- the type of 'x.f' depends on the type of 'x', e.g., '(x:
                  -- Struct1 && x.f is bool || x:Struct2 && x.f: string ||
                  -- ...)'.  This is not easy to do with a unification solver,
                  -- so we let the constraint lie dormant until the solver can
                  -- rule out all but one disjunct.
                  -- 'cExpr' - trigger expression; does not change after the
                  -- constraint is created.
                  -- 'cType' - type expression that represents our current
                  -- knowledge of the type of 'cExpr'.  Gets refined as the
                  -- solver performs substitutions.
                  -- 'cExpand' - once 'cType' is concretized, this function is invoked
                  -- to generate a set of constraints that this lazy constraint resolves
                  -- into.  The function may return
                  --   * 'Nothing', meaning that the trigger type has not yet been
                  --     sufficiently concretized to resolve the constraint,
                  --   * An error indicating type conflict, or
                  --   * A list of constraints that the lazy constraint resolves
                  --     into.
                  -- 'cDefault' - optional default set of constraints to be used
                  -- if the program does not contain enough information to resolve
                  -- the type of the trigger expression, e.g., an integer literal
                  -- can be interpreted as 'u64' by default.
                  -- 'cExplanation' - description of the constraint for
                  -- debugging purposes.
                | CLazy { cType::TExpr
                        , cExpand::(forall me . (MonadError String me) => TExpr -> me (Maybe [Constraint]))
                        , cDefault::Maybe [Constraint]
                        , cExpr::DDExpr
                        , cExplanation::String
                        }

instance Show Constraint where
    show (CPredicate p)               = show p
    show (CIntEq ie1 ie2 WidthEq _)   = show ie1 ++ " = " ++ show ie2
    show (CIntEq ie1 ie2 MutEq _)     = ieShowMut ie1 ++ " = " ++ ieShowMut ie2
    show (CLazy te _ _ _ expl)        = expl ++ " [partial type: " ++ show te ++ "]"

constraintsShow :: [Constraint] -> String
constraintsShow cs = intercalate "\n" $ map show cs

-- A constraint is in the solved form if it has the following shape:
-- 'tv = te' or 'te = tv',
-- where 'tv' is a type variable and 'te' is an arbitrary type expression.
constraintIsSolved :: Constraint -> Bool
constraintIsSolved (CPredicate (PEq TETVar{} _ _)) = True
constraintIsSolved (CPredicate (PEq _ TETVar{} _)) = True
constraintIsSolved (CIntEq IVar{} _ _ _)           = True
constraintIsSolved (CIntEq _ IVar{} _ _)           = True
constraintIsSolved _                               = False

-- Normalization rotates a solved constraint so that the type variable
-- is on the left: 'te = tv' is transformed into 'tv = te'.
constraintNormalize :: Constraint -> Constraint
constraintNormalize (CPredicate (PEq te tv@TETVar{} e)) = CPredicate $ PEq tv te e
constraintNormalize (CIntEq ie iv@IVar{} k e)           = CIntEq iv ie k e
constraintNormalize c                                   = c

constraintIsLazy :: Constraint -> Bool
constraintIsLazy CLazy{} = True
constraintIsLazy _       = False

-- Generate a width constraint in canonical form: all constants are on one side
-- of the constraint, variables that appear on both sides are cancelled out.
cIntEq :: IExpr -> IExpr -> IntEqKind -> IntExplanation -> Maybe Constraint
cIntEq ie1 ie2 kind e = if ie1' == ie2' then Nothing else Just (CIntEq ie1' ie2' kind e)
    where
    ievars1 = ieVars ie1
    ievars2 = ieVars ie2
    ieconst1 = ieConst ie1
    ieconst2 = ieConst ie2
    -- Group constants in one side of the equation.
    cnst = ieconst2 - ieconst1
    -- Cancel out variables that appear on both sides of the equation.
    ie1' = ieSum $ (map IVar $ ievars1 \\ ievars2) ++ if cnst < 0 then [IConst (-cnst)] else []
    ie2' = ieSum $ (map IVar $ ievars2 \\ ievars1) ++ if cnst > 0 then [IConst cnst] else []

cwidthReportConflict :: (?d::DatalogProgram, MonadError String me) => Constraint -> me a
cwidthReportConflict c@(CIntEq _ _ WidthEq e) = err ?d (pos e) $ "Unsatisfiable bit-width constraint '" ++ show c ++ "' in\n" ++ show e
cwidthReportConflict c@(CIntEq _ _ MutEq e) = err ?d (pos e) $ "Conflicting argument mutability attributes '" ++ show c ++ "' in\n" ++ show e
cwidthReportConflict c = error $ "cwidthReportConflict " ++ show c

-- Integer expressions represent widths of bit and signed types.
data IExpr = -- Integer constant.
             IConst Int
             -- Integer variable.
           | IVar IntVar
             -- Sum of two widths.
           | IPlus IExpr IExpr
           deriving (Eq, Ord)

instance Show IExpr where
    show (IConst i)         = show i
    show (IVar v)           = show v
    show (IPlus ie1 ie2)    = show ie1 ++ " + " ++ show ie2

-- Show integer expression as a mutability attribute.
ieShowMut :: IExpr -> String
ieShowMut (IConst 0) = "immutable"
ieShowMut (IConst 1) = "mutable"
ieShowMut (IVar v)   = show v
ieShowMut ie         = error $ "Invalid mutability expression " ++ show ie

-- Integer expression is a constant (i.e., does not contain any variables).
ieIsConstant :: IExpr -> Bool
ieIsConstant IConst{}      = True
ieIsConstant IVar{}        = False
ieIsConstant (IPlus e1 e2) = ieIsConstant e1 && ieIsConstant e2

-- List variables that occur in a width expression.
-- Each variable occurs in the result as many times as it occurs in the
-- expression.
ieVars :: IExpr -> [IntVar]
ieVars IConst{}         = []
ieVars (IVar v)         = [v]
ieVars (IPlus ie1 ie2)  = ieVars ie1 ++ ieVars ie2

-- Sum-up all integer constants in a width expression.
ieConst :: IExpr -> Int
ieConst (IConst w)      = w
ieConst IVar{}          = 0
ieConst (IPlus ie1 ie2) = ieConst ie1 + ieConst ie2

ieSum :: [IExpr] -> IExpr
ieSum es = ieNormalize $ ieSum' es

ieSum' :: [IExpr] -> IExpr
ieSum' []        = IConst 0
ieSum' [ie]      = ie
ieSum' (ie:ies)  = IPlus ie $ ieSum' ies

ieNormalize :: IExpr -> IExpr
ieNormalize ie = ieSum' $ map IVar vs ++ cs
    where
    vs = ieVars ie
    c = ieConst ie
    cs = if null vs || c /= 0
         then [IConst c]
         else []

-- Integer variables used in constraint encoding.
data IntVar = -- Integer variable equal to the width of a DDExpr.
                WidthOfExpr {ivarId :: Int, ivarExpr :: DDExpr}
                -- Integer variable that represents mutability of i'th argument
                -- of an expression of type 'TFunction', e.g., given expression 'f(x)',
                -- 'MutabilityOfArg f 0' represents the mutability attribute of the first
                -- argument of function 'f'.
              | MutabilityOfArg {ivarId :: Int, ivarExpr :: DDExpr, ivarArg :: Int}

instance Eq IntVar where
    (==) ivar1 ivar2 = ivarId ivar1 == ivarId ivar2

instance Ord IntVar where
    compare ivar1 ivar2 = ivarId ivar1 `compare` ivarId ivar2

instance Show IntVar where
    show (MutabilityOfArg _ _ _) = "" -- This makes output noisy and difficult to parse.
                                      -- "mutability of argument " ++ show i ++ " of function " ++ show e
    show (WidthOfExpr _ e) = "width_of(" ++ show e ++ ")"

-- Type variables used in constraint encoding.
-- The goal of type inference is to find a satisfying assignment to all
-- type and integer variables.
data TypeVar = -- Type of a DDlog expression.
               TVarTypeOfExpr {tvarId::Int, tvarExpr::DDExpr}
               -- Type of a DDlog variable.
             | TVarTypeOfVar {tvarId::Int, tvarVar::Var}
               -- We sometimes need to introduce extra type variables to model
               -- unknown type arguments of function calls.  We could use
               -- arbitrary auto-generated names for them, but to avoid a state
               -- monad and enable meaningful error message, we use the
               -- expression where the type argument occurs and its name
               -- to uniquely identify the variable.
             | TVarAux {tvarId::Int, tvarExpr::DDExpr, tvarName::String}

instance Eq TypeVar where
    (==) tv1 tv2 = tvarId tv1 == tvarId tv2

instance Ord TypeVar where
    compare tv1 tv2 = compare (tvarId tv1) (tvarId tv2)

instance WithPos TypeVar where
    pos (TVarTypeOfExpr _ de) = pos de
    pos (TVarTypeOfVar _ v)   = pos v
    pos (TVarAux _ de _)      = pos de
    atPos _ _ = error "TypeVar.atPos: not implemented"

-- Returns a string describing the object whose type this variable refers to.
tvObject :: TypeVar -> String
tvObject (TVarTypeOfExpr _ de) = "expression '"   ++ show de ++ "'" -- TODO: abbreviate long expressions
tvObject (TVarTypeOfVar _ v)   = "variable '"     ++ name v ++ "'"
tvObject (TVarAux _ _ v)       = "type argument " ++ name v -- TODO: type argument v of function/type ...

-- Return existing shortcut for 'tv' or generate a new one.
tvName :: TypeVar -> String
tvName tv = "a" ++ show (tvarId tv)

-- Type expression: an expression that returns a value of 'Type' sort.
data TExpr = TEBool
           | TEString
           | TEBigInt
           | TEBit IExpr
           | TESigned IExpr
           | TEFloat
           | TEDouble
             -- Tuple type whose size and field types may be only partially known.
           | TETuple [TExpr]
             -- User-defined type.
           | TEUser String [TExpr]
             -- Extern type.
           | TEExtern String [TExpr]
             -- In a function context only, a type argument of the function (e.g., 'A).
           | TETArg String
             -- Type variable.
           | TETVar TypeVar
           | -- Function type.
             -- We model the 'mut' attribute as an integer, with '0' for
             -- read-only and '1' for read-write argument.  If by the time type
             -- inference is finished, the exact value has not been established,
             -- we conservatively assume read-only.
             TEFunc [(IExpr, TExpr)] TExpr
           deriving (Eq, Ord)

instance Show TExpr where
    show TEBool                 = "bool"
    show TEString               = "string"
    show TEBigInt               = "bigint"
    show (TEBit w)              = "bit<" ++ show w ++ ">"
    show (TESigned w)           = "signed<" ++ show w ++ ">"
    show TEFloat                = "float"
    show TEDouble               = "double"
    show (TETuple as)           = (++ ")") $ ("(" ++) $ intercalate "," $ map show as
    show (TEUser n as)          = (++ ">") $ ((n ++ "<") ++) $ intercalate "," $ map show as
    show (TEExtern n as)        = (++ ">") $ ((n ++ "<") ++) $ intercalate "," $ map show as
    show (TETArg n)             = "'" ++ n
    show (TETVar tv)            = "'" ++ tvName tv
    show (TEFunc as r)          = "function(" ++ (intercalate "," args) ++ "):" ++ show r
                                  where
                                  args = map (\(m, a) -> showMut m ++ " " ++ show a) as
                                  -- Format IExpr used to represent the mutability attribute.
                                  showMut :: IExpr -> String
                                  showMut (IConst 0)  = ""
                                  showMut (IConst 1)  = "mut"
                                  showMut ie          = show ie

-- Type expression is a constant, i.e., does not contain any type or width variables,
-- other than, possibly, in 'mut' attributes.
teIsConstant :: TExpr -> Bool
teIsConstant TEBool             = True
teIsConstant TEString           = True
teIsConstant TEBigInt           = True
teIsConstant (TEBit w)          = ieIsConstant w
teIsConstant (TESigned w)       = ieIsConstant w
teIsConstant TEFloat            = True
teIsConstant TEDouble           = True
teIsConstant (TETuple as)       = all teIsConstant as
teIsConstant (TEUser _ as)      = all teIsConstant as
teIsConstant (TEExtern _ as)    = all teIsConstant as
teIsConstant TETArg{}           = True
teIsConstant TETVar{}           = False
teIsConstant (TEFunc as r)      = -- Don't check 'mut' attributes.
                                  (all (teIsConstant . snd) as) && teIsConstant r

-- All type variables in a type expression.
teTypeVars :: TExpr -> [TypeVar]
teTypeVars TEBool             = []
teTypeVars TEString           = []
teTypeVars TEBigInt           = []
teTypeVars TEBit{}            = []
teTypeVars TESigned{}         = []
teTypeVars TEFloat            = []
teTypeVars TEDouble           = []
teTypeVars (TETuple as)       = nub $ concatMap teTypeVars as
teTypeVars (TEUser _ as)      = nub $ concatMap teTypeVars as
teTypeVars (TEExtern _ as)    = nub $ concatMap teTypeVars as
teTypeVars TETArg{}           = []
teTypeVars (TETVar v)         = [v]
teTypeVars (TEFunc as r)      = nub $ (concatMap (teTypeVars . snd) as) ++ teTypeVars r

-- Convert constant type expression to a DDlog type.
teToType :: TExpr -> Type
teToType TEBool                 = tBool
teToType TEString               = tString
teToType TEBigInt               = tInt
teToType (TEBit (IConst w))     = tBit w
teToType (TESigned (IConst w))  = tSigned w
teToType TEFloat                = tFloat
teToType TEDouble               = tDouble
teToType (TETuple as)           = tTuple $ map teToType as
teToType (TEUser n as)          = tUser n $ map teToType as
teToType (TEExtern n as)        = tOpaque n $ map teToType as
teToType (TETArg n)             = tVar n
teToType (TEFunc as r)          = tFunction (map (\(m, a) ->
                                                   let m' = case m of
                                                                 IConst 1 -> True
                                                                 IConst 0 -> False
                                                                 IVar{}   -> False -- 'mut' attribute unknown - assume it is read-only.
                                                                 _ -> error $ "teToType: unexpected 'mut' value: " ++ show m
                                                   in ArgType nopos m' (teToType a))
                                                 as) $ teToType r
teToType te                     = error $ "Unification.teToType: non-constant type expression " ++ show te

-- | Expand type aliases (similar to Type.typ'').
teExpandAliases :: (?d::DatalogProgram) => TExpr -> TExpr
teExpandAliases t'@(TEUser n as) =
    case tdefType tdef of
         (Just TStruct{}) -> t'
         Nothing -> TEExtern n as
         Just t  -> teExpandAliases $ teSubstTypeArgs (M.fromList $ zip (tdefArgs tdef) as) $ typeToTExpr t
    where tdef = getType ?d n
teExpandAliases t = t

teSubstTypeArgs :: M.Map String TExpr -> TExpr -> TExpr
teSubstTypeArgs subst (TEUser n as)    = TEUser n (map (teSubstTypeArgs subst) as)
teSubstTypeArgs subst (TEExtern n as)  = TEExtern n (map (teSubstTypeArgs subst) as)
teSubstTypeArgs subst (TETuple ts)     = TETuple $ map (teSubstTypeArgs subst)  ts
teSubstTypeArgs subst (TETArg n)       = subst M.! n
teSubstTypeArgs subst (TEFunc as r)    = TEFunc (map (mapSnd (teSubstTypeArgs subst)) as) (teSubstTypeArgs subst r)
teSubstTypeArgs _     t                = t

-- Convert type to a type expression, replacing type arguments ('A, 'B, ...)
-- with type constants 'TETArg "A", TETArg "B", ...'.  For example, when
-- generating type inference in the body of a function, we treat its type
-- arguments as constants.  Inferred types for variables and expressions inside
-- the body of the function may depend on these constants.
typeToTExpr :: (?d::DatalogProgram) => Type -> TExpr
typeToTExpr TBool{}         = TEBool
typeToTExpr TInt{}          = TEBigInt
typeToTExpr TString{}       = TEString
typeToTExpr TBit{..}        = TEBit $ IConst typeWidth
typeToTExpr TSigned{..}     = TESigned $ IConst typeWidth
typeToTExpr TFloat{}        = TEFloat
typeToTExpr TDouble{}       = TEDouble
typeToTExpr TTuple{..}      = TETuple $ map (typeToTExpr . typ) typeTupArgs
typeToTExpr TUser{..}       = TEUser typeName $ map typeToTExpr typeArgs
typeToTExpr TVar{..}        = TETArg tvarName
typeToTExpr TOpaque{..}     = TEExtern typeName $ map typeToTExpr typeArgs
typeToTExpr t@TStruct{}     = error $ "typeToTExpr: unexpected '" ++ show t ++ "'"
typeToTExpr TFunction{..}   = TEFunc (map (\atype -> (if atypeMut atype then IConst 1 else IConst 0, typeToTExpr $ typ atype)) typeFuncArgs)
                                     (typeToTExpr typeRetType)

-- Unwrap all layers of shared references.
teDeref :: (?d::DatalogProgram) => TExpr -> TExpr
teDeref te = teDeref' $ teExpandAliases te

teDeref' :: (?d::DatalogProgram) => TExpr -> TExpr
teDeref' (TEExtern n [t]) | elem n sref_types = teDeref t
    where
    sref_types = map name
                 $ filter (tdefGetSharedRefAttr ?d)
                 $ M.elems $ progTypedefs ?d
teDeref' t = t

-- Check if type expression is a constant and if not, return an explanation.
-- 'obj' is the object whose type 'te' describes.
teCheckConstant :: (?d::DatalogProgram, MonadError String me) => SolverState -> TypeVar -> TExpr -> me ()
teCheckConstant st tvar te | teIsConstant te' = return ()
                           | not $ null tvars =
    -- 'te' depends on an unresolved type variable 'tv'.  So instead of
    -- complaining that the type of 'te' is unknown, we try to be more specific
    -- and complain that the type of the entity 'tv' represents is unknown.
    err ?d (pos tv) $ "Cannot infer type of " ++ tvObject tv
                           | otherwise =
    -- 'te' is not a constant, but it does not depend on any type variables,
    -- hence it must have parts whose width is not yet resolved into a constant.
    err ?d (pos tvar) $ "Unable to infer complete type of " ++ tvObject tvar ++ ". Partially inferred type: " ++ show te' ++
                        "\nSolver state:\n" ++ show st
    where
    te' = teSubstituteAll st te
    tvars = teTypeVars te'
    tv : _ = tvars

-- Type congruence, e.g., '|e| = BigInt'.
data Predicate = PEq {
    -- Left-hand side of the equation.
    predLHS         ::  TExpr,
    -- Right-hand side.
    predRHS         ::  TExpr,
    -- Explanation of the predicate used in generating an error message
    -- when the predicate becomes a contradiction.
    predExplanation ::  PredicateExplanation
}

instance Eq Predicate where
    (==) (PEq l1 r1 _) (PEq l2 r2 _) = (l1, r1) == (l2, r2)

instance Ord Predicate where
    compare (PEq l1 r1 _) (PEq l2 r2 _) = compare (l1, r1) (l2, r2)

instance Show Predicate where
    show (PEq te1 te2 _) = show te1 ++ " = " ++ show te2

-- Explanation of a predicate contains metadata used to generate meaningful error
-- messages if the predicate becomes a contradiction.
--
-- When a predicate 'P1: t1 = t2' is initially created, 't1' is typically a type
-- variable that represents the type of a program variable or expression.
-- The meaning of the predicate is that the expected type of this variable or
-- expression is 't2'.  Thus the predicate's explanation is 'ExplanationTVar t1'.
--
-- The predicate can undergo two types of transformations: substitution and
-- unification.  After a substitution, the resulting predicate keeps its original
-- explanation, e.g., substitutions 't1 <- Struct1<a>' and 't2 <- Struct1<b>'
-- yield predicate 'P2: Struct1<a> = Struct1<b>' whose explanation is still
-- 'ExplanationTVar t1'.  After unification, we get a new predicate
-- 'P3: a=b' whose explanation is the "parent" predicate it was derived from:
-- 'ExplanationParent P2'.  Consider further substitutions 'a <- bool',
-- 'b <- (bool, string)', which yield an unsatisfiable predicate:
-- 'P3: bool = (bool, string)', whose explanation is still 'P2', except that we
-- also apply the substitution to the explanation, yielding
-- 'P2_2: Struct1<bool> = Struct1<(bool, string)>'.
--
-- This explanation carries enough information to generate the following error
-- messageL
--
-- ~~~~~~~~~
-- Type mistmatch:
-- expected type: (bool, string)
-- ectual type: bool
--
-- in
--
-- expected type: Struct1<(bool, string)>
-- actual type: Struct1<bool>
--
-- in
--
-- <expression or variable that 't1' refers to>.
-- ~~~~~~~~~
data PredicateExplanation =
    ExplanationString Pos String |
    ExplanationParent Predicate |
    ExplanationTVar   TypeVar ExplanationKind

-- A predicate can represent either expected or actual type of a type variable.
-- For example, in expression 'x: bool', variable 'x' has expected type 'bool'.
-- If a different type is derived for 'x' by type inference, we complain
-- that 'x' has expected type 'bool', but its actual type is 'string'".
-- A literal "foo", on the other hand has actual type 'string'.  If a different
-- type is derived for it, we complain the expression "foo" of type 'string' is
-- used in a context that expects a 'bool'.
data ExplanationKind = Expected | Actual deriving (Eq)

instance WithPos PredicateExplanation where
    pos (ExplanationString p _)       = p
    pos (ExplanationParent p)         = pos (predExplanation p)
    pos (ExplanationTVar tv _)        = pos tv
    atPos _ _ = error "PredicateExplanation.atPos not implemented"

-- As an optimization, we do not apply substitutions to explanations until the
-- explanation actually needs to be output in case of a type error, at which
-- point we apply all substitutions in 'SolverState'.
explanationSubstituteAll :: SolverState -> PredicateExplanation -> PredicateExplanation
-- Do not apply substitution to the original explanation, which points to the
-- variable or expression the predicate constrains.
explanationSubstituteAll _   e@ExplanationTVar{}  = e
explanationSubstituteAll _   e@ExplanationString{}  = e
explanationSubstituteAll st (ExplanationParent p) =
    ExplanationParent
    $ (\p' -> p'{predExplanation = explanationSubstituteAll st (predExplanation p')})
    $ fromJust $ predSubstituteAll st p

instance Show PredicateExplanation where
    show (ExplanationString _ s)             = s
    show (ExplanationTVar tv _)              = tvObject tv
    show (ExplanationParent (PEq te1 te2 e)) =
        if explanationKind e == Expected
        then "expected type: " ++ show te2 ++ "\nactual type: " ++ show te1 ++ "\nin\n" ++ show e
        else "expected type: " ++ show te1 ++ "\nactual type: " ++ show te2 ++ "\nin\n" ++ show e

explanationKind :: PredicateExplanation -> ExplanationKind
explanationKind ExplanationString{}   = Expected
explanationKind (ExplanationTVar _ k) = k
explanationKind (ExplanationParent p) = explanationKind (predExplanation p)

explanationDepth :: PredicateExplanation -> Int
explanationDepth ExplanationString{} = 0
explanationDepth ExplanationTVar{} = 0
explanationDepth (ExplanationParent p) = explanationDepth (predExplanation p) + 1

-- Report type resolution conflict in an unsatisfiable predicate (where
-- left and right side cannot be unified).
predReportConflict :: (?d::DatalogProgram, MonadError String me) => SolverState -> Predicate -> me a
predReportConflict st (PEq te1 te2 explanation) =
    err ?d (pos explanation) $
        if explanationKind explanation == Expected
        then "Type mismatch:\nexpected type: " ++ show te2' ++ "\nactual type: " ++ show te1' ++ "\nin\n" ++ es
        else "Type mismatch:\nexpected type: " ++ show te1' ++ "\nactual type: " ++ show te2' ++ "\nin\n" ++ es
    where
    es = show $ explanationSubstituteAll st explanation
    te1' = teSubstituteAll st te1
    te2' = teSubstituteAll st te2

-- Explanation of a width constraint.  For now just the explanation of the
-- predicate it is derived from.
type IntExplanation = PredicateExplanation

{-

Notation:

TV - set of type variables.  A type variable is introduced for each
     subexpression and variable in the program; additional variables
     can be introduced to encode unknown type arguments of structs and
     functions.

IV - set of integer variables, which encode widths of signed and
     unsigned bitvector types.

TE - set of type expressions (TypeInference.TExpr).
IE - set of integer expressions (TypeInference.IExpr).

te, te1, te2 - type expressions from TE.
ie, ie1, ie2 - integer expressions from IV.

State of the algorithm:

TA: TV -> TE - partial type variable assignment computed so far.
    Assigned variables cannot appear in the RHS of an assignment
    or in any of the remaining constraints.

IA: IV -> IE - partial integer variable assignment computed so far.
    Assigned variables cannot appear in the RHS of an assignment
    or in any of the remaining constraints.

CS: 2^Constraint - Unresolved type constraints.

Pseudocode:

Type inference:
    while (CS is not empty) {
        For all constraints 'c in C', apply all recorded variable substitutions to 'c':
           - if the resulting constraint is a tautology, drop it.
           - if 'c' is in solved form 'c = (tv == te)', 'tv in TV', 'te in TE',
             remove 'c' from 'C' and record substitution 'tv -> te'.
           - If 'c' is any other predicate constraint,
             apply Unify to it.
           - If 'c' is a lazy constraint, expand it if possible.
        If no new constraints were discovered and the constraint set is still
        not empty:
           - Pick a lazy constraint with a default clause and replace it with
             the default clause
           - If no such constraint exists, FAIL
    }
    Check that a complete typing has been found: all assignments in 'TA' are
    constant expressions (no variables). Otherwise, FAIL: undespecified types.

The above algorithm uses the following subroutine:

Unify(c), where 'c = (te1 == te2)'
    1. Remove 'c' from 'C'.
    2. Recursively match 'te1' and 'te2' to produce a set of constraints of the form
      'tv = te', 'iv = ie' or a conflict of the form 'te3 == te4' where 'te3' and
      'te4' have different type constructors or 'ie3 == ie4' where 'ie3' and 'ie4' are
      two different integer constants.  A 'tv = te' constraint is also considered a
      conflict if 'te' depends on 'tv' (and is not identity 'tv').
    3. If conflict detected, FAIL: conflicting types.
    4. Otherwise, add newly derived equations to 'C'.
-}

data SolverState = SolverState {
    -- Partial typing computed so far.
    -- Assigned variables cannot appear in the RHS of an assignment
    -- or in any of the remaining constraints.
    solverPartialTyping :: M.Map TypeVar TExpr,

    -- IA: IV -> IE - partial integer variable assignment computed so far.
    -- Assigned variables cannot appear in the RHS of an assignment
    -- or in any of the remaining constraints.
    solverPartialInt :: M.Map IntVar IExpr,

    -- Unresolved type constraints.
    solverConstraints :: [Constraint]
}

emptySolverState :: SolverState
emptySolverState = SolverState {
    solverPartialTyping = M.empty,
    solverPartialInt = M.empty,
    solverConstraints = []
}

instance Show SolverState where
    show SolverState{..} = "Partial typing:\n" ++
                           (intercalate "\n" $ map (\(tv, te) -> tvName tv ++ " (" ++ tvObject tv ++ ") = " ++ show te) $ M.toList solverPartialTyping) ++
                           "\nPartial integer variable assignment:\n" ++
                           (intercalate "\n" $ map (\(iv, ie) -> "  " ++ show iv ++ " = " ++ show ie) $ M.toList solverPartialInt) ++
                           "\nConstraints:\n" ++
                           (constraintsShow solverConstraints)

removeConstraint :: Int -> SolverState -> SolverState
removeConstraint idx st = st{ solverConstraints = take idx cs ++ drop (idx+1) cs }
    where cs = solverConstraints st

addConstraints :: [Constraint] -> SolverState -> SolverState
addConstraints new_cs st = st{ solverConstraints = cs ++ new_cs}
    where cs = solverConstraints st


-- Solve a set of type constraint by finding a satisfying assignment to
-- type variables and width variables.
--
-- When 'full' is True, the function only succeeds if, after resolving all constraints,
-- a complete typing is obtained: i.e., all variable assignments are constant expressions
-- that don't depend on other variables.
solve :: (MonadError String me) => DatalogProgram -> [Constraint] -> Bool -> me Typing
solve d cs full = let ?d = d in solve' (emptySolverState {solverConstraints = cs}) full

solve' :: (?d::DatalogProgram, MonadError String me) => SolverState -> Bool -> me Typing
solve' st full | null (solverConstraints st) = {-trace ("solve': " ++ show st) $-} do
    when full
        $ mapM_ (\(tv, te) -> teCheckConstant st tv te) $ M.toList $ solverPartialTyping st
    return $ M.map (teToType . teSubstituteAll st) $ solverPartialTyping st
               | otherwise = {- trace ("solve': " ++ show st) $ -} do
    -- 1. Pick 'c in C' s.t. 'c = (tv == te)', 'tv in TV', 'te in TE'.
    --   - If such 'c' does not exist, pick any predicate constraint in
    --     'C' and apply Unification to it.
    --   - If there are only lazy and width constraints left in 'C',
    --     pick a disjunctive constraint with a default solution.
    --   - If such constraint does not exist, FAIL.
    --trace ("solve': constraints:\n" ++ constraintsShow (solverConstraints st)) $
    (new_st, progress_made) <- foldM (\(st', progress) c -> do
                                       (st'', progress') <- constraintSubstituteAll st' c
                                       return (st'', progress || progress'))
                               (st{ solverConstraints = [] }, False) $ solverConstraints st
    if not progress_made
    then case findIndex (\c -> constraintIsLazy c && isJust (cDefault c)) $ solverConstraints new_st of
              Just i -> do let st' = removeConstraint i new_st
                           let CLazy _ _ (Just def) _ _ = solverConstraints new_st !! i
                           let st'' = addConstraints def st'
                           solve' st'' full
              Nothing -> -- No substitutions or simplifications were made - report error.
                         case solverConstraints new_st !! 0 of
                              CLazy{..} -> err ?d (pos cExpr) $ "Failed to infer type of expression '" ++ show cExpr ++ "'"
                              c@CIntEq{} -> cwidthReportConflict c
                              c -> error $ "solve': unexpected constraint " ++ show c
    else -- Next iteration
         solve' new_st{solverConstraints = reverse $ solverConstraints new_st} full

-- Do _not_ normalize the constraint before unification, as swapping RHS and LHS
-- sides can confuse the explanation
unify :: (?d::DatalogProgram, MonadError String me) => SolverState -> Predicate -> me [Constraint]
unify st (PEq te1 te2 e) = unify' st (PEq (teExpandAliases te1) (teExpandAliases te2) e)

unify' :: (?d::DatalogProgram, MonadError String me) => SolverState -> Predicate -> me [Constraint]
{- Consider non-recursive cases first. -}

-- Cannot learn anything from a tautology.
unify' _  (PEq te1 te2 _) | te1 == te2 = return []
-- We found a type variable assignment, add it to constraint set,
-- unless it's a contradition.
unify' st p@(PEq (TETVar v) te _) | elem v $ teTypeVars te = predReportConflict st p
                                  | otherwise = return [CPredicate p]
unify' st p@(PEq te (TETVar v) _) | elem v $ teTypeVars te = predReportConflict st p
                                  | otherwise = return [CPredicate p]
unify' st p@(PEq (TETuple args1) (TETuple args2) _) = do
    when (length args1 /= length args2) $ predReportConflict st p
    concat <$> (mapM (\(te1, te2) -> unify st $ PEq te1 te2 (ExplanationParent p))
                $ zip args1 args2)

{- Check recursion depth before making recursive calls to unify. -}

unify' st p | explanationDepth (predExplanation p) >= mAX_UNIFICATION_DEPTH = predReportConflict st p
unify' st p@(PEq (TEUser n1 args1) (TEUser n2 args2) _) | n1 == n2 =
    concat <$> (mapM (\(a1, a2) -> unify st $ PEq a1 a2 (ExplanationParent p)) $ zip args1 args2)
unify' st p@(PEq (TEFunc as1 r1) (TEFunc as2 r2) _) | length as1 /= length as2 = predReportConflict st p
                                                    | otherwise = do
    arg_constraints <- concat <$> (mapM (\((m1, a1), (m2, a2)) -> (maybeToList (cIntEq m1 m2 MutEq (ExplanationParent p)) ++) <$>
                                                                  unify st (PEq a1 a2 (ExplanationParent p)))
                                   $ zip as1 as2)
    ret_constraints <- unify st $ PEq r1 r2 (ExplanationParent p)
    return $ arg_constraints ++ ret_constraints
unify' st p@(PEq (TEExtern n1 args1) (TEExtern n2 args2) _) | n1 /= n2 = predReportConflict st p
                                                            | otherwise =
    concat <$> (mapM (\(a1, a2) -> unify st $ PEq a1 a2 (ExplanationParent p)) $ zip args1 args2)
unify' _  p@(PEq (TEBit ie1) (TEBit ie2) _) = return $ maybeToList $ cIntEq ie1 ie2 WidthEq (ExplanationParent p)
unify' _  p@(PEq (TESigned ie1) (TESigned ie2) _) = return $ maybeToList $ cIntEq ie1 ie2 WidthEq (ExplanationParent p)
unify' st p@(PEq te1 te2 _) | (te1', te2') /= (te1, te2) = unify st $ PEq te1 te2 (ExplanationParent p)
                            | otherwise = predReportConflict st p
    where
    te1' = teExpandAliases te1
    te2' = teExpandAliases te2

substitute :: (?d::DatalogProgram) => TypeVar -> TExpr -> SolverState -> SolverState
substitute v te st =
    st {solverPartialTyping = typing'}
    where
    te' = teExpandAliases te
    typing' = M.insert v te' $ solverPartialTyping st

teSubstituteAll :: SolverState -> TExpr -> TExpr
teSubstituteAll st (TETuple args) = TETuple $ map (teSubstituteAll st) args
teSubstituteAll st (TEUser n args) = TEUser n $ map (teSubstituteAll st) args
teSubstituteAll st (TEFunc args r) = TEFunc (map (\(m, a) -> (ieSubstituteAll st m, teSubstituteAll st a)) args) (teSubstituteAll st r)
teSubstituteAll st (TEExtern n args) = TEExtern n $ map (teSubstituteAll st) args
teSubstituteAll st te@(TETVar v) =
    case M.lookup v (solverPartialTyping st) of
         Nothing  -> te
         Just te' -> teSubstituteAll st te'
teSubstituteAll st (TEBit ie) = TEBit $ ieSubstituteAll st ie
teSubstituteAll st (TESigned ie) = TESigned $ ieSubstituteAll st ie
teSubstituteAll _  te = te

-- Substitute type variables in constraint with type expressions from the current partial typing.
constraintSubstituteAll :: (?d::DatalogProgram, MonadError String me) => SolverState -> Constraint -> me (SolverState, Bool)
constraintSubstituteAll st (CPredicate p) = do
    case CPredicate <$> predSubstituteAll st p of
         Nothing -> return (st, True {- Constraint eliminated. -})
         Just c | constraintIsSolved c ->
             -- Solved? Add substitution.
             case constraintNormalize c of
                  CPredicate (PEq (TETVar v) te _) ->
                      return (substitute v te st, True {- Substitution made -})
                  c' -> error $ "Unification.constraintSubstituteAll CPredicate: unexpected constraint " ++ show c'
                | otherwise -> do
             -- Otherwise, apply unification.
             cs' <- unify st $ cPred c
             return (st{ solverConstraints = cs' ++ solverConstraints st }, True {- Constraint unified. -})
constraintSubstituteAll st (CIntEq ie1 ie2 k e) = do
    let ie1' = ieSubstituteAll st ie1
    let ie2' = ieSubstituteAll st ie2
    case cIntEq ie1' ie2' k e of
         Nothing -> return (st, True)
         Just c | constraintIsSolved c ->
             -- Solved? Add substitution.
             case constraintNormalize c of
                  CIntEq (IVar v) ie2'' _ _ ->
                       return (substituteIVar v ie2'' st, True {- Substitution made -} )
                  c' -> error $ "Unification.constraintSubstituteAll CIntEq: unexpected constraint " ++ show c'
         Just c@(CIntEq ie1'' ie2'' k'' _) ->
             -- Otherwise, add constraint.
             return (st { solverConstraints = c : solverConstraints st }, (ie1, ie2, k) /= (ie1'', ie2'', k''))
         Just c -> error $ "Unification.constraintSubstituteAll CIntEq: unexpected constraint " ++ show c
constraintSubstituteAll st oldc@(CLazy obj expand _ _ _) = do
    -- Attempt to expand lazy constraint.
    let obj' = teSubstituteAll st obj
    if obj' /= obj
       then maybe (st{ solverConstraints = oldc{cType = obj'}:solverConstraints st }, True)
                  (\cs -> (addConstraints cs st, True))
            <$> (expand $ teExpandAliases obj')
       else return (st{ solverConstraints = oldc : solverConstraints st }, False)

predSubstituteAll :: SolverState -> Predicate -> Maybe Predicate
predSubstituteAll st (PEq te1 te2 e) =
    if te1' == te2' then Nothing  else Just (PEq te1' te2' e)
    where
    te1' = teSubstituteAll st te1
    te2' = teSubstituteAll st te2

substituteIVar :: (?d::DatalogProgram) => IntVar -> IExpr -> SolverState -> SolverState
substituteIVar v ie st =
    st { solverPartialInt = width'}
    where
    width' = M.insert v ie $ solverPartialInt st

ieSubstituteAll :: SolverState -> IExpr -> IExpr
ieSubstituteAll _  ie@IConst{} = ie
ieSubstituteAll st ie@(IVar v) =
    case M.lookup v (solverPartialInt st) of
         Nothing  -> ie
         Just ie' -> ieSubstituteAll st ie'
ieSubstituteAll st (IPlus ie1 ie2) =
    ieSum [ieSubstituteAll st ie1, ieSubstituteAll st ie2]
