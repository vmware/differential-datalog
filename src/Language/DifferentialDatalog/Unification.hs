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
Unification-based type inference algorithm.
-}

{-# LANGUAGE FlexibleContexts, RecordWildCards, TupleSections, ImplicitParams, RankNTypes #-}

module Language.DifferentialDatalog.Unification(
    Typing,
    DDExpr(..),
    PredicateExplanation(..),
    ExplanationKind(..),
    Constraint(..),
    constraintsShow,
    WidthVar(..),
    IExpr(..),
    TypeVar(..),
    tvObject,
    TExpr(..),
    teToType,
    teDeref,
    Predicate(..),
    solve
)
where

import Control.Monad.Except
import Control.Monad.State
import Data.List
import qualified Data.Map as M
import Data.Maybe
--import Debug.Trace

import Language.DifferentialDatalog.Attribute
import Language.DifferentialDatalog.Error
import Language.DifferentialDatalog.Name
import Language.DifferentialDatalog.Pos
import Language.DifferentialDatalog.Syntax
import Language.DifferentialDatalog.Var

type Typing = M.Map TypeVar Type

-- Uniquely identifies an expression in a DDlog program.
data DDExpr = DDExpr {ddexprCtx::ECtx, ddexprExpr::Expr} deriving (Eq, Ord)

instance Show DDExpr where
    show e = show $ ddexprExpr e

instance WithPos DDExpr where
    pos e = pos $ ddexprExpr e
    atPos _ _ = error "DDExpr.atPos: not impleemnted"

-- A type constraint is an atomic predicate, that requires two type expressions
-- to be equivalent, a width constraint, that requires two integer expressions
-- representing bit widths to be equivalent, or a lazy constraint.  We use a
-- unification-based solver to solve type constraints, which does not handle
-- arbitrary Boolean combinations of constraints, hence this restricted structure.
data Constraint = CPredicate {cPred::Predicate}
                  -- Width constraint of the form 'expr1 == expr2', where
                  -- 'expr1' and 'expr2'.
                | CWidthEq {cLHS::IExpr, cRHS::IExpr, cWidthExplanation::WidthExplanation}
                  -- A lazy constraint gets activated once the type of a
                  -- specified "trigger" expression ('cExpr') is fully resolved.  It is
                  -- used to represent disjunctive constraints, where the set
                  -- of predicates depends on the type of the trigger.  E.g.,
                  -- the type of 'x.f' depends on the type of 'x', e.g., '(x:
                  -- Struct1 && x.f is bool || x:Struct2 && x.f: string ||
                  -- ...)'.  This is not easy to do with a unification solver,
                  -- so we let the constraint lay dormant until the solver can
                  -- rule out all but one disjunct.
                  -- 'cExpr' - trigger expression; does not change after the
                  -- constraint is created.
                  -- 'cType' - type expression that represents our current
                  -- knowledge of the type of 'cExpr'.  Gets refined as the
                  -- solver performs substitutions.
                  -- 'cExpand' - once 'cType' is resolved into a constant type
                  -- expression, this function is invoked to generate a set of
                  -- constraints that this lazy constraint resolves into.
                  -- 'cDefault' - optional default set of constraints to be used
                  -- if the program does not contain enough information to resolve
                  -- the type of the trigger expression, e.g., an integer literal
                  -- can be interpreted as 'u64' by default.
                  -- 'cExplanation' - description of the constraint for
                  -- debugging purposes.
                  --
                  -- TODO: currently, a lazy constraint is only triggered once
                  -- the type has been completely resolved, at which point it is
                  -- required to produce a set of constraints of fail.  Some use
                  -- cases may benefit from a more subtle design where a partial
                  -- expansion of the trigger expression is sufficient to
                  -- trigger the constraint.  To support this we will need to
                  -- change the 'cExpand' signature to return
                  -- 'Option<[Constraint]>', with 'Nothing' meaning that 'cType'
                  -- is not yet sufficiently concretized to expand the
                  -- constraint.
                | CLazy { cType::TExpr
                        , cExpand::(forall me . (MonadError String me) => TExpr -> me [Constraint])
                        , cDefault::Maybe [Constraint]
                        , cExpr::DDExpr
                        , cExplanation::String
                        }

constraintShow :: Constraint -> TVarShortcuts String
constraintShow (CPredicate p)         = predShow p
constraintShow (CWidthEq ie1 ie2 _)   = return $ show ie1 ++ " = " ++ show ie2
constraintShow (CLazy te _ _ _ expl)  = return $ expl ++ " [partial type: " ++ show te ++ "]"

constraintsShow :: [Constraint] -> String
constraintsShow cs = evalState (
    do cs' <- mapM constraintShow cs
       return $ intercalate "\n" cs' ) M.empty

instance Show Constraint where
    show c = evalState (constraintShow c) M.empty

-- A constraint is in the solved form if it has the following shape:
-- 'tv = te' or 'te = tv',
-- where 'tv' is a type variable and 'te' is an arbitrary type expression.
constraintIsSolved :: Constraint -> Bool
constraintIsSolved (CPredicate (PEq TETVar{} _ _)) = True
constraintIsSolved (CPredicate (PEq _ TETVar{} _)) = True
constraintIsSolved (CWidthEq IVar{} _ _)           = True
constraintIsSolved (CWidthEq _ IVar{} _)           = True
constraintIsSolved _                               = False

-- Normalization rotates a solved constraint so that the type variable
-- is on the left: 'te = tv' is transformed into 'tv = te'.
constraintNormalize :: Constraint -> Constraint
constraintNormalize (CPredicate (PEq te tv@TETVar{} e)) = CPredicate $ PEq tv te e
constraintNormalize (CWidthEq ie iv@IVar{} e)           = CWidthEq iv ie e
constraintNormalize c                                   = c

constraintIsLazy :: Constraint -> Bool
constraintIsLazy CLazy{} = True
constraintIsLazy _       = False

constraintIsPredicate :: Constraint -> Bool
constraintIsPredicate CPredicate{} = True
constraintIsPredicate _            = False

-- Generate a width constraint in canonical form: all constants are on one side
-- of the constraint, variables that appear on both sides are cancelled out.
cWidthEq :: IExpr -> IExpr -> WidthExplanation -> [Constraint]
cWidthEq ie1 ie2 e = if ie1' == ie2' then [] else [CWidthEq ie1' ie2' e]
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
cwidthReportConflict c@(CWidthEq _ _ e) = err ?d (pos e) $ "Unsatisfiable constraint '" ++ show c ++ "' in\n" ++
                                                           evalState (explanationShow e) M.empty
cwidthReportConflict c = error $ "cwidthReportConflict " ++ show c

removeConstraint :: Int -> SolverState -> SolverState
removeConstraint idx st = st{ solverConstraints = take idx cs ++ drop (idx+1) cs }
    where cs = solverConstraints st

addConstraints :: [Constraint] -> SolverState -> SolverState
addConstraints new_cs st = st{ solverConstraints = new_cs ++ cs}
    where cs = solverConstraints st

-- Integer expressions represent widths of bit and signed types.
data IExpr = -- Integer constant.
             IConst Int
             -- Integer variable.
           | IVar WidthVar
             -- Sum of two widths.
           | IPlus IExpr IExpr
           deriving (Eq, Ord)

instance Show IExpr where
    show (IConst i)         = show i
    show (IVar v)           = show v
    show (IPlus ie1 ie2)    = show ie1 ++ " + " ++ show ie2

-- Integer expression is a constant (i.e., does not contain any variables).
ieIsConstant :: IExpr -> Bool
ieIsConstant IConst{}      = True
ieIsConstant IVar{}        = False
ieIsConstant (IPlus e1 e2) = ieIsConstant e1 && ieIsConstant e2

-- List variables that occur in a width expression.
-- Each variable occurs in the result as many times as it occurs in the
-- expression.
ieVars :: IExpr -> [WidthVar]
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
data WidthVar = -- Integer variable equal to the width of a DDExpr.
                WidthOfExpr DDExpr
                deriving (Eq, Ord)

instance Show WidthVar where
    show (WidthOfExpr e) = "width_of(" ++ show e ++ ")"

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

-- State monad that assigns symbolic names 'a, 'b, ... to type variables in one
-- or more type expressions.
--
-- When pretty printing constraints, predicates or type expressions we would
-- like to replace type variables with short symbolic shortcuts, which should be
-- consistent, i.e., the same variable is alway represent by the same symbolic
-- name.  We achieve this by printing all related type expressions, e.g., all
-- type expressions involved in a type resolution conflict within this monad.
type TVarShortcuts a = State (M.Map TypeVar String) a

-- Return existing shortcut for 'tv' or generate a new one.
tvName :: TypeVar -> TVarShortcuts String
tvName tv = do
    -- "a", "b", .. , "z", "aa", "ab", ....
    let allStrings = [ s ++ [c] | s <- "": allStrings, c <- ['a'..'z'] ]
    names <- get
    case M.lookup tv names of
         Just n  -> return n
         Nothing -> do let n = allStrings !! M.size names
                       put $ M.insert tv n names
                       return n

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
           deriving (Eq, Ord)

teShow :: TExpr -> TVarShortcuts String
teShow TEBool                 = return "bool"
teShow TEString               = return "string"
teShow TEBigInt               = return "bigint"
teShow (TEBit w)              = return $ "bit<" ++ show w ++ ">"
teShow (TESigned w)           = return $ "signed<" ++ show w ++ ">"
teShow TEFloat                = return "float"
teShow TEDouble               = return "double"
teShow (TETuple as)           = ((++ ")") . ("(" ++) . intercalate ",") <$> mapM teShow as
teShow (TEUser n as)          = ((++ ">") . ((n ++ "<") ++) . intercalate ",") <$> mapM teShow as
teShow (TEExtern n as)        = ((++ ">") . ((n ++ "<") ++) . intercalate ",") <$> mapM teShow as
teShow (TETArg n)             = return $ "'" ++ n
teShow (TETVar tv)            = ("'" ++) <$> tvName tv

instance Show TExpr where
    show te = evalState (teShow te) M.empty

-- Type expression is a constant (i.e., does not contain any type variables).
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
teToType te                     = error $ "Unification.teToType: non-constant type expression " ++ show te

-- Unwrap all layers of shared references.
teDeref :: (?d::DatalogProgram) => TExpr -> TExpr
teDeref (TEExtern n [t]) | elem n sref_types = teDeref t
    where
    sref_types = map name
                 $ filter (tdefGetSharedRefAttr ?d)
                 $ M.elems $ progTypedefs ?d
teDeref t = t

-- Check if type expression is a constant and if not, return an explanation.
-- 'obj' is the object whose type 'te' describes.
teCheckConstant :: (?d::DatalogProgram, MonadError String me) => TypeVar -> TExpr -> me ()
teCheckConstant tvar te | teIsConstant te = return ()
                        | not $ null tvars =
    -- 'te' depends on an unresolved type variable 'tv'.  So instead of
    -- complaining that the type of 'te' is unknown, we try to be more specific
    -- and complain that the type of the entity 'tv' represents is unknown.
    err ?d (pos tv) $ "Cannot infer type of " ++ tvObject tv
                       | otherwise =
    -- 'te' is not a constant, but it does not depend on any type variables,
    -- hence it must have parts whose width is not yet resolved into a constant.
    err ?d (pos tvar) $ "Unable to infer complete type of " ++ tvObject tvar ++ ". Partially inferred type: " ++ show te
    where tvars = teTypeVars te
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

predShow :: Predicate -> TVarShortcuts String
predShow (PEq te1 te2 _) = do
    s1 <- teShow te1
    s2 <- teShow te2
    return $ s1 ++ " = " ++ s2

instance Show Predicate where
    show p = evalState (predShow p) M.empty

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
-- 'P3: a=b' whose explanation it the "parent" predicate it was derived from:
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
explanationSubstituteAll st (ExplanationParent p) = ExplanationParent $ head $ predSubstituteAll st p

explanationShow :: PredicateExplanation -> TVarShortcuts String
explanationShow (ExplanationTVar tv _)              = return $ tvObject tv
explanationShow (ExplanationParent (PEq te1 te2 e)) = do
    ts1 <- teShow te1
    ts2 <- teShow te2
    es <- explanationShow e
    return $ if explanationKind e == Expected
             then "expected type: " ++ ts2 ++ "\nactual type: " ++ ts1 ++ "\nin\n" ++ es
             else "expected type: " ++ ts1 ++ "\nactual type: " ++ ts2 ++ "\nin\n" ++ es

explanationKind :: PredicateExplanation -> ExplanationKind
explanationKind (ExplanationTVar _ k) = k
explanationKind (ExplanationParent p) = explanationKind (predExplanation p)

-- Report type resolution conflict in an unsatisfiable predicate (where
-- left and right side cannot be unified).
predReportConflict :: (?d::DatalogProgram, MonadError String me) => SolverState -> Predicate -> me a
predReportConflict st (PEq te1 te2 explanation) =
    evalState (do ts1 <- teShow te1
                  ts2 <- teShow te2
                  es <- explanationShow $ explanationSubstituteAll st explanation
                  return $ err ?d (pos explanation) $
                               if explanationKind explanation == Expected
                               then "Type mismatch:\nexpected type: " ++ ts2 ++ "\nactual type: " ++ ts1 ++ "\nin\n" ++ es
                               else "Type mismatch:\nexpected type: " ++ ts1 ++ "\nactual type: " ++ ts2 ++ "\nin\n" ++ es)
              M.empty

-- Explanation of a width constraint.  For now just the explanation of the
-- predicate it is derived from.
type WidthExplanation = PredicateExplanation

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
        1. Pick 'c in C' s.t. 'c = (tv == te)', 'tv in TV', 'te in TE'.
           - If such 'c' does not exist, pick any predicate constraint in
             'C' and apply Unify to it. GOTO 1.
           - If there are only lazy and width constraints left in 'C',
             pick a lazy constraint with a default solution and expand it
             to that solution.
           - If such constraint does not exist, FAIL.
        2. Variable substitution:
           - remove 'c' from 'C'
           - apply Substitution to replace 'tv' with 'te' in all remaining
             constraints and assignments.
           - add 'tv -> te' to 'TA'
    }
    Check that a complete typing has been found: all assignments in 'TA' are
    constant expressions (no variables). Otherwise, FAIL: undespecified types.

The above algorithm uses two subroutines:

Unify(c), where 'c = (te1 == te2)'
    1. Remove 'c' from 'C'.
    2. Recursively match 'te1' and 'te2' to produce a set of constraints of the form
      'tv = te', 'iv = ie' or a conflict of the form 'te3 == te4' where 'te3' and
      'te4' have different type constructors or 'ie3 == ie4' where 'ie3' and 'ie4' are
      two different integer constants.  A 'tv = te' constraint is also considered a
      conflict if 'te' depends on 'tv' (and is not identity 'tv').
    3. If conflict detected, FAIL: conflicting types.
    4. Otherwise, add newly derived equations to 'C'.

Substitution(tv, te):
    Replace all occurrences of 'tv' with 'te' in 'C' and 'TA'.
    Eliminate tautologies like 'te1 == te1'
    Lazy constraints 'c in C' are handled by applying the substitution
    to the trigger expression and expanding the constraint if its type
    is fully resolved.
-}

data SolverState = SolverState {
    -- Partial typing computed so far.
    -- Assigned variables cannot appear in the RHS of an assignment
    -- or in any of the remaining constraints.
    solverPartialTyping :: M.Map TypeVar TExpr,

    -- IA: IV -> IE - partial integer variable assignment computed so far.
    -- Assigned variables cannot appear in the RHS of an assignment
    -- or in any of the remaining constraints.
    solverPartialWidth :: M.Map WidthVar IExpr,

    -- Unresolved type constraints.
    solverConstraints :: [Constraint]
}

solve :: (MonadError String me) => DatalogProgram -> [Constraint] -> me Typing
solve d cs = let ?d = d in solve' init_state
    where
    init_state = SolverState {
        solverPartialTyping = M.empty,
        solverPartialWidth = M.empty,
        solverConstraints = cs
    }

solve' :: (?d::DatalogProgram, MonadError String me) => SolverState -> me Typing
solve' st | null (solverConstraints st) = do
    -- Check that a complete typing has been found: all assignments in 'TA' and
    -- 'IA' are constant expressions (no variables). Otherwise, FAIL: underspecified types.
    mapM_ (\(tv, te) -> teCheckConstant tv te) $ M.toList $ solverPartialTyping st
    return $ M.map teToType $ solverPartialTyping st
          | otherwise =
    -- 1. Pick 'c in C' s.t. 'c = (tv == te)', 'tv in TV', 'te in TE'.
    --   - If such 'c' does not exist, pick any predicate constraint in
    --     'C' and apply Unification to it.
    --   - If there are only lazy and width constraints left in 'C',
    --     pick a disjunctive constraint with a default solution.
    --   - If such constraint does not exist, FAIL.
    --trace ("solve': constraints:\n" ++ constraintsShow (solverConstraints st)) $
    case findIndex constraintIsSolved  $ solverConstraints st of
         Nothing -> case findIndex (constraintIsPredicate) $ solverConstraints st of
                         Nothing -> case findIndex (\c -> constraintIsLazy c && isJust (cDefault c)) $ solverConstraints st of
                                         Just i -> do let st' = removeConstraint i st
                                                      let CLazy _ _ (Just def) _ _ = solverConstraints st !! i
                                                      let st'' = addConstraints (concatMap (constraintSubstituteAll st') def) st'
                                                      solve' st''
                                         Nothing -> case solverConstraints st !! 0 of
                                                         CLazy{..} -> err ?d (pos cExpr) $ "Failed to infer type of expression '" ++ show cExpr ++ "'"
                                                         c@CWidthEq{} -> cwidthReportConflict c
                                                         c -> error $ "solve': unexpected constraint " ++ show c
                                                         {-" partial type: " ++ show cType ++
                                                         "\nconstraints: " ++ constraintsShow (solverConstraints st) ++
                                                         "\ntyping:\n  " ++ (intercalate "\n  " $ map (\(tv, te) -> tvObject tv ++ ": " ++ show te) $ M.toList $ solverPartialTyping st)-}
                         Just i -> do let st' = removeConstraint i st
                                      new_constraints <- unify st $ cPred $ solverConstraints st !! i
                                      let st'' = addConstraints new_constraints st'
                                      solve' st''
         -- 2. Variable substitution:
         --  - remove 'c' from 'C'
         --  - apply Substitution to replace 'tv' with 'te' in all remaining
         --    constraints and assignments.
         --  - add 'tv -> te' to 'TA'
         Just i -> case constraintNormalize $ solverConstraints st !! i of
                        CPredicate (PEq (TETVar v) te _) -> do
                            let st' = removeConstraint i st
                            st'' <- substitute v te st'
                            solve' st''
                        CWidthEq (IVar v) ie2 _ -> do
                            let st' = removeConstraint i st
                            st'' <- substituteIVar v ie2 st'
                            solve' st''
                        c -> error $ "Unification.solve: unexpected constraint " ++ show c

-- Do _not_ normalize the constraint before unification, as swapping RHS and LHS
-- sides can confuse the explanation
unify :: (?d::DatalogProgram, MonadError String me) => SolverState -> Predicate -> me [Constraint]
-- Cannot learn anything from a tautology.
unify _  (PEq te1 te2 _) | te1 == te2 = return []
-- We found a type variable assignment, add it to constraint set,
-- unless it's a contradition.
unify st p@(PEq (TETVar v) te _) | elem v $ teTypeVars te = predReportConflict st p
                                 | otherwise = return [CPredicate p]
unify st p@(PEq te (TETVar v) _) | elem v $ teTypeVars te = predReportConflict st p
                                 | otherwise = return [CPredicate p]
-- We found an integer variable assignment, add it to constraint set.
unify st p@(PEq (TETuple args1) (TETuple args2) _) = do
    when (length args1 /= length args2) $ predReportConflict st p
    concat <$> (mapM (\(te1, te2) -> unify st $ PEq te1 te2 (ExplanationParent p))
                $ zip args1 args2)

unify st p@(PEq (TEUser n1 args1) (TEUser n2 args2) _) | n1 /= n2 = predReportConflict st p
                                                       | otherwise = do
    concat <$> (mapM (\(a1, a2) -> unify st $ PEq a1 a2 (ExplanationParent p)) $ zip args1 args2)
unify st p@(PEq (TEExtern n1 args1) (TEExtern n2 args2) _) | n1 /= n2 = predReportConflict st p
                                                           | otherwise =
    concat <$> (mapM (\(a1, a2) -> unify st $ PEq a1 a2 (ExplanationParent p)) $ zip args1 args2)
unify _  p@(PEq (TEBit ie1) (TEBit ie2) _) = return $ cWidthEq ie1 ie2 (ExplanationParent p)
unify _  p@(PEq (TESigned ie1) (TESigned ie2) _) = return $ cWidthEq ie1 ie2 (ExplanationParent p)
unify st p = predReportConflict st p

substitute :: (?d::DatalogProgram, MonadError String me) => TypeVar -> TExpr -> SolverState -> me SolverState
substitute v te st = do
    let typing' = M.map (teSubstitute v te) $ solverPartialTyping st
    let typing'' = M.insert v te typing'
    let st' = st {solverPartialTyping = typing''}
    constraints' <- concat <$> (mapM (constraintSubstitute st' v te) $ solverConstraints st')
    return $ st' {solverConstraints = constraints'}

teSubstitute :: TypeVar -> TExpr -> TExpr -> TExpr
teSubstitute v with (TETuple args) = TETuple $ map (teSubstitute v with) args
teSubstitute v with (TEUser n args) = TEUser n $ map (teSubstitute v with) args
teSubstitute v with (TEExtern n args) = TEExtern n $ map (teSubstitute v with) args
teSubstitute v with (TETVar v') | v' == v = with
teSubstitute _ _ te = te

teSubstituteAll :: SolverState -> TExpr -> TExpr
teSubstituteAll st (TETuple args) = TETuple $ map (teSubstituteAll st) args
teSubstituteAll st (TEUser n args) = TEUser n $ map (teSubstituteAll st) args
teSubstituteAll st (TEExtern n args) = TEExtern n $ map (teSubstituteAll st) args
teSubstituteAll st te@(TETVar v) =
    case M.lookup v (solverPartialTyping st) of
         Nothing  -> te
         Just te' -> te'
teSubstituteAll st (TEBit ie) = TEBit $ ieSubstituteAll st ie
teSubstituteAll st (TESigned ie) = TESigned $ ieSubstituteAll st ie
teSubstituteAll _  te = te

-- Version that does not eliminate tautologies.  Useful in resolving disjunctive constraints.
constraintSubstitute :: (?d::DatalogProgram, MonadError String me) => SolverState -> TypeVar -> TExpr -> Constraint -> me [Constraint]
constraintSubstitute _  v with (CPredicate p) = return $ map CPredicate $ predSubstitute v with p
constraintSubstitute _  _ _    c@CWidthEq{} = return [c]
constraintSubstitute st v with c@(CLazy obj expand _ _ _) = do
    let obj' = teSubstitute v with obj
    -- The object of the lazy constraint has been fully evaluated -- expand the
    -- constraint.
    if teIsConstant obj'
       then (concatMap (constraintSubstituteAll st)) <$> expand obj'
       else return [c{cType = obj'}]

constraintSubstituteAll :: (?d::DatalogProgram) => SolverState -> Constraint -> [Constraint]
constraintSubstituteAll st (CPredicate p) = map CPredicate $ predSubstituteAll st p
constraintSubstituteAll st (CWidthEq ie1 ie2 e) =
    cWidthEq ie1' ie2' e
    where
    ie1' = ieSubstituteAll st ie1
    ie2' = ieSubstituteAll st ie2
constraintSubstituteAll _  CLazy{} = error "constraintSubstituteAll: recursive lazy constraints are not supported"

predSubstitute :: TypeVar -> TExpr -> Predicate -> [Predicate]
predSubstitute v with (PEq te1 te2 e) =
    if te1' == te2' then [] else [PEq te1' te2' e]
    where
    te1' = teSubstitute v with te1
    te2' = teSubstitute v with te2

predSubstituteAll :: SolverState -> Predicate -> [Predicate]
predSubstituteAll st (PEq te1 te2 e) =
    if te1' == te2' then [] else [PEq te1' te2' e]
    where
    te1' = teSubstituteAll st te1
    te2' = teSubstituteAll st te2

substituteIVar :: (?d::DatalogProgram, MonadError String me) => WidthVar -> IExpr -> SolverState -> me SolverState
substituteIVar v ie st = do
    let typing' = M.map (teSubstituteIVar v ie) $ solverPartialTyping st
    let width' = M.map (ieSubstituteIVar v ie) $ solverPartialWidth st
    let width'' = M.insert v ie width'
    let st' = st { solverPartialTyping = typing'
                 , solverPartialWidth = width''}
    constraints' <- concat <$> (mapM (constraintSubstituteIVar st' v ie) $ solverConstraints st)
    return st' {solverConstraints = constraints'}

teSubstituteIVar :: WidthVar -> IExpr -> TExpr -> TExpr
teSubstituteIVar v with (TETuple args)      = TETuple $ map (teSubstituteIVar v with) args
teSubstituteIVar v with (TEUser n args)     = TEUser n $ map (teSubstituteIVar v with) args
teSubstituteIVar v with (TEExtern n args)   = TEExtern n $ map (teSubstituteIVar v with) args
teSubstituteIVar v with (TEBit ie)          = TEBit $ ieSubstituteIVar v with ie
teSubstituteIVar v with (TESigned ie)       = TESigned $ ieSubstituteIVar v with ie
teSubstituteIVar _ _ te = te

ieSubstituteIVar :: WidthVar -> IExpr -> IExpr -> IExpr
ieSubstituteIVar _ _    ie@IConst{} = ie
ieSubstituteIVar v with ie@(IVar v') | v' == v   = with
                                     | otherwise = ie
ieSubstituteIVar v with (IPlus ie1 ie2) =
    ieSum [ieSubstituteIVar v with ie1, ieSubstituteIVar v with ie2]

ieSubstituteAll :: SolverState -> IExpr -> IExpr
ieSubstituteAll _  ie@IConst{} = ie
ieSubstituteAll st ie@(IVar v) =
    case M.lookup v (solverPartialWidth st) of
         Nothing  -> ie
         Just ie' -> ie'
ieSubstituteAll st (IPlus ie1 ie2) =
    ieSum [ieSubstituteAll st ie1, ieSubstituteAll st ie2]

predSubstituteIVar :: WidthVar -> IExpr -> Predicate -> [Predicate]
predSubstituteIVar v with (PEq te1 te2 e) =
    let te1' = teSubstituteIVar v with te1
        te2' = teSubstituteIVar v with te2
    in if te1' == te2' then [] else [PEq te1' te2' e]

constraintSubstituteIVar :: (?d::DatalogProgram, MonadError String me) => SolverState -> WidthVar -> IExpr -> Constraint -> me [Constraint]
constraintSubstituteIVar _ v with (CPredicate p)                  = return $ map CPredicate $ predSubstituteIVar v with p
constraintSubstituteIVar _ v with (CWidthEq ie1 ie2 e)              = do
    let ie1' = ieSubstituteIVar v with ie1
    let ie2' = ieSubstituteIVar v with ie2
    return $ cWidthEq ie1' ie2' e
constraintSubstituteIVar st v with c@CLazy{..} = do
    let obj' = teSubstituteIVar v with cType
    -- The object of the lazy constraint has been fully evaluated -- expand the
    -- constraint.
    if teIsConstant obj'
       then (concatMap (constraintSubstituteAll st)) <$> cExpand obj'
       else return [c{cType = obj'}]
