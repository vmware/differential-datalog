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

{-# LANGUAGE RecordWildCards, FlexibleContexts, LambdaCase, TupleSections #-}

module Language.DifferentialDatalog.Validate (
    validate,
    ruleCheckAggregate) where

import qualified Data.Map as M
import qualified Data.Set as S
import Control.Monad.Except
import Data.Maybe
import Data.List
import Data.Char
import qualified Data.Graph.Inductive as G
import qualified Data.Graph.Inductive.Query as G
import Debug.Trace

import Language.DifferentialDatalog.Syntax
import Language.DifferentialDatalog.NS
import Language.DifferentialDatalog.Util
import Language.DifferentialDatalog.Pos
import Language.DifferentialDatalog.Name
import Language.DifferentialDatalog.Type
import Language.DifferentialDatalog.Ops
import Language.DifferentialDatalog.ECtx
import Language.DifferentialDatalog.Expr
import Language.DifferentialDatalog.DatalogProgram
import {-# SOURCE #-} Language.DifferentialDatalog.Rule

bUILTIN_2STRING_FUNC :: String
bUILTIN_2STRING_FUNC = "std.__builtin_2string"

tOSTRING_FUNC_SUFFIX :: String
tOSTRING_FUNC_SUFFIX = "2string"

-- | Validate Datalog program
validate :: (MonadError String me) => DatalogProgram -> me DatalogProgram
validate d = do
    uniqNames ("Multiple definitions of constructor " ++)
              $ progConstructors d
    -- Validate typedef's
    mapM_ (typedefValidate d) $ M.elems $ progTypedefs d
    -- No cyclic dependencies between user-defined types
    checkAcyclicTypes d
    -- Desugar.  Must be called after typeValidate.
    d' <- progDesugar d
    -- Validate function prototypes
    mapM_ (funcValidateProto d') $ M.elems $ progFunctions d'
    -- Validate function implementations
    mapM_ (funcValidateDefinition d') $ M.elems $ progFunctions d'
    -- Validate relation declarations
    mapM_ (relValidate d') $ M.elems $ progRelations d'
    -- Validate rules
    mapM_ (ruleValidate d') $ progRules d'
    -- Validate dependency graph
    depGraphValidate d'
    -- Insert string conversion functions
    d'' <- progInjectStringConversions d'
    -- Convert 'int' constants to 'bit<>'.
    let d''' = progConvertIntsToBVs d''
    -- This check must be done after 'depGraphValidate', which may
    -- introduce recursion
    checkNoRecursion d'''
    return d'''

--    mapM_ (relValidate2 r)   refineRels
--    maybe (return ())
--          (\cyc -> errR r (pos $ getRelation r $ snd $ head cyc)
--                     $ "Dependency cycle among relations: " ++ (intercalate ", " $ map (name . snd) cyc))
--          $ (grCycle $ relGraph r)
--    mapM_ (relValidate3 r)   refineRels
--    validateFinal r

-- Reject program with recursion
checkNoRecursion :: (MonadError String me) => DatalogProgram -> me ()
checkNoRecursion d = do
    case grCycle (funcGraph d) of
         Nothing -> return ()
         Just t  -> err (pos $ getFunc d $ snd $ head t)
                        $ "Recursive function definition: " ++ (intercalate "->" $ map (name . snd) t)


funcGraph :: DatalogProgram -> G.Gr String ()
funcGraph DatalogProgram{..} =
    let g0 = foldl' (\g (i,f) -> G.insNode (i,f) g)
                    G.empty $ zip [0..] (M.keys progFunctions) in
    foldl' (\g (i,f) -> case funcDef f of
                             Nothing -> g
                             Just e  -> foldl' (\g' f' -> G.insEdge (i, M.findIndex f' progFunctions, ()) g')
                                               g (exprFuncs e))
           g0 $ zip [0..] $ M.elems progFunctions

-- Remove syntactic sugar
progDesugar :: (MonadError String me) => DatalogProgram -> me DatalogProgram
progDesugar d = progExprMapCtxM d (exprDesugar d)

-- Desugar expressions: convert all type constructor calls to named
-- field syntax.
-- Precondition: typedefs must be validated before calling this
-- function.
exprDesugar :: (MonadError String me) => DatalogProgram -> ECtx -> ENode -> me Expr
exprDesugar d _ e =
    case e of
         EStruct p c as -> do
            cons@Constructor{..} <- checkConstructor p d c
            let desugarPos = do
                    check (length as == length consArgs) p
                           $ "Number of arguments does not match constructor declaration: " ++ show cons
                    return $ zip (map name consArgs) (map snd as)
            let desugarNamed = do
                    uniq' (\_ -> p) id ("Multiple occurrences of a field " ++) $ map fst as
                    mapM (\(n,e) -> check (isJust $ find ((==n) . name) consArgs) (pos e)
                                           $ "Unknown field " ++ n) as
                    return $ map (\f -> (name f, maybe ePHolder id $ lookup (name f) as)) consArgs
            as' <- case as of
                        [] | null consArgs
                           -> return as
                        [] -> desugarNamed
                        _  | all (null . fst) as
                           -> desugarPos
                        _  | any (null . fst) as
                           -> err (pos e) $ "Expression mixes named and positional arguments to type constructor " ++ c
                        _  -> desugarNamed
            return $ E e{exprStructFields = as'}
         _              -> return $ E e

typedefValidate :: (MonadError String me) => DatalogProgram -> TypeDef -> me ()
typedefValidate d@DatalogProgram{..} TypeDef{..} = do
    uniq' (\_ -> tdefPos) id ("Multiple definitions of type argument " ++) tdefArgs
    mapM_ (\a -> check (M.notMember a progTypedefs) tdefPos
                        $ "Type argument " ++ a ++ " conflicts with user-defined type name")
          tdefArgs
    case tdefType of
         Nothing -> return ()
         Just t  -> do
             typeValidate d tdefArgs t
             let dif = tdefArgs \\ typeTypeVars t
             check (null dif) tdefPos
                    $ "The following type variables are not used in type definition: " ++ intercalate "," dif

typeValidate :: (MonadError String me) => DatalogProgram -> [String] -> Type -> me ()
typeValidate _ _     TString{}        = return ()
typeValidate _ _     TInt{}           = return ()
typeValidate _ _     TBool{}          = return ()
typeValidate _ _     (TBit p w)       =
    check (w>0) p "Integer width must be greater than 0"
typeValidate d tvars (TStruct p cs)   = do
    uniqNames ("Multiple definitions of constructor " ++) cs
    mapM_ (consValidate d tvars) cs
    mapM_ (\grp -> check (length (nub $ map typ grp) == 1) p $
                          "Field " ++ (name $ head grp) ++ " is declared with different types")
          $ sortAndGroup name $ concatMap consArgs cs
typeValidate d tvars (TTuple _ ts)    =
    mapM_ (typeValidate d tvars) ts
typeValidate d tvars (TUser p n args) = do
    t <- checkType p d n
    let expect = length (tdefArgs t)
    let actual = length args
    check (expect == actual) p $
           "Expected " ++ show expect ++ " type arguments to " ++ n ++ ", found " ++ show actual
    mapM_ (typeValidate d tvars) args
    return ()
typeValidate d tvars (TVar p v)       =
    check (elem v tvars) p $ "Unknown type variable " ++ v
typeValidate _ _     t                = error $ "typeValidate " ++ show t

consValidate :: (MonadError String me) => DatalogProgram -> [String] -> Constructor -> me ()
consValidate d tvars Constructor{..} = do
    uniqNames ("Multiple definitions of argument " ++) consArgs
    mapM_ (typeValidate d tvars . fieldType) $ consArgs

checkAcyclicTypes :: (MonadError String me) => DatalogProgram -> me ()
checkAcyclicTypes d@DatalogProgram{..} = do
    let g0 :: G.Gr String ()
        g0 = G.insNodes (mapIdx (\(t,_) i -> (i, t)) $ M.toList progTypedefs) G.empty
        typIdx t = M.findIndex t progTypedefs
        gfull = M.foldlWithKey (\g tn tdef ->
                                 foldl' (\g' t' -> G.insEdge (typIdx tn, typIdx t', ()) g') g
                                        $ maybe [] typeUserTypes $ tdefType tdef)
                               g0 progTypedefs
    maybe (return ())
          (\cyc -> throwError $ "Mutually recursive types: " ++
                                (intercalate " -> " $ map snd cyc))
          $ grCycle gfull

funcValidateProto :: (MonadError String me) => DatalogProgram -> Function -> me ()
funcValidateProto d f@Function{..} = do
    uniqNames ("Multiple definitions of argument " ++) funcArgs
    let tvars = funcTypeVars f
    mapM_ (typeValidate d tvars . argType) funcArgs
    typeValidate d tvars funcType

funcValidateDefinition :: (MonadError String me) => DatalogProgram -> Function -> me ()
funcValidateDefinition d f@Function{..} = do
    case funcDef of
         Nothing  -> return ()
         Just def -> exprValidate d (funcTypeVars f) (CtxFunc f) def

relValidate :: (MonadError String me) => DatalogProgram -> Relation -> me ()
relValidate d rel@Relation{..} = do
    typeValidate d [] relType
    check (isNothing relPrimaryKey || relRole == RelInput) (pos rel) 
        $ "Only input relations can be declared with a primary key"
    maybe (return ()) (exprValidate d [] (CtxKey rel) . keyExpr) relPrimaryKey

--relValidate2 :: (MonadError String me) => Refine -> Relation -> me ()
--relValidate2 r rel@Relation{..} = do
--    assertR r ((length $ filter isPrimaryKey relConstraints) <= 1) relPos $ "Multiple primary keys are not allowed"
--    mapM_ (constraintValidate r rel) relConstraints
--    maybe (return ()) (mapM_ (ruleValidate r rel)) relDef
--    maybe (return ()) (\rules -> assertR r (any (not . ruleIsRecursive rel) rules) relPos
--                                         "View must have at least one non-recursive rule") relDef

--relTypeValidate :: (MonadError String me) => Refine -> Relation -> Pos -> Type -> me ()
--relTypeValidate r rel p   TArray{}  = errR r p $ "Arrays are not allowed in relations (in relation " ++ name rel ++ ")"
--relTypeValidate r rel p   TTuple{}  = errR r p $ "Tuples are not allowed in relations (in relation " ++ name rel ++ ")"
--relTypeValidate r rel p   TOpaque{} = errR r p $ "Opaque columns are not allowed in relations (in relation " ++ name rel ++ ")"
--relTypeValidate r rel p   TInt{}    = errR r p $ "Arbitrary-precision integers are not allowed in relations (in relation " ++ name rel ++ ")"
--relTypeValidate _ _   _   TStruct{} = return ()
--relTypeValidate _ _   _   TUser{}   = return ()
--relTypeValidate _ _   _   _         = return ()
--
--relValidate3 :: (MonadError String me) => Refine -> Relation -> me ()
--relValidate3 r rel = do
--    let types = relTypes r rel
--    mapM_ (\t -> relTypeValidate r rel (pos t) t) types
--    maybe (return ())
--          (\cyc -> errR r (pos rel)
--                     $ "Dependency cycle among types used in relation " ++ name rel ++ ":\n" ++
--                      (intercalate "\n" $ map (show . snd) cyc))
--          $ grCycle $ typeGraph r types

ruleValidate :: (MonadError String me) => DatalogProgram -> Rule -> me ()
ruleValidate d rl@Rule{..} = do
    when (not $ null ruleRHS) $ do
        case head ruleRHS of
             RHSLiteral True _ -> return ()
             x                 -> err (pos rl) "Rule must start with positive literal"
    mapIdxM_ (ruleRHSValidate d rl) ruleRHS
    mapIdxM_ (ruleLHSValidate d rl) ruleLHS

ruleRHSValidate :: (MonadError String me) => DatalogProgram -> Rule -> RuleRHS -> Int -> me ()
ruleRHSValidate d rl@Rule{..} (RHSLiteral _ atom) idx = do
    checkRelation (pos atom) d $ atomRelation atom
    exprValidate d [] (CtxRuleRAtom rl idx) $ atomVal atom
    let vars = ruleRHSVars d rl idx
    -- variable cannot be declared and used in the same atom
    uniq' (\_ -> pos atom) fst (\(v,_) -> "Variable " ++ v ++ " is both declared and used inside relational atom " ++ show atom)
        $ filter (\(var, _) -> isNothing $ find ((==var) . name) vars)
        $ atomVarOccurrences (CtxRuleRAtom rl idx) $ atomVal atom

ruleRHSValidate d rl@Rule{..} (RHSCondition e) idx = do
    exprValidate d [] (CtxRuleRCond rl idx) e

ruleRHSValidate d rl@Rule{..} (RHSFlatMap v e) idx = do
    let ctx = CtxRuleRFlatMap rl idx
    exprValidate d [] ctx e
    checkIterable "FlatMap expression" (pos e) d ctx $ exprType d ctx e

ruleRHSValidate d rl (RHSAggregate v vs fname e) idx = do
    _ <- ruleCheckAggregate d rl idx
    return ()

ruleLHSValidate :: (MonadError String me) => DatalogProgram -> Rule -> Atom -> Int -> me ()
ruleLHSValidate d rl a@Atom{..} idx = do
    rel <- checkRelation atomPos d atomRelation
    when (relRole rel == RelInput) $ check (null $ ruleRHS rl) (pos a)
         $ "Input relation " ++ name rel ++ " cannot appear in the head of a rule"
    exprValidate d [] (CtxRuleL rl idx) atomVal

-- Validate Aggregate term, compute type argument map for the aggregate function used in the term.
-- e.g., given an aggregate function:
-- extern function group2map(g: Group<('K,'V)>): Map<'K,'V>
--
-- and its invocation:
-- Aggregate4(x, map) :- AggregateMe1(x,y), Aggregate((x), map = group2map((x,y)))
--
-- compute concrete types for 'K and 'V
ruleCheckAggregate :: (MonadError String me) => DatalogProgram -> Rule -> Int -> me (M.Map String Type)
ruleCheckAggregate d rl idx = do
    let RHSAggregate v vs fname e = ruleRHS rl !! idx
    let ctx = CtxRuleRAggregate rl idx
    exprValidate d [] ctx e
    -- group-by variables are visible in this scope
    mapM (checkVar (pos e) d ctx) vs
    check (notElem v vs) (pos e) $ "Aggregate variable " ++ v ++ " already declared in this scope"
    -- aggregation function exists and takes a group as its sole argument
    f <- checkFunc (pos e) d fname
    check (length (funcArgs f) == 1) (pos e) $ "Aggregation function must take one argument, but " ++
                                               fname ++ " takes " ++ (show $ length $ funcArgs f) ++ " arguments"
    -- figure out type of the aggregate
    funcTypeArgSubsts d (pos e) f [tOpaque gROUP_TYPE [exprType d ctx e]]



-- | Check the following properties of a Datalog dependency graph:
--
--  * Linearity: A rule contains at most one RHS atom that is mutually
--  recursive with its head.
--  * Stratified negation: No loop contains a negative edge.
depGraphValidate :: (MonadError String me) => DatalogProgram -> me ()
depGraphValidate d@DatalogProgram{..} = do
    let g = progDependencyGraph d
    -- strongly connected components of the dependency graph
    let sccs = map (S.fromList . map (fromJust . G.lab g)) $ G.scc g
    -- maps relation name to SCC that contains this relation
    let sccmap = M.fromList
                 $ concat
                 $ mapIdx (\scc i -> map (, i) $ S.toList scc)
                   sccs
    -- Linearity
    {-mapM_ (\rl@Rule{..} ->
            mapM_ (\a ->
                    do let lscc = sccmap M.! (atomRelation a)
                       let rlits = filter ((== lscc) . (sccmap M.!) . atomRelation . rhsAtom)
                                   $ filter rhsIsLiteral ruleRHS
                       when (length rlits > 1)
                            $ err (pos rl)
                            $ "At most one relation in the right-hand side of a rule can be mutually recursive with its head. " ++
                              "The following RHS literals are mutually recursive with " ++ atomRelation a ++ ": " ++
                              intercalate ", " (map show rlits))
                  ruleLHS)
          progRules -}
    -- Stratified negation
    mapM_ (\rl@Rule{..} ->
            mapM_ (\a ->
                    do let lscc = sccmap M.! (atomRelation a)
                       mapM_ (\rhs -> err (pos rl)
                                          $ "Relation " ++ (atomRelation $ rhsAtom rhs) ++ " is mutually recursive with " ++ atomRelation a ++
                                            " and therefore cannot appear negated in this rule")
                             $ filter ((== lscc) . (sccmap M.!) . atomRelation . rhsAtom)
                             $ filter (not . rhsPolarity)
                             $ filter rhsIsLiteral ruleRHS)
                  ruleLHS)
          progRules

exprValidate :: (MonadError String me) => DatalogProgram -> [String] -> ECtx -> Expr -> me ()
exprValidate d tvars ctx e = {-trace ("exprValidate " ++ show e ++ " in \n" ++ show ctx) $ -} do
    exprTraverseCtxM (exprValidate1 d tvars) ctx e
    exprTraverseTypeME d (exprValidate2 d) ctx e
    exprTraverseCtxM (exprCheckMatchPatterns d) ctx e

-- This function does not perform type checking: just checks that all functions and
-- variables are defined; the number of arguments matches declarations, etc.
exprValidate1 :: (MonadError String me) => DatalogProgram -> [String] -> ECtx -> ExprNode Expr -> me ()
exprValidate1 d _ ctx e@EVar{..} | ctxInRuleRHSPattern ctx
                                          = do
    when (ctxInBinding ctx) $
        check (isJust $ lookupVar d ctx exprVar) (pos e) $ "Variable declarations not allowed inside @-bindings"
exprValidate1 d _ ctx (EVar p v)          = do _ <- checkVar p d ctx v
                                               return ()
exprValidate1 d _ ctx (EApply p f as)     = do
    fun <- checkFunc p d f
    check (length as == length (funcArgs fun)) p
          "Number of arguments does not match function declaration"
    mapM_ (\(a, mut) -> when mut $ checkLExpr d ctx a)
          $ zip as (map argMut $ funcArgs fun)
exprValidate1 _ _ _   EField{}            = return ()
exprValidate1 _ _ _   EBool{}             = return ()
exprValidate1 _ _ _   EInt{}              = return ()
exprValidate1 _ _ _   EString{}           = return ()
exprValidate1 _ _ _   EBit{}              = return ()
exprValidate1 d _ ctx (EStruct p c _)     = do -- initial validation was performed by exprDesugar
    let tdef = consType d c
    when (ctxInSetL ctx && not (ctxIsRuleRCond $ ctxParent ctx)) $
        check ((length $ typeCons $ fromJust $ tdefType tdef) == 1) p
               $ "Type constructor in the left-hand side of an assignment is only allowed for types with one constructor, \
                 \ but \"" ++ name tdef ++ "\" has multiple constructors"
exprValidate1 _ _ _   ETuple{}            = return ()
exprValidate1 _ _ _   ESlice{}            = return ()
exprValidate1 _ _ _   EMatch{}            = return ()
exprValidate1 d _ ctx (EVarDecl p v)      = do
    check (ctxInSetL ctx || ctxInMatchPat ctx) p "Variable declaration is not allowed in this context"
    check (not $ ctxInBinding ctx) p "Variable declaration is not allowed inside @-binding"
    checkNoVar p d ctx v
{-                                     | otherwise
                                          = do checkNoVar p d ctx v
                                               check (ctxIsTyped ctx) p "Variable declared without a type"
                                               check (ctxIsSeq1 $ ctxParent ctx) p
                                                      "Variable declaration is not allowed in this context"-}
exprValidate1 _ _ _   ESeq{}              = return ()
exprValidate1 _ _ _   EITE{}              = return ()
exprValidate1 d _ ctx EFor{..}            = checkNoVar exprPos d ctx exprLoopVar
exprValidate1 d _ ctx (ESet _ l _)        = checkLExpr d ctx l
exprValidate1 _ _ _   EBinOp{}            = return ()
exprValidate1 _ _ _   EUnOp{}             = return ()

exprValidate1 _ _ ctx (EPHolder p)        = do
    let msg = case ctx of
                   CtxStruct EStruct{..} _ f -> "Missing field " ++ f ++ " in constructor " ++ exprConstructor
                   _               -> "_ is not allowed in this context"
    check (ctxPHolderAllowed ctx) p msg
exprValidate1 d _ ctx (EBinding p v _)    = checkNoVar p d ctx v
exprValidate1 d tvs _ (ETyped _ _ t)      = typeValidate d tvs t

-- True if a placeholder ("_") can appear in this context
ctxPHolderAllowed :: ECtx -> Bool
ctxPHolderAllowed ctx =
    case ctx of
         CtxSetL{}        -> True
         CtxTyped{}       -> pres
         CtxRuleRAtom{..} -> True
         CtxStruct{}      -> pres
         CtxTuple{}       -> pres
         CtxMatchPat{}    -> True
         CtxBinding{}     -> True
         _                -> False
    where
    par = ctxParent ctx
    pres = ctxPHolderAllowed par

checkNoVar :: (MonadError String me) => Pos -> DatalogProgram -> ECtx -> String -> me ()
checkNoVar p d ctx v = check (isNothing $ lookupVar d ctx v) p
                              $ "Variable " ++ v ++ " already defined in this scope"

-- Traverse again with types.  This pass ensures that all sub-expressions
-- have well-defined types that match their context
exprTraverseTypeME :: (MonadError String me) => DatalogProgram -> (ECtx -> ExprNode Type -> me ()) -> ECtx -> Expr -> me ()
exprTraverseTypeME d = exprTraverseCtxWithM (\ctx e -> do
    --trace ("exprTraverseTypeME " ++ show ctx ++ "\n    " ++ show e) $ return ()
    t <- exprNodeType d ctx e
    case ctxExpectType d ctx of
         Nothing -> return ()
         Just t' -> check (typesMatch d t t') (pos e)
                          $ "Couldn't match expected type " ++ show t' ++ " with actual type " ++ show t ++ " (context: " ++ show ctx ++ ")"
    return t)

exprValidate2 :: (MonadError String me) => DatalogProgram -> ECtx -> ExprNode Type -> me ()
exprValidate2 d _   (ESlice p e h l)    =
    case typ' d e of
        TBit _ w -> do check (h >= l) p
                           $ "Upper bound of the slice must be greater than lower bound"
                       check (h < w) p
                           $ "Upper bound of the slice cannot exceed argument width"
        _        -> err (pos e) $ "Expression is not a bit vector"

exprValidate2 d _   (EMatch _ _ cs)     = do
    let t = snd $ head cs
    mapM_ ((\e -> checkTypesMatch (pos e) d t e) . snd) cs

exprValidate2 d _   (EBinOp p op e1 e2) = do
    case op of
        Eq     -> m
        Neq    -> m
        Lt     -> do {m; isint1}
        Gt     -> do {m; isint1}
        Lte    -> do {m; isint1}
        Gte    -> do {m; isint1}
        And    -> do {m; isbool}
        Or     -> do {m; isbool}
        Impl   -> do {m; isbool}
        Plus   -> do {m; isint1}
        Minus  -> do {m; isint1}
        ShiftR -> do {isint1; isint2}
        ShiftL -> do {isint1; isint2}
        Mod    -> do {isint1; isint2}
        Times  -> do {isint1; isint2}
        Div    -> do {isint1; isint2}
        BAnd   -> do {m; isbit1}
        BOr    -> do {m; isbit1}
        Concat | isString d e1
               -> return ()
        Concat -> do {isbit1; isbit2}
    where m = checkTypesMatch p d e1 e2
          isint1 = check (isInt d e1 || isBit d e1) (pos e1) "Not an integer"
          isint2 = check (isInt d e2 || isBit d e2) (pos e2) "Not an integer"
          isbit1 = check (isBit d e1) (pos e1) "Not a bit vector"
          isbit2 = check (isBit d e2) (pos e2) "Not a bit vector"
          isbool = check (isBool d e1) (pos e1) "Not a Boolean"

exprValidate2 d _   (EUnOp _ BNeg e)    =
    check (isBit d e) (pos e) "Not a bit vector"
--exprValidate2 d ctx (EVarDecl p x)      = check (isJust $ ctxExpectType d ctx) p
--                                                 $ "Cannot determine type of variable " ++ x -- Context: " ++ show ctx
exprValidate2 d _   (EITE p _ t e)       = checkTypesMatch p d t e
exprValidate2 d ctx (EFor p _ i _)       = checkIterable "iterator" p d ctx i
exprValidate2 _ _   _                    = return ()

checkLExpr :: (MonadError String me) => DatalogProgram -> ECtx -> Expr -> me ()
checkLExpr d ctx e | ctxIsRuleRCond ctx =
    check (exprIsPattern e) (pos e)
        $ "Left-hand side of an assignment term can only contain variable declarations, type constructors, and tuples"
                   | otherwise =
    check (exprIsVarOrFieldLVal d ctx e || exprIsDeconstruct d e) (pos e)
        $ "Expression " ++ show e ++ " is not an l-value" -- in context " ++ show ctx

exprCheckMatchPatterns :: (MonadError String me) => DatalogProgram -> ECtx -> ExprNode Expr -> me ()
exprCheckMatchPatterns d ctx e@(EMatch _ x cs) = do
    let t = exprType d (CtxMatchExpr e ctx) x
        ct0 = typeConsTree d t
    ct <- foldM (\ct pat -> do let (leftover, abducted) = consTreeAbduct d ct pat
                               check (not $ consTreeEmpty abducted) (pos pat)
                                      "Unsatisfiable match pattern"
                               return leftover)
                ct0 (map fst cs)
    check (consTreeEmpty ct) (pos x) "Non-exhaustive match patterns"

exprCheckMatchPatterns _ _   _               = return ()

-- Automatically insert string conversion functions in the Concat
-- operator:  '"x:" ++ x', where 'x' is of type int becomes
-- '"x:" ++ int_2string(x)'.
progInjectStringConversions :: (MonadError String me) => DatalogProgram -> me DatalogProgram
progInjectStringConversions d = progExprMapCtxM d (exprInjectStringConversions d)

exprInjectStringConversions :: (MonadError String me) => DatalogProgram -> ECtx -> ENode -> me Expr
exprInjectStringConversions d ctx e@(EBinOp p Concat l r) | (te == tString) && (tr /= tString) = do
    -- find string conversion function
    fname <- case tr of
                  TBool{}     -> return $ bUILTIN_2STRING_FUNC
                  TInt{}      -> return $ bUILTIN_2STRING_FUNC
                  TString{}   -> return $ bUILTIN_2STRING_FUNC
                  TBit{}      -> return $ bUILTIN_2STRING_FUNC
                  TUser{..}   -> return $ mk2string_func typeName
                  TOpaque{..} -> return $ mk2string_func typeName
                  TTuple{}    -> err (pos r) "Automatic string conversion for tuples is not supported"
                  TVar{..}    -> err (pos r) $
                                     "Cannot automatically convert " ++ show r ++
                                     " of variable type " ++ tvarName ++ " to string"
                  TStruct{}   -> error "unexpected TStruct in exprInjectStringConversions"
    f <- case lookupFunc d fname of
              Nothing  -> err (pos r) $ "Cannot find declaration of function " ++ fname ++
                                        " needed to convert expression " ++ show r ++ " to string"
              Just fun -> return fun
    let arg0 = funcArgs f !! 0
    -- validate its signature
    check (isString d $ funcType f) (pos f)
           "string conversion function must return \"string\""
    check ((length $ funcArgs f) == 1) (pos f)
           "string conversion function must take exactly one argument"
    unifyTypes d p
           ("in the call to string conversion function \"" ++ name f ++ "\"")
           [(typ arg0, tr)]
    let r' = E $ EApply (pos r) fname [r]
    return $ E $ EBinOp p Concat l r'
    where te = exprType'' d ctx $ E e
          tr = exprType'' d (CtxBinOpR e ctx) r
          mk2string_func (c:cs) = ((toLower c) : cs) ++ tOSTRING_FUNC_SUFFIX

exprInjectStringConversions _ _   e = return $ E e

progConvertIntsToBVs :: DatalogProgram -> DatalogProgram
progConvertIntsToBVs d = progExprMapCtx d (exprConvertIntToBV d)

exprConvertIntToBV :: DatalogProgram -> ECtx -> ENode -> Expr
exprConvertIntToBV d ctx e@(EInt p v) =
    case exprType' d ctx (E e) of
         TBit _ w -> E $ EBit p w v
         _        -> E e
exprConvertIntToBV _ _ e = E e
