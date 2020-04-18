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
import Control.Monad.Except
import Data.Maybe
import Data.List
import Data.Char
import qualified Data.Graph.Inductive as G
--import Debug.Trace

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
import Language.DifferentialDatalog.Relation
import Language.DifferentialDatalog.Module
import Language.DifferentialDatalog.Attribute

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
    -- Validate indexes
    mapM_ (indexValidate d') $ M.elems $ progIndexes d'
    -- Validate rules
    mapM_ (ruleValidate d') $ progRules d'
    -- Validate transformers
    mapM_ (transformerValidate d') $ progTransformers d'
    -- Validate transformer applications
    mapM_ (applyValidate d') $ progApplys d'
    -- Validate dependency graph
    depGraphValidate d'
    -- Insert string conversion functions
    d'' <- progInjectStringConversions d'
    -- Convert 'int' constants to 'bit<>'.
    let d''' = progConvertIntsToNums d''
    -- This check must be done after 'depGraphValidate', which may
    -- introduce recursion
    checkNoRecursion d'''
    -- Attributes do not affect the semantics of the program and can therefore
    -- be validated last.
    progValidateAttributes d'''
    return d'''

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
                    mapM_ (\(n,e') -> check (isJust $ find ((==n) . name) consArgs) (pos e')
                                           $ "Unknown field " ++ n) as
                    return $ map (\f -> (name f, maybe (E $ EPHolder p) id $ lookup (name f) as)) consArgs
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
    return ()

typeValidate :: (MonadError String me) => DatalogProgram -> [String] -> Type -> me ()
typeValidate _ _     TString{}        = return ()
typeValidate _ _     TInt{}           = return ()
typeValidate _ _     TBool{}          = return ()
typeValidate _ _     TDouble{}        = return ()
typeValidate _ _     TFloat{}         = return ()
typeValidate _ _     (TBit p w)       =
    check (w>0) p "Integer width must be greater than 0"
typeValidate _ _     (TSigned p w)       =
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
typeValidate _ tvars (TVar p v)       =
    check (elem v tvars) p $ "Unknown type variable " ++ v
typeValidate _ _     t                = error $ "typeValidate " ++ show t

consValidate :: (MonadError String me) => DatalogProgram -> [String] -> Constructor -> me ()
consValidate d tvars Constructor{..} = do
    fieldsValidate d tvars consArgs

fieldsValidate :: (MonadError String me) => DatalogProgram -> [String] -> [Field] -> me ()
fieldsValidate d targs fields = do
    uniqNames ("Multiple definitions of argument " ++) fields
    mapM_ (fieldValidate d targs) fields

fieldValidate :: (MonadError String me) => DatalogProgram -> [String] -> Field -> me ()
fieldValidate d targs field@Field{..} = typeValidate d targs $ typ field

checkAcyclicTypes :: (MonadError String me) => DatalogProgram -> me ()
checkAcyclicTypes DatalogProgram{..} = do
    let g0 :: G.Gr String ()
        g0 = G.insNodes (mapIdx (\(t,_) i -> (i, t)) $ M.toList progTypedefs) G.empty
        typIdx t = M.findIndex t progTypedefs
        gfull = M.foldlWithKey (\g tn tdef ->
                                 foldl' (\g' t' -> G.insEdge (typIdx tn, typIdx t', ()) g') g
                                        $ maybe [] typeStaticMemberTypes $ tdefType tdef)
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

indexValidate :: (MonadError String me) => DatalogProgram -> Index -> me ()
indexValidate d idx@Index{..} = do
    fieldsValidate d [] idxVars
    atomValidate d (CtxIndex idx) idxAtom
    check (exprIsPatternImpl $ atomVal idxAtom) (pos idxAtom)
          $ "Index expression is not a pattern"
    -- Atom is defined over exactly the variables in the index.
    -- (variables in 'atom_vars \\ idx_vars' should be caught by 'atomValidate'
    -- above, so we only need to check for 'idx_vars \\ atom_vars' here).
    let idx_vars = map name idxVars
    let atom_vars = exprFreeVars d (CtxIndex idx) (atomVal idxAtom)
    check (null $ idx_vars \\ atom_vars) (pos idx)
          $ "The following index variables are not constrained by the index pattern: " ++
            (show $ idx_vars \\ atom_vars)

ruleValidate :: (MonadError String me) => DatalogProgram -> Rule -> me ()
ruleValidate d rl@Rule{..} = do
    when (not $ null ruleRHS) $ do
        case head ruleRHS of
             RHSLiteral True _ -> return ()
             _                 -> err (pos rl) "Rule must start with positive literal"
    mapIdxM_ (ruleRHSValidate d rl) ruleRHS
    mapIdxM_ (ruleLHSValidate d rl) ruleLHS

atomValidate :: (MonadError String me) => DatalogProgram -> ECtx -> Atom -> me ()
atomValidate d ctx atom = do
    _ <- checkRelation (pos atom) d $ atomRelation atom
    exprValidate d [] ctx $ atomVal atom
    let vars = ctxAllVars d ctx
    -- variable cannot be declared and used in the same atom
    uniq' (\_ -> pos atom) fst (\(v,_) -> "Variable " ++ v ++ " is both declared and used inside relational atom " ++ show atom)
        $ filter (\(var, _) -> isNothing $ find ((==var) . name) vars)
        $ atomVarOccurrences ctx $ atomVal atom

ruleRHSValidate :: (MonadError String me) => DatalogProgram -> Rule -> RuleRHS -> Int -> me ()
ruleRHSValidate d rl@Rule{..} (RHSLiteral _ atom) idx =
    atomValidate d (CtxRuleRAtom rl idx) atom

ruleRHSValidate d rl@Rule{..} (RHSCondition e) idx =
    exprValidate d [] (CtxRuleRCond rl idx) e

ruleRHSValidate d rl@Rule{..} (RHSFlatMap _ e) idx = do
    let ctx = CtxRuleRFlatMap rl idx
    exprValidate d [] ctx e
    checkIterable "FlatMap expression" (pos e) d $ exprType d ctx e

ruleRHSValidate d rl RHSAggregate{} idx = do
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
    -- Group-by variables are visible in this scope.
    mapM_ (checkVar (pos e) d ctx) vs
    let group_by_types = map (typ . getVar d ctx) vs
    check (notElem v vs) (pos e) $ "Aggregate variable " ++ v ++ " already declared in this scope"
    -- Aggregation function exists and takes a group as its sole argument.
    f <- checkFunc (pos e) d fname
    check (length (funcArgs f) == 1) (pos e) $ "Aggregation function must take one argument, but " ++
                                               fname ++ " takes " ++ (show $ length $ funcArgs f) ++ " arguments"
    funcTypeArgSubsts d (pos e) f [tOpaque gROUP_TYPE [tTuple group_by_types, exprType d ctx e]]

-- | Validate relation transformer
-- * input and output argument names must be unique
-- * all return types must be relations
-- * relation names are upper-case, function names are lower-case
-- * validate higher-order types
transformerValidate :: (MonadError String me) => DatalogProgram -> Transformer -> me ()
transformerValidate d Transformer{..} = do
    uniqNames ("Multiple definitions of transformer argument " ++)  $ transInputs ++ transOutputs
    mapM_ (\o -> check (hotypeIsRelation $ hofType o) (pos o)
                       "A transformer can only output relations") transOutputs
    mapM_ (\o -> case hofType o of
                      HOTypeRelation{} -> check (isUpper $ head $ name o)       (pos o) "Relation name must start with an upper-case letter"
                      HOTypeFunction{} -> check (not $ isUpper $ head $ name o) (pos o) "Function name may not start with an upper-case letter"
          ) $ transInputs ++ transOutputs
    mapM_ (hotypeValidate d . hofType) $ transInputs ++ transOutputs

-- | Validate transformer application
-- * Transformer exists
-- * Inputs and outputs refer to valid functions and relations
-- * Input/output types match transformer declaration
-- * Outputs cannot be bound to input relations
-- * Outputs of a transformer cannot be used in the head of a rule
--   or as output of another transformer
applyValidate :: (MonadError String me) => DatalogProgram -> Apply -> me ()
applyValidate d a@Apply{..} = do
    trans@Transformer{..} <- checkTransformer (pos a) d applyTransformer
    check (length applyInputs == length transInputs) (pos a)
          $ "Transformer " ++ name trans ++ " expects " ++ show (length transInputs) ++ " input arguments, but" ++
            show (length applyInputs) ++ " arguments are specified"
    check (length applyOutputs == length transOutputs) (pos a)
          $ "Transformer " ++ name trans ++ " returns " ++ show (length transOutputs) ++ " outputs, but" ++
            show (length applyOutputs) ++ " outputs are provided"
    types <- mapM (\(decl, conc) ->
            case hofType decl of
                 HOTypeFunction{..} -> do
                     f@Function{..} <- checkFunc (pos a) d conc
                     -- FIXME: we don't have a proper unification checker; therefore insist on transformer arguments
                     -- using no type variables.
                     -- A proper unification checker should handle constraints of the form
                     -- '(exists T1 . forall T2 . E(T1,T2))', where 'T1' and 'T2' are lists of type arguments, and 'E' is
                     -- a conjunction of type congruence expressions.
                     check (null $ funcTypeVars f) (pos a)
                           $ "Generic function " ++ conc ++ " cannot be passed as an argument to relation transformer"
                     check (length hotArgs == length funcArgs) (pos a)
                           $ "Transformer " ++ name trans ++ " expects a function that takes " ++ show (length hotArgs) ++ " arguments " ++
                             " but function " ++ name f ++ " takes " ++ show (length funcArgs) ++ " arguments"
                     mapM_ (\(farg, carg) -> check (argMut farg == argMut carg) (pos a) $
                                             "Argument " ++ name farg ++ " of formal argument " ++ name decl ++ " of transformer " ++ name trans ++
                                             " differs in mutability from argument " ++ name carg ++ " of function " ++ name f)
                           $ zip hotArgs funcArgs
                     return $ (zip (map typ hotArgs) (map typ funcArgs)) ++ [(hotType, funcType)]
                 HOTypeRelation{..} -> do
                     rel <- checkRelation (pos a) d conc
                     return [(hotType, relType rel)]
                  ) $ zip (transInputs ++ transOutputs) (applyInputs ++ applyOutputs)
    bindings <- unifyTypes d (pos a) ("in transformer application " ++ show a) $ concat types
    mapM_ (\ta -> case M.lookup ta bindings of
                       Nothing -> err (pos a) $ "Unable to bind type argument '" ++ ta ++
                                                " to a concrete type in transformer application " ++ show a
                       Just _  -> return ())
          $ transformerTypeVars trans
    mapM_ (\o -> check (relRole (getRelation d o) /= RelInput) (pos a)
                 $ "Transformer output cannot be bound to input relation " ++ o
          ) applyOutputs
    -- Things will break if a relation is assigned by an 'Apply' in the top scope and then occurs inside a
    -- recursive fragment.  Keep things simple by disallowing this.
    -- If this proves an important limitation, it can be dropped and replaced with a program
    -- transformation that introduces a new relation for each 'Apply' output and a rule that
    -- concatenates it to the original output relation.  But for now it seems like a useful
    -- restriction
    mapM_ (\o -> check (null $ relRules d o) (pos a)
                       $ "Output of a transformer application may not occur in the head of a rule, but relation " ++ o ++
                          " occurs in the following rules\n" ++ (intercalate "\n" $ map show $ relRules d o))
          applyOutputs
    -- Likewise, to relax this, modify 'compileApplyNode' to concatenate transformer output to
    -- existing relation if it exists.
    mapM_ (\o -> check (length (relApplys d o) == 1) (pos a)
                       $ "Relation " ++ o ++ " occurs as output of multiple transformer applications")
          applyOutputs

hotypeValidate :: (MonadError String me) => DatalogProgram -> HOType -> me ()
hotypeValidate d HOTypeFunction{..} = do
    -- FIXME: hacky way to validate function type by converting it into a function.
    let f = Function hotPos [] "" hotArgs hotType Nothing
    funcValidateProto d f

hotypeValidate d HOTypeRelation{..} = typeValidate d (typeTypeVars hotType) hotType

-- | Check the following properties of a Datalog dependency graph:
--
--  * Linearity: A rule contains at most one RHS atom that is mutually
--  recursive with its head.
--  * Stratified negation: No loop contains a negative edge.
depGraphValidate :: (MonadError String me) => DatalogProgram -> me ()
depGraphValidate d@DatalogProgram{..} = do
    let g = progDependencyGraph d
    -- strongly connected components of the dependency graph
    let sccs = map (map (fromJust . G.lab g)) $ G.scc g
    -- maps relation name to SCC that contains this relation
    let sccmap = M.fromList
                 $ concat
                 $ mapIdx (\scc i -> mapMaybe (\case
                                           DepNodeRel rel -> Just (rel, i)
                                           _              -> Nothing) scc)
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
    -- Stratified negation:
    -- * a relation may not recursively depend on its negation;
    -- * Apply nodes may not occur in recursive loops, as they are assumed to always introduce
    --   negative loops
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
    mapM_ (\scc -> let anode = find depNodeIsApply scc in
                   case anode of
                        Just (DepNodeApply a) -> err (pos a)
                                                 $ "Transformer application appears in a recursive fragment consisting of the following relations: " ++
                                                 (show scc)
                        _ -> return ())
          $ filter ((> 1) . length) sccs

exprValidate :: (MonadError String me) => DatalogProgram -> [String] -> ECtx -> Expr -> me ()
exprValidate d tvars ctx e = {-trace ("exprValidate " ++ show e ++ " in \n" ++ show ctx) $ -} do
    exprTraverseCtxM (exprValidate1 d tvars) ctx e
    exprTraverseTypeME d (exprValidate2 d) ctx e
    exprTraverseCtxM (exprCheckMatchPatterns d) ctx e

-- This function does not perform type checking: just checks that all functions and
-- variables are defined; the number of arguments matches declarations, etc.
exprValidate1 :: (MonadError String me) => DatalogProgram -> [String] -> ECtx -> ExprNode Expr -> me ()
exprValidate1 _ _ ctx EVar{..} | ctxInRuleRHSPositivePattern ctx
                                          = return ()
exprValidate1 d _ ctx (EVar p v)          = do _ <- checkVar p d ctx v
                                               return ()
exprValidate1 d _ ctx (EApply p f as)     = do
    fun <- checkFunc p d f
    check (length as == length (funcArgs fun)) p
          "Number of arguments does not match function declaration"
    mapM_ (\(a, mut) -> when mut $ checkLExpr d ctx a)
          $ zip as (map argMut $ funcArgs fun)
exprValidate1 _ _ _   EField{}            = return ()
exprValidate1 _ _ _   ETupField{}         = return ()
exprValidate1 _ _ _   EBool{}             = return ()
exprValidate1 _ _ _   EInt{}              = return ()
exprValidate1 _ _ _   EFloat{}            = return ()
exprValidate1 _ _ _   EDouble{}           = return ()
exprValidate1 _ _ _   EString{}           = return ()
exprValidate1 _ _ _   EBit{}              = return ()
exprValidate1 _ _ _   ESigned{}           = return ()
exprValidate1 d _ ctx (EStruct p c _)     = do -- initial validation was performed by exprDesugar
    let tdef = consType d c
    case find ctxIsSetL $ ctxAncestors ctx of
         Nothing -> return ()
         Just ctx' -> when (not $ ctxIsRuleRCond $ ctxParent ctx') $ do
            check ((length $ typeCons $ fromJust $ tdefType tdef) == 1) p
                   $ "Type constructor in the left-hand side of an assignment is only allowed for types with one constructor, \
                     \ but \"" ++ name tdef ++ "\" has multiple constructors"
exprValidate1 _ _ _   ETuple{}            = return ()
exprValidate1 _ _ _   ESlice{}            = return ()
exprValidate1 _ _ _   EMatch{}            = return ()
exprValidate1 d _ ctx (EVarDecl p v)      = do
    check (ctxInSetL ctx || ctxInMatchPat ctx) p "Variable declaration is not allowed in this context"
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
exprValidate1 _ _ ctx (EContinue p)       = check (ctxInForLoopBody ctx) p "\"continue\" outside of a loop"
exprValidate1 _ _ ctx (EBreak p)          = check (ctxInForLoopBody ctx) p "\"break\" outside of a loop"
exprValidate1 _ _ ctx (EReturn p _)       = check (isJust $ ctxInFunc ctx) p "\"return\" outside of a function body"
exprValidate1 _ _ _   EBinOp{}            = return ()
exprValidate1 _ _ _   EUnOp{}             = return ()

exprValidate1 _ _ ctx (EPHolder p)        = do
    let msg = case ctx of
                   CtxStruct EStruct{..} _ f -> "Missing field '" ++ f ++ "' in constructor " ++ exprConstructor
                   _               -> "_ is not allowed in this context"
    check (ctxPHolderAllowed ctx) p msg
exprValidate1 d _ ctx (EBinding p v _)    = do
    checkNoVar p d ctx v

exprValidate1 d tvs _ (ETyped _ _ t)      = typeValidate d tvs t
exprValidate1 d tvs _ (EAs _ _ t)         = typeValidate d tvs t
exprValidate1 _ _ ctx (ERef p _)          =
    -- Rust does not allow pattern matching inside 'Arc'
    check (ctxInRuleRHSPattern ctx || ctxInIndex ctx) p "Dereference pattern not allowed in this context"

-- True if a placeholder ("_") can appear in this context
ctxPHolderAllowed :: ECtx -> Bool
ctxPHolderAllowed ctx =
    case ctx of
         CtxSetL{}        -> True
         CtxTyped{}       -> pres
         CtxRuleRAtom{}   -> True
         CtxStruct{}      -> pres
         CtxTuple{}       -> pres
         CtxMatchPat{}    -> True
         CtxBinding{}     -> True
         CtxRef{}         -> True
         CtxIndex{}       -> True
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
        Lt     -> m -- support comparisons for all datatypes
        Gt     -> m
        Lte    -> m
        Gte    -> m
        And    -> do {m; isbool}
        Or     -> do {m; isbool}
        Impl   -> do {m; isbool}
        Plus   -> do {m; isNumber1}
        Minus  -> do {m; isNumber1}
        ShiftR -> do isint1
        ShiftL -> do isint1
        Mod    -> do {m; isint1; isint2}
        Times  -> do {m; isNumber1; isNumber2}
        Div    -> do {m; isNumber1; isNumber2}
        BAnd   -> do {m; isbitOrSigned1}
        BOr    -> do {m; isbitOrSigned1}
        BXor   -> do {m; isbitOrSigned1}
        Concat | isString d e1
               -> return ()
        Concat -> do {isbit1; isbit2}
    where m = checkTypesMatch p d e1 e2
          isint1 = check (isBigInt d e1 || isBit d e1 || isSigned d e1) (pos e1) "Not an integer"
          isint2 = check (isBigInt d e2 || isBit d e2 || isSigned d e2) (pos e2) "Not an integer"
          isNumber1 = check (isInteger d e1 || isFP d e1) (pos e1) "Not a number"
          isNumber2 = check (isInteger d e2 || isFP d e2) (pos e2) "Not a number"
          isbit1 = check (isBit d e1) (pos e1) "Not a bit vector"
          isbitOrSigned1 = check (isBit d e1 || isSigned d e1) (pos e1) "Not a bit<> or signed<> value"
          isbit2 = check (isBit d e2) (pos e2) "Not a bit vector"
          isbool = check (isBool d e1) (pos e1) "Not a Boolean"

exprValidate2 d _   (EUnOp _ BNeg e)    =
    check (isBit d e || isSigned d e) (pos e) "Not a bit vector"
exprValidate2 d _   (EUnOp _ UMinus e)    =
    check (isSigned d e || isBigInt d e || isFP d e) (pos e)
        $ "Cannot negate expression of type " ++ show e ++ ". Negation applies to signed<> and bigint values only."
--exprValidate2 d ctx (EVarDecl p x)      = check (isJust $ ctxExpectType d ctx) p
--                                                 $ "Cannot determine type of variable " ++ x -- Context: " ++ show ctx
exprValidate2 d _   (EITE p _ t e)       = checkTypesMatch p d t e
exprValidate2 d _   (EFor p _ i _)       = checkIterable "iterator" p d i
exprValidate2 d _   (EAs p e t)          = do
    check (not (isBigInt d e && isBit d t)) p
        $ "Direct casts from bigint to bit<> are not supported; consider going through signed<>" ++ (show $ pos e)
    check (isInteger d e || isFP d e) p
        $ "Cannot type-cast expression of type " ++ show e ++ ".  The type-cast operator is only supported for numeric types."
    check (isInteger d t || isFP d t) p
        $ "Cannot type-cast expression to " ++ show t ++ ".  Only numeric types can be cast to."
    check (not (isInteger d t && isFP d e)) p
        $ "There are no direct casts from floating point to integers; use the library functions int_from_*." ++ show e
    when ((isBit d t || isSigned d t) && (isBit d e || isSigned d e)) $
        check (isBit d e == isBit d t || typeWidth e' == typeWidth t') p $
            "Conversion between signed and unsigned bit vectors only supported across types of the same bit width. " ++
            "Try casting to " ++ show (t'{typeWidth = typeWidth e'}) ++ " first."
    where
    e' = typ' d e
    t' = typ' d t
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
        ct0 = typeConsTree t
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
                  TSigned{}   -> return $ bUILTIN_2STRING_FUNC
                  TDouble{}   -> return $ bUILTIN_2STRING_FUNC
                  TFloat{}    -> return $ bUILTIN_2STRING_FUNC
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
    _ <- unifyTypes d p
           ("in the call to string conversion function \"" ++ name f ++ "\"")
           [(typ arg0, tr)]
    let r' = E $ EApply (pos r) fname [r]
    return $ E $ EBinOp p Concat l r'
    where te = exprType'' d ctx $ E e
          tr = exprType'' d (CtxBinOpR e ctx) r
          mk2string_func cs = scoped scope $ ((toLower $ head local) : tail local) ++ tOSTRING_FUNC_SUFFIX
              where scope = nameScope cs
                    local = nameLocal cs

exprInjectStringConversions _ _   e = return $ E e

progConvertIntsToNums :: DatalogProgram -> DatalogProgram
progConvertIntsToNums d = progExprMapCtx d (exprConvertIntToNum d)

exprConvertIntToNum :: DatalogProgram -> ECtx -> ENode -> Expr
exprConvertIntToNum d ctx e@(EInt p v) =
    case exprType' d ctx (E e) of
         TBit _ w    -> E $ EBit p w v
         TSigned _ w -> E $ ESigned p w v
         TFloat _    -> E $ EFloat p (fromInteger v)
         TDouble _   -> E $ EDouble p (fromInteger v)
         _           -> E e
exprConvertIntToNum _ _ e = E e
