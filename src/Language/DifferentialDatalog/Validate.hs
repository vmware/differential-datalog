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

{-# LANGUAGE RecordWildCards, FlexibleContexts, LambdaCase, TupleSections #-}

module Language.DifferentialDatalog.Validate (
    validate) where

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
import Language.DifferentialDatalog.Module
import Language.DifferentialDatalog.Name
import {-# SOURCE #-} Language.DifferentialDatalog.Type
import Language.DifferentialDatalog.TypeInference
import Language.DifferentialDatalog.ECtx
import {-# SOURCE #-} Language.DifferentialDatalog.Expr
import Language.DifferentialDatalog.DatalogProgram
import Language.DifferentialDatalog.Relation
import Language.DifferentialDatalog.Attribute
import Language.DifferentialDatalog.Error

-- | Validate Datalog program
validate :: (MonadError String me) => DatalogProgram -> me DatalogProgram
validate d = do
    uniqNames (Just d) ("Multiple definitions of constructor " ++)
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
    fs' <- mapM (mapM (funcValidateDefinition d')) $ progFunctions d'
    -- Validate relation declarations
    rels' <- sequence $ M.map (relValidate d') $ progRelations d'
    -- Validate indexes
    idxs' <- sequence $ M.map (indexValidate d') $ progIndexes d'
    -- Validate rules
    rules' <- mapM (ruleValidate d') $ progRules d'
    -- Validate transformers
    mapM_ (transformerValidate d') $ progTransformers d'
    -- Validate transformer applications
    mapM_ (applyValidate d') $ progApplys d'
    let d'' = d' { progFunctions = fs'
                 , progRelations = rels'
                 , progIndexes   = idxs'
                 , progRules     = rules'
                 }
    -- Validate dependency graph
    depGraphValidate d''
    -- Attributes do not affect the semantics of the program and can therefore
    -- be validated last.
    progValidateAttributes d''
    return d''

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
                    check d (length as == length consArgs) p
                           $ "Number of arguments does not match constructor declaration: " ++ show cons
                    return $ zip (map name consArgs) (map snd as)
            let desugarNamed = do
                    uniq' (Just d) (\_ -> p) id ("Multiple occurrences of a field " ++) $ map fst as
                    mapM_ (\(n,e') -> check d (isJust $ find ((==n) . name) consArgs) (pos e')
                                           $ "Unknown field " ++ n) as
                    return $ map (\f -> (name f, maybe (E $ EPHolder p) id $ lookup (name f) as)) consArgs
            as' <- case as of
                        [] | null consArgs
                           -> return as
                        [] -> desugarNamed
                        _  | all (null . fst) as
                           -> desugarPos
                        _  | any (null . fst) as
                           -> err d (pos e) $ "Expression mixes named and positional arguments to type constructor " ++ c
                        _  -> desugarNamed
            return $ E e{exprStructFields = as'}
         _              -> return $ E e

typedefValidate :: (MonadError String me) => DatalogProgram -> TypeDef -> me ()
typedefValidate d@DatalogProgram{..} TypeDef{..} = do
    uniq' (Just d) (\_ -> tdefPos) id ("Multiple definitions of type argument " ++) tdefArgs
    mapM_ (\a -> check d (M.notMember a progTypedefs) tdefPos
                        $ "Type argument " ++ a ++ " conflicts with user-defined type name")
          tdefArgs
    case tdefType of
         Nothing -> return ()
         Just t  -> do
             typeValidate d tdefArgs t
             let dif = tdefArgs \\ typeTypeVars t
             check d (null dif) tdefPos
                    $ "The following type variables are not used in type definition: " ++ intercalate "," dif
    return ()

typeValidate :: (MonadError String me) => DatalogProgram -> [String] -> Type -> me ()
typeValidate _ _     TString{}        = return ()
typeValidate _ _     TInt{}           = return ()
typeValidate _ _     TBool{}          = return ()
typeValidate _ _     TDouble{}        = return ()
typeValidate _ _     TFloat{}         = return ()
typeValidate d _     (TBit p w)       =
    check d (w>0) p "Integer width must be greater than 0"
typeValidate d _     (TSigned p w)       =
    check d (w>0) p "Integer width must be greater than 0"
typeValidate d tvars (TStruct p cs)   = do
    uniqNames (Just d) ("Multiple definitions of constructor " ++) cs
    mapM_ (consValidate d tvars) cs
    mapM_ (\grp -> check d (length (nub $ map typ grp) == 1) p $
                          "Field " ++ (name $ head grp) ++ " is declared with different types")
          $ sortAndGroup name $ concatMap consArgs cs
typeValidate d tvars (TTuple _ ts)    =
    mapM_ (typeValidate d tvars) ts
typeValidate d tvars (TUser p n args) = do
    t <- checkType p d n
    let expect = length (tdefArgs t)
    let actual = length args
    check d (expect == actual) p $
           "Expected " ++ show expect ++ " type arguments to " ++ n ++ ", found " ++ show actual
    mapM_ (typeValidate d tvars) args
    return ()
typeValidate d tvars (TOpaque p n args) = do
    t <- checkType p d n
    let expect = length (tdefArgs t)
    let actual = length args
    check d (expect == actual) p $
           "Expected " ++ show expect ++ " type arguments to " ++ n ++ ", found " ++ show actual
    mapM_ (typeValidate d tvars) args
    return ()
typeValidate d tvars (TVar p v)       =
    check d (elem v tvars) p $ "Unknown type variable " ++ v

consValidate :: (MonadError String me) => DatalogProgram -> [String] -> Constructor -> me ()
consValidate d tvars Constructor{..} = do
    fieldsValidate d tvars consArgs

fieldsValidate :: (MonadError String me) => DatalogProgram -> [String] -> [Field] -> me ()
fieldsValidate d targs fields = do
    uniqNames (Just d) ("Multiple definitions of argument " ++) fields
    mapM_ (fieldValidate d targs) fields

fieldValidate :: (MonadError String me) => DatalogProgram -> [String] -> Field -> me ()
fieldValidate d targs field@Field{..} = typeValidate d targs $ typ field

checkAcyclicTypes :: (MonadError String me) => DatalogProgram -> me ()
checkAcyclicTypes d@DatalogProgram{..} = do
    let g0 :: G.Gr String ()
        g0 = G.insNodes (mapIdx (\(t,_) i -> (i, t)) $ M.toList progTypedefs) G.empty
        typIdx t = M.findIndex t progTypedefs
        gfull = M.foldlWithKey (\g tn tdef ->
                                 foldl' (\g' t' -> G.insEdge (typIdx tn, typIdx t', ()) g') g
                                        $ maybe [] (typeStaticMemberTypes d) $ tdefType tdef)
                               g0 progTypedefs
    maybe (return ())
          (\cyc -> throwError $ "Mutually recursive types: " ++
                                (intercalate " -> " $ map snd cyc))
          $ grCycle gfull

funcValidateProto :: (MonadError String me) => DatalogProgram -> [Function] -> me ()
funcValidateProto d fs = do
    let fname = name $ head fs
    let extern_idx = findIndex (isNothing . funcDef) fs
    let extern_fun = fs !! fromJust extern_idx
    check d (length fs == 1 || extern_idx == Nothing) (pos extern_fun)
        $ "Extern function '" ++ name extern_fun ++ "' clashes with function declaration(s) at\n  " ++
          (intercalate "\n  " $ map spos $ take (fromJust extern_idx)  fs ++ drop (fromJust extern_idx + 1) fs) ++
          "\nOnly non-extern functions can be overloaded."
    mapIdxM_ (\f i -> mapM_ (\j -> -- If functions have the same number of arguments, then their first arguments
                                   -- must have different types.
                                   when ((length $ funcArgs f) == (length $ funcArgs $ fs !! j)) $ do
                                       check d ((length $ funcArgs f) /= 0) (pos f)
                                         $ "Multiple declarations of function '" ++ fname ++ "()' at\n  " ++ (spos f) ++ "\n  " ++ (spos $ fs !! j)
                                       check d (not $ unifyTypes d (typ $ head $ funcArgs f) (typ $ head $ funcArgs $ fs !! j)) (pos $ head $ funcArgs f)
                                         $ "Conflicting declarations of function '" ++ name f ++ "' at\n  " ++  (spos f) ++ "\n  " ++ (spos $ fs !! j) ++
                                           "\nFunctions that share the same name and number of arguments must differ in the type of their first argument.")
                            [0..i-1]) fs
    -- Validate individual prototypes.
    mapM_ (\f@Function{..} -> do
            uniqNames (Just d) ("Multiple definitions of argument " ++) funcArgs
            let tvars = funcTypeVars f
            mapM_ (typeValidate d tvars . argType) funcArgs
            typeValidate d tvars funcType)
          fs

funcValidateDefinition :: (MonadError String me) => DatalogProgram -> Function -> me Function
funcValidateDefinition d f@Function{..} = do
    case funcDef of
         Nothing  -> return f
         Just def -> do def' <- exprValidate d (funcTypeVars f) (CtxFunc f) def
                        return f{funcDef = Just def'}

relValidate :: (MonadError String me) => DatalogProgram -> Relation -> me Relation
relValidate d rel@Relation{..} = do
    typeValidate d [] relType
    check d (isNothing relPrimaryKey || relRole == RelInput) (pos rel)
        $ "Only input relations can be declared with a primary key"
    check d (isNothing relPrimaryKey || relSemantics == RelSet) (pos rel)
        $ "Streams and multisets cannot be declared with a primary key"
    case relPrimaryKey of
         Nothing -> return rel
         Just pkey -> do pkey' <- exprValidate d [] (CtxKey rel) $ keyExpr pkey
                         return rel{relPrimaryKey = Just pkey{keyExpr = pkey'}}

indexValidate :: (MonadError String me) => DatalogProgram -> Index -> me Index
indexValidate d idx = do
    fieldsValidate d [] $ idxVars idx
    atomValidate d (CtxIndex idx) $ idxAtom idx
    val' <- exprValidate d [] (CtxIndex idx) $ atomVal $ idxAtom idx
    let idx' = idx {idxAtom = (idxAtom idx) {atomVal = val'}}
    check d (exprIsPatternImpl $ atomVal $ idxAtom idx') (pos $ idxAtom idx')
          $ "Index expression is not a pattern"
    -- Atom is defined over exactly the variables in the index.
    -- (variables in 'atom_vars \\ idx_vars' should be caught by 'atomValidate'
    -- above, so we only need to check for 'idx_vars \\ atom_vars' here).
    let idx_vars = map name $ idxVars idx'
    let atom_vars = map name $ exprFreeVars d (CtxIndex idx') (atomVal $ idxAtom idx')
    check d (null $ idx_vars \\ atom_vars) (pos idx')
          $ "The following index variables are not constrained by the index pattern: " ++
            (show $ idx_vars \\ atom_vars)
    return idx'

ruleValidate :: (MonadError String me) => DatalogProgram -> Rule -> me Rule
ruleValidate d rl@Rule{..} = do
    when (not $ null ruleRHS) $ do
        case head ruleRHS of
             RHSLiteral True _ -> return ()
             _                 -> err d (pos rl) "Rule must start with a positive literal"
    mapIdxM_ (ruleRHSValidate d rl) ruleRHS
    mapM_ (ruleLHSValidate d rl) ruleLHS
    -- It is now safe to perform type inference
    ruleValidateExpressions d rl

-- We must perform type inference on all parts of the rule at the same time.
ruleValidateExpressions :: (MonadError String me) => DatalogProgram -> Rule -> me Rule
ruleValidateExpressions d rl = do
    let rhs_es = concat $
                 mapIdx (\rhs i ->
                          case rhs of
                               RHSLiteral{..} -> [(CtxRuleRAtom rl i, atomVal rhsAtom)]
                               RHSCondition{..} -> [(CtxRuleRCond rl i, rhsExpr)]
                               RHSFlatMap{..} -> [(CtxRuleRFlatMap rl i, rhsMapExpr)]
                               RHSAggregate{..} -> [ (CtxRuleRGroupBy rl i, rhsGroupBy)
                                                   , (CtxRuleRAggregate rl i, rhsAggExpr)]
                               RHSInspect{..} -> [(CtxRuleRInspect rl i, rhsInspectExpr)])
                        $ ruleRHS rl
    let lhs_es = mapIdx (\lhs i -> (CtxRuleL rl i, atomVal lhs)) $ ruleLHS rl
    es' <- exprsTypeCheck d [] (rhs_es++lhs_es)
    -- Put type-annotated expressions back into the rule.
    let (es'', rhss') = foldl' (\(es, rhss) rhs ->
                                case rhs of
                                     RHSLiteral{..} -> (tail es, rhss ++ [rhs{rhsAtom = rhsAtom {atomVal = head es}}])
                                     RHSCondition{..} -> (tail es, rhss ++ [rhs{rhsExpr = head es}])
                                     RHSFlatMap{..} -> (tail es, rhss ++ [rhs{rhsMapExpr = head es}])
                                     RHSAggregate{..} -> (tail $ tail es, rhss ++ [rhs{rhsGroupBy = head es, rhsAggExpr = head $ tail es}])
                                     RHSInspect{..} -> (tail es, rhss ++ [rhs{rhsInspectExpr = head es}]))
                            (es', []) $ ruleRHS rl
    let ([], lhss') = foldl' (\(es, lhss) lhs -> (tail es, lhss ++ [lhs{atomVal = head es}]))
                            (es'', []) $ ruleLHS rl
    let rl' = rl{ruleLHS = lhss', ruleRHS = rhss'}

    -- Post-validate.
    let rhs_es' = concat $
                  mapIdx (\rhs i ->
                           case rhs of
                                RHSLiteral{..} -> [(CtxRuleRAtom rl' i, atomVal rhsAtom)]
                                RHSCondition{..} -> [(CtxRuleRCond rl' i, rhsExpr)]
                                RHSFlatMap{..} -> [(CtxRuleRFlatMap rl' i, rhsMapExpr)]
                                RHSAggregate{..} -> [ (CtxRuleRGroupBy rl' i, rhsGroupBy)
                                                    , (CtxRuleRAggregate rl' i, rhsAggExpr)]
                                RHSInspect{..} -> [(CtxRuleRInspect rl' i, rhsInspectExpr)])
                         $ ruleRHS rl'
    let lhs_es' = mapIdx (\lhs i -> (CtxRuleL rl' i, atomVal lhs)) $ ruleLHS rl'
    exprsPostCheck d (rhs_es'++lhs_es')
    return rl'

atomValidate :: (MonadError String me) => DatalogProgram -> ECtx -> Atom -> me ()
atomValidate d ctx atom = do
    _ <- checkRelation (pos atom) d $ atomRelation atom
    -- variable cannot be declared and used in the same atom
    uniqNames (Just d) (\v -> "Variable " ++ show v ++ " is both declared and used inside relational atom " ++ show atom)
        $ exprVarDecls d ctx $ atomVal atom

-- Validate an RHS term of a rule.  Once all RHS and LHS terms have been
-- validated, it is safe to call 'ruleValidateExpressions'.
ruleRHSValidate :: (MonadError String me) => DatalogProgram -> Rule -> RuleRHS -> Int -> me ()
ruleRHSValidate d rl@Rule{..} (RHSLiteral _ atom) idx =
    atomValidate d (CtxRuleRAtom rl idx) atom

ruleRHSValidate _ _ RHSCondition{} _ = return ()
ruleRHSValidate _ _ RHSFlatMap{} _ = return ()
ruleRHSValidate _ _ RHSInspect{} _ = return ()

ruleRHSValidate d rl RHSAggregate{} idx = do
    let RHSAggregate v group_by fname e = ruleRHS rl !! idx
    let gctx = CtxRuleRGroupBy rl idx
    check d (notElem v $ map name $ exprVars d gctx group_by) (pos e) $ "Aggregate variable " ++ v ++ " already declared in this scope"
    case lookupFuncs d fname 1 of
         Nothing  -> err d (pos e) $ "Aggregation function not found. '" ++ fname ++ "' must refer to a function that takes one argument of type Group<>"
         Just [_] -> return ()
         Just fs  -> err d (pos e) $ "Ambiguous aggregation function name '" ++ fname ++ "' may refer to:\n  " ++
                                     (intercalate "\n  " $ map name fs)
    return ()

ruleLHSValidate :: (MonadError String me) => DatalogProgram -> Rule -> Atom -> me ()
ruleLHSValidate d rl a@Atom{..} = do
    rel <- checkRelation atomPos d atomRelation
    when (relRole rel == RelInput) $ check d (null $ ruleRHS rl) (pos a)
         $ "Input relation " ++ name rel ++ " cannot appear in the head of a rule"

-- | Validate relation transformer
-- * input and output argument names must be unique
-- * all return types must be relations
-- * relation names are upper-case, function names are lower-case
-- * validate higher-order types
transformerValidate :: (MonadError String me) => DatalogProgram -> Transformer -> me ()
transformerValidate d Transformer{..} = do
    uniqNames (Just d) ("Multiple definitions of transformer argument " ++)  $ transInputs ++ transOutputs
    mapM_ (\o -> check d (hotypeIsRelation $ hofType o) (pos o)
                       "A transformer can only output relations") transOutputs
    mapM_ (\o -> case hofType o of
                      HOTypeRelation{} -> check d (isUpper $ head $ name o)       (pos o) "Relation name must start with an upper-case letter"
                      HOTypeFunction{} -> check d (not $ isUpper $ head $ name o) (pos o) "Function name may not start with an upper-case letter"
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
    check d (length applyInputs == length transInputs) (pos a)
          $ "Transformer " ++ name trans ++ " expects " ++ show (length transInputs) ++ " input arguments, but " ++
            show (length applyInputs) ++ " arguments are specified"
    check d (length applyOutputs == length transOutputs) (pos a)
          $ "Transformer " ++ name trans ++ " returns " ++ show (length transOutputs) ++ " outputs, but " ++
            show (length applyOutputs) ++ " outputs are provided"
    types <- mapM (\(decl, conc) ->
            case hofType decl of
                 HOTypeFunction{..} -> do
                     fs' <- checkFuncs (pos a) d conc (length hotArgs)
                     let (f@Function{..}):fs = fs'
                     check d (null fs) (pos a)
                           $ "Multiple definitions of function '" ++ conc ++ "' found"
                     -- FIXME: Use the unification solver to match formal and
                     -- actual type transformer arguments.
                     -- For now, insist on transformer arguments using no type variables.
                     check d (null $ funcTypeVars f) (pos a)
                           $ "Generic function '" ++ conc ++ "' cannot be passed as an argument to relation transformer"
                     mapM_ (\(farg, carg) -> check d (argMut farg == argMut carg) (pos a) $
                                             "Argument '" ++ name farg ++ "' of formal argument '" ++ name decl ++ "' of transformer '" ++ name trans ++
                                             "' differs in mutability from argument '" ++ name carg ++ "' of function '" ++ name f ++ "'")
                           $ zip hotArgs funcArgs
                     return $ (zip (map typ hotArgs) (map typ funcArgs)) ++ [(hotType, funcType)]
                 HOTypeRelation{..} -> do
                     rel <- checkRelation (pos a) d conc
                     return [(hotType, relType rel)]
                  ) $ zip (transInputs ++ transOutputs) (applyInputs ++ applyOutputs)
    bindings <- inferTypeArgs d (pos a) ("in transformer application " ++ show a) $ concat types
    mapM_ (\ta -> case M.lookup ta bindings of
                       Nothing -> err d (pos a) $ "Unable to bind type argument '" ++ ta ++
                                                " to a concrete type in transformer application " ++ show a
                       Just _  -> return ())
          $ transformerTypeVars trans
    mapM_ (\o -> check d (relRole (getRelation d o) /= RelInput) (pos a)
                 $ "Transformer output cannot be bound to input relation " ++ o
          ) applyOutputs
    -- Things will break if a relation is assigned by an 'Apply' in the top scope and then occurs inside a
    -- recursive fragment.  Keep things simple by disallowing this.
    -- If this proves an important limitation, it can be dropped and replaced with a program
    -- transformation that introduces a new relation for each 'Apply' output and a rule that
    -- concatenates it to the original output relation.  But for now it seems like a useful
    -- restriction
    mapM_ (\o -> check d (null $ relRules d o) (pos a)
                       $ "Output of a transformer application may not occur in the head of a rule, but relation " ++ o ++
                          " occurs in the following rules\n" ++ (intercalate "\n" $ map show $ relRules d o))
          applyOutputs
    -- Likewise, to relax this, modify 'compileApplyNode' to concatenate transformer output to
    -- existing relation if it exists.
    mapM_ (\o -> check d (length (relApplys d o) == 1) (pos a)
                       $ "Relation " ++ o ++ " occurs as output of multiple transformer applications")
          applyOutputs

hotypeValidate :: (MonadError String me) => DatalogProgram -> HOType -> me ()
hotypeValidate d HOTypeFunction{..} = do
    -- FIXME: hacky way to validate function type by converting it into a function.
    let f = Function hotPos [] "" hotArgs hotType Nothing
    funcValidateProto d [f]

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
                       mapM_ (\rhs -> err d (pos rl)
                                          $ "Relation " ++ (atomRelation $ rhsAtom rhs) ++ " is mutually recursive with " ++ atomRelation a ++
                                            " and therefore cannot appear negated in this rule")
                             $ filter ((== lscc) . (sccmap M.!) . atomRelation . rhsAtom)
                             $ filter (not . rhsPolarity)
                             $ filter rhsIsLiteral ruleRHS)
                  ruleLHS)
          progRules
    mapM_ (\scc -> let anode = find depNodeIsApply scc in
                   case anode of
                        Just (DepNodeApply a) -> err d (pos a)
                                                 $ "Transformer application appears in a recursive fragment consisting of the following relations: " ++
                                                 (show scc)
                        _ -> return ())
          $ filter ((> 1) . length) sccs

-- Validate and perform type inference on a single expression.
exprValidate :: (MonadError String me) => DatalogProgram -> [String] -> ECtx -> Expr -> me Expr
exprValidate d tvars ctx e = do
    e' <- head <$> exprsTypeCheck d tvars [(ctx, e)]
    exprsPostCheck d [(ctx, e')]
    return e'

-- Multiple expressions (e.g., expressions in a rule) must be validated in two
-- phases.
-- Phase 1: 'exprsTypeCheck': check that the expressions are well-formed
-- (all identifiers point to existng functions and variables) and perform type
-- inference.
-- Phase 2: 'exprsPostCheck': additional checks that can only be safely
-- performed after type inference.  Before calling this function, the caller
-- must substitute modified expressions returned by 'exprsTypeCheck' in 'd'.

exprsTypeCheck :: (MonadError String me) => DatalogProgram -> [String] -> [(ECtx, Expr)] -> me [Expr]
exprsTypeCheck d tvars es = {-trace ("exprValidate " ++ show e ++ " in \n" ++ show ctx) $ -} do
    -- First pass: make sure that expressions are well-formed before performing
    -- type inference.
    mapM_ (\(ctx, e) -> exprTraverseCtxM (exprValidate1 d tvars) ctx e) es
    -- Second pass: type inference.
    inferTypes d es

exprsPostCheck :: (MonadError String me) => DatalogProgram -> [(ECtx, Expr)] -> me ()
exprsPostCheck d es = do
    -- Pass 3,4: additional checks that can be performed once type inference has
    -- succeeded.
    mapM_ (\(ctx, e) -> exprTraverseCtxWithM (\ctx' e' -> return $ exprNodeType d ctx' e') (exprValidate2 d) ctx e) es
    mapM_ (\(ctx, e) -> exprTraverseCtxM (exprValidate3 d) ctx e) es

-- This function does not perform type checking: just checks that all functions and
-- variables are defined; the number of arguments matches declarations, etc.
exprValidate1 :: (MonadError String me) => DatalogProgram -> [String] -> ECtx -> ExprNode Expr -> me ()
exprValidate1 _ _ ctx EVar{..} | ctxInRuleRHSPositivePattern ctx
                                          = return ()
exprValidate1 d _ ctx (EVar p v)          = do _ <- checkVar p d ctx v
                                               return ()
exprValidate1 d _ _   (EApply p fnames as) = do
    fs <- concat <$> mapM (\fname -> checkFuncs p d fname (length as)) fnames
    check d (length fs == 1 || (not $ null as)) p $ "Ambiguous function name '" ++ (nameLocal $ head fnames) ++ "' may refer to:\n  " ++
                                                    (intercalate "\n  " $ map (\f -> name f ++ " at " ++ spos f) fs)
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
            check d ((length $ typeCons $ fromJust $ tdefType tdef) == 1) p
                   $ "Type constructor in the left-hand side of an assignment is only allowed for types with one constructor, \
                     \ but \"" ++ name tdef ++ "\" has multiple constructors"
exprValidate1 _ _ _   ETuple{}            = return ()
exprValidate1 d _ _   (ESlice p _ h l)    =
    check d (h >= l) p
          $ "Upper bound of the slice must be greater than lower bound"
exprValidate1 _ _ _   EMatch{}            = return ()
exprValidate1 d _ ctx (EVarDecl p v)      = do
    check d (ctxInSetL ctx || ctxInMatchPat ctx) p "Variable declaration is not allowed in this context"
    checkNoVar p d ctx v

exprValidate1 _ _ _   ESeq{}              = return ()
exprValidate1 _ _ _   EITE{}              = return ()
exprValidate1 d _ ctx EFor{..}            = checkNoVar exprPos d ctx exprLoopVar
exprValidate1 _ _ _   ESet{}              = return ()
exprValidate1 d _ ctx (EContinue p)       = check d (ctxInForLoopBody ctx) p "\"continue\" outside of a loop"
exprValidate1 d _ ctx (EBreak p)          = check d (ctxInForLoopBody ctx) p "\"break\" outside of a loop"
exprValidate1 d _ ctx (EReturn p _)       = check d (isJust $ ctxInFunc ctx) p "\"return\" outside of a function body"
exprValidate1 _ _ _   EBinOp{}            = return ()
exprValidate1 _ _ _   EUnOp{}             = return ()

exprValidate1 d _ ctx (EPHolder p)        = do
    let msg = case ctx of
                   CtxStruct EStruct{..} _ f -> "Missing field '" ++ f ++ "' in constructor " ++ exprConstructor
                   _               -> "_ is not allowed in this context"
    check d (ctxPHolderAllowed ctx) p msg
exprValidate1 d _ ctx (EBinding p v _)    = do
    checkNoVar p d ctx v

exprValidate1 d tvs _ (ETyped _ _ t)      = typeValidate d tvs t
exprValidate1 d tvs _ (EAs _ _ t)         = typeValidate d tvs t
exprValidate1 d _ ctx (ERef p _)          =
    -- Rust does not allow pattern matching inside 'Arc'
    check d (ctxInRuleRHSPattern ctx || ctxInIndex ctx) p "Dereference pattern not allowed in this context"

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
checkNoVar p d ctx v = check d (isNothing $ lookupVar d ctx v) p
                              $ "Variable " ++ v ++ " already defined in this scope"

exprValidate2 :: (MonadError String me) => DatalogProgram -> ECtx -> ExprNode Type -> me ()
exprValidate2 d _   (ESlice p e h _)    =
    case typ' d e of
        TBit _ w -> do check d (h < w) p
                           $ "Upper bound of the slice exceeds argument width"
        _        -> err d (pos e) $ "Expression is not a bit vector"
exprValidate2 d _   (EAs p e t)          = do
    check d (not (isBigInt d e && isBit d t)) p
        $ "Direct casts from bigint to bit<> are not supported; consider going through signed<>" ++ (show $ pos e)
    check d (isInteger d e || isFP d e) p
        $ "Cannot type-cast expression of type " ++ show e ++ ".  The type-cast operator is only supported for numeric types."
    check d (isInteger d t || isFP d t) p
        $ "Cannot type-cast expression to " ++ show t ++ ".  Only numeric types can be cast to."
    check d (not (isInteger d t && isFP d e)) p
        $ "There are no direct casts from floating point to integers; use the library functions int_from_*." ++ show e
    when ((isBit d t || isSigned d t) && (isBit d e || isSigned d e)) $
        check d (isBit d e == isBit d t || typeWidth e' == typeWidth t') p $
            "Conversion between signed and unsigned bit vectors only supported across types of the same bit width. " ++
            "Try casting to " ++ show (t'{typeWidth = typeWidth e'}) ++ " first."
    where
    e' = typ' d e
    t' = typ' d t
exprValidate2 _ _   _                    = return ()


checkLExpr :: (MonadError String me) => DatalogProgram -> ECtx -> Expr -> me ()
checkLExpr d ctx e | ctxIsRuleRCond ctx =
    check d (exprIsPattern e) (pos e)
        $ "Left-hand side of an assignment term can only contain variable declarations, type constructors, and tuples"
                   | otherwise =
    check d (exprIsVarOrFieldLVal d ctx e || exprIsDeconstruct d e) (pos e)
        $ "Expression is not an l-value" -- in context " ++ show ctx

exprValidate3 :: (MonadError String me) => DatalogProgram -> ECtx -> ExprNode Expr -> me ()
exprValidate3 d ctx e@(EMatch _ x cs) = do
    let t = exprType d (CtxMatchExpr e ctx) x
        ct0 = typeConsTree t
    ct <- foldM (\ct pat -> do let (leftover, abducted) = consTreeAbduct d ct pat
                               check d (not $ consTreeEmpty abducted) (pos pat)
                                      "Unsatisfiable match pattern"
                               return leftover)
                ct0 (map fst cs)
    check d (consTreeEmpty ct) (pos x) "Non-exhaustive match patterns"
exprValidate3 d ctx (ESet _ l _)         = checkLExpr d ctx l
exprValidate3 d ctx e@(EApply _ _ as)      = do
    let fun = applyExprGetFunc d ctx e
    mapM_ (\(a, mut) -> when mut $ checkLExpr d ctx a)
          $ zip as (map argMut $ funcArgs fun)

exprValidate3 _ _   _               = return ()
