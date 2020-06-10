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

{-# LANGUAGE TupleSections, LambdaCase, RecordWildCards #-}

{- |
Module     : DatalogProgram
Description: Helper functions for manipulating 'DatalogProgram'.
-}
module Language.DifferentialDatalog.DatalogProgram (
    progExprMapCtxM,
    progExprMapCtx,
    progTypeMapM,
    progTypeMap,
    progRHSMapM,
    progRHSMap,
    progAtomMapM,
    progAtomMap,
    progAttributeMapM,
    DepGraphNode(..),
    depNodeIsRel,
    depNodeIsApply,
    DepGraph,
    progDependencyGraph,
    depGraphToDot,
    progMirrorInputRelations,
    progOutputInternalRelations,
    injectDebuggingHooks,
)
where

import qualified Data.Graph.Inductive              as G
import qualified Data.Map                          as M
import Data.List
import Data.Maybe
import Data.Char
import Control.Monad.Identity
import qualified Data.Text.Lazy                    as T
import qualified Data.GraphViz                     as GV
import qualified Data.GraphViz.Attributes.Complete as GV
import qualified Data.GraphViz.Printing            as GV

import Language.DifferentialDatalog.Util
import Language.DifferentialDatalog.Name
import Language.DifferentialDatalog.Syntax
import Language.DifferentialDatalog.Expr
import {-# SOURCE #-} Language.DifferentialDatalog.Rule
import {-# SOURCE #-} Language.DifferentialDatalog.Type
import {-# SOURCE #-} qualified Language.DifferentialDatalog.Compile as Compile
import Language.DifferentialDatalog.Pos

-- | Map function 'fun' over all expressions in a program
progExprMapCtxM :: (Monad m) => DatalogProgram -> (ECtx -> ENode -> m Expr) -> m DatalogProgram
progExprMapCtxM d fun = do
    rels' <- M.fromList <$>
             (mapM (\(rname, rel) -> (rname,) <$> relExprMapCtxM fun rel) $ M.toList $ progRelations d)
    idxs' <- M.fromList <$>
             (mapM (\(rname, idx) -> do atom' <- atomExprMapCtxM fun (CtxIndex idx) (idxAtom idx)
                                        return (rname, idx{idxAtom = atom'})) $ M.toList $ progIndexes d)
    funcs' <- mapM (\f -> do e <- case funcDef f of
                                       Nothing -> return Nothing
                                       Just e  -> Just <$> exprFoldCtxM fun (CtxFunc f) e
                             return f{funcDef = e})
                   $ progFunctions d
    rules' <- mapM (\r -> do lhs <- mapIdxM (\a i -> atomExprMapCtxM fun (CtxRuleL r i) a) $ ruleLHS r
                             rhs <- mapIdxM (\x i -> rhsExprMapCtxM fun r i x) $ ruleRHS r
                             return r{ruleLHS = lhs, ruleRHS = rhs})
                   $ progRules d
    return d{ progFunctions = funcs'
            , progRelations = rels'
            , progIndexes   = idxs'
            , progRules     = rules'}

relExprMapCtxM :: (Monad m) => (ECtx -> ENode -> m Expr) -> Relation -> m Relation
relExprMapCtxM fun rel = do
    pkey' <- mapM (\key -> do e' <- exprFoldCtxM fun (CtxKey rel) $ keyExpr key
                              return key{keyExpr = e'})
             $ relPrimaryKey rel
    return rel{relPrimaryKey = pkey'}

atomExprMapCtxM :: (Monad m) => (ECtx -> ENode -> m Expr) -> ECtx -> Atom -> m Atom
atomExprMapCtxM fun ctx a = do
    v <- exprFoldCtxM fun ctx $ atomVal a
    return a{atomVal = v}

rhsExprMapCtxM :: (Monad m) => (ECtx -> ENode -> m Expr) -> Rule -> Int -> RuleRHS -> m RuleRHS
rhsExprMapCtxM fun r rhsidx l@RHSLiteral{}   = do
    a <- atomExprMapCtxM fun (CtxRuleRAtom r rhsidx) (rhsAtom l)
    return l{rhsAtom = a}
rhsExprMapCtxM fun r rhsidx c@RHSCondition{} = do
    e <- exprFoldCtxM fun (CtxRuleRCond r rhsidx) (rhsExpr c)
    return c{rhsExpr = e}
rhsExprMapCtxM fun r rhsidx a@RHSAggregate{} = do
    e <- exprFoldCtxM fun (CtxRuleRAggregate r rhsidx) (rhsAggExpr a)
    return a{rhsAggExpr = e}
rhsExprMapCtxM fun r rhsidx m@RHSFlatMap{}   = do
    e <- exprFoldCtxM fun (CtxRuleRFlatMap r rhsidx) (rhsMapExpr m)
    return m{rhsMapExpr = e}
rhsExprMapCtxM fun r rhsidx i@RHSInspect{}   = do
    e <- exprFoldCtxM fun (CtxRuleRInspect r rhsidx) (rhsInspectExpr i)
    return i{rhsInspectExpr = e}

progExprMapCtx :: DatalogProgram -> (ECtx -> ENode -> Expr) -> DatalogProgram
progExprMapCtx d fun = runIdentity $ progExprMapCtxM d  (\ctx e -> return $ fun ctx e)


-- | Apply function to all types referenced in the program
progTypeMapM :: (Monad m) => DatalogProgram -> (Type -> m Type) -> m DatalogProgram
progTypeMapM d@DatalogProgram{..} fun = do
    ts <- M.traverseWithKey (\_ (TypeDef p atrs n a t) -> TypeDef p atrs n a <$> mapM (typeMapM fun) t) progTypedefs
    fs <- M.traverseWithKey (\_ f -> do ret <- typeMapM fun $ funcType f
                                        as  <- mapM (\a -> setType a <$> (typeMapM fun $ typ a)) $ funcArgs f
                                        def <- mapM (exprTypeMapM fun) $ funcDef f
                                        return f{ funcType = ret, funcArgs = as, funcDef = def }) progFunctions
    trans <- M.traverseWithKey (\_ t -> do inputs  <- mapM (\i -> do t' <- hotypeTypeMapM (hofType i) fun
                                                                     return i{hofType = t'}) $ transInputs t
                                           outputs <- mapM (\o -> do t' <- hotypeTypeMapM (hofType o) fun
                                                                     return o{hofType = t'}) $ transOutputs t
                                           return t{ transInputs = inputs, transOutputs = outputs }) progTransformers
    rels <- M.traverseWithKey (\_ rel -> setType rel <$> (typeMapM fun $ typ rel)) progRelations
    idxs <- M.traverseWithKey (\_ idx -> do vars <- mapM (\v -> setType v <$> (typeMapM fun $ typ v)) $ idxVars idx
                                            atomval <- exprTypeMapM fun $ atomVal $ idxAtom idx
                                            let atom = (idxAtom idx) { atomVal = atomval }
                                            return idx { idxVars = vars, idxAtom = atom }) progIndexes
    rules <- mapM (ruleTypeMapM fun) progRules
    return d { progTypedefs     = ts
             , progFunctions    = fs
             , progTransformers = trans
             , progRelations    = rels
             , progIndexes      = idxs
             , progRules        = rules
             }

hotypeTypeMapM :: (Monad m) => HOType -> (Type -> m Type) -> m HOType
hotypeTypeMapM hot@HOTypeRelation{..} fun = do
    t <- typeMapM fun hotType
    return hot { hotType = t }
hotypeTypeMapM hot@HOTypeFunction{..} fun = do
    ret <- typeMapM fun hotType
    as  <- mapM (\f -> setType f <$> (typeMapM fun $ typ f)) hotArgs
    return hot { hotArgs = as, hotType = ret }

progTypeMap :: DatalogProgram -> (Type -> Type) -> DatalogProgram
progTypeMap d fun = runIdentity $ progTypeMapM d (return . fun)

-- | Apply function to all rule RHS terms in the program
progRHSMapM :: (Monad m) => DatalogProgram -> (RuleRHS -> m RuleRHS) -> m DatalogProgram
progRHSMapM d fun = do
    rs <- mapM (\r -> do
                 rhs <- mapM fun $ ruleRHS r
                 return r { ruleRHS = rhs })
               $ progRules d
    return d { progRules = rs }

progRHSMap :: DatalogProgram -> (RuleRHS -> RuleRHS) -> DatalogProgram
progRHSMap d fun = runIdentity $ progRHSMapM d (return . fun)

-- | Apply function to all atoms in the program
progAtomMapM :: (Monad m) => DatalogProgram -> (Atom -> m Atom) -> m DatalogProgram
progAtomMapM d fun = do
    rs <- mapM (\r -> do
                 lhs <- mapM fun $ ruleLHS r
                 rhs <- mapM (\case
                               lit@RHSLiteral{} -> do a <- fun $ rhsAtom lit
                                                      return lit { rhsAtom = a }
                               rhs              -> return rhs) $ ruleRHS r
                 return r { ruleLHS = lhs, ruleRHS = rhs })
               $ progRules d
    is <- mapM (\i -> do a <- fun $ idxAtom i
                         return i { idxAtom = a })
               $ progIndexes d
    return d { progRules = rs
             , progIndexes = is }

progAtomMap :: DatalogProgram -> (Atom -> Atom) -> DatalogProgram
progAtomMap d fun = runIdentity $ progAtomMapM d (return . fun)

-- | Apply function to all attributes in the program.
progAttributeMapM :: (Monad m) => DatalogProgram -> (Attribute -> m Attribute) -> m DatalogProgram
progAttributeMapM d fun = do
    tdefs' <- M.traverseWithKey (\_ tdef@TypeDef{..} -> do
        atrs' <- mapM fun tdefAttrs
        t' <- mapM (typeAttributeMapM fun) tdefType
        return $ tdef{ tdefAttrs = atrs'
                     , tdefType = t' }) $ progTypedefs d
    return d{progTypedefs = tdefs'}

typeAttributeMapM :: (Monad m) => (Attribute -> m Attribute) -> Type -> m Type
typeAttributeMapM fun t@TStruct{..} = do
    cs' <- mapM (consAttributeMapM fun) typeCons
    return t{typeCons = cs'}
typeAttributeMapM _ t = return t

consAttributeMapM :: (Monad m) => (Attribute -> m Attribute) -> Constructor -> m Constructor
consAttributeMapM fun c@Constructor{..} = do
    attrs' <- mapM fun consAttrs
    fields' <- mapM (fieldAttributeMapM fun) consArgs
    return c{ consAttrs = attrs'
            , consArgs = fields'}

fieldAttributeMapM :: (Monad m) => (Attribute -> m Attribute) -> Field -> m Field
fieldAttributeMapM fun f@Field{..} = do
    attrs' <- mapM fun fieldAttrs
    return f{fieldAttrs = attrs'}

data DepGraphNode = DepNodeRel   String
                  | DepNodeApply Apply

instance Show DepGraphNode where
    show (DepNodeRel rel) = rel
    show (DepNodeApply a) = show a

depNodeIsApply :: DepGraphNode -> Bool
depNodeIsApply (DepNodeApply _) = True
depNodeIsApply _                = False

depNodeIsRel :: DepGraphNode -> Bool
depNodeIsRel (DepNodeRel _) = True
depNodeIsRel _              = False

type DepGraph = G.Gr DepGraphNode Bool

-- | Dependency graph among program relations.  An edge from Rel1 to
-- Rel2 means that there is a rule with Rel1 in the right-hand-side,
-- and Rel2 in the left-hand-side.  Edge label is equal to the
-- polarity with which Rel1 occurs in the rule.
--
-- In addition, we conservatively add both a positive and a negative edge
-- from Rel1 to Rel2 if they appear respectively as input and output of a
-- transformer application (since we currently don't have a way of knowing
-- if the transformer is monotonic).
--
-- Assumes that rules and relations have been validated before calling
-- this function.
progDependencyGraph :: DatalogProgram -> DepGraph
progDependencyGraph DatalogProgram{..} = G.insEdges (edges ++ apply_edges) g1
    where
    g0 = G.insNodes (zip [0..] $ map DepNodeRel $ map name $ M.elems progRelations) G.empty
    indexed_applys = zip [M.size progRelations ..] progApplys
    g1 = G.insNodes (map (mapSnd DepNodeApply) indexed_applys) g0
    relidx rel = M.findIndex rel progRelations
    edges = concatMap (\Rule{..} ->
                        concatMap (\a ->
                                    mapMaybe (\case
                                               RHSLiteral pol a' -> Just (relidx $ atomRelation a', relidx $ atomRelation a, pol)
                                               _ -> Nothing)
                                             ruleRHS)
                                  ruleLHS)
                      progRules
    apply_edges = concatMap (\(idx, Apply{..}) ->
                             let inp_rels = filter (isUpper . head) applyInputs in
                             map (\i -> (relidx i, idx, True)) inp_rels ++
                             map (\o -> (idx, relidx o, True)) applyOutputs ++
                             map (\o -> (idx, relidx o, False)) applyOutputs)
                  indexed_applys

depGraphToDot :: DepGraph -> String
depGraphToDot gr =
  show $ GV.runDotCode $ GV.toDot $ GV.graphToDot params gr
  where
    params = GV.nonClusteredParams {
        GV.fmtNode = \(_, l) -> [GV.Label $ GV.StrLabel $ T.pack $ show l]
    }

-- convert all intermediate relations into output relations
progOutputInternalRelations :: DatalogProgram -> DatalogProgram
progOutputInternalRelations d =
  d { progRelations = M.map
      (\r -> r { relRole = if relRole r == RelInternal
                           then RelOutput else relRole r }) $ progRelations d }

-- create an output relation for each input relation
progMirrorInputRelations :: DatalogProgram -> String -> DatalogProgram
progMirrorInputRelations d prefix =
  let
    inputRels = M.toList $ M.filter (\r -> relRole r == RelInput) $ progRelations d
    relCopies = map (\(n,r) -> (prefix ++ n, r { relRole = RelOutput,
                                                 relName = prefix ++ relName r,
                                                 relPrimaryKey = Nothing
                                               })) $ inputRels
    makeRule relName relation = Rule { rulePos = relPos relation,
                                       ruleLHS = [Atom { atomPos = relPos relation,
                                                         atomRelation = prefix ++ relName,
                                                         atomVal = eVar "x"
                                                       }],
                                       ruleRHS = [RHSLiteral { rhsPolarity = True,
                                                               rhsAtom = Atom { atomPos = relPos relation,
                                                                                atomRelation = relName,
                                                                                atomVal = eVar "x"
                                                                              }}]}
    rules = map (\(n,r) -> makeRule n r) inputRels
  in d { progRelations = M.union (progRelations d) $ M.fromList relCopies,
         progRules     = (progRules d) ++ rules }

-- For RHSLiteral, a binding to the expression is inserted if it's not bound to a variable.
-- For example, R(a, b, z, _) gets transformed into __r0 in R(a, b, z, _),
addBindingToRHSLiteral :: (RuleRHS, Int) -> RuleRHS
addBindingToRHSLiteral (r@RHSLiteral{}, index) =
  let
    bindingName = "__" ++ (map toLower $ atomRelation $ rhsAtom r) ++ (show index)
    expr = atomVal $ rhsAtom r
    exprNode = enode expr
    updatedAtomVal = case exprNode of
                     EBinding{} -> expr
                     _          -> eBinding bindingName expr
    updatedAtom = (rhsAtom r) { atomVal = updatedAtomVal }
  in r { rhsAtom = updatedAtom }
addBindingToRHSLiteral (rule, _) = rule

-- For RHSAggregate, the aggregate function is prepended with __debug_
-- The input to the aggregate function is transformed into a tuple of
-- input to the aggregate operator and the original value.
-- The return variable is also prepended with __inputs_, which will now be
-- a tuple.
-- The corrddesponding compiler-generated function also outputs the set of
-- inputs, so that it is visible to the inspect operator.
-- an RHSCondition is also appended that declares and sets the original
-- return variable of the pre-updated aggregate operator.
updateRHSAggregate :: DatalogProgram -> Rule -> Int -> [RuleRHS]
updateRHSAggregate d rule index =
  let
     r = (ruleRHS rule) !! index
     funcName = "__debug_" ++ (rhsAggFunc r)
     varRet = "__inputs_" ++ (rhsVar r)
     input = eTuple [head $ Compile.recordAfterPrefix d rule (index - 1), (rhsAggExpr r)]
     rAgg = RHSAggregate { rhsVar = varRet,
                           rhsGroupBy = rhsGroupBy r,
                           rhsAggFunc = funcName,
                           rhsAggExpr = input }
     rCond = RHSCondition { rhsExpr = eSet (eVarDecl $ rhsVar r) (eTupField (eVar varRet) 1) }
  in [ rAgg, rCond ]

-- Currently operator id of 0 is used on all injected inspect expression.
-- TODO: Figure out what operator id to use for each rule.
noOperatorIdExpr :: Expr
noOperatorIdExpr =
  E ETuple { exprPos = nopos,
             exprTupleFields = [ E EBit { exprPos = nopos,
                                          exprWidth = 32,
                                          exprIVal = 0 },
                                 E EBit { exprPos = nopos,
                                          exprWidth = 32,
                                          exprIVal = 0 },
                                 E EBit { exprPos = nopos,
                                          exprWidth = 32,
                                          exprIVal = 0 } ] }

ddlogWeightExpr :: Expr
ddlogWeightExpr = eVar "ddlog_weight"

ddlogTimestampExpr :: Expr
ddlogTimestampExpr = eVar "ddlog_timestamp"

generateInspectDebugJoin :: DatalogProgram -> Rule -> Int -> [RuleRHS]
generateInspectDebugJoin d rule index =
  let
    input1 = head $ Compile.recordAfterPrefix d rule (index - 1)
    input2 = eVar $ exprVar $ enode $ atomVal $ rhsAtom (ruleRHS rule !! index)
    outputs = Compile.recordAfterPrefix d rule index
  in map (\output -> RHSInspect {rhsInspectExpr = eApply "debug.debug_event_join"
                                                  [noOperatorIdExpr, ddlogWeightExpr, ddlogTimestampExpr, input1, input2, output]}) outputs

generateInspectDebug :: DatalogProgram -> Rule -> Int -> [RuleRHS]
generateInspectDebug d rule index =
  let
    input1 = if index == 0
                then eVar $ exprVar $ enode $ atomVal $ rhsAtom $ head $ ruleRHS rule
                else head $ Compile.recordAfterPrefix d rule (index - 1)
    outputs = Compile.recordAfterPrefix d rule index
  in map (\output -> RHSInspect {rhsInspectExpr = eApply "debug.debug_event"
                                                  [noOperatorIdExpr, ddlogWeightExpr, ddlogTimestampExpr, input1, output]}) outputs

generateInspectDebugAggregate :: DatalogProgram -> Rule -> Int -> [RuleRHS]
generateInspectDebugAggregate d rule index =
  let
    input1 = eTupField (eVar $ rhsVar $ (ruleRHS rule !! index)) 0
    outputs = Compile.recordAfterPrefix d rule index
  in map (\output -> RHSInspect {rhsInspectExpr = eApply "debug.debug_event"
                                                  [noOperatorIdExpr, ddlogWeightExpr, ddlogTimestampExpr, input1, output]}) outputs

mkInspect :: DatalogProgram -> Rule -> Int -> Maybe [RuleRHS]
mkInspect d rule index =
  let rhsRule = ruleRHS rule
  in if index == 0 && index < length rhsRule - 1
        then Nothing
        else if rhsIsCondition (rhsRule !! index) && index /= length rhsRule - 1 && rhsIsCondition (rhsRule !! (index + 1))
                then Nothing
                else if index == 0
                     then Just $ generateInspectDebug d rule index -- single term rule
                     else case rhsRule !! index of
                          RHSLiteral{rhsPolarity=True} -> Just $ generateInspectDebugJoin d rule index -- join
                          RHSAggregate{} -> Just $ generateInspectDebugAggregate d rule index -- aggregate
                          _ -> Just $ generateInspectDebug d rule index -- antijoin, flatmap, filter/assignment, inspect

-- Insert inspect debug hook after each RHS term, except for the following:
-- 1. If a group of conditions appear consecutively, inspect debug hook is only
-- inserted after the last condition in the group.
-- 2. Inspect debug hook is not inserted after the first term, unless the rule
-- only contains one literal.
-- 3. If a rule has multiple heads, then multiple inspect is inserted after the last
-- term corresponding to each head.
insertRHSInspectDebugHooks :: DatalogProgram -> Rule -> [RuleRHS]
insertRHSInspectDebugHooks d rule =
  concatMap (\i -> let inspect = concat $ maybeToList $ mkInspect d rule i in
                   (ruleRHS rule !! i) : inspect) [0..length (ruleRHS rule) - 1]

updateRHSRules :: DatalogProgram ->  Rule -> [RuleRHS]
updateRHSRules d rule =
  let
    -- First pass updates RHSLiteral without any binding with a binding.
    rhs =  map (\r -> case r of
                      (RHSLiteral True _ , _) -> addBindingToRHSLiteral r
                      _                       -> fst r) $ zip (ruleRHS rule) [0..]
    -- Second pass updates RHSAggregate to use the debug function (so that inputs are not dropped).
    rhs' = concatMap (\i -> case rhs !! i of
                            RHSAggregate{} -> updateRHSAggregate d rule {ruleRHS = rhs} i
                            _              -> [rhs !! i]) $ [0..length rhs - 1]
  in insertRHSInspectDebugHooks d rule {ruleRHS = rhs'}

-- Insert an aggregate function that wraps the original function used in the aggregate term.
-- For example, if an aggregate operator uses std.group_max(), i.e., var c = Aggregate((a), group_max(b)).
-- The following aggregate function is generated:
-- function __debug_std.group_max (g: std.Group<'K,('I, 'V)>): (std.Set<'I>, 'V)
-- {
--    ((var inputs, var original_group) = debug.debug_split_group(g);
--     (inputs, std.group_max(original_group)))
-- }
-- In the above example, fname is the original function name prefixed with __debug_.
-- debug_split_group takes in a Group of tuple ('I, 'V) and splits it into a
-- Set of 'I and Group of 'V.
insertDebugAggregateFunction :: M.Map String Function -> String -> String -> M.Map String Function
insertDebugAggregateFunction functions fname origFname=
  let
    funcBody = eSeq (eSet (eTuple [eVarDecl "inputs", eVarDecl "original_group"])
                          (eApply "debug.debug_split_group" [eVar "g"]))
                    (eTuple [eVar "inputs", eApply origFname [eVar "original_group"]])
    function = Function {funcPos = nopos,
                         funcAttrs = [],
                         funcName = fname,
                         funcArgs = [FuncArg {argPos = nopos,
                                              argName = "g",
                                              argMut = False,
                                              argType = tOpaque "std.Group" [tVar "K", tTuple [tVar "I", tVar "V"]]}],
                         funcType = tTuple [tOpaque "std.Set" [tVar "I"], tVar "V"],
                         funcDef = Just funcBody}
  in M.insert fname function functions

-- Generate and insert into the map of functions a wrapper aggregate function for
-- each aggregate function used in the rule.
updateFunctions :: [Rule] -> M.Map String Function -> M.Map String Function
updateFunctions rules functions =
  let
    aggregates = filter rhsIsAggregate $ concatMap ruleRHS rules
  in foldl' (\acc aggregate -> insertDebugAggregateFunction acc ("__debug_" ++ (rhsAggFunc aggregate)) (rhsAggFunc aggregate)) functions aggregates

-- Perform datalog program transform by injecting debugging hooks
injectDebuggingHooks :: DatalogProgram -> DatalogProgram
injectDebuggingHooks d =
  let
    rules = progRules d
    updatedRules = [r {ruleRHS = updateRHSRules d r}  | r <- rules]
    updatedFunctions = updateFunctions rules (progFunctions d)
  in d { progRules = updatedRules, progFunctions = updatedFunctions }
