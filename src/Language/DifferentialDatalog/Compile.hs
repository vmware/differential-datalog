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

{-# LANGUAGE RecordWildCards, FlexibleContexts, LambdaCase, TupleSections, OverloadedStrings #-}

{- | 
Module     : Compile
Description: Compile 'DatalogProgram' to Rust.  See program.rs for corresponding Rust declarations.
-}

module Language.DifferentialDatalog.Compile (
    compile
) where

-- TODO: 
-- Generate functions
-- Generate type declarations
-- TODO: generate code to fill relations with initial values
-- (corresponding to rules with empty bodies)
-- ??? Generate callback function prototypes, but don't overwrite existing implementations.

import Control.Monad.State
import Text.PrettyPrint
import Data.Tuple
import Data.Tuple.Select
import Data.Maybe
import Data.List
import Data.Int
import Data.Word
import Data.Bits
import Numeric
import qualified Data.Set as S
import qualified Data.Map as M
import qualified Data.Graph.Inductive as G
import qualified Data.Graph.Inductive.Query.DFS as G

import Language.DifferentialDatalog.PP
import Language.DifferentialDatalog.Name
import Language.DifferentialDatalog.Pos
import Language.DifferentialDatalog.Ops
import Language.DifferentialDatalog.Util
import Language.DifferentialDatalog.Syntax
import Language.DifferentialDatalog.NS
import Language.DifferentialDatalog.Expr
import Language.DifferentialDatalog.DatalogProgram
import Language.DifferentialDatalog.ECtx
import Language.DifferentialDatalog.Type
import Language.DifferentialDatalog.Rule

-- Input argument name for Rust functions that take a datalog record.
vALUE_VAR :: Doc
vALUE_VAR = "__v"

-- Input arguments for Rust functions that take a key and one or two
-- values
kEY_VAR :: Doc
kEY_VAR = "__key"

vALUE_VAR1 :: Doc
vALUE_VAR1 = "__v1"

vALUE_VAR2 :: Doc
vALUE_VAR2 = "__v2"

{- The following types model corresponding entities in program.rs -}

-- Arrangement is uniquely identified by its _normalized_ pattern
-- expression.  The normalized pattern only contains variables
-- involved in the arrangement key, with normalized names (so that 
-- two patterns isomorphic modulo variable names have the same 
-- normalized representation) and only expand constructors that 
-- either contain a key variable or are non-unique.
data Arrangement = Arrangement {
    arngPattern :: Expr
} deriving Eq

-- Rust expression kind
data EKind = EVal   -- normal value
           | ELVal  -- l-value that can be written to or moved
           | ERef   -- reference (mutable or immutable)
           deriving (Eq)

-- convert any expression into reference
ref :: (Doc, EKind, ENode) -> Doc
ref (x, ERef, _)  = x
ref (x, _, _)     = parens $ "&" <> x

-- dereference expression if it is a reference; leave it alone
-- otherwise
deref :: (Doc, EKind, ENode) -> Doc
deref (x, ERef, _) = parens $ "*" <> x
deref (x, _, _)    = x

-- convert any expression into mutable reference
mutref :: (Doc, EKind, ENode) -> Doc
mutref (x, ERef, _)  = x
mutref (x, _, _)     = parens $ "&mut" <> x

-- convert any expression to EVal by cloning it if necessary
val :: (Doc, EKind, ENode) -> Doc
val (x, EVal, _) = x
val (x, _, _)    = x <> ".clone()"

-- convert expression to l-value
lval :: (Doc, EKind, ENode) -> Doc
lval (x, ELVal, _) = x
-- this can only be mutable reference in a valid program
lval (x, ERef, _)  = parens $ "*" <> x
lval (x, EVal, _)  = error $ "Compile.lval: cannot convert value to l-value: " ++ show x

-- Relation is a function that takes a list of arrangements and produces a Doc containing Rust 
-- code for the relation (since we won't know all required arrangements till we finish scanning 
-- the program)
type ProgRel = (String, [Doc] -> Doc)

data ProgNode = SCCNode [ProgRel]
              | RelNode ProgRel

nodeRels :: ProgNode -> [ProgRel]
nodeRels (SCCNode rels) = rels
nodeRels (RelNode rel)  = [rel]

{- State accumulated by the compiler as it traverses the program -}
type CompilerMonad = State CompilerState

type RelId = Int
type ArrId = (Int, Int)

data CompilerState = CompilerState {
    cTypes        :: S.Set Type,
    cRels         :: M.Map String RelId,
    cArrangements :: M.Map String [Arrangement]
}

emptyCompilerState :: CompilerState
emptyCompilerState = CompilerState {
    cTypes        = S.empty,
    cRels         = M.empty,
    cArrangements = M.empty
}

getRelId :: String -> CompilerMonad RelId
getRelId rname = gets $ (M.! rname) . cRels

-- t must be normalized
addType :: Type -> CompilerMonad ()
addType t = modify $ \s -> s{cTypes = S.insert t $ cTypes s}

-- Create a new arrangement or return existing arrangement id
addArrangement :: String -> Arrangement -> CompilerMonad ArrId
addArrangement relname arr = do
    arrs <- gets $ (M.! relname) . cArrangements
    rid <- getRelId relname
    let (arrs', aid) = case findIndex (==arr) arrs of
                            Nothing -> (arrs ++ [arr], length arrs)
                            Just i  -> (arrs, i)
    modify $ \s -> s{cArrangements = M.insert relname arrs' $ cArrangements s}
    return (rid, aid)

-- Rust does not like parenthesis around singleton tuples
tuple :: [Doc] -> Doc
tuple [x] = x
tuple xs = parens $ hsep $ punctuate comma xs

-- Structs with a single constructor are compiled into Rust structs;
-- structs with multiple constructor are compiled into Rust enums.
isStructType :: Type -> Bool
isStructType TStruct{..} | length typeCons == 1 = True
isStructType _                                  = False

-- | Compile Datalog program into Rust code that creates 'struct Program' representing 
-- the program for the Rust Datalog library
compile :: DatalogProgram -> CompilerMonad Doc
compile d = do
    let -- Transform away rules with multiple heads
        d' = progExpandMultiheadRules d
        -- Compute ordered SCCs of the dependency graph.  These will define the
        -- structure of the program.
        depgraph = progDependencyGraph d'
        sccs = G.topsort' $ G.condensation depgraph
        -- Assign RelId's
        relids = M.fromList $ map swap $ G.labNodes depgraph
        -- Initialize arrangements map
        arrs = M.fromList $ map ((, []) . snd) $ G.labNodes depgraph
        -- Compile SCCs
        (nodes, cstate) = runState (mapM (compileSCC d' depgraph) sccs) 
                                   $ emptyCompilerState{ cRels = relids
                                                       , cArrangements = arrs}
    prog <- mkProg d (cArrangements cstate) nodes
    let -- Assemble results
        valtype = mkValType d $ cTypes cstate
    return $ valtype $+$ prog
    
-- Generate Value type as an enum with one entry per type in types
mkValType :: DatalogProgram -> S.Set Type -> Doc
mkValType d types = 
    "#[derive(Eq, Ord, Clone, Hash, PartialEq, PartialOrd, Serialize, Deserialize, Debug)]" $$
    "enum Value {"                                                                          $$
    (nest' $ vcat $ punctuate comma $ map mkValCons $ S.toList types)                       $$
    "}"                                                                                     $$
    "unsafe_abomonate!(Value);"
    where
    mkValCons :: Type -> Doc
    mkValCons t = mkConstructorName' d t <> (parens $ mkType t)

-- Generate Rust struct for ProgNode
compileSCC :: DatalogProgram -> DepGraph -> [G.Node] -> CompilerMonad ProgNode
compileSCC d dep nodes | recursive = compileRelNode d (head relnames)
                       | otherwise = compileSCCNode d relnames
    where
    recursive = any (\(from, to) -> elem from nodes && elem to nodes) $ G.edges dep
    relnames = map (fromJust . G.lab dep) nodes

compileRelNode :: DatalogProgram -> String -> CompilerMonad ProgNode
compileRelNode d relname = do
    rel <- compileRelation d relname
    return $ RelNode rel

compileSCCNode :: DatalogProgram -> [String] -> CompilerMonad ProgNode
compileSCCNode d relnames = do
    rels <- mapM (compileRelation d) relnames
    return $ SCCNode rels

{- Generate Rust representation of relation and associated rules.

//Example code generated by this function: 
let ancestorset: Arc<Mutex<ValSet<Value>>> = Arc::new(Mutex::new(FnvHashSet::default()));
let ancestor = {
    let ancestorset = ancestorset.clone();
    Relation {
        name:         "ancestor".to_string(),
        input:        false,
        id:           2,      
        rules:        vec![
            Rule{
                rel: 1, 
                xforms: vec![]
            },
            Rule{
                rel: 2, 
                xforms: vec![XForm::Join{
                    afun:        &(arrange_by_snd as ArrangeFunc<Value>),
                    arrangement: (1,0),
                    jfun:        &(jfun as JoinFunc<Value>)
                }]
            }],
        arrangements: vec![
            Arrangement{
                name: "arrange_by_ancestor".to_string(),
                afun: &(arrange_by_fst as ArrangeFunc<Value>)
            },
            Arrangement{
                name: "arrange_by_self".to_string(),
                afun: &(arrange_by_self as ArrangeFunc<Value>)
            }],
        change_cb:    Arc::new(move |v,pol| set_update("ancestor", &ancestorset, v, pol))
    }
};
-}
compileRelation :: DatalogProgram -> String -> CompilerMonad ProgRel
compileRelation d rname = do
    let Relation{..} = getRelation d rname
    relid <- getRelId rname
    -- collect all rules for this relation
    let rules = filter (not . null . ruleRHS)
                $ filter ((== rname) . atomRelation . head . ruleLHS) 
                $ progRules d
    rules' <- mapM (compileRule d) rules
    let f arrangements =
            "Relation {"                                                                $$
            "    name:         \"" <> pp rname <> "\".to_string(),"                     $$
            "    input:        " <> (if relGround then "true" else "false") <> ","      $$
            "    id:           " <> pp relid <> ","                                     $$
            "    rules:        vec!["                                                   $$
            (nest' $ nest' $ vcat $ punctuate comma rules') <> "],"                     $$
            "    arrangements: vec!["                                                   $$
            (nest' $ nest' $ vcat $ punctuate comma arrangements) <> "],"               $$
            "    change_cb:    Arc::new(move |v,pol| )"                                 $$
            "}"
    return (rname, f)

{- Generate Rust representation of a Datalog rule

// Example Rust code generated by this function
Rule{
    rel: 2, 
    xforms: vec![XForm::Join{
        afun:        &(arrange_by_snd as ArrangeFunc<Value>),
        arrangement: (1,0),
        jfun:        &(jfun as JoinFunc<Value>)
    }]
}
-}
compileRule :: DatalogProgram -> Rule -> CompilerMonad Doc
compileRule d rl@Rule{..} = do
    fstrelid <- getRelId $ atomRelation $ rhsAtom $ head ruleRHS
    xforms <- compileRule' d rl 0
    return $ "Rule{"                                            $$
             "    rel: " <> pp fstrelid <> ","                  $$ 
             "    xforms: vec!["                                $$ 
             (nest' $ nest' $ vcat $ punctuate comma xforms)    $$
             "}"

-- Generates one XForm in the chain
compileRule' :: DatalogProgram -> Rule -> Int -> CompilerMonad [Doc]
compileRule' d rl@Rule{..} last_rhs_idx = do
    -- Open up input constructor; bring Datalog variables into scope
    open <- if last_rhs_idx == 0
               then openAtom  d ("&" <> vALUE_VAR) $ rhsAtom $ head ruleRHS
               else openTuple d ("&" <> vALUE_VAR) $ rhsVarsAfter d rl last_rhs_idx
    -- Apply filters and assignments between last_rhs_idx and the next
    -- join or antijoin
    let filters = mkFilters d rl last_rhs_idx
    let prefix = open $+$ vcat filters
    -- index of the next join
    let join_idx = last_rhs_idx + length filters + 1
    if join_idx == length ruleRHS
       then do
           head <- mkHead d prefix rl
           return [head]
       else do
           (xform, last_idx') <-
               case ruleRHS !! join_idx of
                    RHSLiteral True a  -> mkJoin d prefix a rl join_idx
                    RHSLiteral False a -> (, join_idx) <$> mkAntijoin d prefix a rl join_idx
           if last_idx' == length ruleRHS
              then do rest <- compileRule' d rl last_idx'
                      return $ xform:rest
              else return [xform]

-- Generate Rust code to filter records and bring variables into scope.
-- The Rust code returns None if the record does not pass the filter.
--
-- let (v1,v2) /*v1,v2 are references*/ = match &v {
--     Value::Rel1(v1,v2) => (v1,v2),
--     _ => return None
-- };
openAtom :: DatalogProgram -> Doc -> Atom -> CompilerMonad Doc
openAtom d var Atom{..} = do
    let rel = getRelation d atomRelation
    constructor <- mkConstructorName d $ relType rel
    let varnames = map pp $ exprVars atomVal
        vars = tuple varnames
        (pattern, cond) = mkPatExpr d atomVal
        cond_str = if cond == empty then empty else ("if" <+> cond)
    return $
        "let" <+> vars <+> "= match " <> var <> "{"                                                  $$
        "    Value::" <> constructor <> "(" <> pattern <> " )" <+> cond_str <+> "=> " <> vars <> "," $$
        "    _ => return None"                                                                       $$
        "};"

-- Generate Rust constructor name for a type
mkConstructorName :: DatalogProgram -> Type -> CompilerMonad Doc
mkConstructorName d t = do
    let t' = typeNormalize d t
    addType t'
    return $ mkConstructorName' d t'

-- Assumes that t is normalized
mkConstructorName' :: DatalogProgram -> Type -> Doc
mkConstructorName' d t =
    case t of
         TTuple{..}  -> "tuple" <> pp (length typeTupArgs) <> "__" <>
                        (hcat $ punctuate "_" $ map (mkConstructorName' d) typeTupArgs)
         TBool{}     -> "bool"
         TInt{}      -> "int"
         TString{}   -> "string"
         TBit{..}    -> "bit" <> pp typeWidth
         TUser{}     -> consuser
         TOpaque{}   -> consuser
         _           -> error $ "unexpected type " ++ show t ++ " in Compile.mkConstructorName"
    where
    consuser = pp (typeName t) <> 
               case typeArgs t of
                    [] -> empty
                    as -> "__" <> (hcat $ punctuate "_" $ map (mkConstructorName' d) as)

       
-- Generate Rust code to open up tuples and bring variables into scope.
-- The Rust code returns None if the record does not pass the filter.
openTuple :: DatalogProgram -> Doc -> [Field] -> CompilerMonad Doc
openTuple d var vars = do
    tup <- mkVarsTupleValue d vars
    return $ "let" <+> tup <+> " = " <> var <> ";"

mkValue :: DatalogProgram -> ECtx -> Expr -> CompilerMonad Doc
mkValue d ctx e = do
    constructor <- mkConstructorName d $ exprType d ctx e
    return $ constructor <> (parens $ mkExpr d ctx e EVal)

mkTupleValue :: DatalogProgram -> ECtx -> [Expr] -> CompilerMonad Doc
mkTupleValue d ctx es = do 
    constructor <- mkConstructorName d $ tTuple $ map (exprType'' d ctx) es
    return $ constructor <> (parens $ tuple $ map (\e -> mkExpr d ctx e EVal) es)

mkVarsTupleValue :: DatalogProgram -> [Field] -> CompilerMonad Doc
mkVarsTupleValue d vs = do
    constructor <- mkConstructorName d $ tTuple $ map typ vs
    return $ constructor <> (parens $ tuple $ map (pp . name) vs)


-- Compile all contiguous RHSCondition terms following 'last_idx'
mkFilters :: DatalogProgram -> Rule -> Int -> [Doc]
mkFilters d rl@Rule{..} last_idx = 
    mapIdx (\rhs i -> mkFilter d (CtxRuleRCond rl $ i + last_idx) $ rhsExpr rhs)
    $ takeWhile (\case
                  RHSCondition{} -> True
                  _              -> False)
    $ drop last_idx ruleRHS

-- Implement RHSCondition semantics in Rust; brings new variables into
-- scope if this is an assignment
mkFilter :: DatalogProgram -> ECtx -> Expr -> Doc
mkFilter d ctx (E e@ESet{..}) = mkAssignFilter d ctx e
mkFilter d ctx e              = mkCondFilter d ctx e

mkAssignFilter :: DatalogProgram -> ECtx -> ENode -> Doc
mkAssignFilter d ctx e@(ESet _ l r) =
    "let" <+> vars <+> "= match" <+> r' <+> "{"                     $$
    (nest' $ pattern <+> cond_str <+> "=> " <+> vars <> ",")        $$
    "    _ => return None"                                          $$
    "};"
    where
    r' = mkExpr d (CtxSetR e ctx) r ERef
    (pattern, cond) = mkPatExpr d l
    varnames = map pp $ exprVars l
    vars = tuple varnames
    cond_str = if cond == empty then empty else ("if" <+> cond)

mkCondFilter :: DatalogProgram -> ECtx -> Expr -> Doc
mkCondFilter d ctx e =
    "if !" <> mkExpr d ctx e EVal <+> "{return None;};"

-- Compile XForm::Join
-- Returns generated xform and index of the last RHS term consumed by
-- the XForm
mkJoin :: DatalogProgram -> Doc -> Atom -> Rule -> Int -> CompilerMonad (Doc, Int)
mkJoin d prefix atom rl@Rule{..} join_idx = do
    -- Build arrangement to join with
    let ctx = CtxRuleRAtom rl join_idx
        (arr, vmap) = normalizeArrangement d (getRelation d $ atomRelation atom) ctx $ atomVal atom
    (rid, aid) <- addArrangement (atomRelation atom) arr
    -- Variables from previous terms that will be used in terms
    -- following the join.
    let post_join_vars = (rhsVarsAfter d rl (join_idx - 1)) `intersect`
                         (rhsVarsAfter d rl join_idx)
    -- Arrange variables from previous terms
    akey <- mkTupleValue d ctx $ map snd vmap
    aval <- mkVarsTupleValue d post_join_vars
    let afun = prefix $$ 
               "Some(" <> akey <> "," <+> aval <> ")"
    -- simplify pattern to only extract new variables from it
    let strip (E e@EStruct{..}) = E $ e{exprStructFields = map (\(n,v) -> (n, strip v)) exprStructFields}
        strip (E e@ETuple{..})  = E $ e{exprTupleFields = map strip exprTupleFields}
        strip (E e@EVar{..}) | isNothing $ lookupVar d ctx exprVar 
                                = E e
        strip (E e@ETyped{..})  = E e{exprExpr = strip exprExpr}
        strip _                 = ePHolder
    -- Join function: open up both values, apply filters.
    open <- liftM2 ($$) (openTuple d vALUE_VAR1 post_join_vars)
                        (openAtom d vALUE_VAR2 atom{atomVal = strip $ atomVal atom})
    let filters = mkFilters d rl join_idx 
        last_idx = join_idx + length filters
    -- If we're at the end of the rule, generate head atom; otherwise
    -- return all live variables in a tuple
    (ret, last_idx') <- if last_idx == length ruleRHS - 1
        then (, last_idx + 1) <$> mkValue d (CtxRuleL rl 0) (atomVal $ head $ ruleLHS)
        else (, last_idx)     <$> (mkVarsTupleValue d $ rhsVarsAfter d rl last_idx)
    let jfun = open                     $$ 
               vcat filters             $$ 
               "Some" <> parens ret
    let doc = "XForm::Join{"                                                                                         $$
              "    afun:        &((|" <> vALUE_VAR <> "|" <> afun <>") as ArrangeFunc<Value>),"                      $$
              "    arrangement: (" <> pp rid <> "," <> pp aid <> "),"                                                $$
              "    jfun:        &((|_," <> vALUE_VAR1 <> "," <> vALUE_VAR2 <> "|" <> jfun <> ") as JoinFunc<Value>)" $$
              "}"
    return (doc, last_idx')

-- Compile XForm::Antijoin
mkAntijoin :: DatalogProgram -> Doc -> Atom -> Rule -> Int -> CompilerMonad Doc
mkAntijoin d prefix Atom{..} rl@Rule{..} ajoin_idx = do
    rid <- getRelId atomRelation
    akey <- mkValue d (CtxRuleRAtom rl ajoin_idx) atomVal
    aval <- mkVarsTupleValue d $ rhsVarsAfter d rl ajoin_idx
    let afun = prefix $$ 
               "Some(" <> akey <> "," <+> aval <> ")"
    return $ "XForm::Antijoin{"                                                            $$
             "    afun: &((|" <> vALUE_VAR <> "|" <> afun <> ") as ArrangeFunc<Value>),"   $$
             "    rel:  " <> pp rid                                                        $$
             "}"

-- Normalize pattern expression for use in arrangement
normalizeArrangement :: DatalogProgram -> Relation -> ECtx -> Expr -> (Arrangement, [(String, Expr)])
normalizeArrangement d rel ctx pat = (Arrangement renamed, vmap)
    where
    pat' = exprFoldCtx (normalizePattern d) ctx pat
    (renamed, (_, vmap)) = runState (rename pat') (0, [])
    rename :: Expr -> State (Int, [(String, Expr)]) Expr
    rename (E e) = 
        case e of
             EStruct{..}             -> do
                fs' <- mapM (\(n,e) -> (n,) <$> rename e) exprStructFields
                return $ E e{exprStructFields = fs'}
             ETuple{..}              -> do
                fs' <- mapM rename exprTupleFields
                return $ E e{exprTupleFields = fs'}
             EBool{}                 -> return $ E e
             EInt{}                  -> return $ E e
             EString{}               -> return $ E e
             EBit{}                  -> return $ E e
             EPHolder{}              -> return $ E e
             ETyped{..}              -> do 
                e' <- rename exprExpr
                return $ E e{exprExpr = e'}
             _                       -> do
                vid <- gets fst
                let vname = "_" ++ show vid
                modify $ \(_, vmap) -> (vid + 1, vmap ++ [(vname, E e)])
                return $ eVar vname

-- Simplify away parts of the pattern that do not constrain its value.
normalizePattern :: DatalogProgram -> ECtx -> ENode -> Expr
normalizePattern _ ctx e | ctxInRuleRHSPattern ctx = E e
normalizePattern d ctx e =
    case e of
         -- replace new variables with placeholders
         EVar{..}    | isNothing $ lookupVar d ctx exprVar
                                 -> ePHolder
         -- replace tuples and unique constructors populated with placeholders
         -- with a placeholder
         EStruct{..} | consIsUnique d exprConstructor && all ((== ePHolder) . snd) exprStructFields
                                 -> ePHolder
         ETuple{..}  | all (== ePHolder) exprTupleFields
                                 -> ePHolder
         _                       -> E e

-- Compile XForm::FilterMap that generates the head of the rule
mkHead :: DatalogProgram -> Doc -> Rule -> CompilerMonad Doc
mkHead d prefix rl = do
    v <- mkValue d (CtxRuleL rl 0) (atomVal $ head $ ruleLHS rl)
    let fmfun = prefix $$
                "Some" <> parens v
    return $
        "XForm::FilterMap{"                                                             $$
        "    fmfun: &((|" <> vALUE_VAR <> "|" <> fmfun <>") as FilterMapFunc<Value>)"   $$
        "}"

-- Variables in the RHS of the rule declared before or in i'th term
-- and visible after the term.
rhsVarsAfter :: DatalogProgram -> Rule -> Int -> [Field]
rhsVarsAfter d rl i =
    filter (\f -> elem (name f) $ (map name $ ruleLHSVars d rl) `union` 
                                  (concatMap (ruleRHSTermVars rl) [i..length (ruleRHS rl) - 1]))
           $ ruleRHSVars d rl (i+1)

mkProg :: DatalogProgram -> M.Map String [Arrangement] -> [ProgNode] -> CompilerMonad Doc
mkProg d arrangements nodes = do
    rels <- vcat <$>
            mapM (\(rname, rel) -> do
                  arrs <- mapM (mkArrangement d (getRelation d rname)) 
                               $ arrangements M.! rname
                  return $ "let" <+> pp rname <+> "=" <+> rel arrs <> ";")
                 (concatMap nodeRels nodes)
    let pnodes = map mkNode nodes
        prog = "let prog: Program<Value> = Program {"           $$
               "    nodes: vec!["                               $$
               (nest' $ nest' $ vcat $ punctuate comma pnodes)  $$
               "]};"
    return $ rels $$ prog

mkNode :: ProgNode -> Doc
mkNode (RelNode (rel,_)) = 
    "ProgNode::RelNode{rel:" <+> pp rel <> "}"
mkNode (SCCNode rels)    = 
    "ProgNode::SCCNode{rels: vec![" <> (commaSep $ map (pp . fst) rels) <> "]}"

mkArrangement :: DatalogProgram -> Relation -> Arrangement -> CompilerMonad Doc
mkArrangement d rel (Arrangement pattern) = do
    let (pat, cond) = mkPatExpr d pattern
    -- extract variables with types from pattern, in the order
    -- consistent with that returned by 'rename'.
    let getvars :: Type -> Expr -> [Field]
        getvars t (E EStruct{..}) = 
            concatMap (\(e,t) -> getvars t e) 
            $ zip (map snd exprStructFields) (map typ $ consArgs $ fromJust $ find ((== exprConstructor) . name) cs)
            where TStruct _ cs = typ' d t
        getvars t (E ETuple{..})  = 
            concatMap (\(e,t) -> getvars t e) $ zip exprTupleFields ts
            where TTuple _ ts = typ' d t
        getvars t (E ETyped{..})  = getvars t exprExpr
        getvars t (E EVar{..})    = [Field nopos exprVar t]
        getvars _ _               = []
    patvars <- mkVarsTupleValue d $ getvars (relType rel) pattern
    let afun = "match" <+> vALUE_VAR <+> "{"                                                          $$
               (nest' $ pat <+> cond <+> "=> Some(" <+> patvars <> "," <+> vALUE_VAR <> ".clone()),") $$
               "    _ => None"                                                                        $$
               "}"
    return $
        "Arrangement{"                                                              $$
        "   name: \"" <> pp pattern <> "\".to_string(),"                            $$
        "   afun: &((|" <> vALUE_VAR <> "|" <> afun <> ") as ArrangeFunc<Value>)"   $$
        "}"


-- Compile Datalog pattern expression to Rust.
-- The first element in the return tuple is a Rust match pattern, the second
-- element is a (possibly empty condition attached to the pattern),
-- e.g., the Datalog pattern
-- 'Constructor{f1= x, f2= "foo"}' compiles into
-- '(TypeName::Constructor{f1: x, f2=_0}, *_0 == "foo".to_string())'
-- where '_0' is an auxiliary variable of type 'String'
mkPatExpr :: DatalogProgram -> Expr -> (Doc, Doc)
mkPatExpr d e = evalState (exprFoldM (mkPatExpr' d) e) 0

mkPatExpr' :: DatalogProgram -> ExprNode (Doc, Doc) -> State Int (Doc, Doc)
mkPatExpr' _ EVar{..}                  = return (pp exprVar, empty)
mkPatExpr' _ (EBool _ True)            = return ("true", empty)
mkPatExpr' _ (EBool _ False)           = return ("false", empty)
mkPatExpr' _ EInt{..}                  = do 
    i <- get
    put $ i+1
    let vname = pp $ "_" <> pp i
    return (vname, "*" <> vname <+> "==" <+> mkInt exprIVal)
mkPatExpr' _ EString{..}               = do
    i <- get
    put $ i+1
    let vname = pp $ "_" <> pp i
    return (vname, "*" <> vname <+> "==" <+> "\"" <> pp exprString <> "\"" <> ".to_string()")
mkPatExpr' _ EBit{..} | exprWidth <= 64= return (pp exprIVal, empty)
mkPatExpr' _ EBit{..}                  = do
    i <- get
    put $ i+1
    let vname = pp $ "_" <> pp i
    return (vname, "*" <> vname <+> "==" <+> "Uint::parse_bytes(b\"" <> pp exprIVal <> "\", 10)")
mkPatExpr' d EStruct{..}               = return (e, cond)
    where
    struct_name = name $ consType d exprConstructor
    e = pp struct_name <> "::" <> pp exprConstructor <> 
        (braces' $ hsep $ punctuate comma $ map (\(fname, (e, _)) -> pp fname <> ":" <+> e) exprStructFields)
    cond = hsep $ intersperse "&&" $ map (\(_,(_,c)) -> c) exprStructFields
mkPatExpr' d ETuple{..}                = return (e, cond)
    where
    e = "(" <> (hsep $ punctuate comma $ map (pp . fst) exprTupleFields) <> ")"
    cond = hsep $ intersperse "&&" $ map (pp . snd) exprTupleFields
mkPatExpr' _ EPHolder{}                = return ("_", empty)
mkPatExpr' _ ETyped{..}                = return exprExpr


-- Convert Datalog expression to Rust.
-- We generate the code so that all variables are references and must
-- be dereferenced before use or cloned when passed to a constructor,
-- assigned to another variable or returned.
mkExpr :: DatalogProgram -> ECtx -> Expr -> EKind -> Doc
mkExpr d ctx e k | k == EVal  = val e'
                 | k == ERef  = ref e'
                 | k == ELVal = lval e'
    where   
    e' = exprFoldCtx (mkExpr_ d) ctx e

mkExpr_ :: DatalogProgram -> ECtx -> ExprNode (Doc, EKind, ENode) -> (Doc, EKind, ENode)
mkExpr_ d ctx e = (t', k', e')
    where (t', k') = mkExpr' d ctx e
          e' = exprMap (E . sel3) e

-- Compiled expressions are represented as '(Doc, EKind)' tuple, where
-- the second components is the kind of the compiled representation
mkExpr' :: DatalogProgram -> ECtx -> ExprNode (Doc, EKind, ENode) -> (Doc, EKind)
-- All variables are references
mkExpr' _ _ EVar{..}    = (pp exprVar, ERef) 

-- Function arguments are passed as read-only references
-- Functions return real values.
mkExpr' _ _ EApply{..}  = 
    (pp exprFunc <> (parens $ commaSep $ map ref exprArgs), EVal)

-- Field access automatically dereferences subexpression
mkExpr' _ _ EField{..} = (sel1 exprStruct <> "." <> pp exprField, ELVal)

mkExpr' _ _ (EBool _ True) = ("true", EVal)
mkExpr' _ _ (EBool _ False) = ("false", EVal)
mkExpr' _ _ EInt{..} = (mkInt exprIVal, EVal)
mkExpr' _ _ EString{..} = ("\"" <> pp exprString <> "\".to_string()", EVal)
mkExpr' _ _ EBit{..} | exprWidth <= 64 = (pp exprIVal <+> "as" <+> mkType (tBit exprWidth), EVal)
                     | otherwise       = ("Uint::parse_bytes(b\"" <> pp exprIVal <> "\", 10)", EVal)

-- Struct fields must be values
mkExpr' d ctx EStruct{..} | ctxInSetL ctx
                          = (tname <> fieldlvals, ELVal)
                          | isstruct
                          = (tname <> fieldvals, EVal)
                          | otherwise
                          = (tname <> "::" <> pp exprConstructor <> fieldvals, EVal)
    where fieldvals  = braces $ commaSep $ map (\(fname, v) -> pp fname <> ":" <+> val v) exprStructFields
          fieldlvals = braces $ commaSep $ map (\(fname, v) -> pp fname <> ":" <+> lval v) exprStructFields
          tdef = consType d exprConstructor
          isstruct = isStructType $ fromJust $ tdefType tdef
          tname = pp $ name tdef

-- Tuple fields must be values
mkExpr' _ ctx ETuple{..} | ctxInSetL ctx
                         = (tuple $ map lval exprTupleFields, ELVal)
                         | otherwise
                         = (tuple $ map val exprTupleFields, EVal)

mkExpr' _ _ ESlice{..} = error "not implemented: Compile.mkExpr ESlice"

-- Match expression is a reference
mkExpr' d ctx e@EMatch{..} = (doc, EVal)
    where 
    m = if exprIsVarOrFieldLVal d (CtxMatchExpr (exprMap (E . sel3) e) ctx) (E $ sel3 exprMatchExpr)
           then mutref exprMatchExpr
           else ref exprMatchExpr
    doc = ("match" <+> m <+> "{")
          $$
          (nest' $ vcat $ punctuate comma cases)
          $$
          "}"
    cases = map (\(c,v) -> let (match, cond) = mkPatExpr d (E $ sel3 c)
                               cond' = if cond == empty then empty else ("|" <+> cond) in
                           match <+> cond' <+> "=>" <+> val v) exprCases

-- Variables are mutable references 
mkExpr' _ _ EVarDecl{..} = ("ref mut" <+> pp exprVName, ELVal)
-- TODO VarDecl without assignment

mkExpr' _ ctx ESeq{..} | ctxIsSeq2 ctx = (body, sel2 exprRight)
                       | otherwise     = (braces' body, sel2 exprRight)
    where 
    body = (sel1 exprLeft <> ";") $$ sel1 exprRight
    

mkExpr' _ _ EITE{..} = (doc, EVal)
    where
    doc = ("if" <+> deref exprCond <+> "{") $$
          (nest' $ val exprThen)            $$
          ("}" <+> "else" <+> "{")          $$
          (nest' $ val exprElse)            $$
          "}"
                    
-- Desonctruction expressions in LHS are compiled into let statements, other assignments
-- are compiled into normal assignments.  Note: assignments in rule
-- atoms are handled by a different code path.
mkExpr' d _ ESet{..} | islet     = ("let" <+> assign, EVal)
                     | otherwise = (assign, EVal)
    where
    islet = exprIsDeconstruct d $ E $ sel3 exprLVal
    assign = lval exprLVal <+> "=" <+> val exprRVal

-- operators take values or lvalues and return values
mkExpr' d ctx e@EBinOp{..} = (v', EVal)
    where
    e1 = deref exprLeft
    e2 = deref exprRight
    v = case exprBOp of
             Concat     -> error "not implemented: Compile.mkExpr EBinOp Concat"
             Impl       -> parens $ "!" <> e1 <+> "||" <+> e2
             StrConcat  -> parens $ e1 <> ".push_str(" <> ref exprRight <> ".as_str())"
             op         -> parens $ e1 <+> mkBinOp op <+> e2
    -- Truncate bitvector result in case the type used to represent it
    -- in Rust is larger than the bitvector width.
    v' = case exprType' d (CtxBinOpL (exprMap (E . sel3) e) ctx) (E $ sel3 exprLeft) of
              TBit{..} | elem exprBOp bopsRequireTruncation && needsTruncation typeWidth 
                       -> parens $ v <+> "&" <+> mask typeWidth
              _        -> v
    needsTruncation :: Int -> Bool
    needsTruncation w = mask w /= empty
    mask :: Int -> Doc
    mask w | w < 8 || w > 8  && w < 16 || w > 16 && w < 32 || w > 32 && w < 64 
           = "0x" <> (pp $ showHex (((1::Integer) `shiftL` w) - 1) "")
    mask _ = empty


mkExpr' _ _ EUnOp{..} = (v, EVal)
    where
    e = deref exprOp
    v = case exprUOp of
             Not    -> parens $ "!" <> e
             BNeg   -> parens $ "~" <> e

mkExpr' _ _ EPHolder{} = ("_", ELVal)

-- keep type ascriptions in LHS of assignment and in integer constants
mkExpr' _ ctx ETyped{..} | ctxIsSetL ctx = (e' <+> ":" <+> mkType exprTSpec, cat)
                         | isint         = (parens $ e' <+> "as" <+> mkType exprTSpec, cat)
                         | otherwise     = (e', cat)
    where
    (e', cat, e) = exprExpr
    isint = case e of
                 EInt{} -> True
                 _      -> False

mkType :: Type -> Doc
mkType TBool{}                    = "bool"
mkType TInt{}                     = "Int"
mkType TString{}                  = "String"
mkType TBit{..} | typeWidth <= 8  = "u8"
                | typeWidth <= 16 = "u16"
                | typeWidth <= 32 = "u32"
                | typeWidth <= 64 = "u64"
                | otherwise       = "Uint"
mkType TTuple{..}                 = parens $ commaSep $ map mkType typeTupArgs
mkType TUser{..}                  = pp typeName <>
                                    if null typeArgs 
                                       then empty 
                                       else "<" <> (commaSep $ map mkType typeArgs) <> ">"
mkType TOpaque{..}                = pp typeName <>
                                    if null typeArgs 
                                       then empty 
                                       else "<" <> (commaSep $ map mkType typeArgs) <> ">"

mkBinOp :: BOp -> Doc
mkBinOp Eq     = "=="
mkBinOp Neq    = "!="
mkBinOp Lt     = "<"
mkBinOp Gt     = ">"
mkBinOp Lte    = "<="
mkBinOp Gte    = ">="
mkBinOp And    = "&&"
mkBinOp Or     = "||"
mkBinOp Mod    = "%"
mkBinOp Div    = "/"
mkBinOp ShiftR = ">>"
mkBinOp BAnd   = "&"
mkBinOp BOr    = "|"
mkBinOp ShiftL = "<<"
mkBinOp Plus   = "+"
mkBinOp Minus  = "-"
mkBinOp Times  = "*"

-- These operators require truncating the output value to correct
-- width.
bopsRequireTruncation = [ShiftL, Plus, Minus, Times]

mkInt :: Integer -> Doc
mkInt v | v <= (toInteger (maxBound::Word64)) && v >= (toInteger (minBound::Word64))
        = "Int::from_u64(" <> pp v <> ")"
        | v <= (toInteger (maxBound::Int64))  && v >= (toInteger (minBound::Int64))
        = "Int::from_i64(" <> pp v <> ")"
        | otherwise
        = "Int::parse_bytes(b\"" <> pp v <> "\", 10)"
