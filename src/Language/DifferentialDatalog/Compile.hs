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
import qualified Data.Set as S
import qualified Data.Map as M
import qualified Data.Graph.Inductive as G
import qualified Data.Graph.Inductive.Query.DFS as G

import Language.DifferentialDatalog.PP
import Language.DifferentialDatalog.Name
import Language.DifferentialDatalog.Ops
import Language.DifferentialDatalog.Util
import Language.DifferentialDatalog.Syntax
import Language.DifferentialDatalog.NS
import Language.DifferentialDatalog.Expr
import Language.DifferentialDatalog.DatalogProgram
import Language.DifferentialDatalog.ECtx
import Language.DifferentialDatalog.Type

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
}

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

{- State accumulated by the compiler as it traverses the program -}
type CompilerMonad = State CompilerState

type RelId = Int
type ArrId = (Int, Int)

data CompilerState = CompilerState {
    cTypes        :: S.Set Type,
    cRels         :: M.Map String RelId,
    cArrangements :: M.Map String (M.Map Arrangement Int)
}

emptyCompilerState :: CompilerState
emptyCompilerState = CompilerState {
    cTypes        = S.empty,
    cRels         = M.empty,
    cArrangements = M.empty
}

getRelId :: String -> CompilerMonad RelId
getRelId rname = gets $ (M.! rname) . cRels

-- Create a new arrangement or return existing arrangement id
addArrangement :: String -> Arrangement -> CompilerMonad ArrId
addArrangement relname arr = undefined

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
compile :: DatalogProgram -> Doc
compile d =  valtype $+$ prog
    where
    -- Transform away rules with multiple heads
    d' = progExpandMultiheadRules d
    -- Compute ordered SCCs of the dependency graph.  These will define the
    -- structure of the program.
    depgraph = progDependencyGraph d'
    sccs = G.topsort' $ G.condensation depgraph
    -- Assign RelId's
    relids = M.fromList $ map swap $ G.labNodes depgraph
    -- Compile SCCs
    (nodes, cstate) = runState (mapM (compileSCC d' depgraph) sccs) 
                               $ emptyCompilerState{cRels = relids}
    -- Assemble results
    valtype = mkValType $ cTypes cstate
    prog = mkProg (cArrangements cstate) nodes
    
-- Generate Value type
mkValType :: S.Set Type -> Doc
mkValType types = undefined

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
    let open = if last_rhs_idx == 0
                  then openAtom d $ rhsAtom $ head ruleRHS
                  else openTuple d $ rhsVarsAfter d rl last_rhs_idx
    -- Apply filters and assignments between last_rhs_idx and the next
    -- join or antijoin
    let filters = mapIdx (\rhs i -> mkFilter d (CtxRuleRCond rl $ i + last_rhs_idx) $ rhsExpr rhs)
                  $ takeWhile (\case
                              RHSCondition{} -> True
                              _              -> False)
                  $ drop last_rhs_idx ruleRHS
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
           rest <- compileRule' d rl last_idx'
           return $ xform:rest

-- Generate Rust code to filter records and bring variables into scope.
-- The Rust code returns None if the record does not pass the filter.
--
-- let (v1,v2) /*v1,v2 are references*/ = match &v {
--     Value::Rel1(v1,v2) => (v1,v2),
--     _ => return None
-- };
openAtom :: DatalogProgram -> Atom -> Doc
openAtom d Atom{..} = 
    "let" <+> vars <+> "= match &" <> vALUE_VAR <> "{"                                           $$
    "    Value::" <> constructor <> "(" <> pattern <> " )" <+> cond_str <+> "=> " <> vars <> "," $$
    "    _ => return None"                                                                       $$
    "};"
    where
    rel = getRelation d atomRelation
    constructor = mkConstructorName d $ relType rel
    varnames = map pp $ exprVars atomVal
    vars = tuple varnames
    (pattern, cond) = mkPatExpr d atomVal
    cond_str = if cond == empty then empty else ("if" <+> cond)

-- Generate Rust constructor name for a type
mkConstructorName :: DatalogProgram -> Type -> Doc
mkConstructorName = undefined

-- Generate Rust code to open up tuples and bring variables into scope.
-- The Rust code returns None if the record does not pass the filter.
openTuple :: DatalogProgram -> [Field] -> Doc
openTuple d vars =
    "let" <+> constructor <> "(" <> vartuple <> ") = &" <> vALUE_VAR <> ";"
    where
    constructor = mkConstructorName d $ tTuple $ map typ vars
    vartuple = tuple $ map (pp . name) vars

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
mkJoin d prefix Atom{..} rl@Rule{..} join_idx = do
    let afun = undefined
        (jfun, last_idx) = undefined
        arr = normalizeArrangement d (getRelation d atomRelation) (CtxRuleRAtom rl join_idx) atomVal
    (rid, aid) <- addArrangement atomRelation arr
    let doc = "XForm::Join{"                                                                                                         $$
              "    afun:        &((|" <> vALUE_VAR <> "|" <> afun <>") as ArrangeFunc<Value>),"                                      $$
              "    arrangement: (" <> pp rid <> "," <> pp aid <> "),"                                                                $$
              "    jfun:        &((|" <> kEY_VAR <> "," <> vALUE_VAR1 <> "," <> vALUE_VAR2 <> "|" <> jfun <> ") as JoinFunc<Value>)" $$
              "}"
    return (doc, last_idx)

-- Compile XForm::Antijoin
mkAntijoin :: DatalogProgram -> Doc -> Atom -> Rule -> Int -> CompilerMonad Doc
mkAntijoin d prefix Atom{..} rl@Rule{..} ajoin_idx = undefined

-- Normalize pattern expression for use in arrangement
normalizeArrangement :: DatalogProgram -> Relation -> ECtx -> Expr -> Arrangement
normalizeArrangement d rel ctx pat = Arrangement renamed
    where
    pat' = exprFoldCtx (normalizePattern d) ctx pat
    renamed = evalState (rename pat') 0
    rename :: Expr -> State Int Expr
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
                vid <- get
                put $ vid + 1
                return $ eVar $ "_" ++ show vid

-- Removed redundant info from the pattern
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
mkHead d prefix rl = undefined

-- Variables in the RHS of the rule visible after i'th term.
rhsVarsAfter :: DatalogProgram -> Rule -> Int -> [Field]
rhsVarsAfter = undefined

mkProg :: M.Map String (M.Map Arrangement Int) -> [ProgNode] -> Doc
mkProg = undefined

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
mkExpr' _ _ EBinOp{..} = (v, EVal)
    where
    e1 = deref exprLeft
    e2 = deref exprRight
    v = case exprBOp of
             Concat     -> error "not implemented: Compile.mkExpr EBinOp Concat"
             Impl       -> parens $ "!" <> e1 <+> "||" <+> e2
             StrConcat  -> parens $ e1 <> ".push_str(" <> ref exprRight <> ".as_str())"
             op         -> parens $ e1 <+> mkBinOp op <+> e2

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
mkType = undefined

mkBinOp :: BOp -> Doc
mkBinOp Eq     = "=="
mkBinOp Neq    = "!="
mkBinOp Lt     = "<"
mkBinOp Gt     = ">"
mkBinOp Lte    = "<="
mkBinOp Gte    = ">="
mkBinOp And    = "&&"
mkBinOp Or     = "||"
mkBinOp Plus   = "+"
mkBinOp Minus  = "-"
mkBinOp Mod    = "%"
mkBinOp Times  = "*"
mkBinOp Div    = "/"
mkBinOp ShiftR = ">>"
mkBinOp ShiftL = "<<"
mkBinOp BAnd   = "&"
mkBinOp BOr    = "|"

mkInt :: Integer -> Doc
mkInt v | v <= (toInteger (maxBound::Word64)) && v >= (toInteger (minBound::Word64))
        = "Int::from_u64(" <> pp v <> ")"
        | v <= (toInteger (maxBound::Int64))  && v >= (toInteger (minBound::Int64))
        = "Int::from_i64(" <> pp v <> ")"
        | otherwise
        = "Int::parse_bytes(b\"" <> pp v <> "\", 10)"
