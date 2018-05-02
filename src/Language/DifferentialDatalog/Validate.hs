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
    validate) where

import qualified Data.Map as M
import Control.Monad.Except
import Data.Maybe
import Data.List
import qualified Data.Graph.Inductive as G
-- import Debug.Trace

import Language.DifferentialDatalog.Syntax
import Language.DifferentialDatalog.NS
import Language.DifferentialDatalog.Util
import Language.DifferentialDatalog.Pos
import Language.DifferentialDatalog.Name
import Language.DifferentialDatalog.Type
import Language.DifferentialDatalog.Ops
import Language.DifferentialDatalog.Expr
--import Relation

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
    d' <- progDesugarExprs d
    -- Validate function prototypes
    mapM_ (funcValidateProto d') $ M.elems $ progFunctions d'
    -- Validate function implementations
    mapM_ (funcValidateDefinition d') $ M.elems $ progFunctions d'
    -- Check for recursion
    checkNoRecursion d'
    -- Validate relation declarations
    mapM_ (relValidate d') $ M.elems $ progRelations d'
    return d'

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

-- Desugar expressions: convert all type constructor calls to named
-- field syntax.  
-- Precondition: typedefs must be validated before calling this
-- function.  
progDesugarExprs :: (MonadError String me) => DatalogProgram -> me DatalogProgram
progDesugarExprs d = do
    funcs' <- mapM (\f -> do e <- case funcDef f of
                                       Nothing -> return Nothing
                                       Just e  -> Just <$> exprDesugar d e
                             return f{funcDef = e})
                   $ progFunctions d
    rules' <- mapM (\r -> do lhs <- mapM (atomDesugarExprs d) $ ruleLHS r
                             rhs <- mapM (rhsDesugarExprs d) $ ruleRHS r 
                             return r{ruleLHS = lhs, ruleRHS = rhs})
                   $ progRules d
    return d{ progFunctions = funcs'
            , progRules     = rules'}    

atomDesugarExprs :: (MonadError String me) => DatalogProgram -> Atom -> me Atom
atomDesugarExprs d a = do 
    args <- mapM (\(m, e) -> (m,) <$> exprDesugar d e) $ atomArgs a
    return a{atomArgs = args}

rhsDesugarExprs :: (MonadError String me) => DatalogProgram -> RuleRHS -> me RuleRHS
rhsDesugarExprs d l@RHSLiteral{}   = do
    a <- atomDesugarExprs d (rhsAtom l)
    return l{rhsAtom = a}
rhsDesugarExprs d c@RHSCondition{} = do
    e <- exprDesugar d (rhsExpr c)
    return c{rhsExpr = e}
rhsDesugarExprs d a@RHSAggregate{} = do
    e <- exprDesugar d (rhsAggExpr a)
    return a{rhsAggExpr = e}
rhsDesugarExprs d m@RHSFlatMap{}   = do
    e <- exprDesugar d (rhsMapExpr m)
    return m{rhsMapExpr = e}

exprDesugar :: (MonadError String me) => DatalogProgram -> Expr -> me Expr
exprDesugar d e = exprFoldM (exprDesugar' d) e

exprDesugar' :: (MonadError String me) => DatalogProgram -> ENode -> me Expr
exprDesugar' d e =
    case e of
         EStruct p c as -> do
            cons@Constructor{..} <- checkConstructor p d c
            as' <- case as of
                        [] | null consArgs
                           -> return as
                        [] -> desugarNamedArgs d cons (pos e) as
                        _  | all (null . fst) as
                           -> desugarPosArgs d cons (pos e) as
                        _  | any (null . fst) as
                           -> err (pos e) $ "Expression mixes named and positional arguments to type constructor"
                        _  -> desugarNamedArgs d cons (pos e) as
            return $ E e{exprStructFields = as'}
         _              -> return $ E e

desugarPosArgs :: (MonadError String me) => DatalogProgram -> Constructor -> Pos -> [(String, Expr)] -> me [(String, Expr)]
desugarPosArgs d cons@Constructor{..} p as = do
    assert (length as == length consArgs) p
           $ "Number of arguments does not match constructor declaration: " ++ show cons
    return $ zip (map name consArgs) (map snd as)

desugarNamedArgs :: (MonadError String me) => DatalogProgram -> Constructor -> Pos -> [(String, Expr)] -> me [(String, Expr)]
desugarNamedArgs d cons@Constructor{..} p as = do
    uniq' (\_ -> p) id ("Multiple occurrences of a field " ++) $ map fst as
    mapM (\(n,e) -> assert (isJust $ find ((==n) . name) consArgs) (pos e)
                           $ "Unknown field " ++ n) as
    return $ map (\f -> (name f, maybe ePHolder id $ lookup (name f) as)) consArgs

typedefValidate :: (MonadError String me) => DatalogProgram -> TypeDef -> me ()
typedefValidate d@DatalogProgram{..} TypeDef{..} = do
    uniq' (\_ -> tdefPos) id ("Multiple definitions of type argument " ++) tdefArgs
    mapM_ (\a -> assert (M.notMember a progTypedefs) tdefPos
                        $ "Type argument " ++ a ++ " conflicts with user-defined type name")
          tdefArgs
    case tdefType of
         Nothing -> return ()
         Just t  -> do
             typeValidate d tdefArgs t
             let dif = tdefArgs \\ typeTypeVars t
             assert (null dif) tdefPos 
                    $ "The following type variables are not used in type definition: " ++ intercalate "," dif

typeValidate :: (MonadError String me) => DatalogProgram -> [String] -> Type -> me ()
typeValidate _ _     TString{}        = return ()
typeValidate _ _     TInt{}           = return ()
typeValidate _ _     TBool{}          = return ()
typeValidate _ _     (TBit p w)       =
    assert (w>0) p "Integer width must be greater than 0"
typeValidate d tvars (TStruct p cs)   = do
    uniqNames ("Multiple definitions of constructor " ++) cs
    mapM_ (consValidate d tvars) cs
    mapM_ (\grp -> assert (length (nub $ map typ grp) == 1) p $
                          "Field " ++ (name $ head grp) ++ " is declared with different types")
          $ sortAndGroup name $ concatMap consArgs cs
typeValidate d tvars (TTuple _ ts)    =
    mapM_ (typeValidate d tvars) ts
typeValidate d tvars (TUser p n args) = do
    t <- checkType p d n
    let expect = length (tdefArgs t)
    let actual = length args
    assert (expect == actual) p $
           "Expected " ++ show expect ++ " type arguments to " ++ n ++ ", found " ++ show actual
    mapM_ (typeValidate d tvars) args
    return ()
typeValidate d tvars (TVar p v)       =
    assert (elem v tvars) p $ "Unknown type variable " ++ v
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
    mapM_ (typeValidate d tvars . fieldType) funcArgs
    typeValidate d tvars funcType

funcValidateDefinition :: (MonadError String me) => DatalogProgram -> Function -> me ()
funcValidateDefinition d f@Function{..} = do
    case funcDef of
         Nothing  -> return ()
         Just def -> exprValidate d (funcTypeVars f) (CtxFunc f) def

relValidate :: (MonadError String me) => DatalogProgram -> Relation -> me ()
relValidate d Relation{..} = do 
    uniqNames ("Multiple definitions of column " ++) relArgs
    mapM_ (typeValidate d [] . fieldType) relArgs

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

--ruleValidate :: (MonadError String me) => Refine -> Relation -> Rule -> me ()
--ruleValidate r rel@Relation{..} rl@Rule{..} = do
--    assertR r (length ruleLHS == length relArgs) (pos rl)
--            $ "Number of arguments in the left-hand-side of the rule does not match the number of fields in relation " ++ name rel
--    mapM_ (exprValidate r (CtxRuleR rel rl)) ruleRHS
--    mapIdxM_ (\e i -> exprValidate r (CtxRuleL rel rl i) e) ruleLHS
--    
--    only boolean or assignment in RHSCondition
--    variable cannot be declared and used in the same atom
--    validate aggregate function used
--    aggregate, flatmap, assigned vars are not previously declared
--    no new variables in negative literals
--
--
-- atomValidate: 
--   check number of arguments
--   validate argument expressions
--

exprValidate :: (MonadError String me) => DatalogProgram -> [String] -> ECtx -> Expr -> me ()
exprValidate d tvars ctx e = {-trace ("exprValidate " ++ show e ++ " in \n" ++ show ctx) $ -} do 
    exprTraverseCtxM (exprValidate1 d tvars) ctx e
    exprTraverseTypeME d (exprValidate2 d) ctx e
    exprTraverseCtxM (exprCheckMatchPatterns d) ctx e

-- This function does not perform type checking: just checks that all functions and
-- variables are defined; the number of arguments matches declarations, etc.
exprValidate1 :: (MonadError String me) => DatalogProgram -> [String] -> ECtx -> ExprNode Expr -> me ()
exprValidate1 d _ ctx (EVar p v)          = do _ <- checkVar p d ctx v
                                               return ()
exprValidate1 d _ ctx (EApply p f as)     = do fun <- checkFunc p d f
                                               assert (length as == length (funcArgs fun)) p
                                                      "Number of arguments does not match function declaration"
exprValidate1 _ _ _   EField{}            = return ()
exprValidate1 _ _ _   EBool{}             = return ()
exprValidate1 _ _ _   EInt{}              = return ()
exprValidate1 _ _ _   EString{}           = return ()
exprValidate1 _ _ _   EBit{}              = return ()
exprValidate1 d _ ctx (EStruct p c _)     = do -- initial validation was performed by exprDesugar 
    let tdef = consType d c
    when (ctxInSetL ctx) $
        assert ((length $ typeCons $ fromJust $ tdefType tdef) == 1) p
               $ "Type constructor in the left-hand side of an assignment is only allowed for types with one constructor, \
                 \ but \"" ++ name tdef ++ "\" has multiple constructors"
exprValidate1 _ _ _   ETuple{}            = return ()
exprValidate1 _ _ _   ESlice{}            = return ()
exprValidate1 _ _ _   EMatch{}            = return ()
exprValidate1 d _ ctx (EVarDecl p v) | ctxInSetL ctx || ctxInMatchPat ctx
                                          = checkNoVar p d ctx v
                                     | otherwise 
                                          = do checkNoVar p d ctx v
                                               assert (ctxIsTyped ctx) p "Variable declared without a type"
                                               assert (ctxIsSeq1 $ ctxParent ctx) p 
                                                      "Variable declaration is not allowed in this context"
exprValidate1 _ _ _   ESeq{}              = return ()
exprValidate1 _ _ _   EITE{}              = return ()
exprValidate1 d _ ctx (ESet _ l _)        = checkLExpr d ctx l
exprValidate1 _ _ _   EBinOp{}            = return ()
exprValidate1 _ _ _   EUnOp{}             = return ()

exprValidate1 _ _ ctx (EPHolder p)        = do
    let msg = case ctx of
                   CtxStruct _ _ f -> "Argument " ++ f ++ " must be specified in this context"
                   _               -> "_ is not allowed in this context"
    assert (ctxPHolderAllowed ctx) p msg
exprValidate1 d tvs _ (ETyped _ _ t)      = typeValidate d tvs t

-- True if a placeholder ("_") can appear in this context
ctxPHolderAllowed :: ECtx -> Bool
ctxPHolderAllowed ctx = 
    case ctx of 
         CtxSetL{}      -> True
         CtxTyped{}     -> pres
         CtxRuleL{}     -> True
         CtxRuleRAtom{} -> True
         CtxStruct{}    -> pres
         CtxTuple{}     -> pres
         CtxMatchPat{}  -> True
         _              -> False
    where 
    par = ctxParent ctx
    pres = ctxPHolderAllowed par

checkNoVar :: (MonadError String me) => Pos -> DatalogProgram -> ECtx -> String -> me ()
checkNoVar p d ctx v = assert (isNothing $ lookupVar d ctx v) p 
                              $ "Variable " ++ v ++ " already defined in this scope"

-- Traverse again with types.  This pass ensures that all sub-expressions
-- have well-defined types that match their context
exprTraverseTypeME :: (MonadError String me) => DatalogProgram -> (ECtx -> ExprNode Type -> me ()) -> ECtx -> Expr -> me ()
exprTraverseTypeME d = exprTraverseCtxWithM (\ctx e -> do 
    let e' = exprMap Just e
    --trace ("exprTraverseTypeME " ++ show ctx ++ "\n    " ++ show e) $ return ()
    case exprNodeType d ctx e' of
         Just t  -> do case ctxExpectType d ctx of
                            Nothing -> return ()
                            Just t' -> assert (typesMatch d t t') (pos e) 
                                              $ "Couldn't match expected type " ++ show t' ++ " with actual type " ++ show t
                                                {-++ " (context: " ++ show ctx ++ ")"-}
                       return t
         Nothing -> err (pos e) $ "Expression " ++ show e ++ " has unknown type in " ++ show ctx) 

exprValidate2 :: (MonadError String me) => DatalogProgram -> ECtx -> ExprNode Type -> me ()
exprValidate2 d ctx (EField p e f)      = do 
    case typ' d e of
         t@TStruct{} -> assert (isJust $ find ((==f) . name) $ structFields t) p
                               $ "Unknown field \"" ++ f ++ "\" in struct of type " ++ show t 
         _           -> err (pos e) $ "Expression is not a struct"
    assert (not $ structFieldGuarded (typ' d e) f) p 
           $ "Access to guarded field \"" ++ f ++ "\""
                                                       
exprValidate2 d _   (ESlice p e h l)    = 
    case typ' d e of
        TBit _ w -> do assert (h >= l) p 
                           $ "Upper bound of the slice must be greater than lower bound"
                       assert (h < w) p
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
               -> do {m; isstr2}
        Concat -> do {isbit1; isbit2}
    where m = checkTypesMatch p d e1 e2
          isint1 = assert (isInt d e1 || isBit d e1) (pos e1) "Not an integer"
          isint2 = assert (isInt d e2 || isBit d e2) (pos e2) "Not an integer"
          isbit1 = assert (isBit d e1) (pos e1) "Not a bit vector"
          isbit2 = assert (isBit d e2) (pos e2) "Not a bit vector"
          isstr2 = assert (isString d e2) (pos e2) "Not a string"
          isbool = assert (isBool d e1) (pos e1) "Not a Boolean"

exprValidate2 d _   (EUnOp _ BNeg e)    = 
    assert (isBit d e) (pos e) "Not a bit vector"
--exprValidate2 d ctx (EVarDecl p x)      = assert (isJust $ ctxExpectType d ctx) p 
--                                                 $ "Cannot determine type of variable " ++ x -- Context: " ++ show ctx
exprValidate2 d _  (EITE p _ t e)       = checkTypesMatch p d t e
exprValidate2 _ _   _                   = return ()

checkLExpr :: (MonadError String me) => DatalogProgram -> ECtx -> Expr -> me ()
checkLExpr d ctx e = 
    assert (isLExpr d ctx e) (pos e) 
           $ "Expression " ++ show e ++ " is not an l-value" -- in context " ++ show ctx


exprCheckMatchPatterns :: (MonadError String me) => DatalogProgram -> ECtx -> ExprNode Expr -> me ()
exprCheckMatchPatterns d ctx e@(EMatch _ x cs) = do
    let t = exprType d (CtxMatchExpr e ctx) x
        ct0 = typeConsTree d t
    ct <- foldM (\ct pat -> do let (leftover, abducted) = consTreeAbduct d ct pat
                               assert (not $ consTreeEmpty abducted) (pos pat)
                                      "Unsatisfiable match pattern"
                               return leftover)
                ct0 (map fst cs)
    assert (consTreeEmpty ct) (pos x) "Non-exhaustive match patterns"

exprCheckMatchPatterns _ _   _               = return ()
