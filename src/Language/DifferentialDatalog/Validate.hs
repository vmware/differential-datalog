{-# LANGUAGE RecordWildCards, FlexibleContexts, LambdaCase #-}

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
validate :: (MonadError String me) => DatalogProgram -> me ()
validate d@DatalogProgram{..} = do
    uniqNames ("Multiple definitions of constructor " ++) 
              $ progConstructors d
    -- Validate typedef's
    mapM_ (typedefValidate d) $ M.elems progTypedefs
    -- No cyclic dependencies between user-defined types
    checkAcyclicTypes d
    -- Validate function prototypes
    mapM_ (funcValidateProto d) $ M.elems progFunctions
    mapM_ (funcValidateDefinition d) $ M.elems progFunctions
    -- Validate relation declarations
    mapM_ (relValidate d) $ M.elems progRelations
    return ()

--    mapM_ (relValidate2 r)   refineRels
--    maybe (return ())
--          (\cyc -> errR r (pos $ getRelation r $ snd $ head cyc)
--                     $ "Dependency cycle among relations: " ++ (intercalate ", " $ map (name . snd) cyc))
--          $ (grCycle $ relGraph r)
--    mapM_ (relValidate3 r)   refineRels
--    validateFinal r
--
---- Validate final refinement before generating topology from it
--validateFinal :: (MonadError String me) => Refine -> me ()
--validateFinal r = do
--    case grCycle (funcGraphNoSink r) of
--         Nothing -> return ()
--         Just t  -> err (pos $ getFunc r $ snd $ head t) $ "Recursive function definition: " ++ (intercalate "->" $ map (name . snd) t)


typedefValidate :: (MonadError String me) => DatalogProgram -> TypeDef -> me ()
typedefValidate d@DatalogProgram{..} TypeDef{..} = do
    uniq' (\_ -> tdefPos) id ("Multiple definitions of type argument " ++) tdefArgs
    mapM_ (\a -> assert (M.notMember a progTypedefs) tdefPos 
                        $ "Type argument " ++ a ++ " conflicts with user-defined type name")
          tdefArgs
    -- TODO: all type arguments are used in type declaration
    case tdefType of
         Nothing -> return ()
         Just t  -> typeValidate d tdefArgs t

typeValidate :: (MonadError String me) => DatalogProgram -> [String] -> Type -> me ()
typeValidate _ _     TString{}        = return ()
typeValidate _ _     TInt{}           = return ()
typeValidate _ _     TBool{}          = return ()
typeValidate _ _     (TBit p w)       =
    assert (w>0) p "Integer width must be greater than 0"
typeValidate d tvars (TStruct _ cs)   = do
    uniqNames ("Multiple definitions of constructor " ++) cs
    mapM_ (consValidate d tvars) cs
    mapM_ (uniq (show . typ) (\a -> "Argument " ++ name a ++ " is re-declared with a different type"))
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
          (\cyc -> throwError $ "Dependency cycle among types: " ++ 
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

exprValidate :: (MonadError String me) => DatalogProgram -> [String] -> ECtx -> Expr -> me ()
exprValidate d tvars ctx e = {-trace ("exprValidate " ++ show e ++ " in \n" ++ show ctx) $ -} do 
    exprTraverseCtxM (exprValidate1 d tvars) ctx e
    exprTraverseTypeME d (exprValidate2 d) ctx e

-- This function does not perform type checking: just checks that all functions and
-- variables are defined; the number of arguments matches declarations, etc.
exprValidate1 :: (MonadError String me) => DatalogProgram -> [String] -> ECtx -> ExprNode Expr -> me ()
exprValidate1 d _ ctx (EVar p v)          = do _ <- checkVar p d ctx v
                                               return ()
exprValidate1 d _ ctx (EApply p f as)     = do fun <- checkFunc p d f
                                               assert (length as == length (funcArgs fun)) p
                                                      "Number of arguments does not match function declaration"
exprValidate1 _ _ _   (EField _ _ _)      = return ()
exprValidate1 _ _ _   (EBool _ _)         = return ()
exprValidate1 _ _ _   (EInt _ _)          = return ()
exprValidate1 _ _ _   (EString _ _)       = return ()
exprValidate1 _ _ _   (EBit _ _ _)        = return ()
exprValidate1 d _ _   (EStruct p c as)    = do cons <- checkConstructor p d c
                                               assert (length as == length (consArgs cons)) p
                                                      $ "Number of arguments does not match constructor declaration: " ++ show cons
exprValidate1 _ _ _   (ETuple _ _)        = return ()
exprValidate1 _ _ _   (ESlice _ _ _ _)    = return ()
exprValidate1 _ _ _   (EMatch _ _ _)      = return ()
exprValidate1 d _ ctx (EVarDecl p v) | ctxInSetL ctx || ctxInMatchPat ctx
                                          = checkNoVar p d ctx v
                                     | otherwise 
                                          = do assert (ctxIsTyped ctx) p "Variable declared without a type"
                                               assert (ctxIsSeq1 $ ctxParent ctx) p 
                                                      "Variable declaration is not allowed in this context"
exprValidate1 _ _ _   (ESeq _ _ _)        = return ()
exprValidate1 _ _ _   (EITE _ _ _ _)      = return ()
exprValidate1 d _ ctx (ESet _ l _)        = checkLExpr d ctx l
exprValidate1 _ _ _   (EBinOp _ _ _ _)    = return ()
exprValidate1 _ _ _   (EUnOp _ Not _)     = return ()
exprValidate1 _ _ _   (EUnOp _ BNeg _)    = return ()
exprValidate1 _ _ _   (EPHolder _)        = return ()
exprValidate1 d tvs _ (ETyped _ _ t)      = typeValidate d tvs t

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
           $ "Guarded field " ++ f ++ " accessed in query condition"
                                                       
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
    -- TODO: check completeness of patterns

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
        Concat -> do {isbit1; isbit2}
    where m = checkTypesMatch p d e1 e2
          isint1 = assert (isInt d e1 || isBit d e1) (pos e1) $ "Not an integer"
          isint2 = assert (isInt d e2 || isBit d e2) (pos e2) $ "Not an integer"
          isbit1 = assert (isBit d e1) (pos e1) $ "Not a bit vector"
          isbit2 = assert (isBit d e2) (pos e2) $ "Not a bit vector"
          isbool = assert (isBool d e1) (pos e1) $ "Not a Boolean"

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
