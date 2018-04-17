{-# LANGUAGE ImplicitParams, RecordWildCards, LambdaCase, TupleSections #-}

module Language.DifferentialDatalog.Expr (
    exprMapM,
    exprMap,
    exprFold,
    exprFoldM,
    exprTraverseCtxWithM,
    exprTraverseCtxM,
    exprTraverseM,
    exprFoldCtx,
    exprFoldCtxM,
    exprCollectCtxM,
    exprCollectM,
    exprCollectCtx,
    exprCollect,
    exprVars,
    exprVarDecls,
    exprFuncs,
    exprFuncsRec,
    isLExpr,
    isLVar
    --isLRel
    --exprSplitLHS,
    --exprSplitVDecl,
    --exprInline,
    --expr2Statement,
    --exprModifyResult,
    --ctxExpectsStat,
    --ctxMustReturn,
    --exprIsStatement,
    --exprVarSubst,
    --exprVarRename,
    --exprMutatorsNonRec
    --exprScalars
    --exprDeps
    --exprSubst
    --combineCascades
    ) where

import Data.List
import Data.Maybe
import Control.Monad
import Control.Monad.Identity
import Control.Monad.State
--import Debug.Trace

import Language.DifferentialDatalog.Ops
import Language.DifferentialDatalog.Syntax
import Language.DifferentialDatalog.NS
import Language.DifferentialDatalog.Util
import Language.DifferentialDatalog.Name
--import Language.DifferentialDatalog.Type

-- depth-first fold of an expression
exprFoldCtxM :: (Monad m) => (ECtx -> ExprNode b -> m b) -> ECtx -> Expr -> m b
exprFoldCtxM f ctx (E n) = exprFoldCtxM' f ctx n

exprFoldCtxM' :: (Monad m) => (ECtx -> ExprNode b -> m b) -> ECtx -> ENode -> m b
exprFoldCtxM' f ctx   (EVar p v)              = f ctx $ EVar p v
exprFoldCtxM' f ctx e@(EApply p fun as)       = f ctx =<< EApply p fun <$> (mapIdxM (\a i -> exprFoldCtxM f (CtxApply e ctx i) a) as)
exprFoldCtxM' f ctx e@(EField p s fl)         = do s' <- exprFoldCtxM f (CtxField e ctx) s
                                                   f ctx $ EField p s' fl
exprFoldCtxM' f ctx   (EBool p b)             = f ctx $ EBool p b
exprFoldCtxM' f ctx   (EInt p i)              = f ctx $ EInt p i
exprFoldCtxM' f ctx   (EString p s)           = f ctx $ EString p s
exprFoldCtxM' f ctx   (EBit p w v)            = f ctx $ EBit p w v
exprFoldCtxM' f ctx e@(EStruct p c fs)        = f ctx =<< EStruct p c <$> (mapM (\(fname, fl) -> (fname,) <$> exprFoldCtxM f (CtxStruct e ctx fname) fl) fs)
exprFoldCtxM' f ctx e@(ETuple p fs)           = f ctx =<< ETuple p <$> (mapIdxM (\fl i -> exprFoldCtxM f (CtxTuple e ctx i) fl) fs)
exprFoldCtxM' f ctx e@(ESlice p v h l)        = do v' <- exprFoldCtxM f (CtxSlice e ctx) v
                                                   f ctx $ ESlice p v' h l
exprFoldCtxM' f ctx e@(EMatch p m cs)         = do m' <- exprFoldCtxM f (CtxMatchExpr e ctx) m
                                                   cs' <- mapIdxM (\(e1, e2) i -> (,) <$> (exprFoldCtxM f (CtxMatchPat e ctx i) e1) <*>
                                                                                          (exprFoldCtxM f (CtxMatchVal e ctx i) e2)) cs
                                                   f ctx $ EMatch p m' cs'
exprFoldCtxM' f ctx   (EVarDecl p v)          = f ctx $ EVarDecl p v
exprFoldCtxM' f ctx e@(ESeq p l r)            = f ctx =<< ESeq p <$> exprFoldCtxM f (CtxSeq1 e ctx) l <*> 
                                                                     exprFoldCtxM f (CtxSeq2 e ctx) r
exprFoldCtxM' f ctx e@(EITE p i t el)         = f ctx =<< EITE p <$>
                                                          exprFoldCtxM f (CtxITEIf e ctx) i <*>
                                                          exprFoldCtxM f (CtxITEThen e ctx) t <*>
                                                          exprFoldCtxM f (CtxITEElse e ctx) el
exprFoldCtxM' f ctx e@(ESet p l r)            = f ctx =<< ESet p <$> exprFoldCtxM f (CtxSetL e ctx) l <*> 
                                                                     exprFoldCtxM f (CtxSetR e ctx) r
exprFoldCtxM' f ctx e@(EBinOp p op l r)       = f ctx =<< EBinOp p op <$> exprFoldCtxM f (CtxBinOpL e ctx) l <*>
                                                                          exprFoldCtxM f (CtxBinOpR e ctx) r
exprFoldCtxM' f ctx e@(EUnOp p op x)          = f ctx =<< EUnOp p op <$> (exprFoldCtxM f (CtxUnOp e ctx) x)
exprFoldCtxM' f ctx   (EPHolder p)            = f ctx $ EPHolder p
exprFoldCtxM' f ctx e@(ETyped p x t)          = do x' <- exprFoldCtxM f (CtxTyped e ctx) x
                                                   f ctx $ ETyped p x' t

exprMapM :: (Monad m) => (a -> m b) -> ExprNode a -> m (ExprNode b)
exprMapM g e = case e of
                   EVar p v            -> return $ EVar p v
                   EApply p f as       -> EApply p f <$> mapM g as
                   EField p s f        -> (\s' -> EField p s' f) <$> g s
                   EBool p b           -> return $ EBool p b
                   EInt p i            -> return $ EInt p i
                   EString p s         -> return $ EString p s
                   EBit p w v          -> return $ EBit p w v
                   EStruct p s fs      -> EStruct p s <$> mapM (\(fname, e) -> (fname,) <$> g e) fs
                   ETuple p fs         -> ETuple p <$> mapM g fs
                   ESlice p v h l      -> (\v' -> ESlice p v' h l) <$> g v
                   EMatch p m cs       -> EMatch p <$> g m <*> mapM (\(e1, e2) -> (,) <$> g e1 <*> g e2) cs
                   EVarDecl p v        -> return $ EVarDecl p v
                   ESeq p l r          -> ESeq p <$> g l <*> g r
                   EITE p i t el       -> EITE p <$> g i <*> g t <*> g el
                   ESet p l r          -> ESet p <$> g l <*> g r
                   EBinOp p op l r     -> EBinOp p op <$> g l <*> g r
                   EUnOp p op v        -> EUnOp p op <$> g v
                   EPHolder p          -> return $ EPHolder p
                   ETyped p v t        -> (\v' -> ETyped p v' t) <$> g v


exprMap :: (a -> b) -> ExprNode a -> ExprNode b
exprMap f e = runIdentity $ exprMapM (\e' -> return $ f e') e

exprFoldCtx :: (ECtx -> ExprNode b -> b) -> ECtx -> Expr -> b
exprFoldCtx f ctx e = runIdentity $ exprFoldCtxM (\ctx' e' -> return $ f ctx' e') ctx e

exprFoldM :: (Monad m) => (ExprNode b -> m b) -> Expr -> m b
exprFoldM f e = exprFoldCtxM (\_ e' -> f e') undefined e

exprFold :: (ExprNode b -> b) -> Expr -> b
exprFold f e = runIdentity $ exprFoldM (return . f) e

exprTraverseCtxWithM :: (Monad m) => (ECtx -> ExprNode a -> m a) -> (ECtx -> ExprNode a -> m ()) -> ECtx -> Expr -> m ()
exprTraverseCtxWithM g f ctx e = do {_ <- exprFoldCtxM (\ctx' e' -> do {f ctx' e'; g ctx' e'}) ctx e; return ()}

exprTraverseCtxM :: (Monad m) => (ECtx -> ENode -> m ()) -> ECtx -> Expr -> m ()
exprTraverseCtxM = exprTraverseCtxWithM (\_ x -> return $ E x)

exprTraverseM :: (Monad m) => (ENode -> m ()) -> Expr -> m ()
exprTraverseM f = exprTraverseCtxM (\_ x -> f x) undefined

exprCollectCtxM :: (Monad m) => (ECtx -> ExprNode b -> m b) -> (b -> b -> b) -> ECtx -> Expr -> m b
exprCollectCtxM f op ctx e = exprFoldCtxM g ctx e
    where g ctx' x = do x' <- f ctx' x
                        return $ case x of
                                     EVar _ _              -> x'
                                     EApply _ _ as         -> foldl' op x' as
                                     EField _ s _          -> x' `op` s
                                     EBool _ _             -> x'
                                     EInt _ _              -> x'
                                     EString _ _           -> x'
                                     EBit _ _ _            -> x'
                                     EStruct _ _ fs        -> foldl' (\a (_, x) -> op a x) x' fs
                                     ETuple _ fs           -> foldl' op x' fs
                                     ESlice _ v _ _        -> x' `op` v
                                     EMatch _ m cs         -> foldl' (\a (p,v) -> a `op` p `op` v) (x' `op` m) cs
                                     EVarDecl _ _          -> x'    
                                     ESeq _ l r            -> x' `op` l `op` r       
                                     EITE _ i t el         -> x' `op` i `op` t `op` el 
                                     ESet _ l r            -> x' `op` l `op` r
                                     EBinOp _ _ l r        -> x' `op` l `op` r  
                                     EUnOp _ _ v           -> x' `op` v
                                     EPHolder _            -> x'
                                     ETyped _ v _          -> x' `op` v

exprCollectM :: (Monad m) => (ExprNode b -> m b) -> (b -> b -> b) -> Expr -> m b
exprCollectM f op e = exprCollectCtxM (\_ e' -> f e') op undefined e

exprCollectCtx :: (ECtx -> ExprNode b -> b) -> (b -> b -> b) -> ECtx -> Expr -> b
exprCollectCtx f op ctx e = runIdentity $ exprCollectCtxM (\ctx' x -> return $ f ctx' x) op ctx e

exprCollect :: (ExprNode b -> b) -> (b -> b -> b) -> Expr -> b
exprCollect f op e = runIdentity $ exprCollectM (return . f) op e

-- enumerate all variables that occur in the expression
exprVars :: ECtx -> Expr -> [(String, ECtx)]
exprVars ctx e = exprCollectCtx (\ctx' e' -> case e' of
                                                  EVar _ v -> [(v, ctx')]
                                                  _        -> [])
                                (++) ctx e

-- Variables declared inside expression, visible in the code that follows the expression
exprVarDecls :: ECtx -> Expr -> [(String, ECtx)]
exprVarDecls ctx e = 
    exprFoldCtx (\ctx' e' -> 
                  case e' of
                       EStruct _ _ fs -> concatMap snd fs
                       ETuple _ fs    -> concat fs
                       EVarDecl _ v   -> [(v, ctx')]
                       ESet _ l _     -> l
                       ETyped _ e'' _ -> e''
                       _              -> []) ctx e

-- Non-recursively enumerate all functions invoked by the expression
exprFuncs :: Expr -> [String]
exprFuncs e = exprFuncs' [] e

exprFuncs' :: [String] -> Expr -> [String]
exprFuncs' acc e = nub $ exprCollect (\case
                                       EApply _ f _ -> if' (elem f acc) [] [f]
                                       _            -> []) 
                                     (++) e

-- Recursively enumerate all functions invoked by the expression
exprFuncsRec :: DatalogProgram -> Expr -> [String]
exprFuncsRec d e = exprFuncsRec' d [] e

exprFuncsRec' :: DatalogProgram -> [String] -> Expr -> [String]
exprFuncsRec' d acc e = 
    let new = exprFuncs' acc e in
    new ++ foldl' (\acc' f -> maybe acc' ((acc'++) . exprFuncsRec' d (acc++new++acc')) $ funcDef $ getFunc d f) [] new

isLExpr :: DatalogProgram -> ECtx -> Expr -> Bool
isLExpr d ctx e = exprFoldCtx (isLExpr' d) ctx e

isLExpr' :: DatalogProgram -> ECtx -> ExprNode Bool -> Bool
isLExpr' d ctx (EVar _ v)       = isLVar d ctx v
isLExpr' _ _   (EField _ e _)   = e
isLExpr' _ _   (ETuple _ as)    = and as
isLExpr' _ _   (EStruct _ _ as) = all snd as
isLExpr' _ _   (EVarDecl _ _)   = True
isLExpr' _ _   (EPHolder _)     = True
isLExpr' _ _   (ETyped _ e _)   = e
isLExpr' _ _   _                = False

isLVar :: DatalogProgram -> ECtx -> String -> Bool
isLVar d ctx v = isJust $ find ((==v) . name) $ fst $ ctxVars d ctx

---- Inline method bodies
--exprInline :: Refine -> [String] -> ECtx -> Expr -> Expr
--exprInline r skip ctx e = exprFoldCtx (exprInline' r skip) ctx e'
--    where e' = evalState (exprFoldCtxM (exprPrecomputeArgs r) ctx e) 0
--
---- compute arguments ahead of function invocations
--exprPrecomputeArgs :: Refine -> ECtx -> ENode -> State Int Expr
--exprPrecomputeArgs r ctx e = 
--    case e of
--         EApply _ f as -> do
--             let Function{..} = getFunc r f
--             (ps, as') <- liftM unzip
--                          $ mapIdxM (\a i -> if simple a
--                                                then return (Nothing, a)
--                                                else do let t = typ $ funcArgs !! i
--                                                        arg <- allocArg
--                                                        let vdecl = eSet (eTyped (eVarDecl arg) t) a
--                                                        return (Just vdecl, eVar arg)) as
--             return $ exprSequence $ (catMaybes ps) ++ [eApply f as']
--         _ -> return $ E e
--    where simple :: Expr -> Bool
--          simple (E EVar{})         = True
--          simple (E EBool{})        = True
--          simple (E EBit{})         = True
--          simple (E EInt{})         = True
--          simple (E EString{})      = True
--          simple (E (ETyped _ x _)) = simple x
--          simple _                  = False
--
--allocArg :: State Int String
--allocArg = do modify (1+)
--              liftM (("a#"++) . show) get
--
--exprInline' :: Refine -> [String] -> ECtx -> ENode -> Expr
--exprInline' r skip ctx e@(EApply _ f as) | elem f skip    = E e
--                                         | isJust funcDef = exprVarSubst subst vsubst body
--                                         | otherwise      = E e
--    where func@Function{..} = getFunc r f
--          body = exprInline r skip (CtxFunc func ctx) $ fromJust funcDef
--          -- rename local vars; substitute arguments
--          subst v = case findIndex ((==v) . name) funcArgs of
--                         Just i  -> as !! i
--                         Nothing -> eVar $ f ++ ":" ++ v
--          vsubst v = f ++ ":" ++ v
--exprInline' r skip ctx (EField _ (E (EStruct _ c fs)) f) = exprInline r skip ctx v
--    where v = fs !! (fromJust $ findIndex ((== f) . name) $ consArgs $ getConstructor r c)
--exprInline' r skip ctx e@(EApplyLambda _ (E e'@(ELambda _ as' _ e'')) as) = exprVarSubst subst vsubst body
--    where body = exprInline r skip (CtxLambda e' (CtxApplyLambda e ctx)) e''
--          -- rename local vars; substitute arguments
--          subst v = case findIndex ((==v) . name) as' of
--                         Just i  -> as !! i
--                         Nothing -> eVar $ "lambda!:" ++ v
--          -- XXX: this renaming leads to conflicts for nested lambdas or two lambdas 
--          -- being in scope at the same time.  Instead, we must generate a unique 
--          -- prefix for each lambda invocation.
--          vsubst v = "lambda!:" ++ v
--exprInline' _ _ _   e                                     = E e
--
---- every variable must be declared in a separate statement, e.g.,
---- (x, var y) = ...  ===> var y: Type; (x,y) = ...
----exprNormalizeVarDecls :: Refine -> ECtx -> Expr -> Expr
----exprNormalizeVarDecls = error "exprNormalizeVarDecls is undefined"
--
--
---- Convert expression to "statement" form, in which it can 
---- be easily translated into a statement-based language
--expr2Statement :: Refine -> ECtx -> Expr -> State Int Expr
--expr2Statement r ctx e = do 
--    (_, e') <- exprFoldCtxM (expr2Statement_ r) ctx e
--    return $ prefMerge e'
--
--expr2Statement_ :: Refine -> ECtx -> ExprNode (Expr, ([Expr], Expr)) -> State Int (Expr, ([Expr], Expr))
--expr2Statement_ r ctx e = do let orig = exprMap fst e
--                             (p, e') <- expr2Statement' r ctx orig (exprMap snd e)
--                             return (E orig, (p, e'))
--
--expr2Statement' :: Refine -> ECtx -> ENode -> ExprNode ([Expr], Expr) -> State Int ([Expr], Expr)
--expr2Statement' _ _   _ (EBuiltin _ f as)              = return (concatMap fst as, eBuiltin f $ map snd as)
--expr2Statement' _ _   _ (EApply _ f as)                = return (concatMap fst as, eApply f $ map snd as)
--expr2Statement' _ _   _ (EField _ (p,e) f)             = return (p, exprModifyResult (\e' -> eField e' f) e)
--expr2Statement' _ _   _ (ELocation _ l (p,k) d)        = return (p, eLocation l k d)
--expr2Statement' _ ctx e EStruct{} | ctxInMatchPat ctx  = return ([], E e)
--expr2Statement' _ _   _ (EStruct _ c fs)               = return (concatMap fst fs, eStruct c $ map snd fs)
--expr2Statement' _ _   _ (ETuple _ fs)                  = return (concatMap fst fs, eTuple $ map snd fs)
--expr2Statement' _ _   _ (ESlice _ (p,e) h l)           = return (p, exprModifyResult (\e' -> eSlice e' h l) e)
--expr2Statement' r ctx e (EMatch _ (p,m) cs)            = do (p', e'') <- exprPrecomputeVar ctx (extype r ctx $ E e) e'
--                                                            return (p++p', e'')
--    where e' = eMatch m $ map (\(c,v) -> (noprefMerge c, prefMerge v)) cs
--expr2Statement' r ctx e (EITE _ (p,i) t me)            = do (p', e'') <- exprPrecomputeVar ctx (extype r ctx $ E e) e'
--                                                            return (p++p', e'')
--    where e' = eITE i (prefMerge t) (fmap prefMerge me)
--expr2Statement' _ _   _ (ESet _ (pl,l) (pv,v))         = return (pl ++ pv, exprModifyResult (fset l) v)
--expr2Statement' _ _   _ (ESend _ (p,d))                = return (p, exprModifyResult eSend d)
--expr2Statement' _ _   _ (EBinOp _ op (p1,e1) (p2,e2))  = return (p1 ++ p2, eBinOp op e1 e2)
--expr2Statement' _ _   _ (EUnOp _ op (p,e))             = return (p, exprModifyResult (eUnOp op) e)
--expr2Statement' _ _   _ (ETyped _ (p,e) t)             = return (p, exprModifyResult (\e' -> eTyped e' t) e)
--expr2Statement' _ _   _ (EPut _ t (p,v))               = return (p, exprModifyResult (ePut t) v)
--expr2Statement' _ _   _ (EDelete _ t c)                = return ([], eDelete t $ prefMerge c)
--expr2Statement' r ctx e (EVarDecl _ vn) | operand ctx  = return ([eTyped (eVarDecl vn) $ extype r ctx $ E e], eVar vn)
--                                        | otherwise    = return ([], eVarDecl vn)
--expr2Statement' _ ctx _ (ESeq _ (p1,e1) (p2,e2)) | operand ctx = return (p1++[e1]++p2, e2)
--                                                 | otherwise   = return ([], exprSequence $ p1 ++ [e1] ++ p2 ++ [e2])
--expr2Statement' _ _   _ (EPar _ e1 e2)                 = return ([], ePar (prefMerge e1) (prefMerge e2))
--expr2Statement' r ctx e (EWith _ v t c b md)           = exprPrecomputeVar ctx (extype r ctx $ E e) e'
--    where e' = eWith v t (prefMerge c) (prefMerge b) (fmap prefMerge md)
--expr2Statement' r ctx e (EAny _ v t c b md)            = exprPrecomputeVar ctx (exprType r ctx $ E e) e'
--    where e' = eAny  v t (prefMerge c) (prefMerge b) (fmap prefMerge md)
--expr2Statement' _ _   _ (EFor _ v t c b)               = return ([], eFor v t (prefMerge c) (prefMerge b))
--expr2Statement' _ _   _ (EFork _ v t c b)              = return ([], eFork v t (prefMerge c) (prefMerge b))
--expr2Statement' _ _   e EVar{}                         = return ([], E e)
--expr2Statement' _ _   e EPacket{}                      = return ([], E e)
--expr2Statement' _ _   e EBool{}                        = return ([], E e)
--expr2Statement' _ _   e EBit{}                         = return ([], E e)
--expr2Statement' _ _   e EInt{}                         = return ([], E e)
--expr2Statement' _ _   e EString{}                      = return ([], E e)
--expr2Statement' _ _   e EDrop{}                        = return ([], E e)
--expr2Statement' _ _   e EPHolder{}                     = return ([], E e)
--expr2Statement' _ _   e EAnon{}                        = return ([], E e)
--expr2Statement' _ _   e ERelPred{}                     = error $ "Expr.expr2Statement " ++ show e
--expr2Statement' _ _   e ELambda{}                      = return ([], E e)
--expr2Statement' r ctx e (EApplyLambda _ l as)          = do (p', e'') <- exprPrecomputeVar ctx (extype r ctx $ E e) e'
--                                                            return (fst l ++ (concatMap fst as) ++ p', e'')
--    where e' = eApplyLambda (snd l) (map snd as)
--
--extype r ctx e = exprType r ctx e
--
--fset e1 e2@(E (EBinOp _ op _ _)) | bopReturnsBool op = eITE e2 (eSet e1 eTrue) (Just $ eSet e1 eFalse)
--fset e1 e2@(E (EUnOp _ Not _))                       = eITE e2 (eSet e1 eTrue) (Just $ eSet e1 eFalse)
--fset e1 e2                                           = eSet e1 e2
--
--prefMerge :: ([Expr], Expr) -> Expr
--prefMerge (p,e) = exprSequence $ p++[e]
--
--noprefMerge :: ([Expr], Expr) -> Expr
--noprefMerge ([],e) = e
--noprefMerge (p,e)  = error $ "Expr.expr2Statement: expect empty prefix" ++ show (p,e)
--
--exprModifyResult :: (Expr -> Expr) -> Expr -> Expr
--exprModifyResult f (E e) = exprModifyResult' f e
--
--exprModifyResult' :: (Expr -> Expr) -> ENode -> Expr
--exprModifyResult' f (EMatch _ m cs)      = eMatch m $ map (mapSnd $ exprModifyResult f) cs
--exprModifyResult' f (ESet _ e1 e2)       = exprSequence [eSet e1 e2, f $ eTuple []]
--exprModifyResult' f (ESeq _ e1 e2)       = eSeq e1 $ exprModifyResult f e2
--exprModifyResult' f (EITE _ i t me)      = eITE i (exprModifyResult f t) (fmap (exprModifyResult f) me)
--exprModifyResult' f (EWith _ v t c b md) = eWith v t c (exprModifyResult f b) (fmap (exprModifyResult f) md)
--exprModifyResult' f (EAny _ v t c b md)  = eAny v t c (exprModifyResult f b) (fmap (exprModifyResult f) md)
--exprModifyResult' f e                    = f $ E e
--
--exprPrecomputeVar :: ECtx -> Type -> Expr -> State Int ([Expr], Expr)
--exprPrecomputeVar ctx t e | operand ctx = do v <- allocVar
--                                             let vdecl = eTyped (eVarDecl v) t
--                                             return ([vdecl, exprModifyResult (fset $ eVar v) e], eVar v)
--                          | otherwise = return ([], e)
--
--operand :: ECtx -> Bool
--operand ctx = case ctx of
--                   CtxApply{}     -> True
--                   CtxBuiltin{}   -> True
--                   CtxField{..}   -> operand ctxPar
--                   CtxLocation{}  -> True
--                   CtxStruct{}    -> True
--                   CtxTuple{}     -> True
--                   CtxSlice{..}   -> operand ctxPar
--                   CtxMatchExpr{} -> True
--                   CtxSeq2{..}    -> operand ctxPar
--                   CtxITEIf{}     -> True
--                   CtxSetL{}      -> True
--                   CtxBinOpL{}    -> True
--                   CtxBinOpR{}    -> True
--                   CtxUnOp{..}    -> operand ctxPar
--                   CtxTyped{..}   -> operand ctxPar
--                   CtxPut{}       -> True
--                   _              -> False
--
---- no structs or tuples in the LHS of an assignment, e.g.,
---- C{x,y} = f() ===> var z = f(); x = z.f1; y = z.f2
--exprSplitLHS :: Refine -> ECtx -> Expr -> State Int Expr
--exprSplitLHS r ctx e = exprFoldCtxM (exprSplitLHS' r) ctx e
--
--exprSplitLHS' :: Refine -> ECtx -> ENode -> State Int Expr
--exprSplitLHS' r ctx e@(ESet _ e'@(E (EStruct _ _ _)) rhs) = do 
--    let t = exprType r (CtxSetR e ctx) rhs
--    v <- allocVar
--    let vdecl = eTyped (eVarDecl v) t
--    let assigns = maybeToList $ setfield r (eVar v) (CtxSetL e ctx) e'
--    return $ exprSequence $ vdecl : assigns
--exprSplitLHS' _ _   e = return $ E e
--
--setfield :: Refine -> Expr -> ECtx -> Expr -> Maybe Expr
--setfield r (E e@(EStruct _ c fs)) ctx rhs  = 
--    case catMaybes $ mapIdx (\(a, f) i -> setfield r f (CtxStruct e ctx i) (eField rhs $ name a)) $ zip as fs of
--       [] -> Nothing
--       es -> Just $ exprSequence es
--    where Constructor _ _ as = getConstructor r c
--setfield _ (E (EPHolder _)) _   _          = Nothing
--setfield _ lhs              _   rhs        = Just $ eSet lhs rhs
--
--
--allocVar :: State Int String
--allocVar = do modify (1+)
--              liftM (("v#"++) . show) get
--
---- no structs or tuples in the LHS of an assignment, e.g.,
---- C{x,y} = f() ===> var z = f(); x = z.f1; y = z.f2
--exprSplitVDecl :: Refine -> ECtx -> Expr -> Expr
--exprSplitVDecl r ctx e = exprFoldCtx (exprSplitVDecl' r) ctx e'
--    where e' = exprFoldCtx (exprVDeclSetType r) ctx e
-- 
--exprVDeclSetType :: Refine -> ECtx -> ENode -> Expr
--exprVDeclSetType r ctx decl@(EVarDecl _ _) =
--    case ctx of
--        CtxTyped{} -> E decl
--        _          -> eTyped (E decl) $ exprType r ctx $ E decl
--exprVDeclSetType _ _   e = E e
--
--exprSplitVDecl' :: Refine -> ECtx -> ENode -> Expr
--exprSplitVDecl' _ _ (ESeq _ (E (ESet _ decl@(E (ETyped _ (E (EVarDecl _ v)) _)) rhs)) e2) = 
--    eSeq decl (eSeq (eSet (eVar v) rhs) e2)
--exprSplitVDecl' _ ctx eset@(ESet _ decl@(E (ETyped _ (E (EVarDecl _ v)) _)) rhs) = 
--    case ctx of
--        CtxSeq1 ESeq{} _ -> E eset
--        _                -> eSeq decl (eSet (eVar v) rhs)
--exprSplitVDecl' _ _   e = E e
--
--ctxExpectsStat :: ECtx -> Bool
--ctxExpectsStat CtxFunc{}     = True
--ctxExpectsStat CtxMatchVal{} = True
--ctxExpectsStat CtxSeq1{}     = True
--ctxExpectsStat CtxSeq2{}     = True
--ctxExpectsStat CtxPar1{}     = True
--ctxExpectsStat CtxPar2{}     = True
--ctxExpectsStat CtxITEThen{}  = True
--ctxExpectsStat CtxITEElse{}  = True
--ctxExpectsStat CtxForBody{}  = True
--ctxExpectsStat CtxForkBody{} = True
--ctxExpectsStat CtxWithBody{} = True
--ctxExpectsStat CtxWithDef{}  = True
--ctxExpectsStat CtxAnyBody{}  = True
--ctxExpectsStat CtxAnyDef{}   = True
--ctxExpectsStat _             = False
--
--ctxMustReturn :: ECtx -> Bool
--ctxMustReturn     CtxFunc{}       = True
--ctxMustReturn     CtxSeq1{}       = False
--ctxMustReturn     CtxPar1{}       = True
--ctxMustReturn     CtxPar2{}       = True
--ctxMustReturn ctx@CtxForkBody{}   = ctxMustReturn $ ctxParent ctx
--ctxMustReturn ctx@CtxMatchVal{}   = ctxMustReturn $ ctxParent ctx
--ctxMustReturn ctx@CtxSeq2{}       = ctxMustReturn $ ctxParent ctx
--ctxMustReturn ctx@CtxITEThen{}    = ctxMustReturn $ ctxParent ctx
--ctxMustReturn ctx@CtxITEElse{}    = ctxMustReturn $ ctxParent ctx
--ctxMustReturn ctx@CtxWithBody{}   = ctxMustReturn $ ctxParent ctx
--ctxMustReturn ctx@CtxWithDef{}    = ctxMustReturn $ ctxParent ctx
--ctxMustReturn ctx@CtxAnyBody{}    = ctxMustReturn $ ctxParent ctx
--ctxMustReturn ctx@CtxAnyDef{}     = ctxMustReturn $ ctxParent ctx
--ctxMustReturn     CtxLambda{}     = True
--ctxMustReturn _                   = False
--
--exprIsStatement :: ENode -> Bool
--exprIsStatement (EMatch   {})                 = True
--exprIsStatement (EVarDecl {})                 = True
--exprIsStatement (ESeq     {})                 = True
--exprIsStatement (EPar     {})                 = True
--exprIsStatement (EITE     {})                 = True
--exprIsStatement (EDrop    {})                 = True
--exprIsStatement (ESet     {})                 = True
--exprIsStatement (ESend    {})                 = True
--exprIsStatement (EFor     {})                 = True
--exprIsStatement (EFork    {})                 = True
--exprIsStatement (EWith    {})                 = True
--exprIsStatement (EAny     {})                 = True
--exprIsStatement (ETyped _ (E (EVarDecl{})) _) = True
--exprIsStatement (EPut _ _ _)                  = True
--exprIsStatement (EDelete _ _ _)               = True
--exprIsStatement _                             = False
--
--
--exprVarSubst :: (String -> Expr) -> (String -> String) -> Expr -> Expr
--exprVarSubst f h e = exprFold g e
--    where g (EVar _ v)          = f v
--          g (EVarDecl p v)      = E $ EVarDecl p $ h v
--          g (EWith p v t c b d) = E $ EWith p (h v) t c b d
--          g (EAny p v t c b d)  = E $ EAny p (h v) t c b d
--          g (EFork p v t c b)   = E $ EFork p (h v) t c b
--          g (EFor p v t c b)    = E $ EFor p (h v) t c b
--          g e'                  = E e'
--
--exprVarRename :: (String -> String) -> Expr -> Expr
--exprVarRename f e = exprFold g e
--    where g (EVar pos v) = E $ EVar pos $ f v
--          g e'           = E e'
--
---- Returns subexpressions that mutate state, if any
--exprMutatorsNonRec :: Expr -> [Expr]
--exprMutatorsNonRec e = nub $ execState
--    (exprFoldM (\e' -> do case e' of
--                              EPut{}    -> modify (E e':)
--                              EDelete{} -> modify (E e':)
--                              _         -> return ()
--                          return $ E e') e) 
--    []
