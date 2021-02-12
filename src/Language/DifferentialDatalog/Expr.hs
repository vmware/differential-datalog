{-
Copyright (c) 2018-2021 VMware, Inc.
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

{-# LANGUAGE ImplicitParams, RecordWildCards, LambdaCase, TupleSections, FlexibleContexts #-}

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
    exprVarOccurrences,
    exprVars,
    exprFreeVars,
    exprIsConst,
    exprVarDecls,
    exprStripTypeAnnotations,
    exprStripTypeAnnotationsRec,
    isLVar,
    funcExprGetFunc,
    exprIsPattern,
    exprIsPatternImpl,
    exprContainsPHolders,
    exprIsDeconstruct,
    exprIsVarOrFieldLVal,
    exprIsVarOrField,
    exprIsInjective,
    exprIsPolymorphic,
    exprIsPure,
    exprIsStatic,
    exprTypeMapM,
    exprInjectStringConversion,
    ELocator(..),
    exprFoldWithLocatorM,
    ctxToELocator
    ) where

import Data.List
import Data.Maybe
import Data.Tuple.Select
import Control.Monad.Except
import Control.Monad.Identity
import Control.Monad.State
import qualified Data.Set as S
import qualified Data.Map as M
--import Debug.Trace

import Language.DifferentialDatalog.Attribute
import Language.DifferentialDatalog.Error
import Language.DifferentialDatalog.Pos
import Language.DifferentialDatalog.Ops
import Language.DifferentialDatalog.Syntax
import Language.DifferentialDatalog.Module
import Language.DifferentialDatalog.NS
import Language.DifferentialDatalog.Util
import Language.DifferentialDatalog.Name
import {-# SOURCE #-} Language.DifferentialDatalog.Type
import Language.DifferentialDatalog.ECtx
import {-# SOURCE #-} Language.DifferentialDatalog.Var

bUILTIN_2STRING_FUNC :: String
bUILTIN_2STRING_FUNC = mOD_STD ++ "::__builtin_2string"

-- depth-first fold of an expression
exprFoldCtxM :: (Monad m) => (ECtx -> ExprNode b -> m b) -> ECtx -> Expr -> m b
exprFoldCtxM f ctx (E n) = exprFoldCtxM' f ctx n

exprFoldCtxM' :: (Monad m) => (ECtx -> ExprNode b -> m b) -> ECtx -> ENode -> m b
exprFoldCtxM' f ctx   (EVar p v)              = f ctx $ EVar p v
exprFoldCtxM' f ctx e@(EApply p fun as)       = f ctx =<< EApply p <$> exprFoldCtxM f (CtxApplyFunc e ctx) fun
                                                                   <*> (mapIdxM (\a i -> exprFoldCtxM f (CtxApplyArg e ctx i) a) as)
exprFoldCtxM' f ctx e@(EField p s fl)         = do s' <- exprFoldCtxM f (CtxField e ctx) s
                                                   f ctx $ EField p s' fl
exprFoldCtxM' f ctx e@(ETupField p s fl)      = do s' <- exprFoldCtxM f (CtxTupField e ctx) s
                                                   f ctx $ ETupField p s' fl
exprFoldCtxM' f ctx   (EBool p b)             = f ctx $ EBool p b
exprFoldCtxM' f ctx   (EInt p i)              = f ctx $ EInt p i
exprFoldCtxM' f ctx   (EDouble p i)           = f ctx $ EDouble p i
exprFoldCtxM' f ctx   (EFloat p i)            = f ctx $ EFloat p i
exprFoldCtxM' f ctx   (EString p s)           = f ctx $ EString p s
exprFoldCtxM' f ctx   (EBit p w v)            = f ctx $ EBit p w v
exprFoldCtxM' f ctx   (ESigned p w v)         = f ctx $ ESigned p w v
exprFoldCtxM' f ctx e@(EStruct p c fs)        = f ctx =<< EStruct p c <$> (mapIdxM (\(fname, fl) i -> (fname,) <$> exprFoldCtxM f (CtxStruct e ctx (i, fname)) fl) fs)
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
exprFoldCtxM' f ctx e@(EFor p v i b)          = f ctx =<< EFor p <$>
                                                          exprFoldCtxM f (CtxForVars e ctx) v <*>
                                                          exprFoldCtxM f (CtxForIter e ctx) i <*>
                                                          exprFoldCtxM f (CtxForBody e ctx) b
exprFoldCtxM' f ctx e@(ESet p l r)            = do -- XXX: start with RHS, e.g., in validating an assignment it helps to know RHS type
                                                   -- before validating LHS
                                                   r' <- exprFoldCtxM f (CtxSetR e ctx) r
                                                   l' <- exprFoldCtxM f (CtxSetL e ctx) l
                                                   f ctx $ ESet p l' r'
exprFoldCtxM' f ctx   (EContinue p)           = f ctx $ EContinue p
exprFoldCtxM' f ctx   (EBreak p)              = f ctx $ EBreak p
exprFoldCtxM' f ctx e@(EReturn p v)           = f ctx =<< EReturn p <$> exprFoldCtxM f (CtxReturn e ctx) v
exprFoldCtxM' f ctx e@(EBinOp p op l r)       = f ctx =<< EBinOp p op <$> exprFoldCtxM f (CtxBinOpL e ctx) l <*>
                                                                          exprFoldCtxM f (CtxBinOpR e ctx) r
exprFoldCtxM' f ctx e@(EUnOp p op x)          = f ctx =<< EUnOp p op <$> (exprFoldCtxM f (CtxUnOp e ctx) x)
exprFoldCtxM' f ctx   (EPHolder p)            = f ctx $ EPHolder p
exprFoldCtxM' f ctx e@(EBinding p v x)        = do x' <- exprFoldCtxM f (CtxBinding e ctx) x
                                                   f ctx $ EBinding p v x'
exprFoldCtxM' f ctx e@(ETyped p x t)          = do x' <- exprFoldCtxM f (CtxTyped e ctx) x
                                                   f ctx $ ETyped p x' t
exprFoldCtxM' f ctx e@(EAs p x t)             = do x' <- exprFoldCtxM f (CtxAs e ctx) x
                                                   f ctx $ EAs p x' t
exprFoldCtxM' f ctx e@(ERef p x)              = do x' <- exprFoldCtxM f (CtxRef e ctx) x
                                                   f ctx $ ERef p x'
exprFoldCtxM' f ctx e@(ETry p x)              = do x' <- exprFoldCtxM f (CtxTry e ctx) x
                                                   f ctx $ ETry p x'
exprFoldCtxM' f ctx e@(EClosure p as r x)     = do x' <- exprFoldCtxM f (CtxClosure e ctx) x
                                                   f ctx $ EClosure p as r x'
exprFoldCtxM' f ctx   (EFunc p fs)            = f ctx $ EFunc p fs

exprMapM :: (Monad m) => (a -> m b) -> ExprNode a -> m (ExprNode b)
exprMapM g e = case e of
                   EVar p v            -> return $ EVar p v
                   EApply p f as       -> EApply p <$> g f <*> mapM g as
                   EField p s f        -> (\s' -> EField p s' f) <$> g s
                   ETupField p s f     -> (\s' -> ETupField p s' f) <$> g s
                   EBool p b           -> return $ EBool p b
                   EInt p i            -> return $ EInt p i
                   EFloat p i          -> return $ EFloat p i
                   EDouble p i         -> return $ EDouble p i
                   EString p s         -> return $ EString p s
                   EBit p w v          -> return $ EBit p w v
                   ESigned p w v       -> return $ ESigned p w v
                   EStruct p s fs      -> EStruct p s <$> mapM (\(fname, e') -> (fname,) <$> g e') fs
                   ETuple p fs         -> ETuple p <$> mapM g fs
                   ESlice p v h l      -> (\v' -> ESlice p v' h l) <$> g v
                   EMatch p m cs       -> EMatch p <$> g m <*> mapM (\(e1, e2) -> (,) <$> g e1 <*> g e2) cs
                   EVarDecl p v        -> return $ EVarDecl p v
                   ESeq p l r          -> ESeq p <$> g l <*> g r
                   EITE p i t el       -> EITE p <$> g i <*> g t <*> g el
                   EFor p v i b        -> EFor p <$> g v <*> g i <*> g b
                   ESet p l r          -> ESet p <$> g l <*> g r
                   EBreak p            -> return $ EBreak p
                   EContinue p         -> return $ EContinue p
                   EReturn p v         -> EReturn p <$> g v
                   EBinOp p op l r     -> EBinOp p op <$> g l <*> g r
                   EUnOp p op v        -> EUnOp p op <$> g v
                   EPHolder p          -> return $ EPHolder p
                   EBinding p v x      -> EBinding p v <$> g x
                   ETyped p x t        -> (\x' -> ETyped p x' t) <$> g x
                   EAs p x t           -> (\x' -> EAs p x' t) <$> g x
                   ERef p x            -> ERef p <$> g x
                   ETry p x            -> ETry p <$> g x
                   EClosure p as r x   -> EClosure p as r <$> g x
                   EFunc p fs          -> return $ EFunc p fs

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
                                     EApply _ ef as        -> foldl' op (x' `op` ef) as
                                     EField _ s _          -> x' `op` s
                                     ETupField _ s _       -> x' `op` s
                                     EBool _ _             -> x'
                                     EInt _ _              -> x'
                                     EFloat _ _            -> x'
                                     EDouble _ _           -> x'
                                     EString _ _           -> x'
                                     EBit _ _ _            -> x'
                                     ESigned _ _ _         -> x'
                                     EStruct _ _ fs        -> foldl' (\a (_, _x) -> op a _x) x' fs
                                     ETuple _ fs           -> foldl' op x' fs
                                     ESlice _ v _ _        -> x' `op` v
                                     EMatch _ m cs         -> foldl' (\a (p,v) -> a `op` p `op` v) (x' `op` m) cs
                                     EVarDecl _ _          -> x'
                                     ESeq _ l r            -> x' `op` l `op` r
                                     EITE _ i t el         -> x' `op` i `op` t `op` el
                                     EFor _ v i b          -> x' `op` v `op` i `op` b
                                     ESet _ l r            -> x' `op` l `op` r
                                     EBreak _              -> x'
                                     EContinue _           -> x'
                                     EReturn _ v           -> x' `op` v
                                     EBinOp _ _ l r        -> x' `op` l `op` r
                                     EUnOp _ _ v           -> x' `op` v
                                     EPHolder _            -> x'
                                     EBinding _ _ pat      -> x' `op` pat
                                     ETyped _ v _          -> x' `op` v
                                     EAs _ v _             -> x' `op` v
                                     ERef _ v              -> x' `op` v
                                     ETry _ v              -> x' `op` v
                                     EClosure _ _ _ v      -> x' `op` v
                                     EFunc _ _             -> x'

exprCollectM :: (Monad m) => (ExprNode b -> m b) -> (b -> b -> b) -> Expr -> m b
exprCollectM f op e = exprCollectCtxM (\_ e' -> f e') op undefined e

exprCollectCtx :: (ECtx -> ExprNode b -> b) -> (b -> b -> b) -> ECtx -> Expr -> b
exprCollectCtx f op ctx e = runIdentity $ exprCollectCtxM (\ctx' x -> return $ f ctx' x) op ctx e

exprCollect :: (ExprNode b -> b) -> (b -> b -> b) -> Expr -> b
exprCollect f op e = runIdentity $ exprCollectM (return . f) op e

-- enumerate all variable occurrences in the expression
exprVarOccurrences :: ECtx -> Expr -> [(String, ECtx)]
exprVarOccurrences ctx e = exprCollectCtx (\ctx' e' ->
                                            case e' of
                                                 EVar _ v -> [(v, ctx')]
                                                 _        -> [])
                                          (++) ctx e

-- enumerate all variables that occur in the expression
exprVars :: DatalogProgram -> ECtx -> Expr -> [Var]
exprVars d ctx e = nub $ exprCollectCtx (\ctx' e' ->
                                          case e' of
                                               EVar p v -> [case lookupVar d ctx' v of
                                                                 -- Variable declared inside a rule.
                                                                 Nothing -> ExprVar ctx' $ EVar p v
                                                                 Just var -> var]
                                               _        -> [])
                                        (++) ctx e

-- | Free variables, i.e., variables that are used in the expression, but declared
-- outside of it
exprFreeVars :: DatalogProgram -> ECtx -> Expr -> [Var]
exprFreeVars d ctx e = visible_vars `intersect` used_vars
    where
    visible_vars = ctxAllVars d ctx
    used_vars = exprVars d ctx e

-- True if expression evaluates to a constant
-- Note: this does not guarantee that the expression can be evaluated at compile
-- time.  It may contain a call to an external function, which cannot be
-- evaluated in Haskell.
exprIsConst :: Expr -> Bool
exprIsConst e = null $ exprVars (error "exprIsConst: ctx is undefined") (error "exprIsConst: ctx is undefined") e

-- Strip type annotations from an expression.
exprStripTypeAnnotations :: Expr -> ECtx -> (Expr, ECtx)
exprStripTypeAnnotations (E e@ETyped{..}) ctx = exprStripTypeAnnotations exprExpr (CtxTyped e ctx)
exprStripTypeAnnotations e ctx = (e, ctx)

-- Strip type annotations from an expression and all its subexpressions.
exprStripTypeAnnotationsRec :: Expr -> ECtx -> Expr
exprStripTypeAnnotationsRec e ctx = exprFoldCtx (\ctx' e' -> fst $ exprStripTypeAnnotations (E e') ctx') ctx e

-- Variables declared inside expression, visible in the code that follows the expression.
exprVarDecls :: DatalogProgram -> ECtx -> Expr -> [Var]
exprVarDecls d ctx e =
    snd $
    exprFoldCtx (\ctx' e' ->
                  let e_ = exprMap fst e' in
                  (E e_,
                   case e' of
                        EStruct _ _ fs   -> concatMap (snd . snd) fs
                        ETuple _ fs      -> concatMap snd fs
                        EVarDecl _ _     -> [ExprVar ctx' e_]
                        -- Inside positive literals, variables are declared
                        -- implicitly.
                        EVar _ v | ctxInRuleRHSPositivePattern ctx'
                                         -> let var = ExprVar ctx' e_ in
                                            if isNothing (lookupVar d ctx' v)   
                                            then [var]
                                            else []   
                        ESet _ l _       -> snd l
                        EBinding _ _ e'' -> BindingVar ctx' e_ : snd e''
                        ETyped _ e'' _   -> snd e''
                        ERef _ e''       -> snd e''
                        _                -> [])) ctx e

-- Non-recursively enumerate all functions invoked by the expression
{-exprFuncs :: DatalogProgram -> ECtx -> Expr -> [String]
exprFuncs d ctx e = exprFuncs' [] e

exprFuncs' :: [String] -> Expr -> [String]
exprFuncs' acc e = nub $ exprCollect (\case
                                       EApply _ f _ -> if' (elem f acc) [] [f]
                                       _            -> [])
                                     (++) e
-}

-- Given a function expression, returns the function.
-- Assumes the expression has been validated.
funcExprGetFunc :: DatalogProgram -> ECtx -> ENode -> (Function, M.Map String Type)
funcExprGetFunc d ctx e@EFunc{exprFuncName=[fname]} =
    getFunc d fname (map typ $ typeFuncArgs t) (typeRetType t)
    where
    t = exprType' d ctx $ E e
funcExprGetFunc _ _   e = error $ "funcExprGetFunc " ++ show e

-- Recursively enumerate all functions invoked by the expression.
-- Closures make precise analysis hard.  To protext the callers from
-- using inaccurate results, we return 'Nothing' if closures are used
-- by any of the functions discovered during recursive traveral.
exprFuncsRec :: DatalogProgram -> ECtx -> Expr -> Maybe (S.Set Function)
exprFuncsRec d ctx e = execState (exprFuncsRec_ d ctx e) $ Just S.empty

exprFuncsRec_ :: DatalogProgram -> ECtx -> Expr -> State (Maybe (S.Set Function)) ()
exprFuncsRec_ d ctx e = do
    _ <- exprFoldCtxM (\ctx' e' -> do
                          case e' of
                               EApply{..} -> case exprStripTypeAnnotations exprFunc ctx' of
                                                  (E ef@EFunc{}, ctx'') -> do
                                                      let (f@Function{..}, _) = funcExprGetFunc d ctx'' ef
                                                      new <- gets $ maybe False (S.notMember f)
                                                      when new $ do
                                                          modify $ (\st -> S.insert f <$> st)
                                                          case funcDef of
                                                               Nothing -> return ()
                                                               Just e'' -> exprFuncsRec_ d (CtxFunc f) e''
                                                  _ -> put Nothing
                               _ -> return ()
                          return $ E e')
                     ctx e
    return ()

isLVar :: DatalogProgram -> ECtx -> String -> Bool
isLVar d ctx v = isJust $ find ((==v) . name) $ fst $ ctxVars d ctx


-- | We support three kinds of patterns used in different contexts:
--
-- * Deconstruction patterns are used in left-hand side of
-- assignments that simultaneously deconstruct a value and bind its
-- fields to fresh variables.  They are built out of variable declarations,
-- tuples, placeholders, constructors, type annotations.  Types with multiple
-- constructors cannot be deconstructed in this manner.
--
-- * Field expression: variables, fields, and type annotations.
--
-- * Match patterns are used in match expressions, relational
-- atoms, and in assignment terms in rules. They simultaneously restrict the
-- structure of a value and bind its fields to fresh variables.  They are
-- built out of variable declarations (optionally omitting the 'var' keyword),
-- tuples, constructors, placeholders, constant values, and type annotations.

-- | True if expression can be interpreted as a match pattern.
exprIsPattern :: Expr -> Bool
exprIsPattern e = exprFold exprIsPattern' e

exprIsPattern' :: ExprNode Bool -> Bool
exprIsPattern' EString{}        = True
exprIsPattern' EBit{}           = True
exprIsPattern' ESigned{}        = True
exprIsPattern' EBool{}          = True
exprIsPattern' EInt{}           = True
exprIsPattern' EFloat{}         = True
exprIsPattern' EDouble{}        = True
exprIsPattern' EVarDecl{}       = True
exprIsPattern' (ETuple _ as)    = and as
exprIsPattern' (EStruct _ _ as) = all snd as
exprIsPattern' EPHolder{}       = True
exprIsPattern' (ETyped _ e _)   = e
exprIsPattern' (ERef _ e)       = e
exprIsPattern' (EBinding _ _ e) = e
exprIsPattern' _                = False

-- | Like 'exprIsPattern', but matches implicit variable declarations.
exprIsPatternImpl :: Expr -> Bool
exprIsPatternImpl e = exprFold exprIsPatternImpl' e

exprIsPatternImpl' :: ExprNode Bool -> Bool
exprIsPatternImpl' EString{}        = True
exprIsPatternImpl' EBit{}           = True
exprIsPatternImpl' ESigned{}        = True
exprIsPatternImpl' EBool{}          = True
exprIsPatternImpl' EInt{}           = True
exprIsPatternImpl' EFloat{}         = True
exprIsPatternImpl' EDouble{}        = True
exprIsPatternImpl' EVar{}           = True
exprIsPatternImpl' (ETuple _ as)    = and as
exprIsPatternImpl' (EStruct _ _ as) = all snd as
exprIsPatternImpl' EPHolder{}       = True
exprIsPatternImpl' (ETyped _ e _)   = e
exprIsPatternImpl' (ERef _ e)       = e
exprIsPatternImpl' (EBinding _ _ e) = e
exprIsPatternImpl' _                = False

-- | True if 'e' contains a placeholder ('_')
exprContainsPHolders :: Expr -> Bool
exprContainsPHolders e =
    exprCollect (\case
                  EPHolder _ -> True
                  _          -> False)
                (||) e

-- | True if 'e' is a deconstruction expression.
exprIsDeconstruct :: DatalogProgram -> Expr -> Bool
exprIsDeconstruct d e = exprFold (exprIsDeconstruct' d) e

exprIsDeconstruct' :: DatalogProgram -> ExprNode Bool -> Bool
exprIsDeconstruct' _ EVarDecl{}       = True
exprIsDeconstruct' _ (ETuple _ as)    = and as
exprIsDeconstruct' d (EStruct _ c as) = all snd as && consIsUnique d c
exprIsDeconstruct' _ EPHolder{}       = True
exprIsDeconstruct' _ (ETyped _ e _)   = e
exprIsDeconstruct' _ _                = False

-- | True if 'e' is a variable or field expression, and
-- can be assigned to (i.e., the variable is writable).
exprIsVarOrFieldLVal :: DatalogProgram -> ECtx -> Expr -> Bool
exprIsVarOrFieldLVal d ctx e = snd $ exprFoldCtx (exprIsVarOrFieldLVal' d) ctx e

exprIsVarOrFieldLVal' :: DatalogProgram -> ECtx -> ExprNode (Expr, Bool) -> (Expr, Bool)
exprIsVarOrFieldLVal' d ctx expr =
    case expr of
        (EVar _ v)            -> (E e', isLVar d ctx v)
        (EField _ (e, b) _)   -> (E e', b && (not $ isSharedRef d $ exprType d (CtxField e' ctx) e))
        (ETupField _ (e,b) _) -> (E e', b && (not $ isSharedRef d $ exprType d (CtxTupField e' ctx) e))
        (ETyped _ (_,b) _)    -> (E e', b)
        _                     -> (E e', False)
    where e' = exprMap sel1 expr

-- | True if 'e' is a variable or field expression
exprIsVarOrField :: Expr -> Bool
exprIsVarOrField e = exprFold exprIsVarOrField' e

exprIsVarOrField' :: ExprNode Bool -> Bool
exprIsVarOrField' (EVar _ _)       = True
exprIsVarOrField' (EField _ e _)   = e
exprIsVarOrField' (ETupField _ e _)= e
exprIsVarOrField' (ETyped _ e _)   = e
exprIsVarOrField' _                = False

-- | Expression maps distinct assignments to input variables 'vs'
-- to distinct outputs.
exprIsInjective :: DatalogProgram -> ECtx -> S.Set Var -> Expr -> Bool
exprIsInjective d ctx vs e | isNothing funcs = False
                           | otherwise =
    exprIsInjective_ d ctx vs e &&
    all (\f -> case funcDef f of
                    Nothing -> False
                    Just e' -> exprIsInjective_ d (CtxFunc f) (S.fromList $ mapIdx (\a i -> ArgVar f i $ name a) $ funcArgs f) e')
        (fromJust funcs)
    where
    funcs = exprFuncsRec d ctx e

-- Non-recursive part of exprIsInjective
exprIsInjective_ :: DatalogProgram -> ECtx -> S.Set Var -> Expr -> Bool
exprIsInjective_ d ctx vs e =
    S.fromList (exprVars d ctx e) == vs &&
    exprFold (exprIsInjective' d) e

-- No clever analysis here; just the obvious cases.
exprIsInjective' :: DatalogProgram -> ExprNode Bool -> Bool
exprIsInjective' _ EVar{}        = True
exprIsInjective' _ EApply{..}    = and exprArgs
exprIsInjective' _ EBool{}       = True
exprIsInjective' _ EInt{}        = True
exprIsInjective' _ EFloat{}      = True
exprIsInjective' _ EDouble{}     = True
exprIsInjective' _ EString{}     = True
exprIsInjective' _ EBit{}        = True
exprIsInjective' _ ESigned{}     = True
exprIsInjective' _ EStruct{..}   = all snd exprStructFields
exprIsInjective' _ ETuple{..}    = and exprTupleFields
exprIsInjective' _ EUnOp{..}     = (elem exprUOp [Not, BNeg, UMinus]) && exprOp
exprIsInjective' _ ETyped{..}    = exprExpr
exprIsInjective' _ _             = False

-- | Expression or one of its subexpressions has a polymorphic type.
-- Such an expression may be impossible to evaluate outside of its context, e.g.,
-- as a static constant.
exprIsPolymorphic :: DatalogProgram -> ECtx -> Expr -> Bool
exprIsPolymorphic d ctx e =
    execState
        (exprTraverseCtxM (\ctx' e' -> do
            let t = exprType d ctx' $ E e'
            modify (typeIsPolymorphic t ||)
            return ()) ctx e)
        False

-- | True if expression does not contain calls to functions with side effects.
exprIsPure :: DatalogProgram -> ECtx -> Expr -> Bool
exprIsPure d ctx e | isNothing funcs = False
                   | otherwise = all (\f -> not $ funcGetSideEffectAttr d f) $ fromJust funcs
    where funcs = exprFuncsRec d ctx e

-- | Expression can be evaluated statically.
exprIsStatic :: DatalogProgram -> ECtx -> Expr -> Bool
exprIsStatic d ctx e@(E EApply{}) =
    null (exprFreeVars d ctx e) &&
    (not $ exprIsPolymorphic d ctx e) &&
    exprIsPure d ctx e
exprIsStatic _ _ _ = False

-- | Transform types referenced in the expression
exprTypeMapM :: (Monad m) => (Type -> m Type) -> Expr -> m Expr
exprTypeMapM fun e = exprFoldM fun' e
    where
    fun' (ETyped p e' t)      = (E . ETyped p e') <$> typeMapM fun t
    fun' (EAs p e' t)         = (E . EAs p e') <$> typeMapM fun t
    fun' (EClosure p as r e') = E <$> (EClosure p <$> (mapM (\a -> case ceargType a of
                                                                       Nothing -> return a
                                                                       Just atype -> do t' <- typeMapM fun $ atypeType atype
                                                                                        return a{ceargType = Just $ atype{atypeType = t'}})
                                                      as)
                                                 <*> (maybe (return Nothing) (\rt -> Just <$> typeMapM fun rt) r)
                                                 <*> (return e'))
    fun' e'                  = return $ E e'

-- Automatically insert string conversion functions in the Concat
-- operator:  '"x:" ++ x', where 'x' is of type int becomes
-- '"x:" ++ to_string(x)'.
exprInjectStringConversion :: (MonadError String me) => DatalogProgram -> ENode -> Type -> me Expr
exprInjectStringConversion d e t = do
    let t' = typ'' d t
    -- find string conversion function
    fname <- case t' of
                  TBool{}     -> return $ bUILTIN_2STRING_FUNC
                  TInt{}      -> return $ bUILTIN_2STRING_FUNC
                  TString{}   -> return $ bUILTIN_2STRING_FUNC
                  TBit{}      -> return $ bUILTIN_2STRING_FUNC
                  TSigned{}   -> return $ bUILTIN_2STRING_FUNC
                  TDouble{}   -> return $ bUILTIN_2STRING_FUNC
                  TFloat{}    -> return $ bUILTIN_2STRING_FUNC
                  TUser{..}   -> return $ mk2string_func typeName
                  TOpaque{..} -> return $ mk2string_func typeName
                  TTuple{}    -> err d (pos e) "Automatic string conversion for tuples is not supported"
                  TVar{..}    -> err d (pos e) $
                                     "Cannot automatically convert '" ++ show e ++
                                     "' of variable type '" ++ tvarName ++ "' to string"
                  TStruct{}   -> error "unexpected TStruct in exprInjectStringConversions"
                  TFunction{} -> return $ bUILTIN_2STRING_FUNC
    (f, _) <- case lookupFunc d fname [t'] tString of
                   Nothing  -> err d (pos e) $ "Cannot find declaration of function '" ++ fname ++ "(" ++ show t' ++ "): string'" ++
                                               " needed to convert expression '" ++ show e ++ "' to string"
                   Just fun -> return fun
    -- 'to_string' is a polymorphic function.  Normally, 'TypeInference.inferTypes' would generate type
    -- annotations for such functions, but since 'exprInjectStringConversions' is itself called from
    -- 'inferTypes', we must add type annotation manually.
    return $ E $ EApply (pos e)
                        (E $ ETyped (pos e) (E $ EFunc (pos e) [fname]) $ TFunction (pos f) [ArgType nopos False t] tString)
                        [E e]
    where mk2string_func cs = scoped scope "to_string"
              where scope = nameScope cs

-- A descriptor that uniquely identifies a sub-expression within
-- a given context as a path from the root of the context to the
-- sub-expression.
data ELocator = ELocator {elocatorPath :: [Int]} deriving (Eq, Ord)

-- depth-first fold of an expression
exprFoldWithLocatorM :: (Monad m) => (ELocator -> ExprNode b -> m b) -> ELocator -> Expr -> m b
exprFoldWithLocatorM f loc (E n) = exprFoldWithLocatorM' f loc n

exprFoldWithLocatorM' :: (Monad m) => (ELocator -> ExprNode b -> m b) -> ELocator -> ENode -> m b
exprFoldWithLocatorM' f loc                 (EVar p v)              = f loc $ EVar p v
exprFoldWithLocatorM' f loc@(ELocator path) (EApply p fun as)       = 
    -- 0: function expression
    -- 1..n: argument expression
    f loc =<< EApply p <$> exprFoldWithLocatorM f (ELocator $ 0:path) fun
                                  <*> (mapIdxM (\a i -> exprFoldWithLocatorM f (ELocator $ (1+i) : path) a) as)
exprFoldWithLocatorM' f loc@(ELocator path) (EField p s fl)         =
    do s' <- exprFoldWithLocatorM f (ELocator $ 0:path) s
       f loc $ EField p s' fl
exprFoldWithLocatorM' f loc@(ELocator path) (ETupField p s fl)      = do
    s' <- exprFoldWithLocatorM f (ELocator $ 0:path) s
    f loc $ ETupField p s' fl
exprFoldWithLocatorM' f loc                   (EBool p b)             = f loc $ EBool p b
exprFoldWithLocatorM' f loc                   (EInt p i)              = f loc $ EInt p i
exprFoldWithLocatorM' f loc                   (EDouble p i)           = f loc $ EDouble p i
exprFoldWithLocatorM' f loc                   (EFloat p i)            = f loc $ EFloat p i
exprFoldWithLocatorM' f loc                   (EString p s)           = f loc $ EString p s
exprFoldWithLocatorM' f loc                   (EBit p w v)            = f loc $ EBit p w v
exprFoldWithLocatorM' f loc                   (ESigned p w v)         = f loc $ ESigned p w v
exprFoldWithLocatorM' f loc@(ELocator path) (EStruct p c fs)        =
    f loc =<< EStruct p c <$> (mapIdxM (\(fname, fl) i -> (fname,) <$> exprFoldWithLocatorM f (ELocator $ i : path) fl) fs)
exprFoldWithLocatorM' f loc@(ELocator path) (ETuple p fs)           =
    f loc =<< ETuple p <$> (mapIdxM (\fl i -> exprFoldWithLocatorM f (ELocator $ i : path) fl) fs)
exprFoldWithLocatorM' f loc@(ELocator path) (ESlice p v h l)        = do
    v' <- exprFoldWithLocatorM f (ELocator $ 0 : path) v
    f loc $ ESlice p v' h l
exprFoldWithLocatorM' f loc@(ELocator path) (EMatch p m cs)         = do
    -- 0 - match expression
    -- 1,3,5,..,2n-1 - match patterns
    -- 2,4,6,..,2n   - match values
    m' <- exprFoldWithLocatorM f (ELocator $ 0:path) m
    cs' <- mapIdxM (\(e1, e2) i -> (,) <$> (exprFoldWithLocatorM f (ELocator $ (1+2*i):path) e1) <*>
                                           (exprFoldWithLocatorM f (ELocator $ (2+2*i):path) e2)) cs
    f loc $ EMatch p m' cs'
exprFoldWithLocatorM' f loc                   (EVarDecl p v)          = f loc $ EVarDecl p v
exprFoldWithLocatorM' f loc@(ELocator path) (ESeq p l r)            =
    f loc =<< ESeq p <$> exprFoldWithLocatorM f (ELocator $ 0:path) l <*>
                         exprFoldWithLocatorM f (ELocator $ 1:path) r
exprFoldWithLocatorM' f loc@(ELocator path) (EITE p i t el)         =
    -- 0: if-condition
    -- 1: then-clause
    -- 2: else-clause
    f loc =<< EITE p <$> exprFoldWithLocatorM f (ELocator $ 0:path) i <*>
                         exprFoldWithLocatorM f (ELocator $ 1:path) t <*>
                         exprFoldWithLocatorM f (ELocator $ 2:path) el
exprFoldWithLocatorM' f loc@(ELocator path) (EFor p v i b)          =
    -- 0: iterator expression
    -- 1: loop variables expression
    -- 2: loop body
    f loc =<< EFor p <$> exprFoldWithLocatorM f (ELocator $ 1:path) v <*>
                         exprFoldWithLocatorM f (ELocator $ 0:path) i <*>
                         exprFoldWithLocatorM f (ELocator $ 2:path) b
exprFoldWithLocatorM' f loc@(ELocator path) (ESet p l r)            = do
    r' <- exprFoldWithLocatorM f (ELocator $ 0:path) r
    l' <- exprFoldWithLocatorM f (ELocator $ 1:path) l
    f loc $ ESet p l' r'
exprFoldWithLocatorM' f loc                 (EContinue p)           = f loc $ EContinue p
exprFoldWithLocatorM' f loc                 (EBreak p)              = f loc $ EBreak p
exprFoldWithLocatorM' f loc@(ELocator path) (EReturn p v)           =
    f loc =<< EReturn p <$> exprFoldWithLocatorM f (ELocator $ 0:path) v
exprFoldWithLocatorM' f loc@(ELocator path) (EBinOp p op l r)       =
    f loc =<< EBinOp p op <$> exprFoldWithLocatorM f (ELocator $ 0:path) l <*>
                              exprFoldWithLocatorM f (ELocator $ 1:path) r
exprFoldWithLocatorM' f loc@(ELocator path) (EUnOp p op x)          =
    f loc =<< EUnOp p op <$> (exprFoldWithLocatorM f (ELocator $ 0:path) x)
exprFoldWithLocatorM' f loc                 (EPHolder p)            = f loc $ EPHolder p
exprFoldWithLocatorM' f loc@(ELocator path) (EBinding p v x)        = do
    x' <- exprFoldWithLocatorM f (ELocator $ 0:path) x
    f loc $ EBinding p v x'
exprFoldWithLocatorM' f loc@(ELocator path) (ETyped p x t)          = do
    x' <- exprFoldWithLocatorM f (ELocator $ 0:path) x
    f loc $ ETyped p x' t
exprFoldWithLocatorM' f loc@(ELocator path) (EAs p x t)             = do
    x' <- exprFoldWithLocatorM f (ELocator $ 0:path) x
    f loc $ EAs p x' t
exprFoldWithLocatorM' f loc@(ELocator path) (ERef p x)              = do
    x' <- exprFoldWithLocatorM f (ELocator $ 0:path) x
    f loc $ ERef p x'
exprFoldWithLocatorM' f loc@(ELocator path) (ETry p x)              = do
    x' <- exprFoldWithLocatorM f (ELocator $ 0:path) x
    f loc $ ETry p x'
exprFoldWithLocatorM' f loc@(ELocator path) (EClosure p as r x)     = do
    x' <- exprFoldWithLocatorM f (ELocator $ 0:path) x
    f loc $ EClosure p as r x'
exprFoldWithLocatorM' f loc                 (EFunc p fs)            = f loc $ EFunc p fs

-- Convert context to expression locator relative to a top-level entity,
-- i.e., rule, function, or index.
ctxToELocator :: ECtx -> ELocator
ctxToELocator ctx = ELocator $ reverse $ ctxToLocator' ctx

ctxToLocator' :: ECtx -> [Int]
ctxToLocator' CtxTop{}                   = []
ctxToLocator' CtxFunc{}                  = []
ctxToLocator' (CtxRuleL _ idx)           = [idx, 0]
ctxToLocator' (CtxRuleRAtom _ idx)       = [idx, 1]
ctxToLocator' (CtxRuleRCond _ idx)       = [idx, 1]
ctxToLocator' (CtxRuleRFlatMap _ idx)    = [0, idx, 1]
ctxToLocator' (CtxRuleRFlatMapVars _ idx)= [1, idx, 1]
ctxToLocator' (CtxRuleRInspect _ idx)    = [idx, 1]
ctxToLocator' (CtxRuleRProject _ idx)    = [0,idx,1]
ctxToLocator' (CtxRuleRGroupBy _ idx)    = [1,idx,1]
ctxToLocator' (CtxKey _)                 = []
ctxToLocator' (CtxIndex _)               = []
ctxToLocator' (CtxApplyArg _ ctx i)      = (i+1) : ctxToLocator' ctx
ctxToLocator' (CtxApplyFunc _ ctx)       = 0 : ctxToLocator' ctx
ctxToLocator' (CtxField _ ctx)           = 0 : ctxToLocator' ctx
ctxToLocator' (CtxTupField _ ctx)        = 0 : ctxToLocator' ctx
ctxToLocator' (CtxStruct _ ctx (i,_))    = i : ctxToLocator' ctx
ctxToLocator' (CtxTuple _ ctx i)         = i : ctxToLocator' ctx
ctxToLocator' (CtxSlice _ ctx)           = 0 : ctxToLocator' ctx
ctxToLocator' (CtxMatchExpr _ ctx)       = 0 : ctxToLocator' ctx
ctxToLocator' (CtxMatchPat _ ctx i)      = (1+2*i) : ctxToLocator' ctx
ctxToLocator' (CtxMatchVal _ ctx i)      = (2+2*i) : ctxToLocator' ctx
ctxToLocator' (CtxSeq1 _ ctx)            = 0 : ctxToLocator' ctx
ctxToLocator' (CtxSeq2 _ ctx)            = 1 : ctxToLocator' ctx
ctxToLocator' (CtxITEIf _ ctx)           = 0 : ctxToLocator' ctx
ctxToLocator' (CtxITEThen _ ctx)         = 1 : ctxToLocator' ctx
ctxToLocator' (CtxITEElse _ ctx)         = 2 : ctxToLocator' ctx
ctxToLocator' (CtxForIter _ ctx)         = 0 : ctxToLocator' ctx
ctxToLocator' (CtxForVars _ ctx)         = 1 : ctxToLocator' ctx
ctxToLocator' (CtxForBody _ ctx)         = 2 : ctxToLocator' ctx
ctxToLocator' (CtxSetL _ ctx)            = 1 : ctxToLocator' ctx
ctxToLocator' (CtxSetR _ ctx)            = 0 : ctxToLocator' ctx
ctxToLocator' (CtxReturn _ ctx)          = 0 : ctxToLocator' ctx
ctxToLocator' (CtxBinOpL _ ctx)          = 0 : ctxToLocator' ctx
ctxToLocator' (CtxBinOpR _ ctx)          = 1 : ctxToLocator' ctx
ctxToLocator' (CtxUnOp _ ctx)            = 0 : ctxToLocator' ctx
ctxToLocator' (CtxBinding _ ctx)         = 0 : ctxToLocator' ctx
ctxToLocator' (CtxTyped _ ctx)           = 0 : ctxToLocator' ctx
ctxToLocator' (CtxAs _ ctx)              = 0 : ctxToLocator' ctx
ctxToLocator' (CtxRef _ ctx)             = 0 : ctxToLocator' ctx
ctxToLocator' (CtxTry _ ctx)             = 0 : ctxToLocator' ctx
ctxToLocator' (CtxClosure _ ctx)         = 0 : ctxToLocator' ctx
