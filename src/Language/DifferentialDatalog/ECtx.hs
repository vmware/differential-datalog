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

{-# LANGUAGE RecordWildCards #-}

{- | 
Module     : ECtx
Description: Helper functions for manipulating expression contexts.
 -}
module Language.DifferentialDatalog.ECtx(
     ctxAncestors,
     ctxLocalAncestors,
     ctxIsRuleLAtom,
     ctxInRuleLAtom,
     ctxIsMatchPat,
     ctxInMatchPat,
     ctxIsSetL,
     ctxInSetL,
     ctxIsSeq1,
     ctxInSeq1,
     ctxIsSeq2,
     ctxInBinding,
     ctxIsBinding,
     ctxIsTyped,
     ctxIsRuleRCond,
     ctxInRuleRHSPositivePattern,
     ctxInRuleRHSPattern,
     ctxIsRuleRFlatMapVars,
     ctxInRuleRFlatMapVars,
     ctxIsIndex,
     ctxInIndex,
     ctxIsFunc,
     ctxInFunc,
     ctxIsApplyFunc,
     ctxIsClosure,
     ctxInClosure,
     ctxInFuncOrClosure,
     ctxIsForLoopBody,
     ctxInForLoopBody,
     ctxIsForLoopVars,
     ctxInForLoopVars,
     ctxStripTypeAnnotations)
where

import Data.Maybe
import Data.List

import Language.DifferentialDatalog.Syntax

-- | List all ancestor contexts by recursively calling 'ctxParent'
ctxAncestors :: ECtx -> [ECtx]
ctxAncestors CtxTop = [CtxTop]
ctxAncestors ctx    = ctx : (ctxAncestors $ ctxParent ctx)

-- | List all ancestor contexts, stopping upon reaching a closure context.
-- Example use: when determining the type of 'x' in 'return x' inside a 
-- closure, which is in turn located inside a function, we want to find the
-- containing closure, not the function.
ctxLocalAncestors :: ECtx -> [ECtx]
ctxLocalAncestors CtxTop            = [CtxTop]
ctxLocalAncestors ctx@CtxClosure{}  = [ctx]
ctxLocalAncestors ctx               = ctx : (ctxLocalAncestors $ ctxParent ctx)

ctxIsRuleLAtom :: ECtx -> Bool
ctxIsRuleLAtom CtxRuleLAtom{} = True
ctxIsRuleLAtom _          = False

ctxInRuleLAtom :: ECtx -> Bool
ctxInRuleLAtom ctx = any ctxIsRuleLAtom $ ctxAncestors ctx

ctxIsMatchPat :: ECtx -> Bool
ctxIsMatchPat CtxMatchPat{} = True
ctxIsMatchPat _             = False

ctxInMatchPat :: ECtx -> Bool
ctxInMatchPat ctx = isJust $ ctxInMatchPat' ctx

ctxInMatchPat' :: ECtx -> Maybe ECtx
ctxInMatchPat' ctx = find ctxIsMatchPat $ ctxAncestors ctx

ctxIsSetL :: ECtx -> Bool
ctxIsSetL CtxSetL{} = True
ctxIsSetL _         = False

ctxInSetL :: ECtx -> Bool
ctxInSetL ctx = any ctxIsSetL $ ctxAncestors ctx

ctxIsSeq1 :: ECtx -> Bool
ctxIsSeq1 CtxSeq1{} = True
ctxIsSeq1 _         = False

ctxInSeq1 :: ECtx -> Bool
ctxInSeq1 ctx = any ctxIsSeq1 $ ctxAncestors ctx

ctxIsSeq2 :: ECtx -> Bool
ctxIsSeq2 CtxSeq2{} = True
ctxIsSeq2 _         = False

ctxInBinding :: ECtx -> Bool
ctxInBinding ctx = any ctxIsBinding $ ctxAncestors ctx

ctxIsBinding :: ECtx -> Bool
ctxIsBinding CtxBinding{} = True
ctxIsBinding _            = False

ctxIsTyped :: ECtx -> Bool
ctxIsTyped CtxTyped{} = True
ctxIsTyped _          = False

ctxIsRuleRCond :: ECtx -> Bool
ctxIsRuleRCond CtxRuleRCond{} = True
ctxIsRuleRCond _              = False

ctxIsIndex :: ECtx -> Bool
ctxIsIndex CtxIndex{} = True
ctxIsIndex _          = False

ctxInIndex :: ECtx -> Bool
ctxInIndex ctx = any ctxIsIndex $ ctxAncestors ctx

-- | True if context is inside a positive right-hand-side literal of a
-- rule, in a pattern expression, i.e., an expression where new
-- variables can be declared.
ctxInRuleRHSPositivePattern :: ECtx -> Bool
ctxInRuleRHSPositivePattern (CtxRuleRAtom rl idx) = rhsPolarity $ ruleRHS rl !! idx
ctxInRuleRHSPositivePattern CtxStruct{..}         = ctxInRuleRHSPositivePattern ctxPar
ctxInRuleRHSPositivePattern CtxTuple{..}          = ctxInRuleRHSPositivePattern ctxPar
ctxInRuleRHSPositivePattern CtxTyped{..}          = ctxInRuleRHSPositivePattern ctxPar
ctxInRuleRHSPositivePattern CtxBinding{..}        = ctxInRuleRHSPositivePattern ctxPar
ctxInRuleRHSPositivePattern CtxRef{..}            = ctxInRuleRHSPositivePattern ctxPar
ctxInRuleRHSPositivePattern _                     = False

-- | True if context is inside a (positive or negative) right-hand-side literal of a
-- rule, in a pattern expression, i.e., an expression where new
-- variables can be declared.
ctxInRuleRHSPattern :: ECtx -> Bool
ctxInRuleRHSPattern CtxRuleRAtom{}        = True
ctxInRuleRHSPattern CtxStruct{..}         = ctxInRuleRHSPattern ctxPar
ctxInRuleRHSPattern CtxTuple{..}          = ctxInRuleRHSPattern ctxPar
ctxInRuleRHSPattern CtxTyped{..}          = ctxInRuleRHSPattern ctxPar
ctxInRuleRHSPattern CtxBinding{..}        = ctxInRuleRHSPattern ctxPar
ctxInRuleRHSPattern CtxRef{..}            = ctxInRuleRHSPattern ctxPar
ctxInRuleRHSPattern _                     = False

ctxIsFunc :: ECtx -> Bool
ctxIsFunc CtxFunc{} = True
ctxIsFunc _         = False

ctxInFunc :: ECtx -> Maybe Function
ctxInFunc CtxFunc{..}  = Just ctxFunc
ctxInFunc CtxTop       = Nothing
ctxInFunc ctx          = ctxInFunc $ ctxParent ctx

ctxIsApplyFunc :: ECtx -> Bool
ctxIsApplyFunc CtxApplyFunc{} = True
ctxIsApplyFunc _              = False

ctxIsClosure :: ECtx -> Bool
ctxIsClosure CtxClosure{} = True
ctxIsClosure _            = False

-- | Returns the closure context ('CtxClosure') that 'ctx' belongs to or 'Nothing'
-- if 'ctx' does not belong to a closure.
ctxInClosure :: ECtx -> Maybe ECtx
ctxInClosure ctx@CtxClosure{} = Just ctx
ctxInClosure CtxTop           = Nothing
ctxInClosure ctx              = ctxInClosure $ ctxParent ctx

-- | Returns the closure ('CtxClosure') or function ('CtxFunc') context that 'ctx' belongs
-- to or 'Nothing' if 'ctx' does not belong to a function or closure.
ctxInFuncOrClosure :: ECtx -> Maybe ECtx
ctxInFuncOrClosure ctx@CtxClosure{} = Just ctx
ctxInFuncOrClosure ctx@CtxFunc{}    = Just ctx
ctxInFuncOrClosure CtxTop           = Nothing
ctxInFuncOrClosure ctx              = ctxInFuncOrClosure $ ctxParent ctx

ctxIsForLoopBody :: ECtx -> Bool
ctxIsForLoopBody CtxForBody{} = True
ctxIsForLoopBody _            = False

-- | The context is located in the body of a for-loop, where a 'continue'
-- statement is valid.  This does _not_ include a closure inside a loop.
ctxInForLoopBody :: ECtx -> Bool
ctxInForLoopBody ctx = any ctxIsForLoopBody $ ctxLocalAncestors ctx

ctxIsForLoopVars :: ECtx -> Bool
ctxIsForLoopVars CtxForVars{} = True
ctxIsForLoopVars _            = False

-- | The context is located in the for-loop variable pattern.
ctxInForLoopVars :: ECtx -> Bool
ctxInForLoopVars ctx = any ctxIsForLoopVars $ ctxLocalAncestors ctx

ctxIsRuleRFlatMapVars :: ECtx -> Bool
ctxIsRuleRFlatMapVars CtxRuleRFlatMapVars{} = True
ctxIsRuleRFlatMapVars _                     = False

-- | The context is located in the lhs of a FlatMap operator.
ctxInRuleRFlatMapVars :: ECtx -> Bool
ctxInRuleRFlatMapVars ctx = any ctxIsRuleRFlatMapVars $ ctxAncestors ctx

-- | Find the first parent that is not a type annotation.
ctxStripTypeAnnotations :: ECtx -> ECtx
ctxStripTypeAnnotations CtxTyped{..} = ctxStripTypeAnnotations ctxPar
ctxStripTypeAnnotations ctx          = ctx


