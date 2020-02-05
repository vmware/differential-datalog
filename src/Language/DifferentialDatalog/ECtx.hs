{-
Copyright (c) 2018-2019 VMware, Inc.
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
     ctxIsRuleL,
     ctxInRuleL,
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
     ctxIsIndex,
     ctxInIndex,
     ctxIsFunc,
     ctxInFunc,
     ctxIsForLoopBody,
     ctxInForLoopBody)
where

import Data.Maybe
import Data.List

import Language.DifferentialDatalog.Syntax

-- | List all ancestor contexts by recursively calling 'ctxParent'
ctxAncestors :: ECtx -> [ECtx]
ctxAncestors CtxTop = [CtxTop]
ctxAncestors ctx    = ctx : (ctxAncestors $ ctxParent ctx)

ctxIsRuleL :: ECtx -> Bool
ctxIsRuleL CtxRuleL{} = True
ctxIsRuleL _          = False

ctxInRuleL :: ECtx -> Bool
ctxInRuleL ctx = any ctxIsRuleL $ ctxAncestors ctx

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

-- | Returns the function that the context belongs to or 'Nothing'
-- if 'ctx' is outside the body of a function.
ctxInFunc :: ECtx -> Maybe Function
ctxInFunc (CtxFunc f) = Just f
ctxInFunc CtxTop      = Nothing
ctxInFunc ctx         = ctxInFunc (ctxParent ctx)

ctxIsForLoopBody :: ECtx -> Bool
ctxIsForLoopBody CtxForBody{} = True
ctxIsForLoopBody _            = False

ctxInForLoopBody :: ECtx -> Bool
ctxInForLoopBody ctx = any ctxIsForLoopBody $ ctxAncestors ctx
