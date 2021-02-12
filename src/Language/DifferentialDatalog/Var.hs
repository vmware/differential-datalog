{-
Copyright (c) 2020-2021 VMware, Inc.
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

{-# LANGUAGE RecordWildCards, TypeSynonymInstances, FlexibleInstances #-}
module Language.DifferentialDatalog.Var(
    Var(..),
    VLocator,
    varLocator)
where

import Data.Maybe
import Text.PrettyPrint

import Language.DifferentialDatalog.Name
import Language.DifferentialDatalog.Pos
import Language.DifferentialDatalog.PP
import Language.DifferentialDatalog.Syntax
import Language.DifferentialDatalog.Expr

-- Uniquely identifies a variable declaration in a DDlog program.
data Var = -- Variable declared in an expression ('var v').
           ExprVar {varCtx::ECtx, varExpr::ENode}
           -- Variable declard in a @-binding.
         | BindingVar {varCtx::ECtx, varExpr::ENode}
           -- Function argument.
         | ArgVar {varFunc::Function, varArgIndex::Int, varName::String}
           -- Closure argument.
         | ClosureArgVar {varCtx::ECtx, varExpr::ENode, varArgIndex::Int}
           -- Primary key variable.
         | KeyVar {varRel::Relation}
           -- Index variable.
         | IdxVar {varIndex::Index, varArgIndex::Int, varName::String}
           -- Variable returned by group_by.
         | GroupVar {varRule::Rule, varRhsIdx::Int}
           -- ddlog_weight
         | WeightVar
           -- ddlog_timestamp
         | TSVar Rule
         deriving (Eq, Ord)

instance WithPos Var where
    pos (ExprVar _ e)         = pos e
    pos (BindingVar _ e)      = pos e
    pos (ArgVar f i _)        = pos $ funcArgs f !! i
    pos (ClosureArgVar _ e i) = pos $ exprClosureArgs e !! i
    pos (KeyVar rel)          = pos $ fromJust $ relPrimaryKey rel
    pos (IdxVar idx i _)      = pos $ idxVars idx !! i
    pos (GroupVar rl i)       = (fst $ pos $ rhsProject rhs, snd $ pos $ rhsGroupBy rhs)
                                where rhs = ruleRHS rl !! i
    pos WeightVar             = nopos
    pos (TSVar rl)            = pos rl
    atPos _ _                 = error "atPos 'Var' is not supported"
 
instance WithName Var where
    name (ExprVar _ EVarDecl{..})       = exprVName
    name (ExprVar _ EVar{..})           = exprVar
    name (ExprVar _ e)                  = error $ "ExprVar.name: unexpected expression " ++ show e
    name (BindingVar _ EBinding{..})    = exprVar
    name (BindingVar _ e)               = error $ "BindingVar.name: unexpected expression " ++ show e
    name (ArgVar _ _ v)                 = v
    name (ClosureArgVar _ e i)          = name $ exprClosureArgs e !! i
    name (KeyVar rel)                   = keyVar $ fromJust $ relPrimaryKey rel
    name (IdxVar _ _ s)                 = s
    name (GroupVar rule i)              = rhsVar $ ruleRHS rule !! i 
    name WeightVar                      = "ddlog_weight"
    name TSVar{}                        = "ddlog_timestamp"
    setName _ _                         = error "setName 'Var' is not supported"

instance PP Var where
    pp v = pp $ name v

instance Show Var where
    show = render . pp

-- A descriptor that uniquely identifies a variable within
-- a given context as a path from the root of the context to the
-- variable declaration.
data VLocator = VLocator [Int] deriving (Eq, Ord)

varLocator :: Var -> VLocator
varLocator ExprVar{..}          = VLocator $ 0:(elocatorPath $ ctxToELocator varCtx)
varLocator BindingVar{..}       = VLocator $ 1:(elocatorPath $ ctxToELocator varCtx)
varLocator ArgVar{..}           = VLocator  [2,varArgIndex]
varLocator ClosureArgVar{..}    = VLocator $ 3:(elocatorPath $ ctxToELocator varCtx) ++ [varArgIndex]
varLocator KeyVar{..}           = VLocator  [4]
varLocator IdxVar{..}           = VLocator  [5, varArgIndex]
varLocator GroupVar{..}         = VLocator  [6, varRhsIdx]
varLocator WeightVar            = VLocator  [7]
varLocator TSVar{}              = VLocator  [8]

