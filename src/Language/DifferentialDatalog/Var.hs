{-
Copyright (c) 2020 VMware, Inc.
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
    Var(..))
where

import Data.List
import Data.Maybe
import Text.PrettyPrint

import Language.DifferentialDatalog.Name
import Language.DifferentialDatalog.Pos
import Language.DifferentialDatalog.PP
import Language.DifferentialDatalog.Syntax

-- Uniquely identifies a variable declaration in a DDlog program.
data Var = -- Variable declared in an expression ('var v').
           ExprVar {varCtx::ECtx, varExpr::ENode}
           -- For-loop variable.
         | ForVar {varCtx::ECtx, varExpr::ENode}
           -- Variable declard in a @-binding.
         | BindingVar {varCtx::ECtx, varExpr::ENode}
           -- Function argument.
         | ArgVar {varFunc::Function, varName::String}
           -- Closure argument.
         | ClosureArgVar {varCtx::ECtx, varExpr::ENode, varArgIdx::Int}
           -- Primary key variable.
         | KeyVar {varRel::Relation}
           -- Index variable.
         | IdxVar {varIndex::Index, varName::String}
           -- Variable returned by FlatMap.
         | FlatMapVar {varRule::Rule, varRhsIdx::Int}
           -- Variable returned by Aggregate.
         | AggregateVar {varRule::Rule, varRhsIdx::Int}
           -- ddlog_weight
         | WeightVar
           -- ddlog_timestamp
         | TSVar Rule
         deriving (Eq, Ord)

instance WithPos Var where
    pos (ExprVar _ e)         = pos e
    pos (ForVar _ e)          = pos e
    pos (BindingVar _ e)      = pos e
    pos (ArgVar f v)          = pos $ fromJust $ find ((==v) . name) $ funcArgs f
    pos (ClosureArgVar _ e i) = pos $ exprClosureArgs e !! i
    pos (KeyVar rel)          = pos $ fromJust $ relPrimaryKey rel
    pos (IdxVar idx v)        = pos $ fromJust $ find ((==v) . name) $ idxVars idx
    pos (FlatMapVar rl i)     = pos $ rhsMapExpr $ ruleRHS rl !! i
    pos (AggregateVar rl i)   = pos $ rhsAggExpr $ ruleRHS rl !! i
    pos WeightVar             = nopos
    pos (TSVar rl)            = pos rl
    atPos _ _                 = error "atPos 'Var' is not supported"
 
instance WithName Var where
    name (ExprVar _ EVarDecl{..})       = exprVName
    name (ExprVar _ EVar{..})           = exprVar
    name (ExprVar _ e)                  = error $ "ExprVar.name: unexpected expression " ++ show e
    name (ForVar _ EFor{..})            = exprLoopVar
    name (ForVar _ e)                   = error $ "ForVar.name: unexpected expression " ++ show e
    name (BindingVar _ EBinding{..})    = exprVar
    name (BindingVar _ e)               = error $ "BindingVar.name: unexpected expression " ++ show e
    name (ArgVar _ v)                   = v
    name (ClosureArgVar _ e i)          = name $ exprClosureArgs e !! i
    name (KeyVar rel)                   = keyVar $ fromJust $ relPrimaryKey rel
    name (IdxVar _ s)                   = s
    name (FlatMapVar rule i)            = rhsVar $ ruleRHS rule !! i
    name (AggregateVar rule i)          = rhsVar $ ruleRHS rule !! i 
    name WeightVar                      = "ddlog_weight"
    name TSVar{}                        = "ddlog_timestamp"
    setName _ _                         = error "setName 'Var' is not supported"

instance PP Var where
    pp v = pp $ name v

instance Show Var where
    show = render . pp
