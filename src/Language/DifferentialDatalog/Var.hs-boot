{-# LANGUAGE RecordWildCards, TypeSynonymInstances, FlexibleInstances #-}
module Language.DifferentialDatalog.Var where

import Language.DifferentialDatalog.Name
import Language.DifferentialDatalog.Pos
import Language.DifferentialDatalog.Syntax
import Language.DifferentialDatalog.PP

data Var = ExprVar {varCtx::ECtx, varExpr::ENode}
         | ForVar {varCtx::ECtx, varExpr::ENode}
         | BindingVar {varCtx::ECtx, varExpr::ENode}
         | ArgVar {varFunc::Function, varArgIndex::Int, varName::String}
         | ClosureArgVar {varCtx::ECtx, varExpr::ENode, varArgIndex::Int}
         | KeyVar {varRel::Relation}
         | IdxVar {varIndex::Index, varArgIndex::Int, varName::String}
         | FlatMapVar {varRule::Rule, varRhsIdx::Int}
         | GroupVar {varRule::Rule, varRhsIdx::Int}
         | WeightVar
         | TSVar Rule

instance Eq Var
instance Ord Var
instance WithPos Var
instance WithName Var
instance PP Var
instance Show Var

data VLocator

varLocator :: Var -> VLocator
