module Language.DifferentialDatalog.Rule where

import Language.DifferentialDatalog.Syntax
import {-# SOURCE #-} Language.DifferentialDatalog.Var

ruleRHSVars :: DatalogProgram -> Rule -> Int -> [Var]
ruleVars :: DatalogProgram -> Rule -> [Var]
ruleRHSTermVars :: DatalogProgram -> Rule -> Int -> [Var]
ruleLHSVars :: DatalogProgram -> Rule -> [Var]
ruleTypeMapM :: (Monad m) => (Type -> m Type) -> Rule -> m Rule
ruleHasJoins :: Rule -> Bool
ruleIsDistinctByConstruction :: DatalogProgram -> Rule -> Int -> Bool
ruleHeadIsRecursive :: DatalogProgram -> Rule -> Int -> Bool
ruleIsRecursive :: DatalogProgram -> Rule -> Bool
ruleGroupByKeyType :: DatalogProgram -> Rule -> Int -> Type
ruleGroupByValType :: DatalogProgram -> Rule -> Int -> Type
