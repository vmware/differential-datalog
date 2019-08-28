module Language.DifferentialDatalog.Rule where

import Language.DifferentialDatalog.Syntax

ruleRHSVars :: DatalogProgram -> Rule -> Int -> [Field]
ruleVars :: DatalogProgram -> Rule -> [Field]
ruleRHSTermVars :: Rule -> Int -> [String]
ruleLHSVars :: DatalogProgram -> Rule -> [Field]
ruleTypeMapM :: (Monad m) => (Type -> m Type) -> Rule -> m Rule
ruleHasJoins :: Rule -> Bool
atomVarOccurrences :: ECtx -> Expr -> [(String, ECtx)]
atomVars :: Expr -> [String]
ruleIsDistinctByConstruction :: DatalogProgram -> [RuleRHS] -> Atom -> Bool
