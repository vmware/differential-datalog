{-# LANGUAGE RecordWildCards #-}

module Language.DifferentialDatalog.Rule (
    ruleRHSVars,
    ruleVars
) where

import Language.DifferentialDatalog.Syntax

-- | Variables visible in the 'i'th conjunct in the right-hand side of
-- a rule
ruleRHSVars :: Rule -> Int -> [Field]
ruleRHSVars rl i = undefined

-- | All variables defined in a rule
ruleVars :: Rule -> [Field]
ruleVars rl@Rule{..} = ruleRHSVars rl (length ruleRHS)
