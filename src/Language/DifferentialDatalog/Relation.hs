{-
Copyright (c) 2018 VMware, Inc.
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

{-# LANGUAGE TupleSections, LambdaCase, RecordWildCards #-}

{- | 
Module     : Relation
Description: Helper functions for manipulating Relations.
-}
module Language.DifferentialDatalog.Relation (
    relRules,
    relApplys,
    relIsRecursive,
    relIsDistinctByConstruction,
    relIsDistinct,
    relIdentifier
) 
where

import qualified Data.Set                       as S
import qualified Data.Map                       as M
import qualified Data.Graph.Inductive           as G
import qualified Data.Graph.Inductive.Query.DFS as G
import Data.Maybe
import Data.List

import Language.DifferentialDatalog.Name
import Language.DifferentialDatalog.Syntax
import Language.DifferentialDatalog.NS
import Language.DifferentialDatalog.Expr
import {-# SOURCE #-} Language.DifferentialDatalog.Rule
import Language.DifferentialDatalog.DatalogProgram

-- | Rules that contain given relation in their heads.
relRules :: DatalogProgram -> String -> [Rule]
relRules d rname = filter ((== rname) . atomRelation . head . ruleLHS)
                   $ progRules d

-- | Transformer applications that contain given relation in their heads.
relApplys :: DatalogProgram -> String -> [Apply]
relApplys d rname = filter (elem rname . applyOutputs)
                    $ progApplys d

relIsRecursive :: DatalogProgram -> String -> Bool
relIsRecursive d rel =
    any (\(from, to) -> elem from scc && elem to scc) $ G.edges g
    where
    g = progDependencyGraph d
    nd = fst $ fromJust $ find ((\case
                                  DepNodeRel r -> r == rel
                                  _            -> False) . snd) $ G.labNodes g
    scc = fromJust $ find (elem nd) $ G.scc g
    
-- | Relation only contains records with weight 1 by construction and does not require
-- distinct() or distinct_total() to convert it to that form.
relIsDistinctByConstruction :: DatalogProgram -> Relation -> Bool
-- distinctness is enforced on input relations
relIsDistinctByConstruction _ Relation{relRole = RelInput, ..}  = True
-- recursive collection are distinct in differential dataflow 
relIsDistinctByConstruction d rel | relIsRecursive d (name rel) = True
relIsDistinctByConstruction d rel 
    | -- There's only one rule for this relation
     length rules == 1 && length heads == 1 &&
     -- The first atom in the body of the rule is a distinct relation and does not contain wildcards
     length (ruleRHS rule) >= 1 && relIsDistinct d baserel && (not $ exprContainsPHolders $ atomVal atom1) &&
     -- The head of the rule is an injective function of all the variables from the first atom
     exprIsInjective d (S.fromList $ atomVars $ atomVal atom1) (atomVal head_atom) && 
     -- The rule only contains clauses that filter the collection
     all (\case
           RHSLiteral True a -> relIsDistinct d (getRelation d $ atomRelation a) &&
                                (not $ exprContainsPHolders $ atomVal a) &&
                                (null $ (nub $ atomVars $ atomVal a) \\ (nub $ atomVars $ atomVal head_atom))
           RHSCondition{}    -> True
           _                 -> False)
         (tail $ ruleRHS rule)
    = True
    where
    rules = relRules d $ name rel
    [rule] = rules
    heads = filter (\lhs -> atomRelation lhs == name rel) $ ruleLHS rule
    [head_atom] = heads
    RHSLiteral _ atom1 = head $ ruleRHS rule
    baserel = getRelation d $ atomRelation atom1
relIsDistinctByConstruction _ _ = False

-- | Relation is either distinct by construction or if it is an output relation, in which
-- case we will explicitly enforce distinctness
relIsDistinct :: DatalogProgram -> Relation -> Bool
relIsDistinct d rel = relIsDistinctByConstruction d rel || (relRole rel == RelOutput)

-- | Unique id, assigned to the relation in the generated dataflow graph
relIdentifier :: DatalogProgram -> Relation -> Int
relIdentifier d rel = M.findIndex (name rel) $ progRelations d
