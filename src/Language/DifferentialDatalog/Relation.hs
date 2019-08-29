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
    relIdentifier,
    relsAreMutuallyRecursive,
    relIsBounded
) 
where

import qualified Data.Map                       as M
import qualified Data.Graph.Inductive           as G
import Data.Maybe
import Data.List

import Language.DifferentialDatalog.Name
import Language.DifferentialDatalog.Util
import Language.DifferentialDatalog.Syntax
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
-- NOTE: this only applies in the top-level scope; this function does not
-- reflect the distinctness of the relation inside the nested scope where it is
-- being computed.
relIsDistinctByConstruction :: DatalogProgram -> Relation -> Bool
-- Distinctness is enforced on input relations.
relIsDistinctByConstruction _ Relation{relRole = RelInput, ..}  = True
-- For recursive relations, we enforce distinctness of the relation is
-- _not_ bounded.
relIsDistinctByConstruction d rel | relIsRecursive d (name rel) =
    not $ relIsBounded d (name rel)
relIsDistinctByConstruction d rel 
    | -- There's only one rule for this relation
      length rules == 1 && length heads == 1 &&
      -- And this rule produces distinct outputs
      ruleIsDistinctByConstruction d rule head_atom
    = True
    where
    rules = relRules d $ name rel
    [rule] = rules
    heads = filter (\(lhs,_) -> atomRelation lhs == name rel) $ mapIdx (,) $ ruleLHS rule
    [(_,head_atom)] = heads
relIsDistinctByConstruction _ _ = False

-- | Relation is either distinct by construction or it is an output relation, in which
-- case we will explicitly enforce distinctness.
relIsDistinct :: DatalogProgram -> Relation -> Bool
relIsDistinct d rel = relIsDistinctByConstruction d rel || (relRole rel == RelOutput)

-- | All _recursive_ rules for this relation are distinct; hence
-- their output weights are bounded and there is no need to distinct
-- this relation to ensure that fixed-point computation convergence.
relIsBounded :: DatalogProgram -> String -> Bool
relIsBounded d rel =
    all (\rule@Rule{..} -> all (\(_,i) -> ruleIsDistinctByConstruction d rule i)
                           $ filter (\(_,i) -> ruleIsRecursive d rule i)
                           $ filter (\(lhs,_) -> atomRelation lhs == rel)
                           $ mapIdx (,) ruleLHS)
        $ relRules d rel

-- | Unique id, assigned to the relation in the generated dataflow graph
relIdentifier :: DatalogProgram -> Relation -> Int
relIdentifier d rel = M.findIndex (name rel) $ progRelations d

-- Relations are mutually recursive, i.e., belong to the same stronly connected
-- component of the dependency graph.
relsAreMutuallyRecursive :: DatalogProgram -> String -> String -> Bool
relsAreMutuallyRecursive d rel1 rel2 | rel1 == rel2 = relIsRecursive d rel1
                                     | otherwise = elem nd2 scc
    where
    g = progDependencyGraph d
    nd1 = fst $ fromJust $ find ((\case
                                  DepNodeRel r -> r == rel1
                                  _            -> False) . snd) $ G.labNodes g
    nd2 = fst $ fromJust $ find ((\case
                                  DepNodeRel r -> r == rel2
                                  _            -> False) . snd) $ G.labNodes g
    scc = fromJust $ find (elem nd1) $ G.scc g
