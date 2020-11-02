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

{- |
Module     : Crate
Description: Decompose generated Rust project into crates.
-}

{-# LANGUAGE RecordWildCards, FlexibleContexts, TupleSections, LambdaCase, ImplicitParams, ScopedTypeVariables #-}

module Language.DifferentialDatalog.Crate(
    partitionIntoCrates,
    crateMainModule
) where

import Data.List
import Data.Maybe
import qualified Data.Map as M
import qualified Data.Set as S
import qualified Data.Graph.Inductive as G

import Language.DifferentialDatalog.Module
import Language.DifferentialDatalog.Syntax
import Language.DifferentialDatalog.Util

-- A crate is a set of modules.
type Crate = S.Set ModuleName

-- Returns the top-level module of a crate.  This is well-defined, as by
-- construction crate modules form a crate with a unique root.
crateMainModule :: Crate -> ModuleName
crateMainModule crate =
    maximumBy (\m1 m2 -> (length $ modulePath m1) `compare` (length $ modulePath m2)) crate

-- A crate graph is a graph with crates as vertices and with two
-- types of edges:
-- * Dependency edges represent module dependencies.  Two crates are connected
--   by a dependency edge iff there is a dependency between a pair of modules
--   inside these crates.
-- * Parent edges represent the DDlog module hierarchy.
data CrateGraph = CrateGraph {
    -- Crates.
    cgCrates    :: [Crate],
    -- Map from module names to index of the crate the module belongs to.
    cgMod2Crate :: M.Map ModuleName Int
} deriving (Eq)

cgEmpty :: CrateGraph
cgEmpty =
    CrateGraph {
        cgCrates       = [],
        cgMod2Crate    = M.empty
    }

cgAddCrate :: CrateGraph -> Crate -> CrateGraph
cgAddCrate CrateGraph{..} crate =
    CrateGraph {
        cgCrates = cgCrates ++ [crate],
        cgMod2Crate = foldl' (\modCrates m -> M.insert m (length cgCrates) modCrates)
                             cgMod2Crate (S.toList crate)
    }

-- Module dependencies.  Currently just the list of module imports, but
-- we may want to use a more accurate methos based on actual dependencies
-- used by types and functions in the module.
moduleDeps :: (?mmap :: M.Map ModuleName DatalogModule) => ModuleName -> [ModuleName]
moduleDeps m = fromMaybe [] $ ((map importModule) . progImports . moduleDefs) <$> ?mmap M.!? m

-- Expand crate so that modules in the crate form a single connected
-- subtree of the module graph:
-- Find the lowest common ancestor of all modules in the crate and add
-- modules along all branches from the ancestor to modules in the crate to
-- the crate.
--
-- Example:
-- Module hirarchy:
-- top
--  |
--  |-m1
--    |-m2
--    |-m3
--    |  |-m4
--    |  |-m5
--    |
--    |-m6
--       |-m7
--       |-m8
--
-- crate1 = {m8, m4}.
-- The lowest common ancestor of m8 and m4 is m1.  We therefore
-- extend the crate with m1 and all modules down the path from m1
-- to m8 and m4:
-- crate1' = {m1, m3, m4, m6, m8}.
addLowestCommonAncestor :: Crate -> Crate
addLowestCommonAncestor crate =
    S.foldl (\mods m -> foldl' (\mods' mname -> S.insert (ModuleName mname) mods') (S.insert m mods)
                               $ drop lca_len $ inits $ modulePath m)
            S.empty crate
    where
    -- Lowest common ancestor path length.
    lca_len = length $ lcp $ map modulePath $ S.toList crate
    -- Longest common prefix.
    lcp :: (Eq a) => [[a]] -> [a]
    lcp [] = []
    lcp (x:lst) = foldl' (\prefix x' -> lcp2 prefix x') x lst
    -- Longest common prefix of two lists.
    lcp2 :: (Eq a) => [a] -> [a] -> [a]
    lcp2 []     _      = []
    lcp2 _     []      = []
    lcp2 (x:xs) (y:ys) | x == y    = x : (lcp2 xs ys)
                       | otherwise = []

-- Merge crates that form strongly connected components of the graph.
--
-- Example:
-- crate1 = {m1, m2, m3}
-- crate2 = {m4, m5, m6}
-- dependencies: m1 -> m4, m6 -> m3
--
-- Crates 1 and 2 form a cycle and will get merged by this function.
sccGraph :: (?mmap :: M.Map ModuleName DatalogModule) => CrateGraph -> [Crate]
sccGraph CrateGraph{..} =
    -- For each SCC, compute the union of all crates in the SCC.
    map (S.unions . map (cgCrates !!)) sccs
    where
    -- Build an instance of G.Graph out of 'cg' and use the SCC algorithm from fgl on it.
    nodes = mapIdx (\crate i -> (i, crate)) cgCrates
    edges = concat
            $ mapIdx (\crate i -> concatMap (\m -> map (\m' -> (i, cgMod2Crate M.! m', ()))
                                                       (moduleDeps m))
                                            $ S.toList crate)
            $ cgCrates
    g :: G.Gr Crate () = G.mkGraph nodes edges
    -- Strongly connected components.
    sccs :: [[Int]]
    sccs = G.scc g

-- Merge overlapping crates until there is nothing to merge.
-- Example:
-- crate1 = {m1, m2, m3}
-- crate2 = {m3, m4, m5}
-- crate3 = {m5, m6, m7}.
--
-- Iteration 1: merge crate1 and crate2 -> crate1' = {m1, m2, m3, m4, m5}
-- Iteration 2: merge crate1' and crate3 -> {m1,..,m7}
mergeOverlappingCrates :: [Crate] -> CrateGraph
mergeOverlappingCrates crates =
    foldl' cgAddCrate cgEmpty crates'
    where
    modules = S.unions crates
    -- Build an instance of G.Graph with modules as vertices and with edges
    -- connecting modules that belong to the same crate.
    nodes = mapIdx (\m i -> (i, m)) $ S.toList modules
    -- Add edges from the first module in a crate to all other modules.
    edges = concatMap ((\(m : ms) -> map (\m' -> (S.findIndex m modules, S.findIndex m' modules, ())) ms) . S.toList) crates
    g :: G.Gr ModuleName () = G.mkGraph nodes edges
    -- Merge overlapping crates by computing connected components of the graph.
    components :: [[Int]]
    components = G.components g
    -- For each component, compute the union of all crates in the component.
    crates' = map (S.fromList . map (`S.elemAt` modules)) components

-- Main function: Partition DDlog modules into crates.
--
-- Computes the most fine-grained partitioning such that:
-- 1. modules with circular dependencies belong to the same crate.
-- 2. modules in each crate form a subtree of the module hierarchy.
--   (i.e., for any two modules, their lowest common ancestor
--   and all nodes in between are part of the same crate).
--
-- Algorithm:
-- 1. Start with a crate graph that places each module in its own crate.
-- 2. Apply three transformations to merge crates to satisfy criteria 1 and 2
--    until reaching a fix point:
--    'sccGraph', 'addLowestCommonAncestors', 'joinOverlappingCrates'.
partitionIntoCrates :: [DatalogModule] -> CrateGraph
partitionIntoCrates modules =
    let ?mmap = mmap in partitionIntoCratesRecursive cg0
    where
    mmap = M.fromList $ map (\m -> (moduleName m, m)) modules
    cg0 = foldl' (\cg m -> foldl' (\cg' path -> cgAddCrate cg' (S.singleton $ ModuleName path))
                                  cg $ inits $ modulePath m)
                 cgEmpty $ M.keys mmap

partitionIntoCratesRecursive :: (?mmap :: M.Map ModuleName DatalogModule) => CrateGraph -> CrateGraph
partitionIntoCratesRecursive cg | cg' == cg = cg
                                | otherwise = partitionIntoCratesRecursive cg'
    where
    cg' = mergeOverlappingCrates $
          (map addLowestCommonAncestor) $
          sccGraph cg
