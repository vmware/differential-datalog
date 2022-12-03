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

{-# LANGUAGE TupleSections, LambdaCase, RecordWildCards#-}

module Language.DifferentialDatalog.Debugger.DebugState (
    queryDerivations,
    handleDebugEvents,
    emptyDebuggerMaps,
    getPredecessorOpId,
    DebuggerMaps(..),
    DebuggerRecord(..),
    OperatorInput(..),
    DebuggerRecordMap,
    )where

import qualified Data.Map as M
import Data.Maybe

import Language.DifferentialDatalog.Syntax
import Language.DifferentialDatalog.Debugger.DebugTypes

-- (1) Derivation corresponds to the input records for a specifc output record.
-- (2) One derivation could have at most two elements since Join has at most two input
--     records and other operators have only one input record.
-- (3) One output record may have multiple derivations.
type Derivation = [DebuggerRecord]

-- Output Record -> all derivations of this record observed by the debugger
type DebuggerRecordMap = M.Map DebuggerRecord [Derivation]

-- Derivation -> Weight. This map record all derivations and their weights for one
-- particular output record.
type DerivationWeightMap = M.Map Derivation Int

-- Output Record -> {derivation -> weight}.
type DebuggerRecordWeightMap = M.Map DebuggerRecord DerivationWeightMap

-- Operator id for a specific input/intermediate record. It could be either
-- a valid OperatorId used for further trace or an input relation that mark
-- the source of an input relation.
data OperatorInput = InputOp OperatorId
                   | InputRel String
                   deriving (Show, Eq, Ord)

data DebuggerRecord = DebuggerRecord { dbgRecord :: Record
                                     , dbgOperatorId :: OperatorInput
                                     } deriving (Show, Eq, Ord)

-- DebuggerRecordNode
--      nodeVal : record val and its operator id
--      childrenList : each list element is a children set that construct the parent node
--                     there could be multiple derivations, so it is a list of children set
data DebuggerRecordNode = DebuggerRecordNode { nodeVal :: DebuggerRecord
                                             , childrenList :: [[DebuggerRecordNode]]
                                             } deriving (Show)

-- DebuggerMaps: global map that has all derivation information from the dump file.
data DebuggerMaps = DebuggerMaps { dbgRecordMap :: DebuggerRecordMap
                                 , dbgRecordWeightMap :: DebuggerRecordWeightMap
                                 } deriving (Show)

emptyDebuggerMaps :: DebuggerMaps
emptyDebuggerMaps = DebuggerMaps M.empty M.empty

-- Return root node of the derivation tree
queryDerivations :: DebuggerRecord -> DebuggerRecordMap -> DatalogProgram -> DebuggerRecordNode
queryDerivations debuggerRecord debuggerRecordMap prog =
    let root = DebuggerRecordNode { nodeVal = debuggerRecord, childrenList = []}
    in case (dbgOperatorId debuggerRecord) of
        InputRel relName -> case getRelationPredecessorOpId relName prog of
                             [] -> root
                             _  -> case M.lookup debuggerRecord debuggerRecordMap of
                                    Nothing -> root
                                    Just derivations -> let childrenList = map (derivationToDebuggerRecordNode debuggerRecordMap prog) derivations
                                                        in DebuggerRecordNode {nodeVal = debuggerRecord, childrenList = childrenList}

        InputOp _ -> let derivation = M.lookup debuggerRecord debuggerRecordMap
                     in case derivation of
                        Nothing -> root                   -- control should never arrive here
                        Just derivations -> let childrenList = map (derivationToDebuggerRecordNode debuggerRecordMap prog) derivations
                                            in DebuggerRecordNode {nodeVal = debuggerRecord, childrenList = childrenList}

-- helper function used by `queryDerivations`
-- Derivation: [DebuggerRecord], generally has one/two input records, it stands
--             for one possible derivation for a specific output record

derivationToDebuggerRecordNode :: DebuggerRecordMap -> DatalogProgram -> Derivation -> [DebuggerRecordNode]
derivationToDebuggerRecordNode debuggerRecordMap prog derivation =
    map (\inputrecord -> queryDerivations inputrecord debuggerRecordMap prog) derivation

-- Construct the global DebuggerMaps which contains all record inheritance information
-- and used for processing queries.
handleDebugEvents :: [Event] -> DebuggerMaps -> DatalogProgram -> DebuggerMaps
handleDebugEvents [] dbgMaps _ = dbgMaps
handleDebugEvents (event:events) dbgMaps prog =
    let updatedMaps = handleDebugEvent event dbgMaps prog
    in  handleDebugEvents events updatedMaps prog

-- Process a single debug event entry. Each event corresponds to one row in the
-- debug dump file. Add this entry into the global debuggerMap and update its
-- derivation's weight accordingly.
handleDebugEvent :: Event -> DebuggerMaps -> DatalogProgram -> DebuggerMaps
handleDebugEvent event DebuggerMaps{..} prog =
    let outputRecord = DebuggerRecord { dbgRecord = (evtOutput event), dbgOperatorId = InputOp (evtOperatorId event)}
        predecessorIds = getPredecessorOpId (evtOperatorId event) prog
        derivation = case event of
                      DebugEvent{..} -> let inputRecord = DebuggerRecord { dbgRecord = evtInput, dbgOperatorId = (predecessorIds !! 0)}
                                        in [inputRecord]
                      DebugJoinEvent{..} -> let inputRecord1 = DebuggerRecord { dbgRecord = evtInput1, dbgOperatorId = (predecessorIds !! 0)}
                                                inputRecord2 = DebuggerRecord { dbgRecord = evtInput2, dbgOperatorId = (predecessorIds !! 1)}
                                            in [inputRecord1, inputRecord2]
        derivationWeightMap = case M.lookup outputRecord dbgRecordWeightMap of
                            Nothing -> M.empty
                            Just weightMap -> weightMap

        updatedWeight = case M.lookup derivation derivationWeightMap of
                         Nothing -> evtWeight event
                         Just w -> (evtWeight event) + w
    in if updatedWeight == 0
       then
            let updatedDerivationWeightMap = M.delete derivation derivationWeightMap
                updatedRecordWeightMap = M.insert outputRecord updatedDerivationWeightMap dbgRecordWeightMap
            in DebuggerMaps { dbgRecordMap = dbgRecordMap, dbgRecordWeightMap = updatedRecordWeightMap}
       else
            let traceDerivations = M.lookup outputRecord dbgRecordMap
                updatedDerivationWeightMap = M.insert derivation updatedWeight derivationWeightMap
                updatedRecordWeightMap = M.insert outputRecord updatedDerivationWeightMap dbgRecordWeightMap
            in case traceDerivations of
                Nothing -> let updatedDbgRecordMap = M.insert outputRecord [derivation] dbgRecordMap
                           in DebuggerMaps { dbgRecordMap = updatedDbgRecordMap, dbgRecordWeightMap = updatedRecordWeightMap}
                Just derivations -> let updatedDbgRecordMap = M.insert outputRecord (derivations ++ [derivation]) dbgRecordMap
                                    in DebuggerMaps { dbgRecordMap = updatedDbgRecordMap, dbgRecordWeightMap = updatedRecordWeightMap}

-- Get the operator if for input records in a debug entry
getPredecessorOpId :: OperatorId -> DatalogProgram-> [OperatorInput]
getPredecessorOpId OperatorId{..} DatalogProgram{..} =
    let Rule{..} = progRules !! ruleIdx
        ruleRhs = ruleRHS !! rhsIdx
    in case ruleRhs of
        RHSLiteral{..} ->
            if rhsIdx == 0
            then [InputRel (atomRelation rhsAtom)]
            else let prevRuleRhs = ruleRHS !! (rhsIdx - 1)
                 in case prevRuleRhs of
                        RHSLiteral{rhsAtom = prevAtom} -> [InputRel (atomRelation prevAtom), InputRel (atomRelation rhsAtom)]
                        _ -> [InputOp OperatorId {ruleIdx = ruleIdx, rhsIdx = (rhsIdx - 1), headIdx = headIdx}, InputRel (atomRelation rhsAtom)]
        RHSCondition{..} -> let prevRhsIdx = getPredecessorRHSRuleIdxForCondition rhsIdx ruleRHS
                        in [InputOp OperatorId {ruleIdx = ruleIdx, rhsIdx = prevRhsIdx, headIdx = headIdx}]
        _ -> [InputOp OperatorId {ruleIdx = ruleIdx, rhsIdx = (rhsIdx - 1), headIdx = headIdx}]

getPredecessorRHSRuleIdxForCondition :: Int -> [RuleRHS] -> Int
getPredecessorRHSRuleIdxForCondition rhsIdx rules =
    case (rules !! (rhsIdx-1)) of
        RHSCondition{..} -> getPredecessorRHSRuleIdxForCondition (rhsIdx-1) rules
        _ -> (rhsIdx-1)


-- Search OperatorId for an InputRel. Search the rules and head name, if the head name
-- equal to the relation name, return the corresponding head index, rule index and rhsRule length
-- minus one as rhs index.
getRelationPredecessorOpId :: String -> DatalogProgram -> [OperatorId]
getRelationPredecessorOpId relName DatalogProgram{..} =
    let resultList = map (\ruleIdx -> getOperatorIdFromRule (progRules !! ruleIdx) ruleIdx relName) [0..length (progRules) - 1]
    in mergeResultList resultList

getOperatorIdFromRule :: Rule -> Int -> String -> [OperatorId]
getOperatorIdFromRule Rule{..} ruleIdx relName =
    catMaybes (map (\i -> let atom = ruleLHS !! i
                           in if (atomRelation atom) == relName
                              then Just OperatorId {ruleIdx = ruleIdx, rhsIdx = (length ruleRHS - 1), headIdx = i}
                              else Nothing) [0..length (ruleLHS) - 1])

mergeResultList :: [[a]] -> [a]
mergeResultList [] = []
mergeResultList (element: elements) =
    element ++ (mergeResultList elements)