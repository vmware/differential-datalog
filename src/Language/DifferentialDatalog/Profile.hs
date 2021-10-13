{-
Copyright (c) 2021 VMware, Inc.
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

{-# LANGUAGE ImplicitParams, OverloadedStrings, RecordWildCards #-}

{- |
Module     : Profile
Description: Helper functions to generate debug info records injected in the
    generated Rust code.  These records are used to generate human-readable
    profiling info.  'profile.rs' defines debug info format for each
    differential dataflow operator used to encode DDlog rules.  This module
    contains a function for each operator that generates the corresponding
    record format.
-}

module Language.DifferentialDatalog.Profile where

import Prelude hiding((<>))
import qualified Data.Map as M
import Text.PrettyPrint
import Text.Parsec.Pos

import Language.DifferentialDatalog.Pos
import Language.DifferentialDatalog.Name
import Language.DifferentialDatalog.PP
import Language.DifferentialDatalog.Module
import Language.DifferentialDatalog.Syntax

type ModulePathMap = M.Map FilePath DatalogModule

mkSourcePos :: (?specname::String, ?module_paths::ModulePathMap) => Pos -> Doc
mkSourcePos (start, end) | start == (fst nopos) = "::ddlog_profiler::SourcePosition::default()"
                         | otherwise = 
    "::ddlog_profiler::SourcePosition::new_range(" <> commaSep [file, start_line, start_col, end_line, end_col] <> ")"
    where
    file = pp $ show $ moduleNameToRelPath $ moduleName $ ?module_paths M.! (sourceName start)
    start_line = pp $ sourceLine start
    start_col = pp $ sourceColumn start
    end_line = pp $ sourceLine end
    end_col = pp $ sourceColumn end

cow_str :: String -> Doc
cow_str s = "::std::borrow::Cow::Borrowed(" <> (pp $ show s) <> ")"

cow :: Doc -> Doc
cow s = cow_str $ render s

-- RuleDebugInfo - debug info associated with entire rule.
dbgInfoRule :: (?specname::String, ?module_paths::ModulePathMap) => Rule -> Doc
dbgInfoRule rl = 
    "::ddlog_profiler::RuleDebugInfo::new(" <> mkSourcePos (pos rl) <> ")"

-- ArrangementDebugInfo - debug info associated with an arranged relation.
dbgInfoArrangement :: (?specname::String, ?module_paths::ModulePathMap) => Expr -> [Pos] -> [Index] -> Doc
dbgInfoArrangement pattern used_at indexes = 
    "::ddlog_profiler::ArrangementDebugInfo::new(" <> cow (pp pattern) <> "," <+> used_pos <> "," <+> used_in_indexes <> ")"
    where
    used_pos = "&[" <> commaSep (map mkSourcePos used_at) <> "]"
    used_in_indexes = "&[" <> commaSep (map (cow_str . name) indexes) <> "]"

-- OperatorDebugInfo::Arrange - arrange the prefix of a rule.
--
-- * 'rl' - rule whose prefix is being arranged.
-- * 'prefix_len' - the length of the prefix being arranged.
-- * 'key' - expression that computes the key of the arrangement.
dbgInfoArrange :: (?specname::String, ?module_paths::ModulePathMap) => Rule -> Int -> Doc -> Doc
dbgInfoArrange Rule{..} prefix_len key =
    "::ddlog_profiler::OperatorDebugInfo::arrange(" <> cow key <> "," <> source_pos <> ")"
    where
    start = fst $ pos $ head ruleRHS
    end = snd $ pos $ ruleRHS !! (prefix_len - 1)
    source_pos = mkSourcePos (start, end)

-- OperatorDebugInfo::Flatmap - Flatmap operator.
--
-- * 'rl' - rule.
-- * 'rhs_idx' - index of the Flatmap operator in the RHS of the rule.
dbgInfoFlatmap :: (?specname::String, ?module_paths::ModulePathMap) => Rule -> Int -> Doc
dbgInfoFlatmap Rule{..} rhs_idx =
    "::ddlog_profiler::OperatorDebugInfo::flatmap(" <> source_pos <> ")"
    where
    source_pos = mkSourcePos $ pos $ ruleRHS !! rhs_idx

-- OperatorDebugInfo::FilterMap - FilterMap operator implements one or more
-- filter and variable assignment clauses.
--
-- * 'rl' - rule.
-- * 'fst_idx' - index of the first RHS clause implemented by this operator.
-- * 'last_idx' - index of the last RHS clause implemented by this operator.
dbgInfoFilterMap :: (?specname::String, ?module_paths::ModulePathMap) => Rule -> Int -> Int -> Doc
dbgInfoFilterMap Rule{..} fst_idx last_idx =
    "::ddlog_profiler::OperatorDebugInfo::filter_map(" <> source_pos <> ")"
    where
    source_pos = mkSourcePos $ (fst $ pos $ ruleRHS !! fst_idx, snd $ pos $ ruleRHS !! last_idx)

-- OperatorDebugInfo::Inspect - Inspect clause.
--
-- * 'rl' - rule.
-- * 'idx' - index of the Inspect clause in the RHS of the rule.
dbgInfoInspect :: (?specname::String, ?module_paths::ModulePathMap) => Rule -> Int -> Doc
dbgInfoInspect Rule{..} idx =
    "::ddlog_profiler::OperatorDebugInfo::inspect(" <> source_pos <> ")"
    where
    source_pos = mkSourcePos $ pos $ ruleRHS !! idx

-- OperatorDebugInfo::GroupBy - group_by clause.
--
-- * 'rl' - rule.
-- * 'idx' - index of the group_by clause in the RHS of the rule.
dbgInfoGroupBy :: (?specname::String, ?module_paths::ModulePathMap) => Rule -> Int -> Doc
dbgInfoGroupBy Rule{..} idx =
    "::ddlog_profiler::OperatorDebugInfo::group_by(" <> source_pos <> ")"
    where
    source_pos = mkSourcePos $ pos $ ruleRHS !! idx

-- OperatorDebugInfo::StreamArrSemijoin - Semijoin a prefix of a rule that
-- produces a stream with an arranged relation.
--
-- * 'rl' - rule.
-- * 'idx' - index of the literal to semijoin with.
-- * 'key' - key expression to arrange the input stream.
-- * 'rel' - relation to semijoin with.
-- * 'arr' - pattern to arrange 'rel'.
dbgInfoStreamArrSemijoin :: (?specname::String, ?module_paths::ModulePathMap) => Rule -> Int -> Doc -> String -> Doc -> Doc
dbgInfoStreamArrSemijoin Rule{..} idx key rel arr =
    "::ddlog_profiler::OperatorDebugInfo::stream_arr_semijoin(" <> commaSep [cow key, cow_str rel, cow arr, source_pos] <> ")"
    where
    source_pos = mkSourcePos $ pos $ ruleRHS !! idx

-- OperatorDebugInfo::StreamArrJoin - Join a prefix of a rule that
-- produces a stream with an arranged relation.
--
-- * 'rl' - rule.
-- * 'idx' - index of the literal to join with.
-- * 'key' - key expression to arrange the input stream.
-- * 'rel' - relation to join with.
-- * 'arr' - pattern to arrange 'rel'.
dbgInfoStreamArrJoin :: (?specname::String, ?module_paths::ModulePathMap) => Rule -> Int -> Doc -> String -> Doc -> Doc
dbgInfoStreamArrJoin Rule{..} idx key rel arr =
    "::ddlog_profiler::OperatorDebugInfo::stream_arr_join(" <> commaSep [cow key, cow_str rel, cow arr, source_pos] <> ")"
    where
    source_pos = mkSourcePos $ pos $ ruleRHS !! idx

-- OperatorDebugInfo::ArrStreamJoin - Join arranged prefix of a rule with
-- a stream.
--
-- * 'rl' - rule.
-- * 'idx' - index of the literal to join with.
-- * 'stream' - stream relation to join with.
-- * 'key' - key expression to arrange 'stream'.
dbgInfoArrStreamJoin :: (?specname::String, ?module_paths::ModulePathMap) => Rule -> Int -> String -> Doc -> Doc
dbgInfoArrStreamJoin Rule{..} idx stream key =
    "::ddlog_profiler::OperatorDebugInfo::arr_stream_join(" <> commaSep [cow_str stream, cow key, source_pos] <> ")"
    where
    source_pos = mkSourcePos $ pos $ ruleRHS !! idx

-- OperatorDebugInfo::ArrStreamSemijoin - Semijoin arranged prefix of a rule with
-- a stream.
--
-- * 'rl' - rule.
-- * 'idx' - index of the literal to join with.
-- * 'stream' - stream relation to join with.
-- * 'key' - key expression to arrange 'stream'.
dbgInfoArrStreamSemijoin :: (?specname::String, ?module_paths::ModulePathMap) => Rule -> Int -> String -> Doc -> Doc
dbgInfoArrStreamSemijoin Rule{..} idx stream key =
    "::ddlog_profiler::OperatorDebugInfo::arr_stream_semijoin(" <> commaSep [cow_str stream, cow key, source_pos] <> ")"
    where
    source_pos = mkSourcePos $ pos $ ruleRHS !! idx

-- OperatorDebugInfo::Join - Join arranged prefix of a rule with
-- an arranged relation.
--
-- * 'rl' - rule.
-- * 'idx' - index of the literal to join with.
-- * 'rel' - relation to join with.
-- * 'arr' - pattern to arrange 'rel'.
dbgInfoJoin :: (?specname::String, ?module_paths::ModulePathMap) => Rule -> Int -> String -> Doc -> Doc
dbgInfoJoin Rule{..} idx rel arr =
    "::ddlog_profiler::OperatorDebugInfo::join(" <> commaSep [cow_str rel, cow arr, source_pos] <> ")"
    where
    source_pos = mkSourcePos $ pos $ ruleRHS !! idx

-- OperatorDebugInfo::Semijoin - Semijoin arranged prefix of a rule with
-- an arranged relation.
--
-- * 'rl' - rule.
-- * 'idx' - index of the literal to semijoin with.
-- * 'rel' - relation to semijoin with.
-- * 'arr' - pattern to arrange 'rel'.
dbgInfoSemijoin :: (?specname::String, ?module_paths::ModulePathMap) => Rule -> Int -> String -> Doc -> Doc
dbgInfoSemijoin Rule{..} idx rel arr =
    "::ddlog_profiler::OperatorDebugInfo::semijoin(" <> commaSep [cow_str rel, cow arr, source_pos] <> ")"
    where
    source_pos = mkSourcePos $ pos $ ruleRHS !! idx

-- OperatorDebugInfo::Antijoin - Antijoin arranged prefix of a rule with
-- an arranged relation.
--
-- * 'rl' - rule.
-- * 'idx' - index of the literal to antijoin with.
-- * 'rel' - relation to antijoin with.
-- * 'arr' - pattern to arrange 'rel'.
dbgInfoAntijoin :: (?specname::String, ?module_paths::ModulePathMap) => Rule -> Int -> String -> Doc -> Doc
dbgInfoAntijoin Rule{..} idx rel arr =
    "::ddlog_profiler::OperatorDebugInfo::antijoin(" <> commaSep [cow_str rel, cow arr, source_pos] <> ")"
    where
    source_pos = mkSourcePos $ pos $ ruleRHS !! idx

-- OperatorDebugInfo::Differentiate.
--
-- * 'rl' - rule.
-- * 'idx' - index of the literal where differentiation occurs.
-- * 'rel' - relation to differentiate.
dbgInfoDifferentiate :: (?specname::String, ?module_paths::ModulePathMap) => Rule -> Int -> String -> Doc
dbgInfoDifferentiate Rule{..} idx rel =
    "::ddlog_profiler::OperatorDebugInfo::differentiate(" <> commaSep [cow_str rel, source_pos] <> ")"
    where
    source_pos = mkSourcePos $ pos $ ruleRHS !! idx

-- OperatorDebugInfo::Head - Evaluate the head of a rule.
--
-- * 'rl' - rule.
dbgInfoHead :: (?specname::String, ?module_paths::ModulePathMap) => Rule -> Doc
dbgInfoHead Rule{..} =
    "::ddlog_profiler::OperatorDebugInfo::head(" <> source_pos <> ")"
    where
    source_pos = mkSourcePos $ pos $ head ruleLHS


-- OperatorDebugInfo::StreamXForm - Evaluate an RHS literal as a streaming xform.
--
-- * 'rl' - rule.
-- * 'idx' - RHS literal to be evaluated as a streaming xform.
dbgInfoStreamXForm :: (?specname::String, ?module_paths::ModulePathMap) => Rule -> Int -> Doc
dbgInfoStreamXForm Rule{..} idx =
    "::ddlog_profiler::OperatorDebugInfo::stream_xform(" <> source_pos <> ")"
    where
    source_pos = mkSourcePos $ pos $ ruleRHS !! idx
