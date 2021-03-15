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

{- |
Module     : D3log
Description: Process D3log annotations.
-}

{-# LANGUAGE ImplicitParams #-}

module Language.DifferentialDatalog.D3log (
    processD3logAnnotations
) where

import Data.List
import Data.Maybe
import qualified Data.Map as M
import qualified Data.Set as S

import Language.DifferentialDatalog.Config
import Language.DifferentialDatalog.Module
import Language.DifferentialDatalog.Name
import Language.DifferentialDatalog.Syntax
import Language.DifferentialDatalog.Type

-- Converts a D3log program with location annotations into a regular DDlog
-- program.  In the new program distributed relations (i.e., relations whose
-- records can be produced and consumed by different nodes) are augmented
-- with location information, so that the D3log runtime can determine the
-- destination of each record.
--
-- For each relation 'R[T]' that appears in the head of at least one rule
-- with location annotation, we introduce a fresh output relation
-- '__out_R[(Option<D3logLocationId>, T)]' and replaces all occurrences
-- 'R' in the head of a rule with '__out_R'.
--
-- Returns modified program along with a map from newly introduced output
-- relation names to corresponding input relations: '__out_R -> R'
processD3logAnnotations :: (?cfg::Config) => DatalogProgram -> (DatalogProgram, M.Map String String)
processD3logAnnotations d | confD3log ?cfg == False = (d, M.empty)
                          | otherwise = 
    (d { progRelations = rels', progRules = rules' }, M.fromList $ map (\n -> (outRelName n, n)) $ S.toList loc_rels)
    where
    -- Find annotated relations.
    loc_rels = progLocalizedRelations d
    
    -- For each localized relation 'R[T]', convert 'R'
    -- into an input relation and generate a new output relation
    -- 'output relation __out_R[(Option<D3logLocationId>, T)]'.
    rels' = foldl' (\rels rname ->
                     let rel = progRelations d M.! rname
                         rel_in = rel{relRole = RelInput}
                         rel_out = rel{
                             relName = outRelName $ relName rel,
                             relRole = RelOutput,
                             relType = tTuple [locationSpecifierType, relType rel]
                         }
                     in M.insert (name rel_in) rel_in $
                        M.insert (name rel_out) rel_out $
                        M.delete rname rels)
                   (progRelations d) loc_rels

    -- Rewrite rules:
    --  'R[x] :- Body.' --> '__out_R[(None, x)] :- Body.'
    --  and
    --  'R[x] @n :- Body.' --> '__out_R[(Some{n}, x)] :- Body.'
    rules' = map (\rule ->
                   let rlhead = head $ ruleLHS rule
                       rname = atomRelation $ lhsAtom rlhead
                       loc_expr = case lhsLocation rlhead of
                                       Nothing -> eNone $ tUser lOCATION_TYPE []
                                       Just l  -> eSome l $ tUser lOCATION_TYPE []
                       val' = eTuple [loc_expr, atomVal $ lhsAtom rlhead]
                       atom' = (lhsAtom rlhead) {
                           atomRelation = outRelName rname,
                           atomVal = val'
                       }
                       rlhead' = rlhead {
                           lhsAtom     = atom',
                           lhsLocation = Nothing
                       } in
                   if S.member rname loc_rels
                   then rule{ ruleLHS = [rlhead'] }
                   else rule)
             $ progRules d

-- 'Option<D3logLocationId>'
locationSpecifierType :: Type
locationSpecifierType = tUser oPTION_TYPE [tUser lOCATION_TYPE []]

outRelName :: String -> String
outRelName rel = scoped rel_scope $ "__out_" ++ rel_name
    where
    rel_scope = nameScope rel
    rel_name = nameLocalStr rel

-- A localized relation must appear with location annotation
-- in the head of at least one rule.
progLocalizedRelations :: DatalogProgram -> S.Set String
progLocalizedRelations d =
    S.fromList
    $ map (atomRelation . lhsAtom . head . ruleLHS)
    $ filter (isJust . lhsLocation . head . ruleLHS)
    $ progRules d
