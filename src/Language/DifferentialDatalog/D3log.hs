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

import Language.DifferentialDatalog.Pos
import Language.DifferentialDatalog.Config
import Language.DifferentialDatalog.Module
import Language.DifferentialDatalog.Name
import Language.DifferentialDatalog.Syntax
import Language.DifferentialDatalog.Type
import Language.DifferentialDatalog.NS

tStringInternFunc :: Type
tStringInternFunc = tFunction [ArgType nopos False tString] (tIntern tString)

eStringInternFunc :: Expr
eStringInternFunc = eTyped (eFunc "internment::intern") tStringInternFunc

tToAnyFunc :: Type -> Type
tToAnyFunc t = tFunction [ArgType nopos False t] $ tOpaque (mOD_STD ++ "::Any") []

eToAnyFunc :: Type -> Expr
eToAnyFunc t = eTyped (eFunc $ mOD_STD ++ "::to_any") $ tToAnyFunc t

sTREAM_FACTS :: String
sTREAM_FACTS = "d3log::reflect::DistributedStreamFacts"

rEL_FACTS :: String
rEL_FACTS = "d3log::reflect::DistributedRelFacts"

-- Converts a D3log program with location annotations into a regular DDlog
-- program.  In the new program distributed relations (i.e., relations whose
-- records can be produced and consumed by different nodes) are augmented
-- with location information, so that the D3log runtime can determine the
-- destination of each record.
--
-- The exact behavior depends on whether the program is compiled with '--d3log'
-- or '--d3log-dev' flag.  In the former case, for each relation 'R[T]' that
-- appears in the head of at least one rule with location annotation, we
-- introduce a fresh output relation '__out_R[(Option<D3logLocationId>, T)]'
-- and replace all occurrences of 'R' in the head of a rule with '__out_R'.
--
-- In the latter case, we replace all occurrences of 'R' in the head of a rule
-- with either 'd3log::reflect::DistributedStreamFacts' if 'R' is a stream or
-- 'd3log::reflect::DistributedRelFacts' if it is a relation or multiset.
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
                         -- Remote inputs must be multisets as they can
                         -- receive updates from more than one upstream
                         -- components.
                         rel_in_sem = if relSemantics rel == RelSet
                                      then RelMultiset
                                      else relSemantics rel
                         rel_in = rel{
                            relRole = RelInput,
                            relSemantics = rel_in_sem
                         }
                         rel_out = rel{
                             relName = outRelName $ relName rel,
                             -- When implementing D3log in DDlog, we feed these
                             -- relations to 'DistributedRelFacts' and
                             -- 'DistributedStreamFacts'.
                             relRole = if confD3logDev ?cfg then RelInternal else RelOutput,
                             relType = tTuple [locationSpecifierType, relType rel]
                         }
                         rels_ = M.insert (name rel_in) rel_in $
                                 M.delete rname rels
                     in if confD3logDev ?cfg
                        then rels_
                        else M.insert (name rel_out) rel_out rels_)
                   (progRelations d) loc_rels

    -- Rewrite rules:
    -- If 'confD3logDev' is False:
    --  'R[x] :- Body.' --> '__out_R[(None, x)] :- Body.'
    --  and
    --  'R[x] @n :- Body.' --> '__out_R[(Some{n}, x)] :- Body.'
    --
    -- If 'confD3logDev' is True:
    --  'R[x] :- Body.' --> 'DistributedRelFacts("R", x, None) :- Body.'
    --  and
    --  'R[x] @n :- Body.' --> 'DistributedRelFacts("R", x, Some{n}) :- Body.'
    rules' = map (\rule ->
                   let rlhead = head $ ruleLHS rule
                       rname = atomRelation $ lhsAtom rlhead
                       rel = getRelation d rname
                       loc_expr = case lhsLocation rlhead of
                                       Nothing -> eNone $ tUser lOCATION_TYPE []
                                       Just l  -> eSome l $ tUser lOCATION_TYPE []
                       head_rel = if confD3logDev ?cfg
                                  then reflectRelName rel
                                  else outRelName rname
                       val' = if confD3logDev ?cfg
                              then eStruct head_rel [
                                        (identifierWithPos "relname", eApply eStringInternFunc [eString rname]),
                                        (identifierWithPos "fact", eApply (eToAnyFunc $ relType rel) [atomVal $ lhsAtom rlhead]),
                                        (identifierWithPos "destination", loc_expr) ]
                                           (tUser head_rel [])
                              else eTuple [loc_expr, atomVal $ lhsAtom rlhead, loc_expr]
                       atom' = (lhsAtom rlhead) {
                           atomRelation = head_rel,
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

reflectRelName :: Relation -> String
reflectRelName rel = if relSemantics rel == RelStream
                     then sTREAM_FACTS
                     else rEL_FACTS

-- A localized relation must appear with location annotation
-- in the head of at least one rule.
progLocalizedRelations :: DatalogProgram -> S.Set String
progLocalizedRelations d =
    S.fromList
    $ map (atomRelation . lhsAtom . head . ruleLHS)
    $ filter (isJust . lhsLocation . head . ruleLHS)
    $ progRules d
