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


{- |
Module     : Statement
Description: FLWR-style statement syntax implemented as syntactic sugar over core DDlog
-}

{-# LANGUAGE OverloadedStrings #-}

module Language.DifferentialDatalog.Statement (
    Assignment(..),
    Statement(..),
    convertStatement
) where

import Prelude hiding((<>))
import Data.Maybe
import Text.PrettyPrint

import Language.DifferentialDatalog.Pos
import Language.DifferentialDatalog.PP
import Language.DifferentialDatalog.Syntax

data Assignment = Assignment { assignPos :: Pos
                             , leftAssign :: String
                             , typeAssign :: Maybe Type
                             , rightAssign :: Expr }

instance Eq Assignment where
    (==) (Assignment _ l1 t1 r1) (Assignment _ l2 t2 r2) =
      l1 == l2 && t1 == t2 && r1 == r2

instance WithPos Assignment where
    pos = assignPos
    atPos r p = r{assignPos = p}

instance PP Assignment where
    pp (Assignment _ l t r) = pp l <> (maybe empty (\t' -> ":" <+> pp t') t) <+> "=" <+> pp r

instance Show Assignment where
    show = render . pp

data Statement = ForStatement    { statPos :: Pos
                                 , forAtom :: Atom
                                 , forCondition :: Maybe Expr
                                 , forStatement :: Statement }
               | IfStatement     { statPos :: Pos
                                 , ifCondition :: Expr
                                 , ifStatement :: Statement
                                 , elseStatement :: Maybe Statement}
               | MatchStatement  { statPos :: Pos
                                 , matchExpr :: Expr
                                 , cases :: [(Expr, Statement)] }
               | AssignStatement { statPos :: Pos
                                 , assignExpr :: Expr
                                 , varStatement :: Statement }
               | InsertStatement { statPos :: Pos
                                 , insertAtom :: Atom }
               | BlockStatement  { statPos :: Pos
                                 , seqList :: [Statement] }
               | EmptyStatement  { statPos :: Pos }

instance Eq Statement where
    (==) (ForStatement _ a1 c1 s1) (ForStatement _ a2 c2 s2) =
          a1 == a2 && c1 == c2 && s1 == s2
    (==) (IfStatement _ c1 s1 e1) (IfStatement _ c2 s2 e2) =
          c1 == c2 && s1 == s2 && e1 == e2
    (==) (MatchStatement _ e1 l1) (MatchStatement _ e2 l2) =
          e1 == e2 && l1 == l1
    (==) (AssignStatement _ e1 s1) (AssignStatement _ e2 s2) =
          e1 == e2 && s1 == s2
    (==) (InsertStatement _ a1) (InsertStatement _ a2) =
          a1 == a2
    (==) (BlockStatement _ l1) (BlockStatement _ l2) =
          l1 == l2
    (==) (EmptyStatement _) (EmptyStatement _) = True

instance PP Statement where
    pp (ForStatement _ a c s) = "for" <+> "(" <> pp a <+>
                                maybe empty (("if" <+>) . pp) c <> ")" $$ (nest' . pp) s
    pp (IfStatement _ c s e) = "if" <+> "(" <> (pp c) <> ")" $$ (nest' . pp) s $$
                                     maybe empty (("else" $$) . (nest' . pp)) e
    pp (MatchStatement _ e l) = "match" <+> "(" <> pp e <> ")" $$ "{" $+$
                                (nest' $ vcat $ (punctuate "," $ map (\(e,s) -> pp e <+> "->" <+> pp s) l))
                                $$ "}"
    pp (AssignStatement _ e s) = pp e <+> "in" $$ ((nest' . pp) s)
    pp (InsertStatement _ a) =  pp a
    pp (BlockStatement _ l) =  "{" $+$
                                (nest' $ vcat $ (punctuate ";" $ map pp l))
                                $$ "}"
    pp (EmptyStatement _) = "skip"

instance Show Statement where
    show = render . pp

instance WithPos Statement where
    pos = statPos
    atPos s p = s{statPos = p}


-- given a list of match labels generates a matrix
-- [a,b,c] -> [[(a,true), (b,false), (c,false)],
--             [(a,false), (b,true), (c,false)],
--             [(a,false), (b,false), (c,true)]]
explodeMatchCases :: [Expr] -> [[(Expr, Expr)]]
explodeMatchCases l =
    let pairs = [ [(i1, i2) | i1 <- [0..(length l - 1)]] | i2 <- [0..(length l - 1)]] in
    map (map (\(a,b) -> (l !! a, if a == b then eTrue else eFalse))) pairs

-- add the specified RHS to all the rules
addRhsToRules:: RuleRHS -> [Rule] -> [Rule]
addRhsToRules toAdd rules =
     map (\r -> r{ruleRHS=(toAdd : ruleRHS r)}) rules

-- | convert statement into rules
convertStatement :: Statement -> [Rule]
convertStatement (ForStatement p atom mc s) =
    let rules = convertStatement s
        rhs0 = RHSLiteral True atom
        rhs1 = map RHSCondition $ maybeToList mc in
    map (\r -> r{ruleRHS=(rhs0 : rhs1 ++ ruleRHS r)}) rules
convertStatement (IfStatement p c s Nothing) =
    let rules = convertStatement s in
    addRhsToRules (RHSCondition c) rules
convertStatement (IfStatement p c s (Just e)) =
    let rules0 = convertStatement s
        rules1 = convertStatement e
        rules0' = addRhsToRules (RHSCondition c) rules0
        rules1' = addRhsToRules (RHSCondition $ eNot c) rules1 in
    rules0' ++ rules1'
convertStatement (MatchStatement p e c) =
    let rulesList = map (convertStatement . snd) c
        matchList = explodeMatchCases $ map fst c
        matchExpressions = map (\l -> RHSCondition $ eMatch e l) matchList
    in concat $ zipWith addRhsToRules matchExpressions rulesList
convertStatement (AssignStatement p e s) =
    let rules = convertStatement s in
    map (\r -> r{ruleRHS = (RHSCondition e) : (ruleRHS r)}) rules
convertStatement (BlockStatement p l) =
    let rulesList = map convertStatement l in
    concat rulesList
convertStatement (EmptyStatement p) =
    []
convertStatement (InsertStatement p a) =
    [Rule p [a] []]
