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

{-# LANGUAGE FlexibleContexts, TupleSections #-}

module Language.DifferentialDatalog.Error where

import Control.Monad.Except
import Data.List
import Language.DifferentialDatalog.Pos
import Language.DifferentialDatalog.Name
import Language.DifferentialDatalog.Syntax

errBrief :: (MonadError String me) => Pos -> String -> me a
errBrief p e = throwError $ spos p ++ ": " ++ e

err :: (MonadError String me) => Pos -> DatalogProgram -> String -> me a
err p d e = throwError $ sposFragment p (progSources d) e

check :: (MonadError String me) => Bool -> Pos -> String -> me ()
check b p m =
    if b
       then return ()
       else errBrief p m

-- Check for duplicate declarations
uniq :: (MonadError String me, WithPos a, Ord b) => (a -> b) -> (a -> String) -> [a] -> me ()
uniq = uniq' pos

uniq' :: (MonadError String me, Ord b) => (a -> Pos) -> (a -> b) -> (a -> String) -> [a] -> me ()
uniq' fpos ford msgfunc xs = do
    case filter ((>1) . length) $ groupBy (\x1 x2 -> compare (ford x1) (ford x2) == EQ)
                                $ sortBy (\x1 x2 -> compare (ford x1) (ford x2)) xs of
         g@(x:_):_ -> errBrief (fpos x) $ msgfunc x ++ " at the following locations:\n  " ++ (intercalate "\n  " $ map (spos . fpos) g)
         _         -> return ()

uniqNames :: (MonadError String me, WithPos a, WithName a) => (String -> String) -> [a] -> me ()
uniqNames msgfunc = uniq name (\x -> msgfunc (name x))
