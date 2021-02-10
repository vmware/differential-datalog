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

{-# LANGUAGE RecordWildCards, FlexibleContexts, LambdaCase, TupleSections, OverloadedStrings, TemplateHaskell, QuasiQuotes, ImplicitParams, NamedFieldPuns #-}

{- |
Module     : Rust
Description: Rust language definitions used in the DDlog compiler.
-}

module Language.DifferentialDatalog.Rust where

import Prelude hiding((<>))
import Text.PrettyPrint

import Language.DifferentialDatalog.Syntax

-- Rust expression kind
data EKind = EVal         -- normal value
           | ELVal        -- l-value that can be written to or moved
           | EReference   -- reference (mutable or immutable)
           | ENoReturn    -- expression does not return (e.g., break, continue, or return).
           deriving (Eq, Show)

-- convert any expression into reference
ref :: (Doc, EKind, ENode) -> Doc
ref (x, EReference, _)  = x
ref (x, ENoReturn, _)   = x
ref (x, _, _)           = parens $ "&" <> x

-- dereference expression if it is a reference; leave it alone
-- otherwise
deref :: (Doc, EKind, ENode) -> Doc
deref (x, EReference, _) = parens $ "*" <> x
deref (x, _, _)          = x

-- convert any expression into mutable reference
mutref :: (Doc, EKind, ENode) -> Doc
mutref (x, EReference, _)  = x
mutref (x, ENoReturn, _)   = x
mutref (x, _, _)           = parens $ "&mut" <+> x

-- convert any expression to EVal by cloning it if necessary
val :: (Doc, EKind, ENode) -> Doc
val (x, EVal, _)        = x
val (x, ENoReturn, _)   = x
val (x, EReference, _)  = cloneRef x
val (x, _, _)           = x <> ".clone()"

-- convert expression to l-value
lval :: (Doc, EKind, ENode) -> Doc
lval (x, ELVal, _)      = x
-- this can only be mutable reference in a valid program
lval (x, EReference, _) = parens $ "*" <> x
lval (x, EVal, _)       = error $ "Compile.lval: cannot convert value to l-value: " ++ show x
lval (x, ENoReturn, _)  = error $ "Compile.lval: cannot convert expression to l-value: " ++ show x

-- When calling `clone()` on a reference, Rust occasionally decides to clone
-- the reference instead of the referenced object.  This function makes sure to
-- dereference it first.
cloneRef :: Doc -> Doc
cloneRef r = "(*" <> r <> ").clone()"
