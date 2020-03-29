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

{-# LANGUAGE OverloadedStrings #-}

module Language.DifferentialDatalog.Ops where

import Text.PrettyPrint

import Language.DifferentialDatalog.PP

data BOp = Eq
         | Neq
         | Lt
         | Gt
         | Lte
         | Gte
         | And
         | Or
         | Impl
         | Plus
         | Minus
         | Mod
         | Times
         | Div
         | ShiftR
         | ShiftL
         | BAnd
         | BOr
         | BXor
         | Concat
         deriving (Eq, Ord)

instance PP BOp where
    pp Eq     = "=="
    pp Neq    = "!="
    pp Lt     = "<"
    pp Gt     = ">"
    pp Lte    = "<="
    pp Gte    = ">="
    pp And    = "and"
    pp Or     = "or"
    pp Impl   = "=>"
    pp Plus   = "+"
    pp Minus  = "-"
    pp Times  = "*"
    pp Div    = "/"
    pp Mod    = "%"
    pp ShiftR = ">>"
    pp ShiftL = "<<"
    pp BAnd   = "&"
    pp BOr    = "|"
    pp BXor   = "^"
    pp Concat = "++"

bopReturnsBool :: BOp -> Bool
bopReturnsBool op = elem op [Eq, Neq, Lt, Gt, Lte, Gte, And, Or, Impl]

bopIsComparison :: BOp -> Bool
bopIsComparison op = elem op [Eq, Neq, Lt, Gt, Lte, Gte]

instance Show BOp where
    show = render . pp

data UOp = Not
         | BNeg
         | UMinus
         deriving (Eq, Ord)

instance PP UOp where
    pp Not    = "not"
    pp BNeg   = "~"
    pp UMinus = "-"

instance Show UOp where
    show = render . pp
