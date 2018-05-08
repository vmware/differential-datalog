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

{-# LANGUAGE QuasiQuotes, OverloadedStrings #-}

{- | 
Module     : Preamble
Description: Preamble automatically inserted in all Datalog programs
-}
module Language.DifferentialDatalog.Preamble(
    bUILTIN_2STRING_FUNC,
    tOSTRING_FUNC_SUFFIX,
    datalogPreamble)
where

import Text.RawString.QQ

bUILTIN_2STRING_FUNC :: String
bUILTIN_2STRING_FUNC = "__builtin_2string"

tOSTRING_FUNC_SUFFIX :: String
tOSTRING_FUNC_SUFFIX = "2string"

datalogPreamble :: String
datalogPreamble = [r|/** BEGIN PREAMBLE **/

typedef set<'A>

function |] ++ bUILTIN_2STRING_FUNC ++ [r|(x: 'X): string

/** END PREAMBLE **/
|]
