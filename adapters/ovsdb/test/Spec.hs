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

{-# LANGUAGE FlexibleContexts #-}

import Test.Tasty
import Test.Tasty.Golden
import Test.Tasty.Golden.Advanced
import System.IO
import System.FilePath
import Test.HUnit
import Data.List
import Data.Maybe
import Control.Exception
import Control.Monad
import GHC.IO.Exception
import Text.Printf
import Text.PrettyPrint

import Compile

main :: IO ()
main = defaultMain =<< goldenTests

bUILD_TYPE = "release"

cargo_build_flag = if bUILD_TYPE == "release" then ["--release"] else []

goldenTests :: IO TestTree
goldenTests = do
  return $ testGroup "ovsdb tests" $
        [goldenVsFile "ovn" "test/schema/ovn.dl.expected" "test/schema/ovn.dl" ovnTest]

ovnTest = do
    prog <- compileSchemaFiles ["test/schema/ovn-nb.ovsschema", "test/schema/ovn-sb.ovsschema"] 
                               ["OVN_Southbound_Logical_Flow", "OVN_Southbound_Address_Set"]
    writeFile "test/schema/ovn.dl" (render prog)
