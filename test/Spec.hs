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

import Test.Tasty
import Test.Tasty.Golden
import System.FilePath
import Test.HUnit
import Data.List
import Data.List.Split
import Control.Exception

import Language.DifferentialDatalog.Parse
import Language.DifferentialDatalog.Syntax
import Language.DifferentialDatalog.Validate
import Language.DifferentialDatalog.Preamble

main :: IO ()
main = defaultMain =<< goldenTests

goldenTests :: IO TestTree
goldenTests = do
  dlFiles <- findByExtension [".dl"] "./test/datalog_tests"
  return $ testGroup "datalog parser tests"
    [ goldenVsFile (takeBaseName dlFile) expect output (testParser dlFile output)
    | dlFile <- dlFiles 
    , let expect = replaceExtension dlFile ".ast.expected"
    , let output = replaceExtension dlFile ".ast"]

parseValidate :: FilePath -> String -> IO DatalogProgram
parseValidate file program = do 
    d <- parseDatalogString True program file
    case validate d of 
         Left e   -> errorWithoutStackTrace $ "error: " ++ e
         Right d' -> return d'

-- compile a program that is supposed to fail compilation
compileFailingProgram :: String -> String -> IO String
compileFailingProgram file program =
    (show <$> parseValidate file program) `catch`
             (\e -> return $ show (e::SomeException))

-- test Datalog parser and pretty printer.
-- as a side-effect it must create the file with the name specified
testParser :: FilePath -> FilePath -> IO ()
testParser fname ofname = do
    -- if a file contains .fail. in it's name it indicates a test
    -- that is supposed to fail during compilation.
    body <- readFile fname
    let shouldFail = ".fail." `isInfixOf` fname
    if shouldFail
      then do
        -- To allow multiple negative tests in a single dl file
        -- we treat the file as multiple files separated by this comment
        let parts = splitOn "//---" body
        -- if the file should fail we expect an exception.
        -- the exception message is the expected output
        out <- mapM (compileFailingProgram fname) parts
        writeFile ofname $ (intercalate "\n\n" out) ++ "\n"
      else do
        -- parse Datalog file and output its AST
        prog <- parseValidate fname body
        writeFile ofname (show prog ++ "\n")
        -- parse reference output
        prog' <- parseDatalogFile False ofname
        -- expect the same result
        assertEqual "Pretty-printed Datalog differs from original input" prog prog'
    return ()
