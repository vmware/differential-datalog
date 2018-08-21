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
import System.Directory
import System.Process
import Test.HUnit
import Data.List
import Data.List.Split
import Control.Exception
import Control.DeepSeq
import Control.Monad
import GHC.IO.Exception
import Text.Printf
import qualified Data.ByteString as BS

import Language.DifferentialDatalog.Parse
import Language.DifferentialDatalog.Syntax
import Language.DifferentialDatalog.Validate
import Language.DifferentialDatalog.Preamble
import Language.DifferentialDatalog.Compile

main :: IO ()
main = defaultMain =<< goldenTests

bUILD_TYPE = "release"

cargo_build_flag = if bUILD_TYPE == "release" then ["--release"] else []

goldenTests :: IO TestTree
goldenTests = do
  -- locate datalog files
  dlFiles <- findByExtension [".dl"] "./test/datalog_tests"
  -- some of the tests may have accompanying .dat files
  inFiles <- mapM (\dlFile -> do let datFile = replaceExtension dlFile "dat"
                                 exists <- doesFileExist datFile
                                 return $ if exists then [dlFile, datFile, datFile] else [dlFile]) dlFiles
  return $ testGroup "datalog tests"
    [ goldenVsFiles (takeBaseName $ head files) expect output (testOne $ head files)
    | files <- inFiles
    , let expect = map (uncurry replaceExtension) $ zip files [".ast.expected", ".dump.expected", ".dump.expected"]
    , let output = map (uncurry replaceExtension) $ zip files [".ast", ".dump", ".c.dump"]]

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

-- Run Datalog compiler on spec in 'fname'.
--
-- * Writes parsed AST to specname.ast
--
-- * Creates Cargo project in a directory obtained by removing file
-- extension from 'fname'.
--
-- * Checks if a file with the same name as 'fname' and '.rs' extension
-- (instead of '.dl') exists and passes its content as the 'imports' argument to
-- compiler.
--
-- * If a .dat file exists for the given test, dump its content to the
-- compiled datalog program, producing .dump and .err files
testOne :: FilePath -> IO ()
testOne fname = do
    -- if a file contains .fail. in its name it indicates a test
    -- that is supposed to fail during compilation.
    body <- readFile fname
    let specname = takeBaseName fname
    let astfile  = replaceExtension fname "ast"
    let shouldFail = ".fail." `isInfixOf` fname
    if shouldFail
      then do
        -- To allow multiple negative tests in a single dl file
        -- we treat the file as multiple files separated by this comment
        let parts = splitOn "//---" body
        -- if the file should fail we expect an exception.
        -- the exception message is the expected output
        out <- mapM (compileFailingProgram fname) parts
        writeFile astfile $ (intercalate "\n\n" out) ++ "\n"
      else do
        -- parse Datalog file and output its AST
        prog <- parseValidate fname body
        writeFile astfile (show prog ++ "\n")
        -- parse reference output
        prog' <- parseDatalogFile False astfile
        -- expect the same result
        assertEqual "Pretty-printed Datalog differs from original input" prog prog'
        -- include any user-provided Rust code
        let importsfile = addExtension (dropExtension fname) "rs"
        hasimports <- doesFileExist importsfile
        imports <- if hasimports
                      then readFile importsfile
                      else return ""
        -- generate Rust project
        let rust_dir = joinPath [takeDirectory fname]
        compile prog specname imports rust_dir
        -- compile it with Cargo
        let cargo_proc = (proc "cargo" (["build"] ++ cargo_build_flag)) {
                              cwd = Just $ joinPath [rust_dir, specname]
                         }
        (code, stdo, stde) <- readCreateProcessWithExitCode cargo_proc ""
        when (code /= ExitSuccess) $ do
            errorWithoutStackTrace $ "cargo build failed with exit code " ++ show code ++
                                     "\nstderr:\n" ++ stde ++
                                     "\n\nstdout:\n" ++ stdo
        let cargo_proc = (proc "cargo" (["test"] ++ cargo_build_flag)) {
                              cwd = Just $ joinPath [rust_dir, specname]
                         }
        (code, stdo, stde) <- readCreateProcessWithExitCode cargo_proc ""
        when (code /= ExitSuccess) $ do
            errorWithoutStackTrace $ "cargo test failed with exit code " ++ show code ++
                                     "\nstderr:\n" ++ stde ++
                                     "\n\nstdout:\n" ++ stdo
        cliTest fname specname rust_dir
        ffiTest fname specname rust_dir
    return ()

-- Feed test data via pipe if a .dat file exists
cliTest :: FilePath -> String -> FilePath -> IO ()
cliTest fname specname rust_dir = do
    let dumpfile = replaceExtension fname "dump"
    let errfile  = replaceExtension fname "err"
    let datfile  = replaceExtension fname "dat"
    hasdata <- doesFileExist datfile
    when hasdata $ do
        hout <- openFile dumpfile WriteMode
        herr <- openFile errfile  WriteMode
        hdat <- openFile datfile ReadMode
        code <- withCreateProcess (proc "cargo" (["run", "--bin", specname ++ "_cli"] ++ cargo_build_flag)){
                                       cwd = Just $ joinPath [rust_dir, specname],
                                       std_in=CreatePipe,
                                       std_out=UseHandle hout,
                                       std_err=UseHandle herr} $
            \(Just hin) _ _ phandle -> do
                dat <- hGetContents hdat
                hPutStrLn hin dat
                hPutStrLn hin "exit;"
                hFlush hin
                waitForProcess phandle
        when (code /= ExitSuccess) $ do
            errorWithoutStackTrace $ "cargo run ffi_test failed with exit code " ++ show code ++
                                     "\nstderr written to:\n" ++ errfile ++
                                     "\n\nstdout written to:\n" ++ dumpfile
        hClose hout
        hClose herr
        hClose hdat

-- Convert .dat file into C to test the FFI interface
ffiTest :: FilePath -> String -> FilePath -> IO ()
ffiTest fname specname rust_dir = do
    let cfile    = joinPath [rust_dir, specname, addExtension specname ".c"]
    let errfile  = replaceExtension fname "err"
    let datfile  = replaceExtension fname "dat"
    let dumpfile = replaceExtension fname ".c.dump"
    hasdata <- doesFileExist datfile
    when hasdata $ do
        -- Generate C program
        hout <- openFile cfile WriteMode
        herr <- openFile errfile  WriteMode
        hdat <- openFile datfile ReadMode
        code <- withCreateProcess (proc "cargo" (["run", "--bin", specname ++ "_ffi_test"] ++ cargo_build_flag)){
                                       cwd = Just $ joinPath [rust_dir, specname],
                                       std_in=CreatePipe,
                                       std_out=UseHandle hout,
                                       std_err=UseHandle herr} $
            \(Just hin) _ _ phandle -> do
                dat <- hGetContents hdat
                hPutStrLn hin dat
                hPutStrLn hin "exit;"
                hFlush hin
                waitForProcess phandle
        when (code /= ExitSuccess) $ do
            errorWithoutStackTrace $ "cargo run ffi_test failed with exit code " ++ show code ++
                                     "\nstderr written to:\n" ++ errfile ++
                                     "\n\nstdout written to:\n" ++ cfile
        hClose hout
        hClose herr
        hClose hdat
        -- Compile C program
        let exefile = specname ++ "_test"
        hdat <- openFile datfile ReadMode
        code <- withCreateProcess (proc "gcc" [addExtension specname ".c", "-Ltarget/" ++ bUILD_TYPE, "-l" ++ specname, "-o", exefile]){
                                       cwd = Just $ joinPath [rust_dir, specname],
                                       std_in=CreatePipe} $
            \(Just hin) _ _ phandle -> do
                dat <- hGetContents hdat
                hPutStrLn hin dat
                hPutStrLn hin "exit;"
                hFlush hin
                waitForProcess phandle
        when (code /= ExitSuccess) $ do
            errorWithoutStackTrace $ "gcc failed with exit code " ++ show code
        hClose hdat
        -- Run C program
        hout <- openFile dumpfile WriteMode
        code <- withCreateProcess (proc ("./" ++ exefile) []){
                            cwd = Just $ joinPath [rust_dir, specname],
                            std_out = UseHandle hout,
                            env = Just [("LD_LIBRARY_PATH", "target/" ++ bUILD_TYPE)]} $
            \_ _ _ phandle -> waitForProcess phandle
        hClose hout
        when (code /= ExitSuccess) $ do
            errorWithoutStackTrace $ exefile ++ " failed with exit code " ++ show code

-- A version of golden test that supports multiple output files.
-- Uses strict evluation to avoid errors lazily reading and then writing the
-- same file.
goldenVsFiles :: TestName -> [FilePath] -> [FilePath] -> IO () -> TestTree
goldenVsFiles name ref new act =
  goldenTest name
             (do {refs <- mapM BS.readFile ref; evaluate $ rnf refs; return refs})
             (act >> do {news <- mapM BS.readFile new; evaluate $ rnf news; return news})
             cmp upd
  where
  cmp xs ys = return $ msum $
              map (\((x,r),(y,n)) ->
                    if x == y
                       then Nothing
                       else Just $ printf "Files '%s' and '%s' differ" r n)
                  $ zip (zip xs ref) (zip ys new)
  upd bufs = mapM_ (\(r,b) -> do exists <- doesFileExist r
                                 when (not exists) $ BS.writeFile r b)
             $ zip ref bufs
