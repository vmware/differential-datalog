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
import System.Environment
import Test.HUnit
import Data.List
import Data.Maybe
import Data.List.Split
import Control.Exception
import Control.DeepSeq
import Control.Monad
import Control.Concurrent
import GHC.IO.Exception
import Text.Printf
import Text.PrettyPrint
import qualified Data.ByteString as BS
import GHC.Conc.Sync
import qualified Data.Set as S
import qualified Data.Map as M

import Language.DifferentialDatalog.Parse
import Language.DifferentialDatalog.Module
import Language.DifferentialDatalog.Syntax
import Language.DifferentialDatalog.Validate
import Language.DifferentialDatalog.Compile
import qualified Language.DifferentialDatalog.OVSDB.Compile as OVS

main :: IO ()
main = do
    progress <- isJust <$> lookupEnv "DDLOG_TEST_PROGRESS"
    tests <- goldenTests progress
    defaultMain tests

bUILD_TYPE = "debug"

cargo_build_flag = if bUILD_TYPE == "release" then ["--release"] else []

goldenTests :: Bool -> IO TestTree
goldenTests progress = do
  -- locate datalog files
  dlFiles <- findByExtensionNonRec [".dl"] "./test/datalog_tests"
  -- some of the tests may have accompanying .dat files
  inFiles <- mapM (\dlFile -> do let datFile = replaceExtension dlFile "dat"
                                 exists <- doesFileExist datFile
                                 return $ if exists then [dlFile, datFile, datFile] else [dlFile]) dlFiles
  let parser_tests = testGroup "parser tests" $
          [ goldenVsFile (takeBaseName file) expect output (parserTest file)
            | (file:_) <- inFiles
            , let expect = file -<.> "ast.expected"
            , let output = file -<.> "ast"]
  let compiler_tests = testGroup "compiler tests" $ catMaybes $
          [ if shouldFail $ file
               then Nothing
               else Just $ goldenVsFiles (takeBaseName file) expect output (compilerTest progress file [] ["staticlib", "cdylib"])
            | file:files <- inFiles
            , let expect = map (uncurry replaceExtension) $ zip files [".dump.expected"]
            , let output = map (uncurry replaceExtension) $ zip files [".dump"]]
  return $ testGroup "ddlog tests" [parser_tests, compiler_tests, ovnTests progress, souffleTests progress]

nbTest = do
    prog <- OVS.compileSchemaFile "test/ovn/ovn-nb.ovsschema" [] [] M.empty
    writeFile "test/ovn/OVN_Northbound.dl" (render prog)

sbTest = do
    prog <- OVS.compileSchemaFile "test/ovn/ovn-sb.ovsschema"
                                  [ ("SB_Global", [])
                                  , ("Logical_Flow", [])
                                  , ("Multicast_Group", [])
                                  , ("Meter", [])
                                  , ("Meter_Band", [])
                                  , ("Datapath_Binding", [])
                                  , ("Port_Binding", ["chassis"])
                                  , ("Gateway_Chassis", [])
                                  , ("Port_Group", [])
                                  , ("MAC_Binding", [])
                                  , ("DHCP_Options", [])
                                  , ("DHCPv6_Options", [])
                                  , ("Address_Set", [])
                                  , ("DNS", [])
                                  , ("RBAC_Role", [])
                                  , ("RBAC_Permission", [])]
                                  [ "Datapath_Binding", "Port_Binding"]
                                  (M.fromList [ ("Multicast_Group"  , ["datapath", "name", "tunnel_key"])
                                              , ("Port_Binding"     , ["logical_port"])
                                              , ("DNS"              , ["external_ids"])
                                              , ("Datapath_Binding" , ["external_ids"])
                                              , ("RBAC_Role"        , ["name"])
                                              , ("Address_Set"      , ["name"])
                                              , ("Port_Group"       , ["name"])
                                              , ("Meter"            , ["name"]) ])
    writeFile "test/ovn/OVN_Southbound.dl" (render prog)

ovnTests :: Bool -> TestTree
ovnTests progress =
  testGroup "ovn tests" $
        [ goldenVsFiles "ovn_northd"
          ["./test/ovn/OVN_Northbound.dl.expected", "./test/ovn/OVN_Southbound.dl.expected", "./test/ovn/ovn_northd.dump.expected"]
          ["./test/ovn/OVN_Northbound.dl", "./test/ovn/OVN_Southbound.dl", "./test/ovn/ovn_northd.dump"]
          $ do {nbTest; sbTest; parserTest "test/ovn/ovn_northd.dl"; compilerTest progress "test/ovn/ovn_northd.dl" [] ["staticlib"]}]

sOUFFLE_DIR = "./test/souffle"

souffleTests :: Bool -> TestTree
souffleTests progress =
  testGroup "souffle tests" $
        [ goldenVsFile "doop"
          (sOUFFLE_DIR </> "souffle.dl.expected")
          (sOUFFLE_DIR </> "souffle.dl")
          $ do {convertSouffle progress; compilerTest progress (sOUFFLE_DIR </> "souffle.dl") ["--no-print", "-w", "1"] ["cdylib"]}]

convertSouffle :: Bool -> IO ()
convertSouffle progress = do
    dir <- makeAbsolute $ sOUFFLE_DIR
    let inputDl = dir </> "self-contained.dl"
        outputDl = dir </> "souffle.dl"
        outputDat = dir </> "souffle.dat"
        log = dir </> "souffle.log"
        convert_proc = (proc (dir </> "convert.py") [inputDl, outputDl, outputDat, log]) { cwd = Just dir }
    (code, stdo, stde) <- withProgress progress $ readCreateProcessWithExitCode convert_proc ""
    when (code /= ExitSuccess) $ do
        errorWithoutStackTrace $ "convert.py failed with exit code " ++ show code ++
                                 "\nstderr:\n" ++ stde ++
                                 "\n\nstdout:\n" ++ stdo

parseValidate :: FilePath -> String -> IO (DatalogProgram, Doc)
parseValidate file program = do
    (d, rs_code) <- parseDatalogProgram [takeDirectory file, "lib"] True program file
    case validate d of
         Left e   -> errorWithoutStackTrace $ "error: " ++ e
         Right d' -> return (d', rs_code)

-- compile a program that is supposed to fail compilation
compileFailingProgram :: String -> String -> IO String
compileFailingProgram file program =
    (show <$> parseValidate file program) `catch`
             (\e -> return $ show (e::SomeException))

shouldFail fname = ".fail." `isInfixOf` fname

-- Test Datalog parser on spec in 'fname'.
--
-- * Parses the input spec; writes parsed AST to 'specname.ast', parses the generated AST file and
-- checks that both ASTs are identical.
parserTest :: FilePath -> IO ()
parserTest fname = do
    -- if a file contains .fail. in its name it indicates a test
    -- that is supposed to fail during compilation.
    body <- readFile fname
    let astfile  = replaceExtension fname "ast"
    if shouldFail fname
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
        (prog, _) <- parseValidate fname body
        writeFile astfile (show prog ++ "\n")
        -- parse reference output
        fdata <- readFile astfile
        prog' <- parseDatalogString fdata astfile
        -- expect the same result
        assertEqual "Pretty-printed Datalog differs from original input" prog prog'

-- Run Datalog compiler on spec in 'fname'.
--
-- * Creates Cargo project in a directory obtained by removing file
-- extension from 'fname'.
--
-- * If a .dat file exists for the given test, dump its content to the
-- compiled datalog program, producing .dump and .err files
compilerTest :: Bool -> FilePath -> [String] -> [String] -> IO ()
compilerTest progress fname cli_args crate_types = do
    fname <- makeAbsolute fname
    body <- readFile fname
    let specname = takeBaseName fname
    (prog, rs_code) <- parseValidate fname body
    -- generate Rust project
    let rust_dir = takeDirectory fname
    compile prog specname rs_code rust_dir crate_types
    -- compile it with Cargo
    let cargo_proc = (proc "cargo" (["build"] ++ cargo_build_flag)) {
                          cwd = Just $ rust_dir </> rustProjectDir specname
                     }
    (code, stdo, stde) <- withProgress progress $ readCreateProcessWithExitCode cargo_proc ""
    when (code /= ExitSuccess) $ do
        errorWithoutStackTrace $ "cargo build failed with exit code " ++ show code ++
                                 "\nstdout:\n" ++ stde ++
                                 "\n\nstdout:\n" ++ stdo
    {-let cargo_proc = (proc "cargo" (["test"] ++ cargo_build_flag)) {
                          cwd = Just $ rust_dir </> specname
                     }

    (code, stdo, stde) <- withProgress progress $ readCreateProcessWithExitCode cargo_proc ""
    when (code /= ExitSuccess) $ do
        errorWithoutStackTrace $ "cargo test failed with exit code " ++ show code ++
                                 "\nstderr:\n" ++ stde ++
                                 "\n\nstdout:\n" ++ stdo -}
    cliTest progress fname specname rust_dir cli_args

progressThread :: IO ()
progressThread = do
    threadDelay 1000000
    putStr "."
    hFlush stdout
    progressThread

-- execute IO action with progress indicator
withProgress :: Bool -> IO a -> IO a
withProgress False action = action
withProgress True action = do
    hprogress <- forkIO progressThread
    res <- action
    killThread hprogress
    return res

-- Feed test data via pipe if a .dat file exists
cliTest :: Bool -> FilePath -> String -> FilePath -> [String] -> IO ()
cliTest progress fname specname rust_dir extra_args = do
    let extra_args' = if null extra_args then [] else ("--" : extra_args)
    let dumpfile = replaceExtension fname "dump"
    let errfile  = replaceExtension fname "err"
    let datfile  = replaceExtension fname "dat"
    hasdata <- doesFileExist datfile
    when hasdata $ do
        hout <- openFile dumpfile WriteMode
        herr <- openFile errfile  WriteMode
        hdat <- openFile datfile ReadMode
        let cli_proc = (proc "cargo" (["run", "--bin", specname ++ "_cli"] ++ cargo_build_flag ++ extra_args')) {
                cwd = Just $ rust_dir </> rustProjectDir specname,
                std_in=UseHandle hdat,
                std_out=UseHandle hout,
                std_err=UseHandle herr
            }
        code <- withCreateProcess cli_proc $
            \_ _ _ phandle -> withProgress progress $ waitForProcess phandle
        hClose hout
        hClose herr
        hClose hdat
        when (code /= ExitSuccess) $ do
            err <- readFile errfile
            errorWithoutStackTrace $ "cargo run cli failed with exit code " ++ show code ++
                                     "\nstderr:\n" ++ err ++
                                     "\n\nstdout written to:\n" ++ dumpfile

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
  cmp [] [] = return Nothing
  cmp xs ys = return $ (\errs -> if null errs then Nothing else Just (intercalate "\n" errs)) $
              mapMaybe (\((x,r),(y,n)) ->
                    if x == y
                       then Nothing
                       else Just $ printf "Files '%s' and '%s' differ" r n)
                  $ zip (zip xs ref) (zip ys new)
  upd bufs = mapM_ (\(r,b) -> do exists <- doesFileExist r
                                 when (not exists) $ BS.writeFile r b)
             $ zip ref bufs

-- A non-recursive version of findByExtension
findByExtensionNonRec
  :: [FilePath] -- ^ extensions
  -> FilePath -- ^ directory
  -> IO [FilePath] -- ^ paths
findByExtensionNonRec extsList = go where
  exts = S.fromList extsList
  go dir = do
    allEntries <- getDirectoryContents dir
    let entries = filter (not . (`elem` [".", ".."])) allEntries
    liftM concat $ forM entries $ \e -> do
      let path = dir ++ "/" ++ e
      isDir <- doesDirectoryExist path
      if isDir
        then return []
        else
          return $
            if takeExtension path `S.member` exts
              then [path]
              else []
