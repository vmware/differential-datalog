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

{-# LANGUAGE FlexibleContexts, ImplicitParams #-}

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
import qualified Data.ByteString.Lazy as LBS
import qualified Data.Set as S
import qualified Codec.Compression.GZip as GZ

import Language.DifferentialDatalog.Parse
import Language.DifferentialDatalog.Module
import Language.DifferentialDatalog.Syntax
import Language.DifferentialDatalog.Validate
import Language.DifferentialDatalog.Compile
import Language.DifferentialDatalog.FlatBuffer

main :: IO ()
main = do
    progress <- isJust <$> lookupEnv "DDLOG_TEST_PROGRESS"
    tests <- goldenTests progress
    defaultMain tests

bUILD_TYPE :: String
--bUILD_TYPE = "debug"
bUILD_TYPE = "release"

cargo_build_flag :: [String]
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
               else Just $ goldenVsFiles (takeBaseName file) expect output (compilerTest progress True file [] ["staticlib"])
            | file:files <- inFiles
            , let expect = map (uncurry replaceExtension) $ zip files [".dump.expected"]
            , let output = map (uncurry replaceExtension) $ zip files [".dump"]]
  return $ testGroup "ddlog tests" [parser_tests, compiler_tests, souffleTests progress]

sOUFFLE_BASE :: String
sOUFFLE_BASE = "./test"

-- These should be all directories, but currently some tests do not work with
-- the Souffle translator
sOUFFLE_DIRS :: [String]
sOUFFLE_DIRS = ["souffle0", -- large Doop example
                "souffle1", "souffle2", "souffle3", "souffle4", "souffle5", "souffle6",
                 -- "souffle7", -- uses a recursive type
                 "souffle9", "souffle10", "souffle11", "souffle12", "souffle13",
                 "souffle14", "souffle15", "souffle16", "souffle17", "souffle18", "souffle19", "souffle20", "souffle21"]

souffleTests :: Bool -> TestTree
souffleTests progress =
  testGroup "souffle tests" $ map (\t -> souffleTest (sOUFFLE_BASE </> t) progress) sOUFFLE_DIRS

souffleTest :: String -> Bool -> TestTree
souffleTest testdir progress =
  testGroup "souffle tests" $
        [ goldenVsFiles testdir
          [testdir </> "souffle.dump.expected.gz"]
          [testdir </> "souffle.dump"]
          $ do {convertSouffle testdir progress; compilerTest progress True (testdir </> "souffle.dl") ["--no-print", "-w", "1"] ["cdylib"]}]

convertSouffle :: String -> Bool -> IO ()
convertSouffle testdir progress = do
    dir <- makeAbsolute testdir
    let inputDl = dir </> "test.dl"  -- input file always called test.dl
        convert_proc = (proc (dir </> "../../tools/souffle_converter.py") [inputDl, "souffle", "--convert-dnf"]) { cwd = Just dir }
    (code, stdo, stde) <- withProgress progress $ readCreateProcessWithExitCode convert_proc ""
    when (code /= ExitSuccess) $ do
        errorWithoutStackTrace $ "souffle_converter.py failed with exit code " ++ show code ++
                                 "\nstderr:\n" ++ stde ++
                                 "\n\nstdout:\n" ++ stdo

parseValidate :: FilePath -> Bool -> String -> IO (DatalogProgram, Doc, Doc)
parseValidate file java program = do
    (d, rs_code, toml_code) <- parseDatalogProgram [takeDirectory file, "lib"] True program file
    d' <- case validate d of
               Left e   -> errorWithoutStackTrace $ "error: " ++ e
               Right d' -> return d'
    when java $
        case flatBufferValidate d' of
             Left e  -> errorWithoutStackTrace $ "error: " ++ e
             Right{} -> return ()
    return (d', rs_code, toml_code)

-- compile a program that is supposed to fail compilation
compileFailingProgram :: String -> String -> IO String
compileFailingProgram file program =
    (show <$> parseValidate file False program) `catch`
             (\e -> return $ show (e::SomeException))

shouldFail :: String -> Bool
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
        (prog, _, _) <- parseValidate fname False body
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
compilerTest :: Bool -> Bool -> FilePath -> [String] -> [String] -> IO ()
compilerTest progress java file cli_args crate_types = do
    fname <- makeAbsolute file
    body <- readFile fname
    let specname = takeBaseName fname
    (prog, rs_code, toml_code) <- parseValidate fname java body
    -- generate Rust project
    let dir = takeDirectory fname
    let ?cfg = defaultCompilerConfig { cconfJava = java }
    compile prog specname rs_code toml_code dir crate_types
    -- compile generated Java code
    classpath <- (maybe "" (":" ++ )) <$> lookupEnv "CLASSPATH"
    p <- (maybe "" id) <$> lookupEnv "PATH"
    let javac_proc = (shell $ "javac ddlog" </> specname </> "*.java") {
                          cwd = Just $ dir </> rustProjectDir specname </> "flatbuf" </> "java",
                          env = Just [("CLASSPATH", (dir </> "../../java") ++ classpath),
                                      ("PATH", p)]
                     }
    (jcode, jstdo, jstde) <- readCreateProcessWithExitCode javac_proc ""
    when (jcode /= ExitSuccess) $ do
        errorWithoutStackTrace $ "javac failed with exit code " ++ show jcode ++
                                 "\nstdout:\n" ++ jstde ++
                                 "\n\nstdout:\n" ++ jstdo

    -- compile it with Cargo
    let cargo_proc = (proc "cargo" (["build", "--features=flatbuf"] ++ cargo_build_flag)) {
                          cwd = Just $ dir </> rustProjectDir specname
                     }
    (ccode, cstdo, cstde) <- withProgress progress $ readCreateProcessWithExitCode cargo_proc ""
    when (ccode /= ExitSuccess) $ do
        errorWithoutStackTrace $ "cargo build failed with exit code " ++ show ccode ++
                                 "\nstdout:\n" ++ cstde ++
                                 "\n\nstdout:\n" ++ cstdo
    {-let cargo_proc = (proc "cargo" (["test"] ++ cargo_build_flag)) {
                          cwd = Just $ dir </> specname
                     }

    (code, stdo, stde) <- withProgress progress $ readCreateProcessWithExitCode cargo_proc ""
    when (code /= ExitSuccess) $ do
        errorWithoutStackTrace $ "cargo test failed with exit code " ++ show code ++
                                 "\nstderr:\n" ++ stde ++
                                 "\n\nstdout:\n" ++ stdo -}
    cliTest progress fname specname dir cli_args

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
             (do {refs <- mapM readDecompress ref; evaluate $ rnf refs; return refs})
             (act >> do {news <- mapM BS.readFile new; evaluate $ rnf news; return news})
             cmp upd
  where
  readDecompress :: FilePath -> IO BS.ByteString
  readDecompress f = do
    dat <- BS.readFile f
    return $ if takeExtension f == ".gz"
       then LBS.toStrict $ GZ.decompress $ LBS.fromStrict dat
       else dat
  writeCompress :: FilePath -> BS.ByteString -> IO ()
  writeCompress f dat =
    if takeExtension f == ".gz"
       then LBS.writeFile f $ GZ.compress $ LBS.fromStrict dat
       else BS.writeFile f dat
  cmp [] [] = return Nothing
  cmp xs ys = return $ (\errs -> if null errs then Nothing else Just (intercalate "\n" errs)) $
              mapMaybe (\((x,r),(y,n)) ->
                    if x == y
                       then Nothing
                       else Just $ printf "Files '%s' and '%s' differ" r n)
                  $ zip (zip xs ref) (zip ys new)
  upd bufs = mapM_ (\(r,b) -> do exists <- doesFileExist r
                                 when (not exists) $ writeCompress r b)
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
      let fpath = dir ++ "/" ++ e
      isDir <- doesDirectoryExist fpath
      if isDir
        then return []
        else
          return $
            if takeExtension fpath `S.member` exts
              then [fpath]
              else []
