{-
Copyright (c) 2018-2021 VMware, Inc.
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

{-# LANGUAGE RecordWildCards, ImplicitParams, LambdaCase, FlexibleContexts, TemplateHaskell #-}

import Prelude hiding (readFile, writeFile)
import System.Environment
import System.FilePath.Posix
import System.Console.GetOpt
import System.Console.ANSI
import Control.Exception
import Control.Monad.Trans.Except
import Control.Monad
import Data.List
import Data.Time.Clock
import Data.Time.Format
import qualified Data.Map as M
import Text.PrettyPrint

import Language.DifferentialDatalog.Config
import Language.DifferentialDatalog.Util
import Language.DifferentialDatalog.Version
import Language.DifferentialDatalog.Syntax
import Language.DifferentialDatalog.Module
import Language.DifferentialDatalog.Validate
import Language.DifferentialDatalog.Compile
import Language.DifferentialDatalog.FlatBuffer
import Language.DifferentialDatalog.DatalogProgram

data TOption = Help
             | Version
             | Datalog String
             | Action String
             | LibDir String
             | OutputDir String
             | Java
             | OutputInternal
             | OutputInput String
             | DynLib
             | NoDynLib
             | StaticLib
             | NoStaticLib
             | DebugHooks
             | DumpFlat
             | DumpValid
             | DumpDebug
             | DumpOpt
             | ReValidate
             | OmitProfile
             | OmitWorkspace
             | RunRustfmt
             | RustFlatBuffers
             | NestedTS32

options :: [OptDescr TOption]
options = [ Option ['h'] ["help"]             (NoArg Help)                      "Display help message."
          , Option ['v'] ["version"]          (NoArg Version)                   "Display DDlog version."
          , Option ['i'] []                   (ReqArg Datalog  "FILE")          "DDlog program to compile."
          , Option []    ["action"]           (ReqArg Action   "ACTION")        "Action: [validate, compile]"
          , Option ['L'] []                   (ReqArg LibDir   "PATH")          "Extra DDlog library directory."
          , Option ['o'] ["output-dir"]       (ReqArg OutputDir "DIR")          "Output directory (default based on program name)."
          , Option []    ["dynlib"]           (NoArg DynLib)                    "Generate dynamic library."
          , Option ['j'] ["java"]             (NoArg Java)                      "Generate Java bindings. Implies '--rust-flatbuffers'."
          , Option []    ["output-internal-relations"]  (NoArg OutputInternal)  "All non-input relations are marked as output relations."
          , Option []    ["output-input-relations"]  (ReqArg OutputInput "PREFIX") "Mirror each input relation into an output relation named by prepending the prefix."
          , Option []    ["no-dynlib"]        (NoArg NoDynLib)                  "Do not generate dynamic library (default)."
          , Option []    ["staticlib"]        (NoArg StaticLib)                 "Generate static library (default)."
          , Option []    ["no-staticlib"]     (NoArg NoStaticLib)               "Do not generate static library."
          , Option ['g'] []                   (NoArg DebugHooks)                "Enable debugging hooks."
          , Option []    ["pp-flattened"]     (NoArg DumpFlat)                  "Dump the source after compilation pass 1 (flattening module hierarchy) to PROG.flat.ast."
          , Option []    ["pp-validated"]     (NoArg DumpValid)                 "Dump the source after compilation pass 2 (validation, including several source transformations) to PROG.valid.ast."
          , Option []    ["pp-debug"]         (NoArg DumpDebug)                 "Dump the source after compilation pass 3 (injecting debugging hooks) to FILE.debug.ast.  If the '-g' option is not specified, then pass 3 is a no-op and will produce identical output to pass 2."
          , Option []    ["pp-optimized"]     (NoArg DumpOpt)                   "Dump the source after compilation pass 4 (optimization) to FILE.opt.ast."
          , Option []    ["re-validate"]      (NoArg ReValidate)                "[developers only] Re-validate the program after type inference and optimization passes."
          , Option []    ["omit-profile"]     (NoArg OmitProfile)               "Skip adding a Cargo profile (silences warnings for some rust builds, included by default)"
          , Option []    ["omit-workspace"]   (NoArg OmitWorkspace)             "Skip adding a Cargo workspace (silences errors for some rust builds, included by default)"
          , Option []    ["run-rustfmt"]      (NoArg RunRustfmt)                "Run rustfmt on the generated code"
          , Option []    ["rust-flatbuffers"] (NoArg RustFlatBuffers)           "Build flatbuffers bindings for Rust"
          , Option []    ["nested-ts-32"]     (NoArg NestedTS32)                "Use 32-bit instead of 16-bit nested timestamps. Supports recursive programs that may perform >65,536 iterations. Slightly increases the memory footprint of the program."
          ]

addOption :: Config -> TOption -> IO Config
addOption config (Datalog f)      = return config{ confDatalogFile  = f}
addOption config (Action a)       = do a' <- case a of
                                                  "validate"   -> return ActionValidate
                                                  "compile"    -> return ActionCompile
                                                  _            -> errorWithoutStackTrace "invalid action"
                                       return config{confAction = a'}
addOption config Java             = return config { confJava = True }
addOption config (LibDir d)       = return config { confLibDirs = nub (d:confLibDirs config)}
addOption config (OutputDir d)    = return config { confOutputDir = d }
addOption config DynLib           = return config { confDynamicLib = True }
addOption config NoDynLib         = return config { confDynamicLib = False }
addOption config OutputInternal   = return config { confOutputInternal = True }
addOption config (OutputInput p)  = return config { confOutputInput = p }
addOption config StaticLib        = return config { confStaticLib = True }
addOption config NoStaticLib      = return config { confStaticLib = False }
addOption config Help             = return config { confAction = ActionHelp}
addOption config Version          = return config { confAction = ActionVersion}
addOption config DebugHooks       = return config { confDebugHooks = True }
addOption config DumpFlat         = return config { confDumpFlat = True }
addOption config DumpValid        = return config { confDumpValid = True }
addOption config DumpDebug        = return config { confDumpDebug = True }
addOption config DumpOpt          = return config { confDumpOpt = True }
addOption config ReValidate       = return config { confReValidate = True }
addOption config OmitProfile      = return config { confOmitProfile = True }
addOption config OmitWorkspace    = return config { confOmitWorkspace = True }
addOption config RunRustfmt       = return config { confRunRustfmt = True }
addOption config RustFlatBuffers  = return config { confRustFlatBuffers = True }
addOption config NestedTS32       = return config { confNestedTS32 = True }

validateConfig :: Config -> IO ()
validateConfig Config {..} = do
  when (confDatalogFile == "" && confAction /= ActionHelp && confAction /= ActionVersion) $
    errorWithoutStackTrace "input file not specified"

main :: IO ()
main = do
    args <- getArgs
    prog <- getProgName
    home <- lookupEnv "DDLOG_HOME"
    config <- case getOpt Permute options args of
        (flags, [], []) ->
            do
                conf <- foldM addOption defaultConfig flags
                validateConfig conf
                return conf
                `catch` ( \e -> do
                              putStrLn $ usageInfo ("Usage: " ++ prog ++ " [OPTION...]") options
                              throw (e :: SomeException)
                        )
        _ -> errorWithoutStackTrace $ usageInfo ("Usage: " ++ prog ++ " [OPTION...]") options
    config' <- case home of
        Just (p) -> addOption config (LibDir $ p ++ "/lib")
        _ -> return config
    case confAction config' of
        ActionHelp -> putStrLn $ usageInfo ("Usage: " ++ prog ++ " [OPTION...]") options
        ActionVersion -> do
            putStrLn $ "DDlog " ++ dDLOG_VERSION ++ " (" ++ gitHash ++ ")"
            putStrLn $ "Copyright (c) 2019-2020 VMware, Inc. (MIT License)"
        ActionValidate -> do
            timeAction ("validating " ++ show (confDatalogFile config)) $ do
                _ <- parseValidate config'
                return ()
        ActionCompile -> do
            timeAction ("compiling " ++ show (confDatalogFile config)) $ do
                compileProg config'
                -- Run rustfmt on the generated code if it's enabled
                when (confRunRustfmt config') $
                    runCommandReportingErr "rustfmt" "cargo" ["fmt", "--all"] $ Just (confOutputDir config')

-- Perform IO action and measure its duration.
timeAction :: String -> IO () -> IO ()
timeAction description action = do
    start_time <- getCurrentTime
    action
    end_time <- getCurrentTime
    setSGR [SetColor Foreground Vivid Green, SetConsoleIntensity BoldIntensity]
    putStr $ "Finished " ++ description
    setSGR []
    putStrLn $ " in " ++ (formatTime defaultTimeLocale "%-2Ess" $ diffUTCTime end_time start_time)

parseValidate :: Config -> IO ([DatalogModule], DatalogProgram, M.Map ModuleName (Doc, Doc, Doc))
parseValidate Config{..} = do
    fdata <- readFile confDatalogFile
    parsed <- runExceptT $ parseDatalogProgram (takeDirectory confDatalogFile:confLibDirs) True fdata confDatalogFile
    (modules, d, rs_code) <- case parsed of
                                  Left e    -> compilerError e
                                  Right res -> return res 
    let d'' = case confOutputInternal of
                  False -> d
                  True  -> progOutputInternalRelations d
    let d''' = case confOutputInput of
                    "" -> d''
                    x  -> progMirrorInputRelations d'' x
    when confDumpFlat $
        writeFile (replaceExtension confDatalogFile ".flat.ast") (show d''')
    d'''' <- case validate d''' of
                  Left e      -> compilerError e
                  Right d'''' -> return d''''
    when confDumpValid $
        writeFile (replaceExtension confDatalogFile ".valid.ast") (show d'''')
    d' <- case confDebugHooks of
         False -> return d''''
         True  -> return $ progInjectDebuggingHooks d''''
    when confDumpDebug $
        writeFile (replaceExtension confDatalogFile ".debug.ast") (show d')
    when confJava $
        case flatBufferValidate d of
             Left e  -> compilerError e
             Right{} -> return ()
    return (modules, d', rs_code)

compileProg :: Config -> IO ()
compileProg conf@Config{..} = do
    let specname = takeBaseName confDatalogFile
    (modules, prog, rs_code) <- parseValidate conf
    -- generate Rust project
    let dir = if confOutputDir == "" then takeDirectory confDatalogFile else confOutputDir
    let crate_types = (if confStaticLib then ["staticlib"] else []) ++
                      (if confDynamicLib then ["cdylib"] else [])
    let ?cfg = conf
    compile prog specname modules rs_code dir crate_types
