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

{-# LANGUAGE RecordWildCards, ImplicitParams, LambdaCase, FlexibleContexts #-}

import System.Environment
import System.FilePath.Posix
import System.Console.GetOpt
import Control.Exception
import Control.Monad
import Data.List
import Text.PrettyPrint

import Language.DifferentialDatalog.Syntax
import Language.DifferentialDatalog.Module
import Language.DifferentialDatalog.Validate
import Language.DifferentialDatalog.Compile

data TOption = Datalog String
             | Action String
             | LibDir String
             | DynLib
             | NoDynLib
             | StaticLib
             | NoStaticLib
             | Help

data DLAction = ActionCompile
              | ActionValidate
              | ActionHelp
              deriving Eq

options :: [OptDescr TOption]
options = [ Option ['i'] []                   (ReqArg Datalog  "FILE")        "DDlog program"
          , Option []    ["action"]           (ReqArg Action   "ACTION")      "action: [validate, compile]"
          , Option ['L'] []                   (ReqArg LibDir   "PATH")        "extra DDlog library directory"
          , Option []    ["dynlib"]           (NoArg DynLib)                  "generate dynamic library"
          , Option []    ["no-dynlib"]        (NoArg NoDynLib)                "do not generate dynamic library (default)"
          , Option []    ["staticlib"]        (NoArg StaticLib)               "generate static library (default)"
          , Option []    ["no-staticlib"]     (NoArg NoStaticLib)             "do not generate static library"
          , Option ['h'] ["help"]             (NoArg Help)                    "print help message"
          ]

data Config = Config { confDatalogFile   :: FilePath
                     , confAction        :: DLAction
                     , confLibDirs       :: [FilePath]
                     , confStaticLib     :: Bool
                     , confDynamicLib    :: Bool
                     }

defaultConfig = Config { confDatalogFile   = ""
                       , confAction        = ActionCompile
                       , confLibDirs       = []
                       , confStaticLib     = True
                       , confDynamicLib    = False
                       }


addOption :: Config -> TOption -> IO Config
addOption config (Datalog f)    = return config{ confDatalogFile  = f}
addOption config (Action a)     = do a' <- case a of
                                                "validate"   -> return ActionValidate
                                                "compile"    -> return ActionCompile
                                                _            -> errorWithoutStackTrace "invalid action"
                                     return config{confAction = a'}
addOption config (LibDir d)     = return config { confLibDirs = nub (d:confLibDirs config)}
addOption config DynLib         = return config { confDynamicLib = True }
addOption config NoDynLib       = return config { confDynamicLib = False }
addOption config StaticLib      = return config { confStaticLib = True }
addOption config NoStaticLib    = return config { confStaticLib = False }
addOption config Help           = return config { confAction = ActionHelp}

validateConfig :: Config -> IO ()
validateConfig Config{..} = do
    when (confDatalogFile == "" && confAction /= ActionHelp)
         $ errorWithoutStackTrace "input file not specified"

main = do
    args <- getArgs
    prog <- getProgName
    config <- case getOpt Permute options args of
                   (flags, [], []) -> do conf <- foldM addOption defaultConfig flags
                                         validateConfig conf
                                         return conf
                                      `catch`
                                      (\e -> do putStrLn $ usageInfo ("Usage: " ++ prog ++ " [OPTION...]") options
                                                throw (e::SomeException))
                   _ -> errorWithoutStackTrace $ usageInfo ("Usage: " ++ prog ++ " [OPTION...]") options
    case confAction config of
         ActionHelp -> putStrLn $ usageInfo ("Usage: " ++ prog ++ " [OPTION...]") options
         ActionValidate -> do { parseValidate config; return () }
         ActionCompile -> compileProg config


parseValidate :: Config -> IO (DatalogProgram, Doc)
parseValidate Config{..} = do
    fdata <- readFile confDatalogFile
    (d, rs_code) <- parseDatalogProgram (takeDirectory confDatalogFile:confLibDirs) True fdata confDatalogFile
    case validate d of
         Left e   -> errorWithoutStackTrace $ "error: " ++ e
         Right d' -> return (d', rs_code)

compileProg :: Config -> IO ()
compileProg conf@Config{..} = do
    let specname = takeBaseName confDatalogFile
    (prog, rs_code) <- parseValidate conf
    -- generate Rust project
    let rust_dir = takeDirectory confDatalogFile
    let crate_types = (if confStaticLib then ["staticlib"] else []) ++
                      (if confDynamicLib then ["cdylib"] else [])
    compile prog specname rs_code rust_dir crate_types
