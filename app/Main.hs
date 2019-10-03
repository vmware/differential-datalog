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

{-# LANGUAGE RecordWildCards, ImplicitParams, LambdaCase, FlexibleContexts, TemplateHaskell #-}

import Prelude hiding(readFile, writeFile)
import System.Environment
import System.FilePath.Posix
import System.Console.GetOpt
import Control.Exception
import Control.Monad
import Data.List
import Text.PrettyPrint

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
             | Java
             | OutputInternal
             | OutputInput String
             | DynLib
             | NoDynLib
             | StaticLib
             | NoStaticLib
             | BoxThreshold String

data DLAction = ActionCompile
              | ActionValidate
              | ActionHelp
              | ActionVersion
              deriving Eq

options :: [OptDescr TOption]
options = [ Option ['h'] ["help"]             (NoArg Help)                      "Display help message."
          , Option ['v'] ["version"]          (NoArg Version)                   "Display DDlog version."
          , Option ['i'] []                   (ReqArg Datalog  "FILE")          "DDlog program to compile."
          , Option []    ["action"]           (ReqArg Action   "ACTION")        "Action: [validate, compile]"
          , Option ['L'] []                   (ReqArg LibDir   "PATH")          "Extra DDlog library directory."
          , Option []    ["dynlib"]           (NoArg DynLib)                    "Generate dynamic library."
          , Option ['j'] ["java"]             (NoArg Java)                      "Generate Java bindings."
          , Option []    ["output-internal-relations"]  (NoArg OutputInternal)  "All non-input relations are marked as output relations."
          , Option []    ["output-input-relations"]  (ReqArg OutputInput "PREFIX") "Mirror each input relation into an output relation named by prepending the prefix."
          , Option []    ["no-dynlib"]        (NoArg NoDynLib)                  "Do not generate dynamic library (default)."
          , Option []    ["staticlib"]        (NoArg StaticLib)                 "Generate static library (default)."
          , Option []    ["no-staticlib"]     (NoArg NoStaticLib)               "Do not generate static library."
          , Option []    ["box-threshold"]    (ReqArg BoxThreshold "THRESHOLD") ("Maximum inline DDlog record size. Records larger than this\n"  ++
                                                                                 "threshold will be stored on the heap. Used to fine-tune the\n" ++
                                                                                 "memory footprint of the program. THRESHOLD must be >=8.\n"     ++
                                                                                 "Recommended values are in the [8,32] range. Default is " ++ (show $ cconfBoxThreshold defaultCompilerConfig) ++ ".")
          ]

data Config = Config { confDatalogFile   :: FilePath
                     , confAction        :: DLAction
                     , confLibDirs       :: [FilePath]
                     , confOutputInput   :: String
                     , confStaticLib     :: Bool
                     , confDynamicLib    :: Bool
                     , confJava          :: Bool
                     , confOutputInternal:: Bool
                     , confBoxThreshold  :: Int
                     }

defaultConfig :: Config
defaultConfig = Config { confDatalogFile   = ""
                       , confAction        = ActionCompile
                       , confLibDirs       = []
                       , confStaticLib     = True
                       , confDynamicLib    = False
                       , confOutputInternal= False
                       , confOutputInput   = ""
                       , confJava          = False
                       , confBoxThreshold  = cconfBoxThreshold defaultCompilerConfig
                       }


addOption :: Config -> TOption -> IO Config
addOption config (Datalog f)      = return config{ confDatalogFile  = f}
addOption config (Action a)       = do a' <- case a of
                                                  "validate"   -> return ActionValidate
                                                  "compile"    -> return ActionCompile
                                                  _            -> errorWithoutStackTrace "invalid action"
                                       return config{confAction = a'}
addOption config Java             = return config { confJava = True }
addOption config (LibDir d)       = return config { confLibDirs = nub (d:confLibDirs config)}
addOption config DynLib           = return config { confDynamicLib = True }
addOption config NoDynLib         = return config { confDynamicLib = False }
addOption config OutputInternal   = return config { confOutputInternal = True }
addOption config (OutputInput p)  = return config { confOutputInput = p }
addOption config StaticLib        = return config { confStaticLib = True }
addOption config NoStaticLib      = return config { confStaticLib = False }
addOption config Help             = return config { confAction = ActionHelp}
addOption config Version          = return config { confAction = ActionVersion}
addOption config (BoxThreshold t) =
    case reads t of
        [(tint, "")] -> if tint < 8
                           then errorWithoutStackTrace "Box threshold must be greater than or equal to 8."
                           else return config { confBoxThreshold = tint }
        _  -> errorWithoutStackTrace "Box threshold must be a positive integer."


validateConfig :: Config -> IO ()
validateConfig Config{..} = do
    when (confDatalogFile == "" && confAction /= ActionHelp && confAction /= ActionVersion)
         $ errorWithoutStackTrace "input file not specified"

main :: IO ()
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
         ActionVersion -> do putStrLn $ "DDlog " ++ dDLOG_VERSION ++ " (" ++ gitHash ++ ")"
                             putStrLn $ "Copyright (c) 2019 VMware, Inc. (MIT License)"
         ActionValidate -> do _ <- parseValidate config
                              return ()
         ActionCompile -> compileProg config

parseValidate :: Config -> IO (DatalogProgram, Doc, Doc)
parseValidate Config{..} = do
    fdata <- readFile confDatalogFile
    (d, rs_code, toml_code) <- parseDatalogProgram (takeDirectory confDatalogFile:confLibDirs) True fdata confDatalogFile
    d'' <- case confOutputInternal of
         False -> return d
         True ->  return $ progOutputInternalRelations d
    d''' <- case confOutputInput of
         "" -> return d''
         x  ->  return $ progMirrorInputRelations d'' x
    d' <- case validate d''' of
               Left e   -> errorWithoutStackTrace $ "error: " ++ e
               Right d' -> return d'
    when confJava $
        case flatBufferValidate d of
             Left e  -> errorWithoutStackTrace $ "error: " ++ e
             Right{} -> return ()
    return (d', rs_code, toml_code)

compileProg :: Config -> IO ()
compileProg conf@Config{..} = do
    let specname = takeBaseName confDatalogFile
    (prog, rs_code, toml_code) <- parseValidate conf
    -- generate Rust project
    let dir = takeDirectory confDatalogFile
    let crate_types = (if confStaticLib then ["staticlib"] else []) ++
                      (if confDynamicLib then ["cdylib"] else [])
    let ?cfg = CompilerConfig{ cconfBoxThreshold = confBoxThreshold
                             , cconfJava = confJava }
    compile prog specname rs_code toml_code dir crate_types
