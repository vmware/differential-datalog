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

import Language.DifferentialDatalog.Syntax
import Language.DifferentialDatalog.Parse
import Language.DifferentialDatalog.Validate
import Language.DifferentialDatalog.Compile

data TOption = Datalog String
             | Action String
             | RustFile String

data DLAction = ActionCompile
              | ActionValidate
              | ActionNone
              deriving Eq

options :: [OptDescr TOption]
options = [ Option ['i'] []                   (ReqArg Datalog  "FILE")        "DDlog program"
          , Option []    ["action"]           (ReqArg Action   "ACTION")      "action: [validate, compile]"
          , Option ['r'] ["inline-rust-file"] (ReqArg RustFile "FILE")        "extra Rust source to be inlined in the generated library"
          ]

data Config = Config { confDatalogFile   :: FilePath
                     , confAction        :: DLAction
                     , confRustFiles     :: [FilePath]
                     }

defaultConfig = Config { confDatalogFile   = ""
                       , confAction        = ActionNone
                       , confRustFiles     = []
                       }


addOption :: Config -> TOption -> IO Config
addOption config (Datalog f)    = return config{ confDatalogFile  = f}
addOption config (Action a)     = do a' <- case a of
                                                "validate"   -> return ActionValidate
                                                "compile"    -> return ActionCompile
                                                _            -> error "invalid action"
                                     return config{confAction = a'}
addOption config (RustFile f)   = return config { confRustFiles = nub (f:confRustFiles config)}

validateConfig :: Config -> IO ()
validateConfig Config{..} = do
    when (confAction == ActionNone)
         $ error "action not specified"
    when (confDatalogFile == "")
         $ error "input file not specified"

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
                   _ -> error $ usageInfo ("Usage: " ++ prog ++ " [OPTION...]") options
    case confAction config of
         ActionValidate -> do { parseValidate config; return () }
         ActionCompile -> compileProg config
         ActionNone -> error "action not specified"


parseValidate :: Config -> IO DatalogProgram
parseValidate Config{..} = do
    d <- parseDatalogFile True confDatalogFile
    case validate d of
         Left e   -> errorWithoutStackTrace $ "error: " ++ e
         Right d' -> return d'

compileProg :: Config -> IO ()
compileProg conf@Config{..} = do
    let specname = takeBaseName confDatalogFile
    prog <- parseValidate conf
    -- include any user-provided Rust code
    imports <- mapM readFile confRustFiles
    -- generate Rust project
    let rust_dir = joinPath [takeDirectory confDatalogFile]
    compile prog specname (concat imports) rust_dir
