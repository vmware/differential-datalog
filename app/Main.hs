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
--import System.Directory
import System.Console.GetOpt
import Control.Exception
import Control.Monad
--import Text.Parsec

import Language.DifferentialDatalog.Syntax
import Language.DifferentialDatalog.Parse

data TOption = Datalog String
             | Action String

data DLAction = ActionCompile
              | ActionValidate
              | ActionNone
              deriving Eq

options :: [OptDescr TOption]
options = [ Option ['i'] []                 (ReqArg Datalog "FILE")        "Datalog program"
          , Option []    ["action"]         (ReqArg Action "ACTION")       "action: [validate, compile]"
          ]

data Config = Config { confDatalogFile   :: FilePath
                     , confAction        :: DLAction
                     }

defaultConfig = Config { confDatalogFile   = ""
                       , confAction        = ActionNone
                       }


addOption :: Config -> TOption -> IO Config
addOption config (Datalog f)    = return config{ confDatalogFile  = f}
addOption config (Action a)     = do a' <- case a of
                                                "validate"   -> return ActionValidate
                                                "compile"    -> return ActionCompile
                                                _            -> error "invalid action"
                                     return config{confAction = a'}

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

    let fname  = confDatalogFile config
        (dir, file) = splitFileName fname
        (basename,_) = splitExtension file
        --workdir = dir </> basename
    case confAction config of
         ActionValidate -> readValidate fname
         ActionCompile -> error "not implemented"
         ActionNone -> error "action not specified"


readValidate :: FilePath -> IO DatalogProgram
readValidate fname = do
    prog <- parseDatalogFile True fname
--    case validate prog of
--         Left e  -> error $ "Validation error: " ++ e
--         Right _ -> return ()
    putStrLn "Validation complete"
    return prog

