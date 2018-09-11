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

{-# LANGUAGE RecordWildCards #-} 

import System.Environment
import Text.PrettyPrint
import System.Console.GetOpt
import Control.Exception
import Data.List
import Control.Monad

import Parse
import Compile

data TOption = OVSFile     String
             | InputTable  String
             | OutputTable String

options :: [OptDescr TOption]
options = [ Option ['f'] ["schema-file"]  (ReqArg OVSFile     "FILE")  "OVSDB schema file"
          , Option ['o'] ["output-table"] (ReqArg OutputTable "TABLE") "output table name"
          ]

data Config = Config { confOVSFiles     :: [FilePath]
                     , confOutputTables :: [String]
                     }

defaultConfig = Config { confOVSFiles     = []
                       , confOutputTables = []
                       }


addOption :: Config -> TOption -> IO Config
addOption config (OVSFile f)     = return config{ confOVSFiles     = nub (f : confOVSFiles     config)}
addOption config (OutputTable t) = return config{ confOutputTables = nub (t : confOutputTables config)}

validateConfig :: Config -> IO ()
validateConfig Config{..} = do
    when (null confOVSFiles) $ error "Input files not specified"

main = do
    args <- getArgs
    prog <- getProgName
    Config{..} <- case getOpt Permute options args of
                       (flags, [], []) -> do conf <- foldM addOption defaultConfig flags
                                             validateConfig conf
                                             return conf
                                          `catch`
                                          (\e -> do putStrLn $ usageInfo ("Usage: " ++ prog ++ " [OPTION...]") options
                                                    throw (e::SomeException))
                       _ -> errorWithoutStackTrace $ usageInfo ("Usage: " ++ prog ++ " [OPTION...]") options
    schemas <- mapM (\fname -> do 
                     content <- readFile fname
                     case parseSchema content fname of
                          Left  e    -> errorWithoutStackTrace $ "Failed to parse input file: " ++ e
                          Right prog -> return prog)
                   confOVSFiles
    dlschema <- case compileSchema schemas confOutputTables of
                     Left e    -> errorWithoutStackTrace e
                     Right doc -> return doc
    putStrLn $ render dlschema
    return ()
