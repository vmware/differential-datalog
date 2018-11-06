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
import Data.Maybe
import Data.List.Split
import Control.Monad
import qualified Data.Map as M

import Language.DifferentialDatalog.OVSDB.Compile

data TOption = OVSFile     String
             | OutputTable String
             | ROColumn    String
             | ProxyTable  String

options :: [OptDescr TOption]
options = [ Option ['f'] ["schema-file"]  (ReqArg OVSFile     "FILE")         "OVSDB schema file"
          , Option ['o'] ["output-table"] (ReqArg OutputTable "TABLE")        "mark TABLE as output"
          , Option []    ["ro"]           (ReqArg ROColumn    "TABLE.COLUMN") "mark COLUMN as read-only"
          , Option ['p'] ["gen-proxy"]    (ReqArg ProxyTable  "TABLE")        "generate output proxy table for TABLE"
          ]

data Config = Config { confOVSFile      :: FilePath
                     , confOutputTables :: [(String, [String])]
                     , confProxyTables  :: [String]
                     }

defaultConfig = Config { confOVSFile      = ""
                       , confOutputTables = []
                       , confProxyTables  = []
                       }


addOption :: Config -> TOption -> IO Config
addOption config (OVSFile f) = do
    when (confOVSFile config == "") $ errorWithoutStackTrace "Multiple input files specified"
    return config {confOVSFile = f}
addOption config (OutputTable t) = return config{ confOutputTables = nub ((t,[]) : confOutputTables config)}
addOption config (ProxyTable t) = return config{ confProxyTables = nub (t : confProxyTables config)}
addOption config (ROColumn c) = do
    case splitOn "." c of
         [table, col] -> do
            when (isNothing $ lookup table $ confOutputTables config)
                 $ errorWithoutStackTrace $ "Unknown output table name " ++ table
            let outtabs = map (\(t,ro) -> if t == table then (t, nub $ c:ro) else (t,ro))
                              $ confOutputTables config
            return $ config{confOutputTables = outtabs}
         _ -> errorWithoutStackTrace $ "Invalid column name " ++ c

validateConfig :: Config -> IO ()
validateConfig Config{..} = do
    when (confOVSFile == "") $ errorWithoutStackTrace "Input file not specified"

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
    dlschema <- compileSchemaFile confOVSFile confOutputTables confProxyTables M.empty
    putStrLn $ render dlschema
    return ()
