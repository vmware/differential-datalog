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

{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE OverloadedStrings #-}

import System.Environment
import Text.PrettyPrint
import System.Console.GetOpt
import Control.Exception
import Data.List
import Data.Maybe
import Data.List.Split
import Control.Monad
import Data.Aeson (FromJSON, ToJSON, eitherDecode, encode)
import GHC.Generics (Generic)
import qualified Data.ByteString.Lazy.Char8 as LZ

import Language.DifferentialDatalog.OVSDB.Compile
import Language.DifferentialDatalog.Version

data TOption = OVSFile     String
             | OutputTable String
             | ROColumn    String
             | ConfigJsonI String
             | ConfigJsonO String
             | OutputFile  String
             | Version

data Action = ActionCompile
            | ActionVersion
            deriving Eq

options :: [OptDescr TOption]
options = [ Option ['v'] ["version"]       (NoArg Version)                     "Display DDlog version."
          , Option ['f'] ["schema-file"]   (ReqArg OVSFile     "FILE")         "OVSDB schema file."
          , Option ['c'] ["input-config"]  (ReqArg ConfigJsonI "FILE.json")    "Read options from Json configuration file (preceding options are ignored)."
          , Option ['O'] ["output-config"] (ReqArg ConfigJsonO "FILE.json")    "Write preceding options to Json configuration file."
          , Option ['o'] ["output-table"]  (ReqArg OutputTable "TABLE")        "Mark TABLE as output."
          , Option []    ["ro"]            (ReqArg ROColumn    "TABLE.COLUMN") "Mark COLUMN as read-only."
          , Option []    ["output-file"]   (ReqArg OutputFile  "FILE.dl")      "Write output to FILE.dl. If this option is not specified, output will be written to stdout."
          ]

data Config = Config { ovsSchemaFile:: FilePath
                     , outputFile   :: Maybe FilePath
                     , outputTables :: [(String, [String])]
                     }
              deriving (Eq, Show, Generic)

instance FromJSON Config
instance ToJSON   Config

defaultConfig :: Config
defaultConfig = Config { ovsSchemaFile= ""
                       , outputFile   = Nothing
                       , outputTables = []
                       }

addOption :: (Action, Config) -> TOption -> IO (Action, Config)
addOption (_, config) Version = do return (ActionVersion, config)
addOption (a, config) (OVSFile f) = do
    when (ovsSchemaFile config /= "") $ errorWithoutStackTrace "Multiple input files specified"
    return (a, config {ovsSchemaFile = f})
addOption (a, config) (OutputFile f) = do
    when (isJust $ outputFile config) $ errorWithoutStackTrace "Multiple output files specified"
    return (a, config {outputFile = Just f})
addOption (a, config) (OutputTable t) = return (a, config{ outputTables = nub ((t,[]) : outputTables config)})
addOption (a, config) (ROColumn c) = do
    case splitOn "." c of
         [table, col] -> do
            when (isNothing $ lookup table $ outputTables config)
                 $ errorWithoutStackTrace $ "Unknown output table name " ++ table
            let outtabs = map (\(t,ro) -> if t == table then (t, nub $ col:ro) else (t,ro))
                              $ outputTables config
            return $ (a, config{outputTables = outtabs})
         _ -> errorWithoutStackTrace $ "Invalid column name " ++ c
addOption (a, config) (ConfigJsonI c) = do
    when (config /= defaultConfig) $ errorWithoutStackTrace "-c option causes previous command-line options to be ignored"
    cdata <- readFile c
    let ce = (eitherDecode $ LZ.pack cdata) :: (Either String Config)
    case ce of
       Right conf -> do return (a, conf)
       Left msg   -> errorWithoutStackTrace $ "Error parsing configuration file " ++ c ++ ": " ++ msg

addOption (a, config) (ConfigJsonO c) = do
    writeFile c $ LZ.unpack $ encode config
    return (a, config)

validateConfig :: (Action, Config) -> IO ()
validateConfig (action, Config{..}) = do
    when (ovsSchemaFile == "" && action == ActionCompile) $ errorWithoutStackTrace "Input file not specified"

main :: IO ()
main = do
    args <- getArgs
    prog <- getProgName
    (action, Config{..}) <- case getOpt Permute options args of
                       (flags, [], []) -> do actionAndConf <- foldM addOption (ActionCompile, defaultConfig) flags
                                             validateConfig actionAndConf
                                             return actionAndConf
                                          `catch`
                                          (\e -> do putStrLn $ usageInfo ("Usage: " ++ prog ++ " [OPTION...]") options
                                                    throw (e::SomeException))
                       _ -> errorWithoutStackTrace $ usageInfo ("Usage: " ++ prog ++ " [OPTION...]") options
    if action == ActionVersion
       then do putStrLn $ "OVSDB-to-DDlog compiler " ++ dDLOG_VERSION ++ " (" ++ gitHash ++ ")"
               putStrLn $ "Copyright (c) 2019 VMware, Inc. (MIT License)"
       else do
           dlschema <- render <$> compileSchemaFile ovsSchemaFile outputTables
           case outputFile of
                Nothing -> putStrLn dlschema
                Just ofile -> writeFile ofile dlschema
           return ()
