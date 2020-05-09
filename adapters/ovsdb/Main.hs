{-
Copyright (c) 2018-2020 VMware, Inc.
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

data TOption = OVSFile         String
             | OutputTable     String
             | OutputOnlyTable String
             | ROColumn        String
             | RWColumn        String
             | ConfigJsonI     String
             | ConfigJsonO     String
             | OutputFile      String
             | Version

data Action = ActionCompile
            | ActionVersion
            deriving Eq

options :: [OptDescr TOption]
<<<<<<< HEAD
options = [ Option ['v'] ["version"]            (NoArg Version)                     "Display DDlog version."
          , Option ['f'] ["schema-file"]        (ReqArg OVSFile     "FILE")         "OVSDB schema file."
          , Option ['c'] ["input-config"]       (ReqArg ConfigJsonI "FILE.json")    "Read options from Json configuration file (preceding options are ignored)."
          , Option ['O'] ["output-config"]      (ReqArg ConfigJsonO "FILE.json")    "Write preceding options to Json configuration file."
          , Option ['o'] ["output-table"]       (ReqArg OutputTable "TABLE")        "Mark TABLE as output."
          , Option []    ["output-only-table"]  (ReqArg OutputTable "TABLE")        "Mark TABLE as output."
          , Option []    ["ro"]                 (ReqArg ROColumn    "TABLE.COLUMN") "Mark COLUMN as read-only.  If this option is specified for at least one column of TABLE, all TABLE columns that are not labeled read-only are presumed writable."
          , Option []    ["rw"]                 (ReqArg RWColumn    "TABLE.COLUMN") "Mark COLUMN as read-write.  If this option is specified for at least one column of TABLE, all TABLE columns that are not labeled read-write are presumed read-only.  This option is mutually exclusive with '--ro': at most one of the two options can be used for each TABLE."
          , Option []    ["output-file"]        (ReqArg OutputFile  "FILE.dl")      "Write output to FILE.dl. If this option is not specified, output will be written to stdout."
=======
options = [ Option ['v'] ["version"]            (NoArg Version)                         "Display DDlog version."
          , Option ['f'] ["schema-file"]        (ReqArg OVSFile         "FILE")         "OVSDB schema file."
          , Option ['c'] ["input-config"]       (ReqArg ConfigJsonI     "FILE.json")    "Read options from Json configuration file (preceding options are ignored)."
          , Option ['O'] ["output-config"]      (ReqArg ConfigJsonO     "FILE.json")    "Write preceding options to Json configuration file."
          , Option ['o'] ["output-table"]       (ReqArg OutputTable     "TABLE")        "Mark TABLE as output."
          , Option []    ["output-only-table"]  (ReqArg OutputOnlyTable "TABLE")        "Mark TABLE as output-only.  DDlog will send updates to this table directly to OVSDB without comparing it with current OVSDB state."
          , Option []    ["ro"]                 (ReqArg ROColumn        "TABLE.COLUMN") "Mark COLUMN as read-only."
          , Option []    ["output-file"]        (ReqArg OutputFile      "FILE.dl")      "Write output to FILE.dl. If this option is not specified, output will be written to stdout."
>>>>>>> 1c31a48... fixup
          ]

data Config = Config { ovsSchemaFile:: FilePath
                     , outputFile   :: Maybe FilePath
                     , outputTables :: [OutputRelConfig]
                     , outputOnlyTables :: [String]
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
addOption (a, config) (OutputOnlyTable t) = do
    when (isJust $ lookup t $ outputOnlyTables config)
         $ errorWithoutStackTrace $ "Conflicting options --output-table and --output-only-table specified for table '" ++ t ++ "'"
    return (a, config{ outputTables = nub ((t, Left []) : outputTables config)})
addOption (a, config) (OutputOnlyTable t) = do
    when (isJust $ lookup t $ outputTables config)
         $ errorWithoutStackTrace $ "Conflicting options --output-table and --output-only-table specified for table '" ++ t ++ "'"
    return (a, config{ outputOnlyTables = nub (t : outputOnlyTables config)})
addOption (a, config) (ROColumn c) = do
    case splitOn "." c of
         [table, col] -> do
            when (isNothing $ lookup table $ outputTables config)
                 $ errorWithoutStackTrace $ "Unknown output table name " ++ table
            outtabs <- mapM (\(t, restrictions) ->
                              if t == table
                                 then case restrictions of
                                           Left ro -> return (t, Left $ nub $ col:ro)
                                           Right _ -> errorWithoutStackTrace $ "Options '--rw' and '--ro' are mutually exclusive and cannot be both applied to table '" ++ table ++ "'"
                                 else return (t, restrictions))
                            $ outputTables config
            return $ (a, config{outputTables = outtabs})
         _ -> errorWithoutStackTrace $ "Invalid column name " ++ c
addOption (a, config) (RWColumn c) = do
    case splitOn "." c of
         [table, col] -> do
            when (isNothing $ lookup table $ outputTables config)
                 $ errorWithoutStackTrace $ "Unknown output table name " ++ table
            outtabs <- mapM (\(t, restrictions) ->
                              if t == table
                                 then case restrictions of
                                      Left (_:_) -> errorWithoutStackTrace $ "Options '--rw' and '--ro' are mutually exclusive and cannot be both applied to table '" ++ table ++ "'"
                                      Left []    -> return (t, Right [col])
                                      Right rw   -> return (t, Right $ nub $ col:rw)
                                 else return (t, restrictions))
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
