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

{-# LANGUAGE RecordWildCards, ImplicitParams, LambdaCase, FlexibleContexts, TemplateHaskell #-}

import Control.Exception
import Control.Monad
import Data.List
import System.Console.GetOpt
import System.Environment
import System.FilePath.Posix
import Text.Parsec

import Language.DifferentialDatalog.Debugger.DebugTypes
import Language.DifferentialDatalog.Debugger.DebugState
import Language.DifferentialDatalog.Debugger.DebugEventParser
import Language.DifferentialDatalog.Module
import Language.DifferentialDatalog.Syntax

data TOption = Help
             | DebugDumpFile String
             | Datalog String
             | LibDir String
             | OutputDir String

options :: [OptDescr TOption]
options = [ Option ['h'] ["help"]             (NoArg Help)                          "Display help message."
          , Option ['d'] []                   (ReqArg DebugDumpFile "DEBUG_FILE")   "Debug dumped file."
          , Option ['i'] []                   (ReqArg Datalog  "FILE")              "DDlog program to compile."
          , Option ['L'] []                   (ReqArg LibDir   "PATH")              "Extra DDlog library directory."
          , Option ['o'] ["output-dir"]       (ReqArg OutputDir "DIR")              "Output directory (default based on program name)."
          ]

addOption :: Config -> TOption -> IO Config
addOption config (DebugDumpFile f) = return config { confDebugDumpFile = f}
addOption config (Datalog f)       = return config { confDatalogFile  = f}
addOption config (LibDir d)        = return config { confLibDirs = nub (d:confLibDirs config)}
addOption config (OutputDir d)    = return config { confOutputDir = d }
addOption config Help              = return config { confAction = ActionHelp}

validateConfig :: Config -> IO ()
validateConfig Config{..} = do
    when (confDatalogFile == "" && confAction /= ActionHelp && confAction /= ActionVersion)
         $ errorWithoutStackTrace "input file not specified"

main :: IO ()
main = do
       args <- getArgs
       prog <- getProgName
       home <- lookupEnv "DDLOG_HOME"
       config <- case getOpt Permute options args of
                      (flags, [], []) -> do
                                            conf <- foldM addOption defaultConfig flags
                                            validateConfig conf
                                            return conf
                                         `catch`
                                         (\e -> do putStrLn $ usageInfo ("Usage: " ++ prog ++ " [OPTION...]") options
                                                   throw (e::SomeException))
                      _ -> errorWithoutStackTrace $ usageInfo ("Usage: " ++ prog ++ " [OPTION...]") options
       config' <- case home of
           Just(p) -> addOption config (LibDir $ p ++ "/lib")
           _       -> return config
       do
            datalogProg <- parseProgram config'
            events <- parseEventFromDumpFile config'
            let recordMap = handleDebugEvents events emptyDebuggerMaps datalogProg
                s = queryAll events (dbgRecordMap recordMap) datalogProg
            dumpQueryResultToFile config' s

queryAll :: [Event] -> DebuggerRecordMap -> DatalogProgram-> String
queryAll [] _ _ = "\n"
queryAll (event: events) dgbRecordMap prog =
    let outputRecord = evtOutput event
        operatorId = evtOperatorId event
        operatorInput = InputOp operatorId
        dbgRecord = DebuggerRecord {dbgRecord=outputRecord, dbgOperatorId=operatorInput}
        dbgRecordNodes = queryDerivations dbgRecord dgbRecordMap prog
    in (show dbgRecordNodes) ++ "\n\n" ++ (queryAll events dgbRecordMap prog)

parseProgram :: Config -> IO (DatalogProgram)
parseProgram Config{..} = do
    fdata <- readFile confDatalogFile
    (d, _, _) <- parseDatalogProgram (takeDirectory confDatalogFile:confLibDirs) True fdata confDatalogFile
    return d

parseEventFromDumpFile :: Config -> IO ([Event])
parseEventFromDumpFile Config{..} = do
    contents <- readFile confDebugDumpFile
    return (parseDebugEvents contents)

parseDebugEvents :: String -> [Event]
parseDebugEvents inputString = do
    case parse eventsParser "" inputString of
        Left e      -> errorWithoutStackTrace $ "Failed to parse input file: " ++ show e
        Right r     -> r

dumpQueryResultToFile :: Config -> String -> IO()
dumpQueryResultToFile Config{..} content =
    case confOutputDir of
        "" -> return ()
        _ -> writeFile confOutputDir content
