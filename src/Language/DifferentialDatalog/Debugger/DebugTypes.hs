{-
Copyright (c) 2020 VMware, Inc.
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

{-# LANGUAGE RecordWildCards, FlexibleContexts, LambdaCase, TupleSections, OverloadedStrings, TemplateHaskell, QuasiQuotes, ImplicitParams, NamedFieldPuns #-}

module Language.DifferentialDatalog.Debugger.DebugTypes where

data Operator = OpMap | OpAggregate | OpCondition | OpJoin
                | OpAntijoin | OpInspect | OpUndefined deriving (Show)

data OperatorId = OperatorId {ruleIdx:: Int, rhsIdx::Int, headIdx::Int} deriving (Show, Eq, Ord)

data Event = DebugEvent { evtOperatorId :: OperatorId
                        , evtWeight :: Int
                        , evtTimestamp :: Integer
                        , evtOperator :: Operator
                        , evtInput :: Record
                        , evtOutput :: Record
                        }
            | DebugJoinEvent { evtOperatorId :: OperatorId
                             , evtWeight :: Int
                             , evtTimestamp :: Integer
                             , evtOperator :: Operator
                             , evtInput1 :: Record
                             , evtInput2 :: Record
                             , evtOutput :: Record
                             }
            deriving (Show)

data Record = IntRecord {intVal :: Integer}
            | BoolRecord {boolVal :: Bool}
            | DoubleRecord {doubleVal :: Double}
            | StringRecord {stringVal :: String}
            | NamedStructRecord {name :: String, val :: [(String, Record)]}
            | TupleRecord {tupleVal :: [Record]}
            | ArrayRecord {arrayVal :: [Record]}
            deriving (Show, Eq, Ord)

data DLAction = ActionCompile
              | ActionValidate
              | ActionHelp
              | ActionVersion
              deriving Eq

data Config = Config { confDatalogFile     :: FilePath
                     , confAction          :: DLAction
                     , confLibDirs         :: [FilePath]
                     , confOutputDir       :: FilePath
                     , confDebugDumpFile :: FilePath
                     }

defaultConfig :: Config
defaultConfig = Config { confDatalogFile     = ""
                       , confAction          = ActionCompile
                       , confLibDirs         = []
                       , confOutputDir       = ""
                       , confDebugDumpFile   = ""
                       }
