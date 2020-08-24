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

{- |
Module     : Config
Description: Declares the 'Config' data type that describes compiler configuration.
-}

module Language.DifferentialDatalog.Config  where

data DLAction = ActionCompile
              | ActionValidate
              | ActionHelp
              | ActionVersion
              deriving Eq

data Config = Config { confDatalogFile     :: FilePath
                     , confAction          :: DLAction
                     , confLibDirs         :: [FilePath]
                     , confOutputDir       :: FilePath
                     , confOutputInput     :: String
                     , confStaticLib       :: Bool
                     , confDynamicLib      :: Bool
                     , confJava            :: Bool
                     , confOutputInternal  :: Bool
                     , confDebugHooks      :: Bool
                     , confDumpFlat        :: Bool
                     , confDumpValid       :: Bool
                     , confDumpDebug       :: Bool
                     , confDumpOpt         :: Bool
                     , confReValidate      :: Bool
                     , confOmitProfile     :: Bool
                     , confOmitWorkspace   :: Bool
                     , confRunRustfmt      :: Bool
                     , confRustFlatBuffers :: Bool
                     }

defaultConfig :: Config
defaultConfig = Config { confDatalogFile     = ""
                       , confAction          = ActionCompile
                       , confLibDirs         = []
                       , confOutputDir       = ""
                       , confStaticLib       = True
                       , confDynamicLib      = False
                       , confOutputInternal  = False
                       , confOutputInput     = ""
                       , confJava            = False
                       , confDebugHooks      = False
                       , confDumpFlat        = False
                       , confDumpValid       = False
                       , confDumpDebug       = False
                       , confDumpOpt         = False
                       , confReValidate      = False
                       , confOmitProfile     = False
                       , confOmitWorkspace   = False
                       , confRunRustfmt      = False
                       , confRustFlatBuffers = False
                       }
