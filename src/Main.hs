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
    prog <- parseDatalogFile fname
--    case validate prog of
--         Left e  -> error $ "Validation error: " ++ e
--         Right _ -> return ()
    putStrLn "Validation complete"
    return prog

