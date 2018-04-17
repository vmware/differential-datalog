import Test.Tasty
import Test.Tasty.Golden
import System.FilePath
import Test.HUnit
import Data.List
import Data.List.Split
import Control.Exception

import Language.DifferentialDatalog.Parse
import Language.DifferentialDatalog.Syntax
import Language.DifferentialDatalog.Validate

main :: IO ()
main = defaultMain =<< goldenTests

goldenTests :: IO TestTree
goldenTests = do
  dlFiles <- findByExtension [".dl"] "./test/datalog_tests"
  return $ testGroup "datalog parser tests"
    [ goldenVsFile (takeBaseName dlFile) expect output (testParser dlFile output)
    | dlFile <- dlFiles 
    , let expect = replaceExtension dlFile ".ast.expected"
    , let output = replaceExtension dlFile ".ast"]

parseValidate :: FilePath -> String -> IO DatalogProgram
parseValidate file program = do 
    d <- parseDatalogString program file
    case validate d of 
         Left e  -> errorWithoutStackTrace $ "error: " ++ show e
         Right _ -> return d 

-- compile a program that is supposed to fail compilation
compileFailingProgram :: String -> String -> IO String
compileFailingProgram file program =
    (show <$> parseValidate file program) `catch`
             (\e -> return $ show (e::SomeException))

-- test Datalog parser and pretty printer.
-- as a side-effect it must create the file with the name specified
testParser :: FilePath -> FilePath -> IO ()
testParser fname ofname = do
    -- if a file contains .fail. in it's name it indicates a test
    -- that is supposed to fail during compilation.
    body <- readFile fname
    let shouldFail = ".fail." `isInfixOf` fname
    if shouldFail
      then do
        -- To allow multiple negative tests in a single dl file
        -- we treat the file as multiple files separated by this comment
        let parts = splitOn "//---" body
        -- if the file should fail we expect an exception.
        -- the exception message is the expected output
        out <- mapM (compileFailingProgram fname) parts
        writeFile ofname (intercalate "" out)
      else do
        -- parse Datalog file and output its AST
        prog <- parseValidate fname body
        writeFile ofname (show prog)
        -- parse reference output
        prog' <- parseDatalogFile ofname
        -- expect the same result
        assertEqual "Pretty-printed Datalog differs from original input" prog prog'
    return ()
