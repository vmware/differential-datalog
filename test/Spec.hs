import Test.Tasty
import Test.Tasty.Golden
import System.FilePath
import Test.HUnit
import Data.List
import Control.Exception

import Language.DifferentialDatalog.Parse
import Language.DifferentialDatalog.Syntax

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


-- test Datalog parser and pretty printer.
-- as a side-effect it must create the file with the name specified
testParser :: FilePath -> FilePath -> IO ()
testParser fname ofname = do
    -- if a file contains .fail. in it's name it indicates a test
    -- that is supposed to fail during compilation.
    let shouldFail = isInfixOf ".fail." fname
    if shouldFail
      then do
        -- if the file should fail we expect an exception.
        -- the exception message is the expected output
        out <- (show <$> parseDatalogFile fname) `catch` (\e -> return $ show (e::SomeException))
        writeFile ofname out
      else do
        -- parse Datalog file and output its AST
        prog <- parseDatalogFile fname
        writeFile ofname (show prog)
        -- parse reference output
        prog' <- parseDatalogFile ofname
        -- expect the same result
        assertEqual "Pretty-printed Datalog differs from original input" prog prog'
    return ()
