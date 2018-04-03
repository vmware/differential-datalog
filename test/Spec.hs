import Test.Tasty
import Test.Tasty.Golden
import System.FilePath
import Test.HUnit

import Language.DifferentialDatalog.Parse

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


-- test Datalog parser and pretty printer
testParser :: FilePath -> FilePath -> IO ()
testParser fname ofname = do
    -- parse Datalog file and output its AST
    prog <- parseDatalogFile fname
    writeFile ofname (show prog)
    -- parse AST, expect the same result
    prog' <- parseDatalogFile ofname
    assertEqual "Pretty-printed Datalog differs from original input" prog prog'
    return ()
