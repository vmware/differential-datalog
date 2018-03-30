import Test.Tasty (defaultMain, TestTree, testGroup)
import Test.Tasty.Golden (goldenVsString, findByExtension)

main :: IO ()
main = defaultMain =<< goldenTests

goldenTests :: IO TestTree
goldenTests = do
--  yamlFiles <- findByExtension [".yaml"] "."
  return $ testGroup "OVN tests"
    [    ]
