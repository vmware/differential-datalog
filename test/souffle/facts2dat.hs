{-# LANGUAGE ScopedTypeVariables #-}

import System.Environment
import Data.Bits.Utils

import Data.Csv

import Data.List
import qualified Data.Vector as V
import qualified Data.ByteString.Lazy as BL

encodeDDL :: String -> [[String]] -> String
encodeDDL label sss = 
  "start;\n" 
  ++ intercalate ",\n" (map wrapRel sss) ++ ";\n"
  ++ "commit;\ndump;\n"
  where wrapRel ss = 
          "insert " 
          ++ label 
          ++ "(" ++ (intercalate "," (map quote ss)) ++ ")"
        quote s = "\"" ++ s ++ "\""

decodeCsv :: BL.ByteString -> IO (V.Vector (V.Vector String))
decodeCsv csvData =
    case decodeWith tabs NoHeader csvData of
        Left err -> error err
        Right (l::V.Vector (V.Vector String)) -> return l
  where tabs = defaultDecodeOptions { decDelimiter = c2w8 '\t' }

main :: IO ()
main = do
    [fn] <- getArgs
    csvData <- BL.readFile fn
    let label = takeWhile (/= '.') fn
    l <- decodeCsv csvData 
    putStrLn $ encodeDDL label (V.toList (V.map V.toList l))
