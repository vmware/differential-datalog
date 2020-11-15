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

{-# LANGUAGE FlexibleContexts, TupleSections #-}

module Language.DifferentialDatalog.Util where

import Prelude hiding(readFile, writeFile)
import Data.Graph.Inductive
import Control.Monad.Except
import Data.List
import Data.List.Split
import Data.Maybe
import Data.Bits
import Data.Word
import Data.Binary.Get
import qualified Data.Map as M
import qualified Data.ByteString.Lazy as BL
import System.Directory
import System.FilePath
import System.IO hiding (readFile, writeFile, stdout, stderr)
import System.Process
import System.Exit

if' :: Bool -> a -> a -> a
if' True  x _ = x
if' False _ y = y

-- Tuples
mapFst :: (a -> b) -> (a, c) -> (b, c)
mapFst f (x,y) = (f x,y)
mapSnd :: (a -> b) -> (c, a) -> (c, b)
mapSnd f (x,y) = (x,f y)

-- Find a cycle in a graph
grCycle :: Graph gr => gr a b -> Maybe [LNode a]
grCycle g = case mapMaybe (grCycleThroughNode g) (nodes g) of
                 []  -> Nothing
                 c:_ -> Just c

-- Find a cycle through a specified node.
grCycleThroughNode :: Graph gr => gr a b -> Node -> Maybe [LNode a]
grCycleThroughNode g n =
    listToMaybe $ map (\s -> map (\i -> (i, fromJust $ lab g i)) (n:(esp s n g))) $
                  filter (\s -> elem n (reachable s g)) $ suc g n

-- Group graph nodes; aggregate edges
grGroup :: (DynGraph gr) => gr a b -> [[Node]] -> gr [Node] b
grGroup g groups = insEdges ((concatMap (concatMap gsuc)) groups)
                   $ insNodes (zip [0..] groups) empty
    where nodegroup :: M.Map Node Int
          nodegroup = M.fromList $ concat $ mapIdx (\gr i -> map (,i) gr) groups
          gsuc node = map (\(_,s,l) -> (nodegroup M.! node, nodegroup M.! s, l)) $ out g node

--Logarithm to base 2. Equivalent to floor(log2(x))
log2 :: Integer -> Int
log2 0 = 0
log2 1 = 0
log2 n
    | n>1 = 1 + log2 (n `div` 2)
    | otherwise = error "log2: negative argument"

-- The number of bits required to encode range [0..i]
bitWidth :: (Integral a) => a -> Int
bitWidth i = 1 + log2 (fromIntegral i)

mapIdx :: (a -> Int -> b) -> [a] -> [b]
mapIdx f xs = map (uncurry f) $ zip xs [0..]

mapIdxM :: (Monad m) => (a -> Int -> m b) -> [a] -> m [b]
mapIdxM f xs = mapM (uncurry f) $ zip xs [0..]

mapIdxM_ :: (Monad m) => (a -> Int -> m ()) -> [a] -> m ()
mapIdxM_ f xs = mapM_ (uncurry f) $ zip xs [0..]

foldIdx :: (a -> b -> Int -> a) -> a -> [b] -> a
foldIdx f acc xs = foldl' (\acc' (x,idx) -> f acc' x idx) acc $ zip xs [0..]

foldIdxM :: (Monad m) => (a -> b -> Int -> m a) -> a -> [b] -> m a
foldIdxM f acc xs = foldM (\acc' (x,idx) -> f acc' x idx) acc $ zip xs [0..]

-- parse binary number
readBin :: String -> Integer
readBin s = foldl' (\acc c -> (acc `shiftL` 1) +
                              case c of
                                   '0' -> 0
                                   '1' -> 1
                                   _   -> error $ "readBin" ++ s) 0 s

-- Determine the most significant set bit of a non-negative number
-- (returns 0 if not bits are set)
msb :: (Bits b, Num b) => b -> Int
msb 0 = 0
msb 1 = 0
msb n = 1 + (msb $ n `shiftR` 1)

bitSlice :: (Bits a, Num a) => a -> Int -> Int -> a
bitSlice v h l = (v `shiftR` l) .&. (2^(h-l+1) - 1)

bitRange :: (Bits a, Num a) => Int -> Int -> a
bitRange h l = foldl' (\a i -> setBit a i) (fromInteger 0) [l..h]

-- Group elements in the list (unlike groupBy, this function groups all
-- equivalent elements, not just adjacent ones)
sortAndGroup :: (Ord b) => (a -> b) -> [a] -> [[a]]
sortAndGroup f = groupBy (\x y -> f x == f y) .
                 sortBy (\x y -> compare (f x) (f y))


--Returns all combinations of a list of lists. Example:
--allcomb [[1, 2], [3], [2, 3, 4]] = [[1, 3, 2], [1, 3, 3], [1, 3, 4], [2, 3, 2], [2, 3, 3], [2, 3, 4]]
allComb :: [[a]] -> [[a]]
allComb [] = [[]]
--allComb ([]:xs) = allComb xs
allComb (x:xs) = concatMap (h (allComb xs)) x
    where h xs' x' = map (x' :) xs'

checksum16 :: [Word16] -> Word16
checksum16 ws = let total = sum (map fromIntegral ws) :: Word32
                    total' = (total .&. 0xffff) + (total `shiftR` 16)
                    total'' = (total' .&. 0xffff) + (total' `shiftR` 16)
                in complement $ fromIntegral total''


checksum16BS :: BL.ByteString -> Word16
checksum16BS bs | l `mod` 2 == 0 = checksum16 $ runGet (sequence $ replicate (l `div` 2) getWord16be) bs
                | otherwise      = checksum16 $ runGet (sequence $ replicate ((l `div` 2) + 1)  getWord16be) $ BL.snoc bs 0
    where l = fromIntegral $ BL.length bs

removeIndices :: [a] -> [Int] -> [a]
removeIndices xs indices =
    foldl' (\xs' i -> take i xs' ++ drop (i+1) xs') xs $ reverse $ sort indices

-- Unicode-aware file access.  We deliberately overload standard function names
-- to force the user to hide non-unicode-aware versions and use these instead.

readFile :: FilePath -> IO String
readFile path = do
  h <- openFile path ReadMode
  hSetEncoding h utf8
  hGetContents h

writeFile :: FilePath -> String -> IO ()
writeFile path txt =
    withFile path WriteMode $ \h -> do
        hSetEncoding h utf8
        hPutStr h txt

-- Replace file content if changed
updateFile :: FilePath -> String -> IO ()
updateFile path content = do
    createDirectoryIfMissing True $ takeDirectory path
    exists <- doesFileExist path
    let tmppath = addExtension path "tmp"
    if exists
       then do
            oldcontent <- readFile path
            when (oldcontent /= content) $ do
                writeFile tmppath content
                renameFile tmppath path
       else writeFile path content

-- | Runs a command, exiting with an error for users if it fails
--
-- Emits an error in this format if the command fails to run:
-- ```
-- <process name> failed to exit with code <exit code>
-- while running `<command> <args>`
-- stdout:
-- <stdout>
-- stderr:
-- <stderr>
-- ```
--
-- 'process_name' - The name of the process being run, will be displayed to users if
--                  it fails
--
-- 'command' - The command to be run
--
-- 'args' - The arguments passed to the command
--
-- 'working_dir' - The working directory the command should be executed in, use `Nothing`
--                 for the current directory
runCommandReportingErr :: String -> FilePath -> [String] -> Maybe FilePath -> IO ()
runCommandReportingErr process_name command args working_dir = do
    let process = (proc command args) {cwd = working_dir}
    (exit_code, proc_stdout, proc_stderr) <- readCreateProcessWithExitCode process ""
    case exit_code of
        ExitSuccess -> return ()
        ExitFailure err_code ->
            errorWithoutStackTrace $
                process_name
                    ++ " failed with exit code "
                    ++ show err_code
                    ++ "\nwhile running `"
                    ++ (show command ++ " " ++ intercalate " " args)
                    ++ "`\n"
                    ++ "stdout:\n"
                    ++ proc_stdout
                    ++ "\nstderr:\n"
                    ++ proc_stderr

-- Replace all occurrences of 'old' with 'replacement' without using the
-- non-Windows-compatibles 'MissingH' crate.
replace :: (Eq a) => [a] -> [a] -> [a] -> [a]
replace old replacement = intercalate replacement . splitOn old
