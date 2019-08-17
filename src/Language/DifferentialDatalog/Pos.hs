{-
Copyright (c) 2018 VMware, Inc.
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

{-# LANGUAGE TypeSynonymInstances, FlexibleInstances #-}

module Language.DifferentialDatalog.Pos(
        Pos, 
        WithPos(..),
        spos,
        nopos,
        posInside,
        withPos,
        withPosMany) where

import Text.Parsec
import Text.Parsec.Pos

type Pos = (SourcePos,SourcePos)

class WithPos a where
    pos   :: a -> Pos
    atPos :: a -> Pos -> a

instance WithPos Pos where
    pos       = id
    atPos _ p = p

spos :: (WithPos a) => a -> String
spos x = let (s,e) = pos x
         in sourceName s ++ ":" ++ (show $ sourceLine s) ++ ":" ++ (show $ sourceColumn s) ++ "-"
                                ++ (show $ sourceLine e) ++ ":" ++ (show $ sourceColumn e)

nopos::Pos 
nopos = (initialPos "",initialPos "")

posInside :: SourcePos -> Pos -> Bool
posInside p (p1, p2) = sourceLine p >= sourceLine p1 && sourceLine p <= sourceLine p2 &&
                       (if sourceLine p == sourceLine p1
                           then sourceColumn p >= sourceColumn p1
                           else True) &&
                       (if sourceLine p == sourceLine p2
                           then sourceColumn p <= sourceColumn p2
                           else True)

withPos :: (WithPos b, Monad m) => ParsecT s u m b -> ParsecT s u m b
withPos x = (\ s a e -> atPos a (s,e)) <$> getPosition <*> x <*> getPosition

withPosMany :: (WithPos b, Monad m) => ParsecT s u m [b] -> ParsecT s u m [b]
withPosMany x = (\ s as e -> map (\a -> atPos a (s,e)) as) <$> getPosition <*> x <*> getPosition
