{-# LANGUAGE TypeSynonymInstances, FlexibleInstances #-}

module Language.DifferentialDatalog.Pos(
        Pos, 
        WithPos(..),
        spos,
        nopos,
        posInside) where

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

