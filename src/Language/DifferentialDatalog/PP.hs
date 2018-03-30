{-# LANGUAGE TypeSynonymInstances, FlexibleInstances #-}

module Language.DifferentialDatalog.PP(
        PP(..),
        nest',
        braces') where

import Text.PrettyPrint

ppshift :: Int
ppshift = 4

-- pretty-printing class
class PP a where
    pp :: a -> Doc

instance PP Doc where
    pp = id

instance PP String where
    pp s = text s

instance PP Int where
    pp = int

instance PP Bool where
    pp True  = text "true"
    pp False = text "false"

instance PP Integer where
    pp = integer

instance (PP a) => PP (Maybe a) where
    pp Nothing = empty
    pp (Just x) = pp x

nest' :: Doc -> Doc
nest' = nest ppshift

braces' :: Doc -> Doc
braces' x = lbrace $+$ nest' x $+$ rbrace

