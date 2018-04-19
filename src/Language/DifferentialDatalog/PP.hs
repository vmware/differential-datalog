{-
Copyright (c) 2018 VMware, Inc.

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

