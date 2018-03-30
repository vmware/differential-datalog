{-# LANGUAGE OverloadedStrings #-}

module Language.DifferentialDatalog.Ops where

import Text.PrettyPrint

import Language.DifferentialDatalog.PP

data BOp = Eq
         | Neq
         | Lt
         | Gt
         | Lte
         | Gte
         | And
         | Or
         | Impl
         | Plus
         | Minus
         | Mod
         | ShiftR
         | ShiftL
         | BAnd
         | BOr
         | Concat
         deriving (Eq, Ord)

instance PP BOp where
    pp Eq     = "=="
    pp Neq    = "!="
    pp Lt     = "<"
    pp Gt     = ">"
    pp Lte    = "<="
    pp Gte    = ">="
    pp And    = "and"
    pp Or     = "or"
    pp Impl   = "=>"
    pp Plus   = "+"
    pp Minus  = "-"
    pp Mod    = "%"
    pp ShiftR = ">>"
    pp ShiftL = "<<"
    pp BAnd   = "&"
    pp BOr    = "|"
    pp Concat = "++"

bopReturnsBool :: BOp -> Bool
bopReturnsBool op = elem op [Eq, Neq, Lt, Gt, Lte, Gte, And, Or, Impl]

instance Show BOp where
    show = render . pp

data UOp = Not
         | BNeg
         deriving (Eq, Ord)

instance PP UOp where
    pp Not  = "not"
    pp BNeg = "~"

instance Show UOp where
    show = render . pp
