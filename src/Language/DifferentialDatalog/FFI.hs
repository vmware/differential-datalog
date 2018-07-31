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

{- | 
Module     : FFI
Description: Generate C foreign function interface to Rust programs compiled from Datalog
-}

{-# LANGUAGE RecordWildCards, OverloadedStrings #-}

module Language.DifferentialDatalog.FFI (
) where

import Text.PrettyPrint
import Data.Maybe

import Language.DifferentialDatalog.Syntax
import Language.DifferentialDatalog.Type
import Language.DifferentialDatalog.PP

import {-# SOURCE #-} Language.DifferentialDatalog.Compile

-- Build FFI interface to a struct
--
-- Must be called once for each normalized struct that appears in
-- rules or relations.
--
-- 't' must be a TUser type pointing to a struct with all type
-- arguments assigned to concrete types. 
--
-- 't' must be normalized
ffiMkStruct :: DatalogProgram -> Type -> Doc
ffiMkStruct d t =
    (vcat $ map mkcons $ typeCons t')                                          $$
    "#[no_mangle]"                                                             $$
    "pub extern \"C\" fn" <+> tname <> "_free(x:" <+> ffiMutCType d t <> ") {" $$
    "    if x.is_null() { return; }"                                           $$
    "    unsafe { Box::from_raw(x); }"                                         $$
    "}"
    where
    t' = typ' d t
    isstruct = isStructType t'
    tname = mkValConstructorName' d t'

    mkcons :: Constructor -> Doc
    mkcons c@Constructor{..} = 
        "#[no_mangle]"                                                                                                   $$
        "pub extern \"C\" fn" <+> tname <> "_" <> pp consName <> "_new(" <> args <> ") -> *mut" <+> ffiCType d t <+> "{" $$
        (nest' nullcheck)                                                                                                $$
        "    Box::into_raw(Box::new(" <> cname <> "{" <> args' <> "}))"                                                  $$
        "}"
        where 
        cname = mkConstructorName t' (name c)
        args = map (\a -> pp (name a) <> ":" <+> ffiConstCType d a) consArgs
        args' = map (\a -> ffiClone d (name a) (typ a)) consArgs
        null_checks = mapMaybe (\a -> ffiNullCheck d (name a) (typ a)) consArgs
        null_check = if null null_checks
                        then empty
                        else "if" <+> (hcat $ punctuate " || " null_checks) <+> "{" $$
                             "    return ptr::null_mut();"                          $$
                             "};"

ffiNullCheck :: DatalogProgram -> String -> Type -> Maybe Doc
ffiCType :: (WithType a) => DatalogProgram -> a -> Doc
ffiConstCType :: (WithType a) => DatalogProgram -> a -> Doc
ffiMutCType :: (WithType a) => DatalogProgram -> a -> Doc
ffiIsPtr :: DatalogProgram -> String -> Type -> Bool
ffiClone :: DatalogProgram -> String -> Type -> Doc

