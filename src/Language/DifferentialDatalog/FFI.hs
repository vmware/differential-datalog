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

import Language.DifferentialDatalog.PP
import Language.DifferentialDatalog.Name
import Language.DifferentialDatalog.Syntax
import Language.DifferentialDatalog.Type

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
    (vcat $ map mkCons $ typeCons t')                                          $$
    "#[no_mangle]"                                                             $$
    "pub extern \"C\" fn" <+> tname <> "_free(x:" <+> ffiMutCType d t <> ") {" $$
    "    if x.is_null() { return; }"                                           $$
    "    unsafe { Box::from_raw(x); }"                                         $$
    "}"
    where
    t' = typ' d t
    tname = mkValConstructorName' d t

    mkCons :: Constructor -> Doc
    mkCons c@Constructor{..} = 
        "#[no_mangle]"                                                                                                   $$
        "pub extern \"C\" fn" <+> tname <> "_" <> pp consName <> "_new(" <> args <> ") -> *mut" <+> ffiCType d t <+> "{" $$
        (nest' null_check)                                                                                               $$
        "    Box::into_raw(Box::new(" <> cname <> "{" <> args' <> "}))"                                                  $$
        "}"                                                                                                              $$
        (vcat $ map mkAccessor consArgs)
        where 
        cname = mkConstructorName (typeName t) t' (name c)
        args = hsep $ punctuate comma $ map (\a -> pp (name a) <> ":" <+> ffiConstCType d (typ a)) consArgs
        args' = hsep $ punctuate comma $ map (\a -> ffiClone d (name a) (typ a)) consArgs
        null_checks = mapMaybe (\a -> ffiNullCheck d (name a) (typ a)) consArgs
        null_check = if null null_checks
                        then empty
                        else "if" <+> (hcat $ punctuate " || " null_checks) <+> "{" $$
                             "    return ptr::null_mut();"                          $$
                             "};"
    
    mkAccessor :: Field -> Doc
    mkAccessor f@Field{..} = 
        "#[no_mangle]"                                                                                                                 $$
        "pub extern \"C\" fn" <+> tname <> "_" <> c <> fname <> "(x: *const" <+> tname <>") ->" <+> ffiConstCType d (typ f) <+> "{"    $$
        "    let x = unsafe{&*x};"                                                                                                     $$
        "    &x." <> fname <+> "as *const Int"                                                                                         $$
        "}"
        where
        fname = pp $ name f
        c = if isStructType t'
               then pp (name c) <> "_"
               else empty
        ret = case typ f of 
                  TString{} -> x <> fname <>
                  _ | ffiIsScalar d (typ f)
                    -> "x." <> fname
                    | otherwise
                    -> "&x." <> fname <+> "as *const "


-- True if 't' is represented as a scalar value in C.
ffiIsScalar :: DatalogProgram -> Type -> Bool
ffiIsScalar d t = 
    case typ' d t of
         TBool{}  -> True
         TBit{..} | typeWidth <= 64
                  -> True
         _        -> False

-- Generate a NULL-check for pointer variables 't' 
--
-- TODO: return Nothing for option_t
ffiNullCheck :: DatalogProgram -> String -> Type -> Maybe Doc
ffiNullCheck d v t | ffiIsScalar d t = Nothing
                   | otherwise       = Just $ pp v <> ".is_null()"

-- Type used to represent 'typ x' in the C API.
ffiCType :: (WithType a) => DatalogProgram -> a -> Doc
ffiCType d x = ffiCType' $ typeNormalize d $ typ x

ffiCType' :: Type -> Doc
ffiCType' TString{} = "c_char"
ffiCType' t         = mkType t

-- const pointer to type 't'
ffiConstCType :: DatalogProgram -> Type -> Doc
ffiConstCType d t | ffiIsScalar d t = ffiCType d t
                  | otherwise       = "* const" <+> ffiCType d t

-- mutable pointer to type 't'
ffiMutCType :: DatalogProgram -> Type -> Doc
ffiMutCType d t | ffiIsScalar d t = error $ "FFI.ffiMutCType called for scalar type " ++ show t
                | otherwise       = "* mut" <+> ffiCType d t

-- clone variable for use in the Rust world. 
-- Note: currently exits the function returning NULL if v is not a
-- valid UTF8 string.  Can be parameterized with this behavior if
-- needed.
ffiClone :: DatalogProgram -> String -> Type -> Doc
ffiClone d v t = 
    case typ' d t of
         TString{}           -> "(match (unsafe { CStr::from_ptr(" <> pp v <> ")}.to_str()) {Ok(" <> pp v <> ") => " <> pp v <> ".to_string(), _ => {return ptr::null_mut()} })"
         _ | ffiIsScalar d t -> pp v <> ".clone()"
           | otherwise       -> "unsafe{(*" <> pp v <> ").clone()}"
