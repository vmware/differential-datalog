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
    ffiMkFFIInterface
) where

import Text.PrettyPrint
import Data.Maybe
import Data.List
import qualified Data.Map as M

import Language.DifferentialDatalog.Pos
import Language.DifferentialDatalog.Util
import Language.DifferentialDatalog.PP
import Language.DifferentialDatalog.Name
import Language.DifferentialDatalog.Syntax
import Language.DifferentialDatalog.Type

import {-# SOURCE #-} Language.DifferentialDatalog.Compile


ffiMkFFIInterface :: DatalogProgram -> Doc
ffiMkFFIInterface d = 
    vcat tagenums $$
    vcat structs
    where
    tagenums = mapMaybe (\tdef -> case tdefType tdef of
                                       Just t@TStruct{} -> ffiMkTagEnum d (name tdef) t
                                       _                -> Nothing)
               $ M.elems $ progTypedefs d
    structs = map (ffiMkStruct d)
              $ nub
              $ concatMap tstructs
              $ map relType
              $ M.elems $ progRelations d

    tstructs :: Type -> [Type]
    tstructs = nub . tstructs' . typeNormalize d

    tstructs' :: Type -> [Type]
    tstructs' TTuple{..}  = concatMap tstructs typeTupArgs
    tstructs' t@TUser{..} = t : (concatMap (concatMap (tstructs . typ) . consArgs) $ typeCons $ typ' d t)
    tstructs' TOpaque{..} = concatMap tstructs typeArgs
    tstructs' _           = []



-- Call once for each struct to generate a C-style for its tag enum.
-- Returns 'Nothing' for structs with unique constructor, which don't
-- need a tag.
ffiMkTagEnum :: DatalogProgram -> String -> Type -> Maybe Doc
ffiMkTagEnum d tname t@TStruct{..} | isStructType t = Nothing
                                   | otherwise = Just $
    "#[repr(C)]"                                                                              $$
    "enum" <+> tagEnumName (pp tname) <+> "{"                                                 $$
    (nest' $ vcat $ punctuate comma $ mapIdx (\c i -> pp (name c) <+> "=" <+> pp i) typeCons) $$
    "}"

-- Generates FFI interface to a normalized TStruct or TTuple type.
--
-- Assumes: 't' does not contain free type variables.
--          't' is normalized
--
-- Must be called for each struct and tuple type occurring in normalized 
-- input and output relations.
ffiMkStruct :: DatalogProgram -> Type -> Doc
ffiMkStruct d t@TUser{..}  = 
    mkStruct d (pp typeName) (cStructName d t) (typeCons $ typ' d t)
ffiMkStruct d t@TTuple{..} = 
    mkStruct d (error "FFI.ffiMkStruct: tuple should not need tag enum") (cStructName d t) 
               [Constructor nopos (error "FFI.ffiMkStruct: tuple should not need a tag") 
                $ mapIdx (\at i -> Field nopos ("x" ++ show i) at) typeTupArgs]
ffiMkStruct _ t          = error $ "FFI.ffiMkStruct " ++ show t

mkStruct :: DatalogProgram -> Doc -> Doc -> [Constructor] -> Doc
mkStruct d _ type_name [Constructor _ _ cfields] =
    "#[repr(C)]"                            $$
    "struct" <+> type_name <+> "{"          $$
    (nest' $ vcat $ punctuate comma fields) $$
    "}"
    where 
    fields = map (\f -> pp (name f) <> ":" <+> mkCType d (typeNormalize d $ typ f)) cfields

mkStruct d struct_name type_name cons  =
    vcat constructors                                                 $$
    "#[repr(C)]"                                                      $$
    "union" <+> union_name <+> "{"                                    $$
    (nest' $ vcat $ punctuate comma fields)                           $$
    "}"                                                               $$
    "#[repr(C)]"                                                      $$
    "struct" <+> type_name <+> "{"                                    $$
    "    tag:" <+> tagEnumName struct_name <> ","                     $$
    "    x:"   <+> union_name                                         $$
    "}"
    where 
    union_name = "__union_" <> type_name
    constructors = map (\c -> mkStruct d undefined (consStructName type_name c) [c]) cons
    fields = map (\c -> pp (name c) <> ": *const" <+> consStructName type_name c) cons

cStructName :: DatalogProgram -> Type -> Doc
cStructName d t = "__c_" <> mkValConstructorName' d t

consStructName :: Doc -> Constructor -> Doc
consStructName tname c = "__struct_" <> tname <> "_" <> (pp $ name c)

tagEnumName :: Doc -> Doc
tagEnumName tname = "__enum_" <> tname

mkCType :: DatalogProgram -> Type -> Doc
mkCType _ TBool{}        = "bool"
mkCType _ TInt{}         = "*const Int"
mkCType _ TString{}      = "*const c_char"
mkCType _ TBit{..}       | typeWidth <= 8  = "uint8_t"
                         | typeWidth <= 16 = "uint16_t"
                         | typeWidth <= 32 = "uint32_t"
                         | typeWidth <= 64 = "uint64_t"
                         | otherwise       = "*const Uint"
mkCType d t@TTuple{..}   = cStructName d t
mkCType d t@TUser{..}    = cStructName d t
mkCType d t@TOpaque{..}  = "*const" <+> mkValConstructorName' d t
mkCType _ t              = error $ "FFI.mkCType " ++ show t
