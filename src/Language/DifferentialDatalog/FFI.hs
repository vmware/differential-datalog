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
    mkFFIInterface
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


mkFFIInterface :: DatalogProgram -> (Doc, Doc)
mkFFIInterface d =
    (vcat rtagenums $$
     vcat rstructs  $$
     rvalue,
     vcat ctagenums $$
     vcat cstructs  $$
     cvalue)
    where
    (rvalue, cvalue) = mkCValue d
    (rtagenums, ctagenums) = unzip
                             $ mapMaybe (\tdef ->
                                          case tdefType tdef of
                                               Just t@TStruct{} -> ffiMkTagEnum d (name tdef) t
                                               _                -> Nothing)
                             $ M.elems $ progTypedefs d
    (rstructs, cstructs) = unzip
                           $ map (ffiMkStruct d)
                           $ nub
                           $ concatMap tstructs
                           $ map relType
                           $ M.elems $ progRelations d

    tstructs :: Type -> [Type]
    tstructs = nub . tstructs' . typeNormalize d

    -- Order output to place dependency declarations before types that depend on them.
    -- Rust does not care, but C expects this.
    tstructs' :: Type -> [Type]
    tstructs' t@TTuple{..} = concatMap tstructs typeTupArgs ++ [t]
    tstructs' t@TUser{..}  = (concatMap (concatMap (tstructs . typ) . consArgs) $ typeCons $ typ' d t) ++ [t]
    tstructs' TOpaque{..}  = concatMap tstructs typeArgs
    tstructs' _            = []

mkCRelEnum :: DatalogProgram -> Doc
mkCRelEnum d =
    "enum Relations {"                                                                                                   $$
    (nest' $ vcat $ punctuate comma $ mapIdx (\rel i -> cRelEnumerator rel <+> "=" <+> pp i) $ M.keys $ progRelations d) $$
    "};"

mkCValue :: DatalogProgram -> (Doc, Doc)
mkCValue d = (rust, hdr)
    where
    rust =
        "#[repr(C)]"                                                           $$
        "pub union __union_c_Value {"                                          $$
        (nest' $ vcommaSep fields)                                             $$
        "}"                                                                    $$
        "#[repr(C)]"                                                           $$
        "pub struct __c_Value {"                                               $$
        "    tag: Relations,"                                                  $$
        "    val: __union_c_Value"                                             $$
        "}"                                                                    $$
        "impl __c_Value {"                                                     $$
        "    pub fn to_native(&self) -> Value {"                               $$
        "        match self.tag {"                                             $$
        (nest' $ nest' $ nest' $ vcommaSep matches)                            $$
        "        }"                                                            $$
        "    }"                                                                $$
        "    pub fn from_val(relid: RelId, val: &Value) -> Option<Self> {"     $$
        "        match relid {"                                                $$
        (nest' $ nest' $ nest' $ vcat $ map (<> ",") from_matches)             $$
        "            _ => None"                                                $$
        "        }"                                                            $$
        "    }"                                                                $$
        "}"                                                                    $$
        "#[no_mangle]"                                                         $$
        "pub extern \"C\" fn val_free(val: *mut __c_Value){"                   $$
        "    let mut val = unsafe{ Box::from_raw(val) };"                      $$
        "    match val.tag {"                                                  $$
        (nest' $ nest' $ vcommaSep free_matches)                               $$
        "    }"                                                                $$
        "}"                                                                    $$
        "pub fn val_to_ccode(relid: RelId, val: &Value) -> Option<String> {"   $$
        "   match relid {"                                                     $$
        (nest' $ nest' $ nest' $ vcat $ map (<> ",") c_matches)                $$
        "            _ => None"                                                $$
        "   }"                                                                 $$
        "}"
    hdr =
        mkCRelEnum d                                                           $$
        "union Value_union {"                                                  $$
        (nest' $ vcat c_fields)                                                $$
        "};"                                                                   $$
        "struct Value {"                                                       $$
        "    enum Relations tag;"                                              $$
        "    union Value_union val;"                                           $$
        "};"                                                                   $$
        "void val_free(struct Value *val);"

    rels = M.elems $ progRelations d
    fields = map (\rel -> let t = typeNormalize d rel in
                          rname (name rel) <> ":" <+> ffiScalarize t) rels
    c_fields = map (\rel -> let t = typeNormalize d rel in
                            cScalarize t <+> rname (name rel) <> ";") rels
    matches = map (\rel -> let t = typeNormalize d rel in
                           "Relations::" <> rname (name rel) <+> "=> Value::" <> mkValConstructorName' d t
                             <> "(" <> tonative (name rel) t <> ")") rels
    free_matches = map (\rel ->
                         let t = typeNormalize d rel
                             rn = rname $ name rel in
                         "Relations::" <> rn <+> "=> unsafe {"                                               $$
                         (if isStruct d t || isTuple d t
                             then "    <" <> mkType t <> ">::free(&mut *val.val." <> rn <> ");" $$
                                  "    Box::from_raw(val.val." <> rn <> ");"
                             else "    <" <> mkType t <> ">::free(&mut val.val." <> rn <> ");")              $$
                         "}") rels
    from_matches = mapIdx (\rel i ->
                           let t  = typeNormalize d rel
                               rn = rname $ name rel
                               ptr = isStruct d t || isTuple d t
                               toffi = if ptr
                                          then "Box::into_raw(Box::new(v.to_ffi()))"
                                          else "v.to_ffi()"
                           in
                           --"Relations::" <> rn <+> "=> {"                                                   $$
                           pp i <+> "=> {"                                                                  $$
                           "    match val {"                                                                $$
                           "        Value::" <> mkValConstructorName' d t <> "(v) => Some("                 $$
                           "            __c_Value {"                                                        $$
                           "                tag: Relations::" <> rn <> ","                                  $$
                           "                val: __union_c_Value{" <> rn <> ":" <+> toffi <> "},"           $$
                           "            }),"                                                                $$
                           "        v => {"                                                                 $$
                           "            eprintln!(\"__c_Value::from_val: Invalid value {}\", *v);"          $$
                           "            None"                                                               $$
                           "        }"                                                                      $$
                           "    }"                                                                          $$
                           "}") rels
    c_matches = mapIdx (\rel i ->
                         let t  = typeNormalize d rel
                             rn = name rel
                             ref = if isStruct d t || isTuple d t then "&" else empty
                         in
                         pp i <+> "=> {"                                                                  $$
                         "    match val {"                                                                $$
                         "        Value::" <> mkValConstructorName' d t <> "(v) => Some("                 $$
                         "            format!(\"&(struct Value){{.tag= " <> cRelEnumerator rn <> ", .val= (union Value_union){{."
                                              <> rname rn <> "=" <+> ref <> "{} }} }}\", v.c_code())"     $$
                         "        ),"                                                                     $$
                         "        v => {"                                                                 $$
                         "            eprintln!(\"val_to_ccode: Invalid value {}\", *v);"                 $$
                         "            None"                                                               $$
                         "        }"                                                                      $$
                         "    }"                                                                          $$
                         "}") rels
    -- rust currently does not allow non-scalar types in unions
    ffiScalarize :: Type -> Doc
    ffiScalarize t =
        case t of
             TTuple{} -> "*mut" <+> t'
             TUser{}  -> "*mut" <+> t'
             _        -> t'
        where t' = mkFFIType d t
    cScalarize :: Type -> Doc
    cScalarize t =
        case t of
             TTuple{} -> t' <> "*"
             TUser{}  -> t' <> "*"
             _        -> t'
        where t' = mkCType d t
    tonative :: String -> Type -> Doc
    tonative relname t =
        case t of
             TBool{}        -> n
             TInt{}         -> borrown <> ".clone()"
             TString{}      -> "unsafe { CStr::from_ptr(" <> n <> ")}.to_str().unwrap().to_string()"
             TBit{..}       | typeWidth <= 8  -> n <+> "as u8"
                            | typeWidth <= 16 -> n <+> "as u16"
                            | typeWidth <= 32 -> n <+> "as u32"
                            | typeWidth <= 64 -> n <+> "as u64"
                            | typeWidth <= 128-> n <+> ".to_u128()"
                            | otherwise       -> borrown <> ".clone()"
             TTuple{}       -> borrown <> ".to_native()"
             TUser{..}      -> borrown <> ".to_native()"
             TOpaque{..}    -> borrown <> ".clone()"
             t              -> error $ "FFI.tonative " ++ show t
        where
        n = "self.val." <> rname relname
        borrown = "unsafe{&*" <> n <> "}"

-- Call once for each struct to generate a C-style for its tag enum.
-- Returns 'Nothing' for structs with unique constructor, which don't
-- need a tag.
ffiMkTagEnum :: DatalogProgram -> String -> Type -> Maybe (Doc, Doc)
ffiMkTagEnum d tname t@TStruct{..} | isStructType t = Nothing
                                   | otherwise = Just (rust, hdr)
    where
    rust = "#[repr(C)]"                                                                    $$
           "pub enum" <+> ffiTagEnumName tname <+> "{"                                     $$
           (nest' $ vcommaSep $ mapIdx (\c i -> rname (name c) <+> "=" <+> pp i) typeCons) $$
           "}"
    hdr = "enum" <+> cTagEnumName tname <+> "{"                                            $$
          (nest' $ vcommaSep $ mapIdx (\c i -> rname (name c) <+> "=" <+> pp i) typeCons)  $$
          "};"

-- Generates FFI interface to a normalized TStruct or TTuple type,
-- including Rust and C declarations, and C forward declaration
-- to be included at the top of the header file.
--
-- Assumes: 't' does not contain free type variables.
--          't' is normalized
--
-- Must be called for each struct and tuple type occurring in normalized
-- input and output relations.
ffiMkStruct :: DatalogProgram -> Type -> (Doc, Doc)
ffiMkStruct d t@TUser{..}  =
    (rust $$ mkToFFIStruct d t, hdr)
    where
    (rust, hdr) = mkStruct d t (cStructName d t) (ffiStructName d t) (typeCons $ typ' d t)
ffiMkStruct d t@TTuple{..} =
    (rust $$ mkToFFITuple d t, hdr)
    where
    (rust, hdr) = mkStruct d t (cStructName d t) (ffiStructName d t)
                  [Constructor nopos (error "FFI.ffiMkStruct: tuple should not need a tag")
                   $ mapIdx (\at i -> Field nopos ("x" ++ show i) at) typeTupArgs]
ffiMkStruct _ t            = error $ "FFI.ffiMkStruct " ++ show t

mkToFFITuple :: DatalogProgram -> Type -> Doc
mkToFFITuple d t@TTuple{..} =
    "impl ToFFI for" <+> mkType t <+> "{"                                                   $$
    "    type FFIType =" <+> mkFFIType d t <> ";"                                           $$
    "    fn to_ffi(&self) -> Self::FFIType {"                                               $$
    (nest' $ nest' $ ffiStructName d t <> "{" <> commaSep fields <> "}")                    $$
    "    }"                                                                                 $$
    "    fn free(x: &mut Self::FFIType) {"                                                  $$
    (nest' $ nest' $ vcat free_fields)                                                      $$
    "    }"                                                                                 $$
    "    fn c_code(&self) -> String {"                                                      $$
    "        format!(\"(" <> mkCType d t <> "){{" <> (commaSep $ replicate (length typeTupArgs) "{}")
                     <> "}}\"," <+> commaSep c_fields <> ")"                                $$
    "    }"                                                                                 $$
    "}"
    where
    fields = mapIdx (\at i -> "x" <> pp i <> ": self." <> pp i <> ".to_ffi()") $ typeTupArgs
    free_fields = mapIdx (\at i -> "<" <> mkType at <> ">::free(&mut x.x" <> pp i <> ");") $ typeTupArgs
    c_fields = mapIdx (\at i -> "self." <> pp i <> ".c_code()") $ typeTupArgs

mkToFFIStruct :: DatalogProgram -> Type -> Doc
mkToFFIStruct d t@TUser{..} | isStructType t' =
    "impl ToFFI for" <+> mkType t <+> "{"                                                   $$
    "    type FFIType =" <+> mkFFIType d t <> ";"                                           $$
    "    fn to_ffi(&self) -> Self::FFIType {"                                               $$
    (nest' $ nest' $ ffiStructName d t <> "{" <> commaSep fields <> "}")                    $$
    "    }"                                                                                 $$
    "    fn free(x: &mut Self::FFIType) {"                                                  $$
    (nest' $ nest' $ vcat free_fields)                                                      $$
    "    }"                                                                                 $$
    "    fn c_code(&self) -> String {"                                                      $$
    "        format!(\"(" <> mkCType d t <> "){{" <> (commaSep $ replicate (length cargs) "{}")
                     <> "}}\"," <+> commaSep c_fields <> ")"                                $$
    "    }"                                                                                 $$
    "}"
    where
    cargs = consArgs $ head $ typeCons t'
    fields = map (\a -> pp (name a) <> ": self." <> pp (name a) <> ".to_ffi()") cargs
    free_fields = map (\a -> "<" <> mkType (typeNormalize d a) <> ">::free(&mut x." <> pp (name a) <> ");") cargs
    c_fields = map (\a -> "self." <> pp (name a) <> ".c_code()") cargs
    t' = typ' d t

mkToFFIStruct d t@TUser{..} =
    "impl ToFFI for" <+> mkType t <+> "{"                                                   $$
    "    type FFIType =" <+> cstruct <> ";"                                                 $$
    "    fn to_ffi(&self) -> Self::FFIType {"                                               $$
    "        match self {"                                                                  $$
    (nest' $ nest' $ nest' $ vcommaSep matches)                                             $$
    "        }"                                                                             $$
    "    }"                                                                                 $$
    "    fn free(x: &mut Self::FFIType) {"                                                  $$
    "        match x.tag {"                                                                 $$
    (nest' $ nest' $ nest' $ vcommaSep free_matches)                                        $$
    "        }"                                                                             $$
    "    }"                                                                                 $$
    "    fn c_code(&self) -> String {"                                                      $$
    "        match self {"                                                                  $$
    (nest' $ nest' $ nest' $ vcommaSep c_matches)                                           $$
    "        }"                                                                             $$
    "    }"                                                                                 $$
    "}"
    where
    t' = typ' d t
    cstruct = ffiStructName d t
    matches = map (\c ->
                    let cname = rname $ name c in
                    mkConstructorName typeName t' (name c) <>
                    "{" <> (commaSep $ map (pp . name) $ consArgs c) <> "} =>" <+> cstruct <+> "{"                                   $$
                    "    tag:" <+> ffiTagEnumName typeName <> "::" <> cname <> ","                                                   $$
                    "    x:" <+> "__union_" <> cstruct <+> "{"                                                                       $$
                    "        " <> cname <> ": Box::into_raw(Box::new(" <> ffiConsStructName cstruct c <+> "{"                        $$
                    (nest' $ nest' $ nest' $ vcommaSep $ map (\a -> pp (name a) <> ":" <+> pp (name a) <> ".to_ffi()") $ consArgs c) $$
                    "        }))"                                                                                                    $$
                    "    }"                                                                                                          $$
                    "}")
                  $ typeCons t'
    c_matches =
              map (\c ->
                    let cname = rname $ name c
                        ccons = "struct" <+> cConsStructName (cStructName d t) c
                        nargs = length $ consArgs c in
                    mkConstructorName typeName t' (name c) <>
                    "{" <> (commaSep $ map (pp . name) $ consArgs c) <> "} =>" <+> "{"                                               $$
                    "    format!(\"(" <> mkCType d t <> "){{.tag =" <+> cname <>
                                  (if nargs > 0
                                      then ", .x = (union " <> cStructName d t <> "_union){{." <> cname <> " = &(" <+> ccons <> ")" <> "{{" <+> (commaSep $ replicate nargs "{}") <> "}} }} }}\"," <+>
                                           (commaSep $ map ((<> ".c_code()") . pp . name) $ consArgs c)
                                      else "}}\"") <> ")"                                                                             $$
                    "}")
                  $ typeCons t'
    free_matches = map (\c ->
                        let cname = rname $ name c in
                        ffiTagEnumName typeName <> "::" <> cname <+> "=> {"         $$
                        (nest' $ vcat $ map (\a -> let at = mkType (typeNormalize d a) in
                                                   "<" <> at <> ">::free(unsafe{&mut (*x.x." <> cname <> ")." <> pp (name a) <> "});")
                                      $ consArgs c)                                 $$
                        "}")
                   $ typeCons t'

-- assumes: 't' is normalized
mkStruct :: DatalogProgram -> Type -> Doc -> Doc -> [Constructor] -> (Doc, Doc)
mkStruct d t c_struct_name ffi_struct_name [Constructor _ cname cfields] = (rust, hdr)
    where
    rust =
         "#[repr(C)]"                                                                            $$
         "pub struct" <+> ffi_struct_name <+> "{"                                                $$
         (nest' $ vcommaSep fields)                                                              $$
         "}"                                                                                     $$
         "impl" <+> ffi_struct_name <+> "{"                                                      $$
         "    pub fn to_native(&self) ->" <+> mkType t <+> "{"                                   $$
         (nest' $ nest' $
          case typ' d t of
               t'@TStruct{} -> mkConstructorName (typeName t) t' cname <> "{" $$
                               (nest' $ vcommaSep
                                $ map (\f -> pp (name f) <> ":" <+> mkfield f) cfields) $$
                               "}"
               TTuple{}     -> "(" <> (hsep $ punctuate comma $ map mkfield cfields) <> ")")     $$
         "    }"                                                                                 $$
         "}"
    hdr =
         "struct" <+> c_struct_name <+> "{"                                                      $$
         (nest' $ vcat $ hdr_fields)                                                             $$
         "};"
    fields = map (\f -> pp (name f) <> ":" <+> mkFFIType d (typeNormalize d f)) cfields
    hdr_fields = map (\f -> mkCType d (typeNormalize d f) <+> pp (name f) <> ";") cfields
    mkfield :: Field -> Doc
    mkfield f =
        case typeNormalize d f of
             TBool{}        -> n
             TInt{}         -> borrown <> ".clone()"
             TString{}      -> "unsafe { CStr::from_ptr(" <> n <> ")}.to_str().unwrap().to_string()"
             TBit{..}       | typeWidth <= 8  -> n <+> "as u8"
                            | typeWidth <= 16 -> n <+> "as u16"
                            | typeWidth <= 32 -> n <+> "as u32"
                            | typeWidth <= 64 -> n <+> "as u64"
                            | typeWidth <= 128-> n <+> ".to_u128()"
                            | otherwise       -> borrown <> ".clone()"
             TTuple{}       -> n <> ".to_native()"
             TUser{..}      -> n <> ".to_native()"
             TOpaque{..}    -> borrown <> ".clone()"
             t              -> error $ "FFI.mkfield " ++ show t
        where n = "self." <> pp (name f)
              borrown = "unsafe{&*" <> n <> "}"

mkStruct d t c_struct_name ffi_struct_name cons  = (rust, hdr)
    where
    rust =
        vcat constructors                                                 $$
        "#[repr(C)]"                                                      $$
        "pub union" <+> ffi_union_name <+> "{"                            $$
        (nest' $ vcommaSep fields)                                        $$
        "}"                                                               $$
        "#[repr(C)]"                                                      $$
        "pub struct" <+> ffi_struct_name <+> "{"                          $$
        "    tag:" <+> ffiTagEnumName (typeName t) <> ","                 $$
        "    x:"   <+> ffi_union_name                                     $$
        "}"                                                               $$
        "impl" <+> ffi_struct_name <+> "{"                                $$
        "    pub fn to_native(&self) ->" <+> mkType t <+> "{"             $$
        "        match self.tag {"                                        $$
        (nest' $ nest' $ nest' $ vcommaSep cases)                         $$
        "        }"                                                       $$
        "    }"                                                           $$
        "}"
    hdr =
        vcat c_constructors                                               $$
        "union" <+> c_union_name <+> "{"                                  $$
        (nest' $ vcat c_fields)                                           $$
        "};"                                                              $$
        "struct" <+> c_struct_name <+> "{"                                $$
        "    enum" <+> cTagEnumName (typeName t) <+> "tag;"               $$
        "    union" <+> c_union_name <+> "x;"                             $$
        "};"

    ffi_union_name = "__union_" <> ffi_struct_name
    c_union_name = c_struct_name <> "_union"
    (constructors, c_constructors) = unzip $
        map (\c -> mkStruct d t (cConsStructName c_struct_name c)
                                (ffiConsStructName ffi_struct_name c) [c])
        cons
    fields = map (\c -> rname (name c) <> ": *mut" <+> ffiConsStructName ffi_struct_name c) cons
    c_fields = map (\c -> "struct" <+> cConsStructName c_struct_name c <> "*" <+> rname (name c) <> ";") cons
    cases = map (\c -> ffiTagEnumName (typeName t) <> "::" <> rname (name c) <+> "=>" <+>
                       "unsafe {&*self.x." <> rname (name c) <> "}.to_native()") cons

ffiStructName :: DatalogProgram -> Type -> Doc
ffiStructName d t = "__c_" <> cStructName d t

cStructName :: DatalogProgram -> Type -> Doc
cStructName d t = mkValConstructorName' d t

cConsStructName :: Doc -> Constructor -> Doc
cConsStructName tname c = tname <> "_" <> (rname $ name c)

ffiConsStructName :: Doc -> Constructor -> Doc
ffiConsStructName tname c = "__struct_" <> cConsStructName tname c

ffiTagEnumName :: String -> Doc
ffiTagEnumName tname = "__enum_" <> rname tname

cTagEnumName :: String -> Doc
cTagEnumName tname = rname tname <> "_enum"

cRelEnumerator :: String -> Doc
cRelEnumerator rel = "Relation_" <> rname rel

-- type must be normalized
mkFFIType :: DatalogProgram -> Type -> Doc
mkFFIType _ TBool{}        = "bool"
mkFFIType _ TInt{}         = "*mut Int"
mkFFIType _ TString{}      = "*mut c_char"
mkFFIType _ TBit{..}       | typeWidth <= 8  = "uint8_t"
                           | typeWidth <= 16 = "uint16_t"
                           | typeWidth <= 32 = "uint32_t"
                           | typeWidth <= 64 = "uint64_t"
                           | typeWidth <= 128= "Uint128_le_t"
                           | otherwise       = "*mut Uint"
mkFFIType d t@TTuple{..}   = ffiStructName d t
mkFFIType d t@TUser{..}    = ffiStructName d t
mkFFIType d t@TOpaque{..}  = "*mut" <+> mkValConstructorName' d t
mkFFIType _ t              = error $ "FFI.mkFFIType " ++ show t

-- type must be normalized
mkCType :: DatalogProgram -> Type -> Doc
mkCType _ TBool{}        = "bool"
mkCType _ TInt{}         = "struct Int*"
mkCType _ TString{}      = "char*"
mkCType _ TBit{..}       | typeWidth <= 8  = "uint8_t"
                         | typeWidth <= 16 = "uint16_t"
                         | typeWidth <= 32 = "uint32_t"
                         | typeWidth <= 64 = "uint64_t"
                         | typeWidth <= 128= "struct Uint128_le_t"
                         | otherwise       = "struct Uint*"
mkCType d t@TTuple{..}   = "struct" <+> cStructName d t
mkCType d t@TUser{..}    = "struct" <+> cStructName d t
mkCType d t@TOpaque{..}  = mkValConstructorName' d t <> "*"
mkCType _ t              = error $ "FFI.mkCType " ++ show t
