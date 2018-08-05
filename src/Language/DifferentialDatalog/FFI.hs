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


mkFFIInterface :: DatalogProgram -> Doc
mkFFIInterface d = 
    vcat tagenums $$
    vcat structs  $$
    mkCValue d
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
    tstructs' t@TTuple{..} = t : concatMap tstructs typeTupArgs
    tstructs' t@TUser{..}  = t : (concatMap (concatMap (tstructs . typ) . consArgs) $ typeCons $ typ' d t)
    tstructs' TOpaque{..}  = concatMap tstructs typeArgs
    tstructs' _            = []

mkCValue :: DatalogProgram -> Doc
mkCValue d =
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
    "}"
    where
    rels = M.elems $ progRelations d
    fields = map (\rel -> let t = typeNormalize d $ relType rel in 
                          pp (name rel) <> ":" <+> scalarize t) rels
    matches = map (\rel -> let t = typeNormalize d $ relType rel in
                           "Relations::" <> pp (name rel) <+> "=> Value::" <> mkValConstructorName' d t
                             <> "(" <> tonative (name rel) t <> ")") rels
    free_matches = map (\rel -> 
                         let t = typeNormalize d $ relType rel
                             rn = pp $ name rel in
                         "Relations::" <> rn <+> "=> unsafe {"                                               $$
                         (if isStruct d t || isTuple d t
                             then "    <" <> mkType t <> ">::free(&mut *val.val." <> rn <> ");" $$
                                  "    Box::from_raw(val.val." <> rn <> ");"
                             else "    <" <> mkType t <> ">::free(&mut val.val." <> rn <> ");")              $$
                         "}") rels
    from_matches = mapIdx (\rel i -> 
                           let t  = typeNormalize d $ relType rel 
                               rn = pp $ name rel 
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
    -- rust currently does not allow non-scalar types in unions
    scalarize :: Type -> Doc
    scalarize t = 
        case t of
             TTuple{} -> "*mut" <+> t'
             TUser{}  -> "*mut" <+> t'
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
                            | otherwise       -> borrown <> ".clone()"
             TTuple{}       -> borrown <> ".to_native()"
             TUser{..}      -> borrown <> ".to_native()"
             TOpaque{..}    -> borrown <> ".clone()"
             t              -> error $ "FFI.tonative " ++ show t
        where 
        n = "self.val." <> pp relname
        borrown = "unsafe{&*" <> n <> "}"

-- Call once for each struct to generate a C-style for its tag enum.
-- Returns 'Nothing' for structs with unique constructor, which don't
-- need a tag.
ffiMkTagEnum :: DatalogProgram -> String -> Type -> Maybe Doc
ffiMkTagEnum d tname t@TStruct{..} | isStructType t = Nothing
                                   | otherwise = Just $
    "#[repr(C)]"                                                                              $$
    "pub enum" <+> tagEnumName tname <+> "{"                                                  $$
    (nest' $ vcommaSep $ mapIdx (\c i -> pp (name c) <+> "=" <+> pp i) typeCons)              $$
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
    mkStruct d t (cStructName d t) (typeCons $ typ' d t) $$
    mkToFFIStruct d t
ffiMkStruct d t@TTuple{..} = 
    mkStruct d t (cStructName d t) 
                 [Constructor nopos (error "FFI.ffiMkStruct: tuple should not need a tag") 
                  $ mapIdx (\at i -> Field nopos ("x" ++ show i) at) typeTupArgs] $$
    mkToFFITuple d t
ffiMkStruct _ t            = error $ "FFI.ffiMkStruct " ++ show t

mkToFFITuple :: DatalogProgram -> Type -> Doc
mkToFFITuple d t@TTuple{..} =
    "impl ToFFI for" <+> mkType t <+> "{"                                                   $$
    "    type FFIType =" <+> mkCType d t <> ";"                                             $$
    "    fn to_ffi(&self) -> Self::FFIType {"                                               $$
    (nest' $ nest' $ cStructName d t <> "{" <> commaSep fields <> "}")                      $$
    "    }"                                                                                 $$
    "    fn free(x: &mut Self::FFIType) {"                                                  $$
    (nest' $ nest' $ vcat free_fields)                                                      $$
    "    }"                                                                                 $$
    "}"
    where
    fields = mapIdx (\at i -> "x" <> pp i <> ": self." <> pp i <> ".to_ffi()") $ typeTupArgs
    free_fields = mapIdx (\at i -> "<" <> mkType at <> ">::free(&mut x.x" <> pp i <> ");") $ typeTupArgs

mkToFFIStruct :: DatalogProgram -> Type -> Doc
mkToFFIStruct d t@TUser{..} | isStructType t' =
    "impl ToFFI for" <+> mkType t <+> "{"                                                   $$
    "    type FFIType =" <+> mkCType d t <> ";"                                             $$
    "    fn to_ffi(&self) -> Self::FFIType {"                                               $$
    (nest' $ nest' $ cStructName d t <> "{" <> commaSep fields <> "}")                      $$
    "    }"                                                                                 $$
    "    fn free(x: &mut Self::FFIType) {"                                                  $$
    (nest' $ nest' $ vcat free_fields)                                                      $$
    "    }"                                                                                 $$
    "}"
    where
    fields = map (\a -> pp (name a) <> ": self." <> pp (name a) <> ".to_ffi()") $ consArgs $ head $ typeCons t'
    free_fields = map (\a -> "<" <> mkType (typeNormalize d $ typ a) <> ">::free(&mut x." <> pp (name a) <> ");") $ consArgs $ head $ typeCons t'
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
    "}"
    where
    t' = typ' d t
    cstruct = cStructName d t
    matches = map (\c -> 
                    let cname = pp $ name c in
                    mkConstructorName typeName t' (name c) <> 
                    "{" <> (commaSep $ map (pp . name) $ consArgs c) <> "} =>" <+> cstruct <+> "{"                                   $$
                    "    tag:" <+> tagEnumName typeName <> "::" <> cname <> ","                                                      $$
                    "    x:" <+> "__union_" <> cstruct <+> "{"                                                                       $$
                    "        " <> cname <> ": Box::into_raw(Box::new(" <> consStructName cstruct c <+> "{"                           $$ 
                    (nest' $ nest' $ nest' $ vcommaSep $ map (\a -> pp (name a) <> ":" <+> pp (name a) <> ".to_ffi()") $ consArgs c) $$ 
                    "        }))"                                                                                                    $$
                    "    }"                                                                                                          $$
                    "}")
                  $ typeCons t'
    free_matches = map (\c -> 
                        let cname = pp $ name c in
                        tagEnumName typeName <> "::" <> cname <+> "=> {"         $$
                        (nest' $ vcat $ map (\a -> let at = mkType (typeNormalize d $ typ a) in
                                                   "<" <> at <> ">::free(unsafe{&mut (*x.x." <> cname <> ")." <> pp (name a) <> "});") 
                                      $ consArgs c)                              $$
                        "    unsafe{ Box::from_raw(x.x." <> cname <> ") };"      $$
                        "}")
                   $ typeCons t'

-- assumes: 't' is normalized
mkStruct :: DatalogProgram -> Type -> Doc -> [Constructor] -> Doc
mkStruct d t struct_name [Constructor _ cname cfields] =
    "#[repr(C)]"                                                                            $$
    "pub struct" <+> struct_name <+> "{"                                                    $$
    (nest' $ vcommaSep fields)                                                              $$
    "}"                                                                                     $$
    "impl" <+> struct_name <+> "{"                                                          $$
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
    where 
    fields = map (\f -> pp (name f) <> ":" <+> mkCType d (typeNormalize d $ typ f)) cfields
    mkfield :: Field -> Doc
    mkfield f = 
        case typeNormalize d $ typ f of
             TBool{}        -> n
             TInt{}         -> borrown <> ".clone()"
             TString{}      -> "unsafe { CStr::from_ptr(" <> n <> ")}.to_str().unwrap().to_string()"
             TBit{..}       | typeWidth <= 8  -> n <+> "as u8"
                            | typeWidth <= 16 -> n <+> "as u16"
                            | typeWidth <= 32 -> n <+> "as u32"
                            | typeWidth <= 64 -> n <+> "as u64"
                            | otherwise       -> borrown <> ".clone()"
             TTuple{}       -> n <> ".to_native()"
             TUser{..}      -> n <> ".to_native()"
             TOpaque{..}    -> borrown <> ".clone()"
             t              -> error $ "FFI.mkfield " ++ show t
        where n = "self." <> pp (name f)
              borrown = "unsafe{&*" <> n <> "}"

mkStruct d t struct_name cons  =
    vcat constructors                                                 $$
    "#[repr(C)]"                                                      $$
    "pub union" <+> union_name <+> "{"                                $$
    (nest' $ vcommaSep fields)                                        $$
    "}"                                                               $$
    "#[repr(C)]"                                                      $$
    "pub struct" <+> struct_name <+> "{"                              $$
    "    tag:" <+> tagEnumName (typeName t) <> ","                    $$
    "    x:"   <+> union_name                                         $$
    "}"                                                               $$
    "impl" <+> struct_name <+> "{"                                    $$
    "    pub fn to_native(&self) ->" <+> mkType t <+> "{"             $$
    "        match self.tag {"                                        $$
    (nest' $ nest' $ nest' $ vcommaSep cases)                         $$
    "        }"                                                       $$
    "    }"                                                           $$
    "}"
    where 
    union_name = "__union_" <> struct_name
    constructors = map (\c -> mkStruct d t (consStructName struct_name c) [c]) cons
    fields = map (\c -> pp (name c) <> ": *mut" <+> consStructName struct_name c) cons
    cases = map (\c -> tagEnumName (typeName t) <> "::" <> pp (name c) <+> "=>" <+>
                       "unsafe {&*self.x." <> pp (name c) <> "}.to_native()") cons

cStructName :: DatalogProgram -> Type -> Doc
cStructName d t = "__c_" <> mkValConstructorName' d t

consStructName :: Doc -> Constructor -> Doc
consStructName tname c = "__struct_" <> tname <> "_" <> (pp $ name c)

tagEnumName :: String -> Doc
tagEnumName tname = "__enum_" <> pp tname

-- type must be normalized
mkCType :: DatalogProgram -> Type -> Doc
mkCType _ TBool{}        = "bool"
mkCType _ TInt{}         = "*mut Int"
mkCType _ TString{}      = "*mut c_char"
mkCType _ TBit{..}       | typeWidth <= 8  = "uint8_t"
                         | typeWidth <= 16 = "uint16_t"
                         | typeWidth <= 32 = "uint32_t"
                         | typeWidth <= 64 = "uint64_t"
                         | otherwise       = "*mut Uint"
mkCType d t@TTuple{..}   = cStructName d t
mkCType d t@TUser{..}    = cStructName d t
mkCType d t@TOpaque{..}  = "*mut" <+> mkValConstructorName' d t
mkCType _ t              = error $ "FFI.mkCType " ++ show t
