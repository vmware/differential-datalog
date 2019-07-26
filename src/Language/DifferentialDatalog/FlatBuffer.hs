{-
Copyright (c) 2019 VMware, Inc.
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

-- This should eventually be merged with CompileJava.hs

-- FIXME: add generated code examples

{-# LANGUAGE RecordWildCards, FlexibleContexts, LambdaCase, OverloadedStrings, ImplicitParams, TemplateHaskell #-}

module Language.DifferentialDatalog.FlatBuffer(
    flatBufferValidate,
    compileFlatBufferBindings)
where

-- FIXME: support `DeleteKey` and `Modify` commands.  The former require collecting
-- primary key types; the latter requires adding support for type-erased value
-- representation (aka `Record`).

import qualified Data.Set as S
import qualified Data.Map as M
import Data.List
import Data.Char
import Data.Maybe
import Data.String.Utils
import Data.FileEmbed
import Control.Monad.State
import Text.PrettyPrint
import Control.Monad.Except
import Prelude hiding((<>))
import System.FilePath
import System.Process
import GHC.IO.Exception
import Debug.Trace
import qualified Data.ByteString.Char8 as BS

import Language.DifferentialDatalog.Syntax
import Language.DifferentialDatalog.Type
import Language.DifferentialDatalog.PP
import Language.DifferentialDatalog.Util
import Language.DifferentialDatalog.Pos
import Language.DifferentialDatalog.Name
import Language.DifferentialDatalog.NS
import Language.DifferentialDatalog.Relation
import qualified Language.DifferentialDatalog.Compile as R -- "R" for "Rust"

compileFlatBufferBindings :: (?cfg::R.CompilerConfig) => DatalogProgram -> String -> FilePath -> IO ()
compileFlatBufferBindings prog specname dir = do
    let flatbuf_dir = dir </> "flatbuf"
    let java_dir = dir </> "flatbuf" </> "java"
    --updateFile (java_dir </> specname <.> ".java") (render $ compileJava prog specname)
    updateFile (flatbuf_dir </> "flatbuf.fbs") (render $ compileFlatBufferSchema prog specname)
    mapM_ (\(fname, doc) -> updateFile (java_dir </> fname) $ render doc)
          $ compileFlatBufferJavaBindings prog specname
    compileFlatBufferRustBindings prog specname dir
    -- compile Java bindings for FlatBuffer schema
    let flatc_proc = (proc "flatc" (["-j", "-o", "java/", "flatbuf.fbs"])) {
                          cwd = Just $ dir </> "flatbuf"
                     }
    (code, stdo, stde) <- readCreateProcessWithExitCode flatc_proc ""
    when (code /= ExitSuccess) $ do
        errorWithoutStackTrace $ "flatc failed with exit code " ++ show code ++
                                 "\nstdout:\n" ++ stde ++
                                 "\n\nstdout:\n" ++ stdo
    -- compile Rust bindings for FlatBuffer schema
    let flatc_proc = (proc "flatc" (["-r", "flatbuf.fbs"])) {
                          cwd = Just $ dir </> "flatbuf"
                     }
    (code, stdo, stde) <- readCreateProcessWithExitCode flatc_proc ""
    when (code /= ExitSuccess) $ do
        errorWithoutStackTrace $ "flatc failed with exit code " ++ show code ++
                                 "\nstdout:\n" ++ stde ++
                                 "\n\nstdout:\n" ++ stdo
    -- Copy generated file if it's changed (we could generate it in place, but
    -- flatc always overwrites its output, forcing Rust re-compilation)
    fbgenerated <- readFile $ flatbuf_dir </> "flatbuf_generated.rs"
    updateFile (dir </> "src" </> "flatbuf_generated.rs") fbgenerated

-- | Checks that we are able to generate FlatBuffer schema for all types used in
-- program's input and output relations.
--
-- This function must be invoked after the normal DDlog validation has
-- succeeded.
--
-- If this function succeeds, then `compileFlatBufferSchema` must be guaranteed
-- to succeed.
flatBufferValidate :: (MonadError String me) => DatalogProgram -> me ()
flatBufferValidate d = do
    let ?d = d
    check (not $ null progIORelations) nopos
        $ "Program has no input or output relations; cannot generate FlatBuffers schema"
    mapM_ (\case
            t@TOpaque{..} ->
                check (elem typeName $ [rEF_TYPE, mAP_TYPE, iNTERNED_TYPE] ++ sET_TYPES) (pos t) $
                    "Cannot generate FlatBuffer schema for extern type " ++ show t
            _ -> return ())
          progTypesToSerialize

-- | Generate FlatBuffer schema for DDlog program.
compileFlatBufferSchema :: DatalogProgram -> String -> Doc
compileFlatBufferSchema d prog_name =
    let ?d = d
        ?prog_name = prog_name in

    let rels = progIORelations
        -- Schema for all program types visible from outside
        types = map typeFlatbufSchema progTypesToSerialize
        relenum_type | length rels <= 256    = "uint8"
                     | length rels <= 65536  = "uint16"
                     | otherwise             = "uint32"
        default_relid = pp $ relIdentifier ?d $ head rels
    in
    "namespace" <+> jFBPackage <> ";"                                           $$
    "table __BigUint { bytes: [uint8]; }"                                       $$
    "table __BigInt { sign: bool; bytes: [uint8]; }"                            $$
    ""                                                                          $$
    "// Program type declarations"                                              $$
    (vcat $ intersperse "" types)                                               $$
    ""                                                                          $$
    "// Relation identifiers"                                                   $$
    "enum __RelId:" <+> relenum_type                                            $$
    (braces' $ vcommaSep
             -- values must match DDlog relation identifiers.
             $ map  (\r -> mkRelId r <+> "=" <+> pp (relIdentifier ?d r)) rels) $$
    ""                                                                          $$
    "// Union of all program relation types"                                    $$
    "union __Value"                                                             $$
    (braces' $ vcommaSep $ nub $ map typeTableName rels)                        $$
    ""                                                                          $$
    "// DDlog commands"                                                         $$
    "enum __CommandKind: uint8 { Insert, Delete }"                              $$
    "table __Command {"                                                         $$
    "   kind: __CommandKind;"                                                   $$
    "   relid: __RelId =" <+> default_relid <> ";"                              $$
    "   val:  __Value;"                                                         $$
    "}"                                                                         $$
    "table __Commands {"                                                        $$
    "   commands: [__Command];"                                                 $$
    "}"                                                                         $$
    "root_type __Commands;"

-- | Generate FlatBuffer Rust bindings.
compileFlatBufferRustBindings :: (?cfg::R.CompilerConfig) => DatalogProgram -> String -> FilePath ->  IO ()
compileFlatBufferRustBindings d prog_name dir = do
    let ?d = d
    let rels = progIORelations
    -- `FromFlatBuf/ToFlatBuf` implementation for all program types visible from outside
    let types = map typeFlatbufRustBinding progTypesToSerialize
    let rust_template = replace "datalog_example" prog_name $ BS.unpack $(embedFile "rust/template/src/flatbuf.rs")
    updateFile (dir </> "src/flatbuf.rs") $ render $
        (pp rust_template)                                      $$
        rustValueFromFlatbuf                                    $$
        (vcat $ map rustTypeFromFlatbuf progTypesToSerialize)

    --"// FlatBuffers serialization/deserialization logic for program types"      $$
    --(vcat $ intersperse "" types)
    -- `FromFlatBuf/ToFlatBuf` implementation for `Value`
    -- `FromFlatBuf/ToFlatBuf` for `Update`

-- | Generate Java convenience API that provides a type-safe way to serialize/deserialize
-- commands to a FlatBuffer.
compileFlatBufferJavaBindings :: DatalogProgram -> String -> [(FilePath, Doc)]
compileFlatBufferJavaBindings d prog_name =
    let ?d = d
        ?prog_name = prog_name in
    let types = mapMaybe typeFlatbufJavaBinding progTypesToSerialize
    in
    ("ddlog" </> prog_name </> builderClass <.> "java", mkJavaBuilder prog_name):
    types
    --rels = progIORelations d
    -- `Constructors/accessors` for DDlog types visible from outside.
    -- `FromFlatBuf/ToFlatBuf` implementation for `Value`
    -- `FromFlatBuf/ToFlatBuf` for `Update`

{- Helpers -}

jFBPackage :: (?prog_name :: String) => Doc
jFBPackage = "ddlog.__" <> pp ?prog_name

-- True if 'a' is of a type that is declared as a table and therefore does not
-- need to be wrapped in another table in order to be used inside a vector or
-- union.
typeIsTable :: (WithType a, ?d::DatalogProgram) => a -> Bool
typeIsTable x =
    -- struct with unique constructor.
    isStruct ?d x && typeHasUniqueConstructor x ||
    isTuple ?d x                                ||
    fbType (typ x) == "__BigInt"                ||
    fbType (typ x) == "__BigUint"

typeHasUniqueConstructor :: (WithType a, ?d::DatalogProgram) => a -> Bool
typeHasUniqueConstructor x =
    case typ' ?d x of
         TStruct{..} -> length typeCons == 1
         _           -> True

-- Type is stored inline as a primitive FlatBuffer type.
typeIsScalar :: (WithType a, ?d::DatalogProgram) => a -> Bool
typeIsScalar x =
    case typeNormalizeForFlatBuf x of
         TBool{}     -> True
         TBit{..}    -> typeWidth <= 64
         TSigned{..} -> typeWidth <= 64
         _           -> False

-- Table name to the type of 'a'.  Defined even if 'a' is of a type
-- that is not declared as a table, in which case this is the name of the
-- wrapper table.
typeTableName :: (WithType a, ?d::DatalogProgram) => a -> Doc
typeTableName x =
    case typeNormalizeForFlatBuf x of
         TBool{}       -> "__Bool"
         TInt{}        -> "__BigInt"
         TString{}     -> "__String"
         TBit{..}    | typeWidth <= 64
                       -> "__Bit" <> pp typeWidth
                     | otherwise
                       -> "__BigUint"
         TSigned{..} | typeWidth <= 64
                       -> "__Signed" <> pp typeWidth
                     | otherwise
                       -> "__BigInt"
         TTuple{..}    -> fbTupleName typeTupArgs
         TUser{..} | typeHasUniqueConstructor x
                       -> fbStructName typeName typeArgs
                   | otherwise
                       -> "__Table_" <> fbStructName typeName typeArgs
         TOpaque{..}   -> "__Table_" <> mkTypeIdentifier x
         t             -> error $ "typeTableName: Unexpected type " ++ show t

-- True if type is serialized into a vector inside FlatBuffer.
typeIsVector :: (WithType a, ?d::DatalogProgram) => a -> Bool
typeIsVector x =
    case typeNormalizeForFlatBuf x of
         TOpaque{..} -> elem typeName sET_TYPES || typeName == mAP_TYPE
         _ -> False

-- Types for which FlatBuffers schema, including FlatBuffers 'table' declaration
-- must be generated.  This includes all types that occur in input and output
-- relations, except for primitive types, unless the primitive type is used as
-- relation type, in which case we still need a table for it.
progTypesToSerialize :: (?d::DatalogProgram) => [Type]
progTypesToSerialize =
    nub
    $ concatMap relTypesToSerialize
    $ progIORelations

progIORelations :: (?d::DatalogProgram) => [Relation]
progIORelations =
    filter (\rel -> elem (relRole rel) [RelInput, RelOutput])
           $ M.elems $ progRelations ?d

-- Types used in relation declaration (possibly, recursively), for which serialization
-- logic must be generated.
relTypesToSerialize :: (?d::DatalogProgram) => Relation -> [Type]
relTypesToSerialize Relation{..} =
    execState (typeSubtypes relType)
              [typeNormalizeForFlatBuf relType] -- Relation type must appear in 'union Value'; therefore we
                                                -- generate a table for it even if it's a primitive
                                                -- type.

-- Types that occur in type expression `t`, for which serialization logic must be
-- generated, including:
-- * the type itself, if it's not a primitive or extern type
-- * struct fields, if it's a struct type
-- * tuple fields, if it's a tuple type
-- * type arguments for generic types
-- * container types (sets, vectors, and maps): these are typically not used,
--   as containers are serialized directly into arrays, but since FlatBuffers do
--   not support nested arrays, or using arrays inside unions, additional wrapper
--   type is needed for nesting to work.
typeSubtypes :: (?d::DatalogProgram) => Type -> State [Type] ()
typeSubtypes t =
    -- t'' may have already been added, but we add it again at the head of the list
    -- to ensure that dependencies are declared before types that depend on them
    -- (as required by the FlatBuffers schema language).
    -- FIXME: this will go into an infinite loop with recursive types, but we
    -- don't support them at the moment, and in any case some additonal tricks
    -- will be needed to encode them in FlatBuffers.
    case t'' of
        TBool{}       -> return ()
        TInt{}        -> return ()
        TString{}     -> return ()
        TBit{}        -> return ()
        TSigned{}     -> return ()
        TTuple{..} -> do addtype t''
                         mapM_ typeSubtypes typeTupArgs
        TUser{}    -> do addtype t''
                         mapM_ (mapM_ (typeSubtypes . typ) . consArgs)
                               $ typeCons $ typ' ?d t''
        TOpaque{..}   -> do -- In case we need to nest this.
                            addtype t''
                            -- Maps are encoded as arrays of (key,value)
                            -- tuples; we must therefore generate an
                            -- appropriate tuple type
                            when (typeName == mAP_TYPE)
                                 $ addtype $ tTuple [typeArgs !! 0, typeArgs !! 1]
                            mapM_ typeSubtypes typeArgs
        _             -> error $ "typeSubtypes: Unexpected type " ++ show t
    where
    -- Prepend 'x' to the list so that dependency declarations are generated before
    -- types that depend on them.
    addtype x = modify (nub . (x:))
    t'' = typeNormalizeForFlatBuf t

{- FlatBuffer schema generation -}

-- Generate FlatBuffer schema for the type, including a 'table' declaration, so
-- that the type can be nested inside unions and vectors.
typeFlatbufSchema :: (?d::DatalogProgram) => Type -> Doc
typeFlatbufSchema x =
    case typeNormalizeForFlatBuf x of
         TBit{}      -> empty
         TSigned{}   -> empty
         TString{}   -> empty
         TBool{}     -> empty
         TInt{}      -> empty
         TTuple{..}  -> tupleFlatBufferSchema typeTupArgs
         t@TUser{..} ->
             -- For structs, only generate a union if the struct has more than one
             -- constructor; otherwise, create a table whose name matches the name of the
             -- struct (not the name of the constructor).
             vcat constructors $+$
             union
             where
             tstruct = typ' ?d t
             constructors = map (constructorFlatbufSchema typeArgs) $ typeCons tstruct
             union = if typeHasUniqueConstructor t
                        then empty
                        else "union" <+> fbStructName typeName typeArgs <+> (braces $ commaSep $ map (fbConstructorName typeArgs) $ typeCons tstruct) $$
                             "table" <+> typeTableName t $$
                             (braces' $ "v:" <+> fbType t <> ";")
         t -> "table" <+> typeTableName t $+$
              (braces' $ "v:" <+> fbType t <> ";")

-- Generate FlatBuffer "table" declaration for a constructor
-- Constructor arguments may _not_ be normalized.
constructorFlatbufSchema :: (?d::DatalogProgram) => [Type] -> Constructor -> Doc
constructorFlatbufSchema targs c@Constructor{..} =
    "table" <+> fbConstructorName targs c $+$
    (braces' $ vcat fields)
    where
    fields = map (\a -> pp (name a) <> ":" <+> fbType (typ a) <> ";") consArgs

-- Generate FlatBuffer "table" declaration for a tuple type
-- Assumes 'types' are normalized
tupleFlatBufferSchema :: (?d::DatalogProgram) => [Type] -> Doc
tupleFlatBufferSchema types =
    "table" <+> fbTupleName types $+$
    (braces' $ vcat fields)
    where
    fields = mapIdx (\t i -> "a" <> pp i <> ":" <+> fbType t <> ";") types

-- Map DDlog types to FlatBuffer schema types.
-- 'typ x' may _not_ be normalized.
-- 'nested' is true if the resulting type must be embedded in a union or vector
fbType :: (WithType a, ?d::DatalogProgram) => a -> Doc
fbType x =
    case typeNormalizeForFlatBuf x of
         TBool{}            -> "bool"
         TInt{}             -> "__BigInt"
         TString{}          -> "string"
         TBit{..} | typeWidth <= 8
                            -> "uint8"
         TBit{..} | typeWidth <= 16
                            -> "uint16"
         TBit{..} | typeWidth <= 32
                            -> "uint32"
         TBit{..} | typeWidth <= 64
                            -> "uint64"
         TBit{..}           -> "__BigUint"
         TSigned{..} | typeWidth <= 8
                            -> "int8"
         TSigned{..} | typeWidth <= 16
                            -> "int16"
         TSigned{..} | typeWidth <= 32
                            -> "int32"
         TSigned{..} | typeWidth <= 64
                            -> "int64"
         TSigned{..}        -> "__BigInt"
         TTuple{..}         -> fbTupleName typeTupArgs
         t@TUser{..}        -> fbStructName typeName typeArgs
         t@TOpaque{typeArgs = [elemType], ..} | elem typeName sET_TYPES
                            -> -- unions and arrays are not allowed in FlatBuffers arrays
                               "[" <> (if typeHasUniqueConstructor elemType && not (typeIsVector elemType)
                                          then fbType elemType
                                          else typeTableName elemType) <> "]"
         TOpaque{typeArgs = [keyType,valType],..} | typeName == mAP_TYPE
                            -> "[" <> fbTupleName [keyType, valType] <> "]"
         t'                 -> error $ "FlatBuffer.fbType: unsupported type " ++ show t'

-- Convert type into a valid FlatBuf identifier.
mkTypeIdentifier :: (WithType a, ?d::DatalogProgram) => a -> Doc
mkTypeIdentifier = pp . legalize . render . pp . typeNormalizeForFlatBuf

fbTupleName :: (?d::DatalogProgram) => [Type] -> Doc
fbTupleName ts =
    if isJust $ lookupConstructor ?d $ render candidate
       then "__" <> candidate
       else candidate
    where
    candidate = "Tuple" <> pp (length ts) <> fields
    fields = hcat $ map (("__" <>) . mkTypeIdentifier) ts

fbStructName :: (?d::DatalogProgram) => String -> [Type] -> Doc
fbStructName n args =
    -- avoid collisions between constructor and struct names.
    if (isJust $ lookupConstructor ?d $ render candidate) &&
       (not $ typeHasUniqueConstructor $ fromJust $ tdefType $ getType ?d n)
       then "__Struct_" <> candidate
       else candidate
    where
    candidate = pp (legalize n) <> targs
    targs = hcat $ map (("__" <>) . mkTypeIdentifier) args

fbConstructorName :: (?d::DatalogProgram) => [Type] -> Constructor -> Doc
fbConstructorName args c | consIsUnique ?d (name c) = fbStructName (name $ consType ?d $ name c) args
                         | otherwise                = pp (legalize $ name c) <> targs
    where
    targs = hcat $ map (("__" <>) . mkTypeIdentifier) args

mkRelId :: (?d::DatalogProgram) => Relation -> Doc
mkRelId rel@Relation{..} = pp $ legalize relName

-- capitalize the first letter of a string
capitalize :: String -> String
capitalize str = (toUpper (head str) : tail str)

-- convert a DDlog identifier (possibly including namespaces) into a Java identifier
legalize :: String -> String
legalize n =
    map legalizeChar
    $ replace "]" ""
    $ replace "[" "_Array_" n

-- convert characters that are illegal in Java identifiers into underscores
legalizeChar :: Char -> Char
legalizeChar c = if isAlphaNum c then c else '_'

-- Like 'Type.typeNormalize', but also unwraps Ref<> and IObj<> types, which are
-- transparent to FlatBuffers.
typeNormalizeForFlatBuf :: (WithType a, ?d::DatalogProgram) => a -> Type
typeNormalizeForFlatBuf x =
    case typeNormalize ?d x of
         TOpaque{typeArgs = [innerType],..} | elem typeName [rEF_TYPE,  iNTERNED_TYPE]
                                            -> typeNormalizeForFlatBuf innerType
         t'                                 -> t'

{- Rust serialization/deserialization logic. -}

typeFlatbufRustBinding :: (?d::DatalogProgram) => Type -> Doc
typeFlatbufRustBinding _ = error "typeFlatbufRustBinding is not implemented"

{- Functions to work with the FlatBuffers-generate Java API. -}

jFBCallConstructor :: (?prog_name::String) => Doc -> [Doc] -> Doc
jFBCallConstructor table [] = "0"
jFBCallConstructor table args =
    jFBPackage <> "." <> table <> ".create" <> table <>
           (parens $ commaSep $ "this.fbbuilder" : args)


{- Java convenience API. -}

-- Java types used in the FlafBuffers-generated API
-- (primitive types are passed as is, tables are passed as offsets)
jFBType :: (WithType a, ?d::DatalogProgram) => Bool -> a -> Doc
jFBType nested x =
    case typeNormalizeForFlatBuf x of
        TBool{}            -> "boolean"
        TInt{}             -> "int"
        TString{}          -> "int"
        TBit{..} | typeWidth <= 8
                           -> "byte"
        TBit{..} | typeWidth <= 16
                           -> "short"
        TBit{..} | typeWidth <= 32
                           -> "int"
        TBit{..} | typeWidth <= 64
                           -> "long"
        TBit{..}           -> "int"
        TSigned{..} | typeWidth <= 8
                           -> "byte"
        TSigned{..} | typeWidth <= 16
                           -> "short"
        TSigned{..} | typeWidth <= 32
                           -> "int"
        TSigned{..} | typeWidth <= 64
                           -> "long"
        TSigned{..}        -> "int"
        TTuple{..}         -> "int"
        TUser{..}          -> "int"
        TOpaque{typeArgs = [elemType], ..} | elem typeName sET_TYPES
                           -> if nested
                                 then "int"
                                 else jFBType True elemType <> "[]"
        TOpaque{typeArgs = [keyType,valType],..} | typeName == mAP_TYPE
                           -> if nested
                                 then "int"
                                 else "int []"
        t'                 -> error $ "FlatBuffer.jConvType: unsupported type " ++ show t'

-- Types used in the convenience API.
jConvType :: (WithType a, ?d::DatalogProgram) => a -> Doc
jConvType x =
    case typeNormalizeForFlatBuf x of
        TBool{}            -> "boolean"
        TInt{}             -> "BigInteger"
        TString{}          -> "String"
        TBit{..} | typeWidth <= 8
                           -> "byte"
        TBit{..} | typeWidth <= 16
                           -> "short"
        TBit{..} | typeWidth <= 32
                           -> "int"
        TBit{..} | typeWidth <= 64
                           -> "long"
        TBit{..}           -> "BigInteger"
        TSigned{..} | typeWidth <= 8
                           -> "byte"
        TSigned{..} | typeWidth <= 16
                           -> "short"
        TSigned{..} | typeWidth <= 32
                           -> "int"
        TSigned{..} | typeWidth <= 64
                           -> "long"
        TSigned{..}        -> "BigInteger"
        TTuple{..}         -> fbTupleName typeTupArgs
        TUser{..}          -> fbStructName typeName typeArgs
        TOpaque{typeArgs = [elemType], ..} | elem typeName sET_TYPES
                           -> jConvType elemType <> "[]"
        TOpaque{typeArgs = [keyType,valType],..} | typeName == mAP_TYPE
                           -> "java.util.Map<" <> jConvObjType keyType <> "," <+> jConvObjType valType <> ">"
        t'                 -> error $ "FlatBuffer.jConvType: unsupported type " ++ show t'

-- Like jConvType, but returns Java object types instead of scalars for
-- primitive types.
jConvObjType :: (WithType a, ?d::DatalogProgram) => a -> Doc
jConvObjType x =
    case typeNormalizeForFlatBuf x of
         TBool{}            -> "Boolean"
         TBit{..} | typeWidth <= 8
                            -> "Byte"
         TBit{..} | typeWidth <= 16
                            -> "Short"
         TBit{..} | typeWidth <= 32
                            -> "Integer"
         TBit{..} | typeWidth <= 64
                            -> "Long"
         TSigned{..} | typeWidth <= 8
                            -> "Byte"
         TSigned{..} | typeWidth <= 16
                            -> "Short"
         TSigned{..} | typeWidth <= 32
                            -> "Integer"
         TSigned{..} | typeWidth <= 64
                            -> "Long"
         _                  -> jConvType x

-- Context where generated value is to be written.
data FBCtx = -- value to be stored in field 'fbctxField' of 'fbctxTable'
             FBField {fbctxTable::Doc, fbctxField::String}
             -- value to be stored in union (must be table)
           | FBUnion
             -- value to be stored in array
           | FBArray

-- Convert expression from Java convenience type to corresponsing FlatBuffers
-- native Java type using FlatBufferBuilder instance.
jConv2FBType :: (?d::DatalogProgram, ?prog_name::String) => FBCtx -> Doc -> Type -> Doc
jConv2FBType fbctx e t =
   case fbctx of
        -- Anything that's not a table must be wrapped in a table to be stored
        -- in a union
        FBUnion | typeIsTable t
                -> e'
                | otherwise
                -> jFBCallConstructor (typeTableName t) [jConv2FBType (FBField (typeTableName t) "v") e t]
        -- Anything that's not a union or array can be stored in an array; unions and arrays must be
        -- wrapped in a table first.
        FBArray | typeHasUniqueConstructor t && not (typeIsVector t)
                -> e'
                | otherwise
                -> jFBCallConstructor (typeTableName t) [jConv2FBType (FBField (typeTableName t) "v") e t]
        -- FlatBuffers has a special method for embedding vectors in a buffer
        FBField table fld | typeIsVector t
                -> jFBPackage <> "." <> table <> ".create" <> pp (capitalize fld) <> "Vector(this.fbbuilder," <> e' <> ")"
                          | otherwise
                -> e'
    where
    e' = case typeNormalizeForFlatBuf t of
            TBool{}            -> e
            TInt{}             -> bigint
            TString{}          -> "this.fbbuilder.createString(" <> e <> ")"
            TBit{..} | typeWidth <= 64
                               -> e
            TBit{..}           -> biguint
            TSigned{..} | typeWidth <= 64
                               -> e
            TSigned{..}        -> bigint
            TTuple{..}         -> e <> ".offset"
            ut@TUser{..} | typeHasUniqueConstructor ut
                               -> e <> ".offset"
                         | otherwise
                               -> e <> ".type," <+> e <> ".offset"
            ot@TOpaque{typeArgs = [elemType], ..} | elem typeName sET_TYPES && (jConvType elemType == jFBType True elemType)
                               -> e
                                                  | otherwise
                               -> "this.create_" <> mkTypeIdentifier ot <> parens e
            ot@TOpaque{typeArgs = [keyType,valType], ..} | typeName == mAP_TYPE
                               -> "this.create_" <> mkTypeIdentifier ot <> parens e
            t'                 -> error $ "FlatBuffer.jConv2FBType: unsupported type " ++ show t'
    bigint = jFBPackage <> ".__BigInt.create__BigInt(this.fbbuilder,"
         <+> e <> ".signum() < 0 ? false : true,"
         <+> jFBPackage <> ".__BigInt.createBytesVector(this.fbbuilder," <+> e <> ".abs().toByteArray()))"
    biguint = jFBPackage <> ".__BigUint.create__BigUint(this.fbbuilder,"
          <+> jFBPackage <> ".__BigUint.createBytesVector(this.fbbuilder," <+> e <> ".toByteArray()))"

-- Like jConv2FBType, but assumes that input is a Java object type, not a
-- primitive.
jConvObj2FBType :: (?d::DatalogProgram, ?prog_name::String) => FBCtx -> Doc -> Type -> Doc
jConvObj2FBType fbctx e t =
    case typeNormalizeForFlatBuf t of
         TBool{}            -> e <> ".booleanValue()"
         TBit{..} | typeWidth <= 8
                            -> e <> ".byteValue()"
         TBit{..} | typeWidth <= 16
                            -> e <> ".shortValue()"
         TBit{..} | typeWidth <= 32
                            -> e <> ".intValue()"
         TBit{..} | typeWidth <= 64
                            -> e <> ".longValue"
         TSigned{..} | typeWidth <= 8
                            -> e <> ".byteValue()"
         TSigned{..} | typeWidth <= 16
                            -> e <> ".shortValue()"
         TSigned{..} | typeWidth <= 32
                            -> e <> ".intValue()"
         TSigned{..} | typeWidth <= 64
                            -> e <> ".longValue()"
         _                  -> jConv2FBType fbctx e t

-- Call FlatBuffers API to create a table instance for the given type.
jConvCreateTable :: (?d::DatalogProgram, ?prog_name::String) =>Type -> Maybe Constructor -> Doc
jConvCreateTable t cons =
    case typeNormalizeForFlatBuf t of
         TBool{}            -> prim
         TInt{}             -> jConv2FBType FBUnion "v" t
         TString{}          -> jConv2FBType FBUnion "v" t
         TBit{..} | typeWidth <= 64
                            -> prim
         TBit{..}           -> jConv2FBType FBUnion "v" t
         TSigned{..} | typeWidth <= 64
                            -> prim
         TSigned{..}        -> jConv2FBType FBUnion "v" t
         TTuple{..}         ->
             jFBCallConstructor (typeTableName t) (mapIdx (\at i -> jConv2FBType (FBField (typeTableName t) ("a" ++ show i)) ("a" <> pp i) at)
                                                          typeTupArgs)
         ut@TUser{..} | typeHasUniqueConstructor ut ->
             jFBCallConstructor (typeTableName t) (map (\a -> jConv2FBType (FBField (typeTableName t) (name a)) (pp $ name a) $ typ a)
                                                       (consArgs $ head $ typeCons $ typ' ?d ut))
                      | otherwise ->
             let Just c@Constructor{..} = cons
                 cons_name = fbConstructorName typeArgs c in
             jFBCallConstructor (typeTableName t)
                 [ jFBPackage <> "." <> fbStructName typeName typeArgs <> "." <> cons_name
                 , jFBCallConstructor cons_name
                                      (map (\a -> jConv2FBType (FBField cons_name (name a)) (pp $ name a) $ typ a) consArgs)]
         TOpaque{..} -> jConv2FBType FBUnion "v" t
         t'                 -> error $ "FlatBuffer.jConvCreateTable: unsupported type " ++ show t'

    where
    -- Table for primitive type
    prim = jFBPackage <> "." <> typeTableName t <> ".create" <> typeTableName t <> "(this.fbbuilder, v)"


builderClass :: (?prog_name::String) => String
builderClass = ?prog_name ++ "UpdateBuilder"

-- Create builder class with methods to create FlatBuffer-backed instances of DDlog
-- types.
mkJavaBuilder :: (?d::DatalogProgram, ?prog_name::String) => String -> Doc
mkJavaBuilder prog_name =
    "// Automatically generated by the DDlog compiler."                 $$
    "package ddlog." <> pp prog_name <> ";"                             $$
    "import ddlogapi.DDlogAPI;"                                         $$
    "import com.google.flatbuffers.*;"                                  $$
    "import java.math.BigInteger;"                                      $$
    "public class" <+> builder_class                                    $$
    (braces' $ "private FlatBufferBuilder fbbuilder;"                   $$
               "private java.util.Vector<Integer> commands;"            $$
               "private boolean finished;"                              $$
               "public" <+> builder_class <> "() {"                     $$
               "    this.fbbuilder = new FlatBufferBuilder();"          $$
               "    this.commands = new java.util.Vector<Integer>();"   $$
               "    this.finished = false;"                             $$
               "}"                                                      $$
               "public int applyUpdates(DDlogAPI hddlog){"              $$
               "    if (this.finished) return -1;"                      $$
               "    int[] cmds = new int[this.commands.size()];"        $$
               "    for(int i = 0; i < cmds.length; i++)"               $$
               "        cmds[i] = this.commands.get(i);"                $$
               "    int cmdvec =" <+> jFBCallConstructor "__Commands" [jFBPackage <> ".__Commands.createCommandsVector(this.fbbuilder,cmds)"] <> ";"
                                                                        $$
               "    this.fbbuilder.finish(cmdvec);"                     $$
               "    int res = hddlog.applyUpdatesFromFlatBuf(this.fbbuilder.dataBuffer());" $$
               "    this.finished = true;"                              $$
               "    return res;"                                        $$
               "}"                                                      $$
               (vcat $ map mk_command_constructors
                     $ filter ((== RelInput) . relRole)
                     $ M.elems $ progRelations ?d)                      $$
               (vcat $ map mk_type_factory progTypesToSerialize))
    where
    builder_class = pp builderClass

    mk_command_constructors :: Relation -> Doc
    mk_command_constructors rel =
        mk_command_constructor "Insert" rel $$
        mk_command_constructor "Delete" rel

    mk_command_constructor cmd rel@Relation{..} =
        let lcmd = pp $ (toLower $ head cmd) : tail cmd in
        if typeHasUniqueConstructor relType
           then -- Relation type has a unique constructor (e.g., it's a struct with a
                -- unique constructor, a tuple or a primitive type).
                let args = case typ' ?d $ typeNormalizeForFlatBuf relType of
                                TStruct{..} -> map (\a -> jConvType a <+> pp (name a)) $ consArgs $ typeCons !! 0
                                TTuple{..}  -> mapIdx (\a i -> jConvType a <+> "a" <> pp i) typeTupArgs
                                _           -> [jConvType relType <+> "v"] in
                "public void" <+> lcmd <> "_" <> mkRelId rel <> "(" <> commaSep args <> ")" $$
                (braces' $ "int cmd =" <+> jFBCallConstructor "__Command"
                                           [ jFBPackage <> ".__CommandKind." <> pp cmd
                                           , jFBPackage <> ".__RelId." <> mkRelId rel
                                           , jFBPackage <> ".__Value." <> typeTableName relType
                                           , jConvCreateTable relType Nothing] <> ";"  $$
                           "this.commands.add(Integer.valueOf(cmd));")
           else -- Relation type is a struct with multiple constructors
                vcat $
                map (\c@Constructor{..} ->
                     -- Relation type has a unique constructor (e.g., it's a struct with a
                     -- unique constructor, a tuple or a primitive type).
                     "public void" <+> lcmd <> "_" <> mkRelId rel <> "_" <> pp (name c) <>
                         "(" <> (commaSep $ map (\a -> jConvType a <+> pp (name a)) consArgs) <> ")" $$
                     (braces' $ "int cmd =" <+> jFBCallConstructor "__Command"
                                                [ jFBPackage <> ".__CommandKind." <> pp cmd
                                                , jFBPackage <> ".__RelId." <> mkRelId rel
                                                , jFBPackage <> ".__Value." <> typeTableName relType
                                                , jConvCreateTable relType (Just c)] <> ";"  $$
                                "this.commands.add(Integer.valueOf(cmd));"))
                    $ typeCons $ typ' ?d $ typeNormalizeForFlatBuf relType

    -- Create constructor methods
    mk_type_factory t@TUser{..} | typeHasUniqueConstructor t =
        -- Unique constructor: generate a single 'create_XXX' method with the
        -- name that matches type name, not constructor name.
        "public" <+> tname <+> "create_" <> typeTableName t <> "(" <> commaSep (map (\arg -> jConvType arg <+> pp (name arg)) consArgs) <> ")" $$
        (braces' $ "return new" <+> tname <> "(" <>
                   (jFBCallConstructor (typeTableName t) $ map (\a -> jConv2FBType (FBField (fbType t) (name a)) (pp $ name a) (typ a)) consArgs) <> ");")
                                | otherwise =
        -- Multiple constructors: create method per constructor with method name
        -- matching constructor name.
        vcat (map (mk_cons_factory t) $ typeCons tstruct)
        where
        tname = jConvType t
        tstruct = typ' ?d t
        Constructor{..} = head $ typeCons tstruct

    mk_type_factory t@TTuple{..}  =
        "public" <+> jConvType t <+> "create_" <> typeTableName t <> "(" <> commaSep (mapIdx (\a i -> jConvType a <+> "a" <> pp i) typeTupArgs) <> ")" $$
        (braces' $ "return new" <+> jConvType t <> "(" <>
                   (jFBCallConstructor (typeTableName t) $ mapIdx (\a i -> jConv2FBType (FBField (fbType t) ("a" ++ show i)) ("a" <> pp i) a) typeTupArgs) <> ");")

    mk_type_factory t@TOpaque{typeArgs = [elemType], ..} | elem typeName sET_TYPES && (jConvType elemType == jFBType True elemType) = empty
                                                         | elem typeName sET_TYPES =
        -- Generate a private method to convert vector of 'elemType'
        -- into vector of offsets (the method is only used by code generated by
        -- 'jConv2FBType')
        "private" <+> jFBType False t <+> "create_" <> mkTypeIdentifier t <> (parens $ jConvType t <+> "v") $$
        (braces' $ jFBType False t <+> "res = new" <+> jFBType True elemType <> "[v.length];"   $$
                   "for (int __i = 0; __i < v.length; __i++)"                                   $$
                   (braces' $ "res[__i] =" <+> jConv2FBType FBArray "v[__i]" elemType <> ";")   $$
                   "return res;")

    mk_type_factory t@TOpaque{typeArgs = [keyType, valType], ..} | typeName == mAP_TYPE =
        -- Generate a private method to Map<keyType,valType> into vector of offsets
        "private" <+> jFBType False t <+> "create_" <> mkTypeIdentifier t <> (parens $ jConvType t <+> "v") $$
        (braces' $ "int [] res = new int[v.size()];"                                                                            $$
                   "int __i = 0;"                                                                                               $$
                   "java.util.Iterator<" <> tentry <> "> it = v.entrySet().iterator();"                                         $$
                   "while (it.hasNext()) {"                                                                                     $$
                   "    " <> tentry <+> "pair = (" <> tentry <> ")it.next();"                                                   $$
                   "    res[__i] =" <+> jFBCallConstructor entry_table [jConvObj2FBType (FBField entry_table "a0") "pair.getKey()" keyType,
                                                                        jConvObj2FBType (FBField entry_table "a1") "pair.getValue()" valType] <> ";"
                                                                                                                                $$
                   "    __i++;"                                                                                                 $$
                   "}"                                                                                                          $$
                   "return res;")
        where
        tentry = "java.util.Map.Entry<" <> jConvObjType keyType <> "," <+> jConvObjType valType <> ">"
        entry_table = typeTableName $ tTuple [keyType, valType]

    mk_type_factory _ = empty

    mk_cons_factory t c@Constructor{..} =
        "public" <+> tname <+> "create_" <> cname <> "(" <> commaSep (map (\arg -> jConvType arg <+> pp (name arg)) consArgs) <> ")" $$
        (braces' $ "return new" <+> tname <> "(" <>
                   jFBPackage <> "." <> fbtname <> "." <> cname <> "," <+>
                   (jFBCallConstructor cname $ map (\a -> jConv2FBType (FBField cname (name a)) (pp $ name a) (typ a)) consArgs) <> ");")
        where
        tname = jConvType t
        cname = fbConstructorName (typeArgs t) c
        fbtname = fbType t

typeFlatbufJavaBinding :: (?d::DatalogProgram, ?prog_name::String) => Type -> Maybe (FilePath, Doc)
typeFlatbufJavaBinding t@TUser{..} = Just ( "ddlog" </> ?prog_name </> (render class_name) <.> "java"
                                          , "package ddlog." <> pp ?prog_name <> ";" $$
                                            code)
    where
    class_name = fbStructName typeName typeArgs
    type_type = if length (typeCons $ typ' ?d t) <= 127
                   then "byte"
                   else "short"

    code | typeHasUniqueConstructor t =
        "public class" <+> class_name $$
        (braces' $ "protected" <+> class_name <> "(int offset) { this.offset = offset; }" $$
                   "protected int offset;")
         | otherwise =
        "public class " <> class_name $$
        (braces' $ "protected" <+> class_name <> "(" <> type_type <+> "type, int offset) { this.type = type; this.offset = offset; }" $$
                   "protected" <+> type_type <+> "type;"                                                                              $$
                   "protected int offset;")

typeFlatbufJavaBinding t@TTuple{..} = Just ( "ddlog" </> ?prog_name </> (render class_name) <.> "java"
                                           , "package ddlog." <> pp ?prog_name <> ";" $$
                                             code)
    where
    class_name = fbTupleName typeTupArgs
    code = "public class" <+> class_name $$
           (braces' $ "protected" <+> class_name <> "(int offset) { this.offset = offset; }" $$
                      "protected int offset;")

typeFlatbufJavaBinding _ = Nothing

{- Rust bindings -}

rustValueFromFlatbuf :: (?d::DatalogProgram, ?cfg::R.CompilerConfig) => Doc
rustValueFromFlatbuf =
    "impl <'a> FromFlatBuffer<(fb::__Value, fbrt::Table<'a>)> for Value {"              $$
    "    fn from_flatbuf(v: (fb::__Value, fbrt::Table<'a>)) -> Response<Self> {"        $$
    "        match v.0 {"                                                               $$
    (nest' $ nest' $ nest' $ vcat enums)                                                $$
    "            _ => Err(\"Value::from_flatbuf: invalid value type\".to_string())"     $$
    "        }"                                                                         $$
    "    }"                                                                             $$
    "}"
    where
    enums = map (\rel@Relation{..} ->
                    "fb::__Value::" <> typeTableName rel <+> "=> Ok(" <>
                        R.mkValue' ?d ("<" <> R.mkType relType <> ">::from_flatbuf(fb::" <> typeTableName rel <> "::init_from_table(v.1))?") relType <> "),")
                progIORelations

-- Deserialize struct with unique constructor.  Such structs are stored in
-- tables.
rustTypeFromFlatbuf :: (?d::DatalogProgram) => Type -> Doc
rustTypeFromFlatbuf t@TUser{..} | typeHasUniqueConstructor t =
    "impl <'a> FromFlatBuffer<fb::" <> typeTableName t <> "<'a>> for" <+> rtype <+> "{" $$
    "    fn from_flatbuf(v: fb::" <> typeTableName t <> "<'a>) -> Response<Self> {"     $$
    "        Ok(" <> R.rname typeName <> (braces $ commaSep args) <> ")"                $$
    "    }"                                                                             $$
    "}"
    where
    rtype = R.mkType t
    tstruct = typ' ?d t
    args = map (\a -> pp (name a) <> ": <" <> R.mkType a <> ">::from_flatbuf(" <> extract_field rtype a <> ")?")
               $ consArgs $ head $ typeCons tstruct

-- Deserialize struct with multiple constructor.  Such structs are stored in
-- unions and can be additionally wrapped in a table if the struct occurs inside
-- the Value union or vector.  We therefore generate to FromFlatBuffer
-- implementations: one to deserialize the struct from union, represented as a
-- (type, table) pair, and one to deserialize it from a table.
rustTypeFromFlatbuf t@TUser{..} =
    "impl <'a> FromFlatBuffer<(" <> fbstruct <> ", fbrt::Table<'a>)> for" <+> rtype <+> "{"                                                     $$
    "    fn from_flatbuf(v: (" <> fbstruct <> ", fbrt::Table<'a>)) -> Response<Self> {"                                                         $$
    "        match v.0 {"                                                                                                                       $$
    (nest' $ nest' $ nest' $ vcat cons)                                                                                                         $$
    "            _ => Err(\"Value::from_flatbuf: invalid value type\".to_string())"                                                             $$
    "        }"                                                                                                                                 $$
    "    }"                                                                                                                                     $$
    "}"                                                                                                                                         $$
    "impl <'a> FromFlatBuffer<fb::" <> typeTableName t <> "<'a>> for" <+> rtype <+> "{"                                                         $$
    "    fn from_flatbuf(v: fb::" <> typeTableName t <> "<'a>) -> Response<Self> {"                                                             $$
    "        let v_type = v.v_type();"                                                                                                          $$
    "        let v_table = v.v().ok_or_else(||format!(\"" <> rtype <> "::from_flatbuf: invalid buffer: failed to extract nested table\"))?;"    $$
    "        <" <> rtype <> ">::from_flatbuf((v_type,v_table))"                                                                                 $$
    "    }"                                                                                                                                     $$
    "}"
    where
    rtype = R.mkType t
    fbstruct = "fb::" <> fbStructName typeName typeArgs
    tstruct = typ' ?d t
    cons = map (\c -> let fbcname = fbConstructorName typeArgs c
                          cname = R.rname (name typeName) <> "::" <> R.rname (name c)
                          args = map (\a -> pp (name a) <> ": <" <> R.mkType a <> ">::from_flatbuf(" <> extract_field cname a <> ")?")
                                     $ consArgs c
                      in fbstruct <> "::" <> fbcname <+> "=> {"                                $$
                         "    let v = fb::" <> fbcname <> "::init_from_table(v.1);"            $$
                         "    Ok(" <> cname <> (braces $ commaSep args) <> ")"                 $$
                         "},")
               $ typeCons tstruct

-- Deserialize Tuples.
rustTypeFromFlatbuf t@TTuple{..} =
    "impl <'a> FromFlatBuffer<fb::" <> typeTableName t <> "<'a>> for" <+> rtype <+> "{" $$
    "    fn from_flatbuf(v: fb::" <> typeTableName t <> "<'a>) -> Response<Self> {"     $$
    "        Ok((" <> commaSep args <> "))"                                             $$
    "    }"                                                                             $$
    "}"
    where
    rtype = R.mkType t
    tstruct = typ' ?d t
    args = mapIdx (\a i -> "<" <> R.mkType a <> ">::from_flatbuf(" <> extract_field rtype (Field nopos ("a" ++ show i) a) <> ")?")
                  $ typeTupArgs

-- Container types (vectors, sets, maps).  We implement 'FromFlatBuffer<fb::Vector>' for
-- containers in their corresponding libraries.  Here we additionally generate
-- 'FromFlatBuffer<fb::>' for wrapper tables.
rustTypeFromFlatbuf t@TOpaque{} =
    "impl <'a> FromFlatBuffer<fb::" <> typeTableName t <> "<'a>> for" <+> rtype <+> "{"                                                         $$
    "    fn from_flatbuf(v: fb::" <> typeTableName t <> "<'a>) -> Response<Self> {"                                                             $$
    "        let vec = v.v().ok_or_else(||format!(\"" <> rtype <> "::from_flatbuf: invalid buffer: failed to extract nested vector\"))?;"       $$
    "        <" <> rtype <> ">::from_flatbuf(vec)"                                                                                              $$
    "    }"                                                                                                                                     $$
    "}"
    where
    rtype = R.mkType t

-- For builtin types ('bool', 'bit<>', 'signed', 'string', 'bigint'),
-- 'FromFlatBuffer<>' is implemented in the 'flatbuf.rs' template.
-- For library types (containers, 'Ref<>', etc), 'FromFlatBuffer<>' is
-- implemented in their corresponding library modules.
rustTypeFromFlatbuf _ = empty

extract_field :: (?d::DatalogProgram) => Doc -> Field -> Doc
extract_field container f | typeIsScalar f = "v." <> pp (name f) <> "()"
                          | typeHasUniqueConstructor f =
    "v." <> pp (name f) <>
    "().ok_or_else(||format!(\"" <> container <> "::from_flatbuf: invalid buffer: failed to extract " <> pp (name f) <> "\"))?"
                          | otherwise =
    "(v." <> pp (name f) <> "_type()," <+>
    "v." <> pp (name f) <>
    "().ok_or_else(||format!(\"" <> container <> "::from_flatbuf: invalid buffer: failed to extract " <> pp (name f) <> "\"))?)"
