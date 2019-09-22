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
import qualified Text.Casing as Casing
--import Debug.Trace
import qualified Data.ByteString.Char8 as BS

import Language.DifferentialDatalog.Syntax
import Language.DifferentialDatalog.Type
import Language.DifferentialDatalog.PP
import Language.DifferentialDatalog.Util
import Language.DifferentialDatalog.Pos
import Language.DifferentialDatalog.Name
import Language.DifferentialDatalog.NS
import Language.DifferentialDatalog.Relation
import {-# SOURCE #-} qualified Language.DifferentialDatalog.Compile as R -- "R" for "Rust"

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
    let flatcj_proc = (proc "flatc" (["-j", "-o", "java/", "flatbuf.fbs"])) {
                          cwd = Just $ dir </> "flatbuf"
                     }
    (jcode, jstdo, jstde) <- readCreateProcessWithExitCode flatcj_proc ""
    when (jcode /= ExitSuccess) $ do
        errorWithoutStackTrace $ "flatc failed with exit code " ++ show jcode ++
                                 "\nstdout:\n" ++ jstde ++
                                 "\n\nstdout:\n" ++ jstdo
    -- compile Rust bindings for FlatBuffer schema
    let flatcr_proc = (proc "flatc" (["-r", "flatbuf.fbs"])) {
                          cwd = Just $ dir </> "flatbuf"
                     }
    (rcode, rstdo, rstde) <- readCreateProcessWithExitCode flatcr_proc ""
    when (rcode /= ExitSuccess) $ do
        errorWithoutStackTrace $ "flatc failed with exit code " ++ show rcode ++
                                 "\nstdout:\n" ++ rstde ++
                                 "\n\nstdout:\n" ++ rstdo
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
        types = map typeFlatbufSchema
                    $ nubBy (\t1 t2 -> fbType t1 == fbType t2)
                    $ progTypesToSerialize
        default_relid = if null rels then "0" else (pp $ relIdentifier ?d $ head rels)
    in
    "namespace" <+> jFBPackage <> ";"                                           $$
    "table __BigUint { bytes: [uint8]; }"                                       $$
    "table __BigInt { sign: bool; bytes: [uint8]; }"                            $$
    "table __String { v: string; }"                                             $$
    ""                                                                          $$
    "// Program type declarations"                                              $$
    (vcat $ intersperse "" types)                                               $$
    ""                                                                          $$
    "// Union of all program relation types"                                    $$
    "union __Value"                                                             $$
    (braces' $ vcommaSep $ nub $ map typeTableName rels)                        $$
    ""                                                                          $$
    "// DDlog commands"                                                         $$
    "enum __CommandKind: uint8 { Insert, Delete }"                              $$
    "table __Command {"                                                         $$
    "   kind: __CommandKind;"                                                   $$
    "   relid: uint64 =" <+> default_relid <> ";"                               $$
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
    let ?prog_name = prog_name
    let rust_template = replace "datalog_example" prog_name $ BS.unpack $(embedFile "rust/template/src/flatbuf.rs")
    updateFile (dir </> "src/flatbuf.rs") $ render $
        (pp rust_template)                                              $$
        "use flatbuf_generated::ddlog::" <> rustFBModule <+> " as fb;"  $$
        -- Re-export '__String', so we can use it to implement 'To/FromFlatBuf'
        -- for 'IString' in 'intern.rs'.
        "pub use flatbuf_generated::ddlog::" <> rustFBModule <+> "::__String;"  $$
        rustValueFromFlatbuf                                            $$
        (vcat $ map rustTypeFromFlatbuf
              -- One FromFlatBuffer implementation per Rust type
              $ nubBy (\t1 t2 -> R.mkType t1 == R.mkType t2)
              $ progTypesToSerialize)

-- | Generate Java convenience API that provides a type-safe way to serialize/deserialize
-- commands to a FlatBuffer.
compileFlatBufferJavaBindings :: DatalogProgram -> String -> [(FilePath, Doc)]
compileFlatBufferJavaBindings d prog_name =
    let ?d = d
        ?prog_name = prog_name in
    let writers = mapMaybe typeFlatbufJavaWriter progTypesToSerialize
        readers = mapMaybe typeFlatbufJavaReader progTypesToSerialize
    in
    mkJavaRelEnum : mkJavaBuilder : mkJavaParser : mkCommandReader : writers ++ readers

{- Helpers -}

jFBPackage :: (?prog_name :: String) => Doc
jFBPackage = "ddlog.__" <> pp ?prog_name

rustFBModule :: (?prog_name :: String) => Doc
rustFBModule = "__" <> (pp $ Casing.quietSnake ?prog_name)

-- Convert DDlog field name to Java accessor method name,
-- replicating the following logic from the `flatc` compiler.
{-
std::string MakeCamel(const std::string &in, bool first) {
  std::string s;
  for (size_t i = 0; i < in.length(); i++) {
    if (!i && first)
      s += static_cast<char>(toupper(in[0]));
    else if (in[i] == '_' && i + 1 < in.length())
      s += static_cast<char>(toupper(in[++i]));
    else
      s += in[i];
  }
  return s;
}
-}
jAccessorName :: String -> Doc
jAccessorName ""        = ""
jAccessorName "_"       = "_"
jAccessorName ('_':x:s) = char (toUpper x) <> jAccessorName s
jAccessorName (x:s)     = char x <> jAccessorName s

-- True if 'a' is of a type that is declared as a table and therefore does not
-- need to be wrapped in another table in order to be used inside a vector or
-- union.
typeIsTable :: (WithType a, ?d::DatalogProgram) => a -> Bool
typeIsTable x =
    let t = typeNormalizeForFlatBuf x in
    -- struct with unique constructor.
    isStruct ?d t && typeHasUniqueConstructor t ||
    isTuple ?d t                                ||
    fbType t == "__BigInt"                      ||
    fbType t == "__BigUint"

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
         TBit{..}    | typeWidth <= 8
                       -> "__Bit8"
         TBit{..}    | typeWidth <= 16
                       -> "__Bit16"
         TBit{..}    | typeWidth <= 32
                       -> "__Bit32"
         TBit{..}    | typeWidth <= 64
                       -> "__Bit64"
                     | otherwise
                       -> "__BigUint"
         TSigned{..} | typeWidth <= 8
                       -> "__Signed8"
         TSigned{..} | typeWidth <= 16
                       -> "__Signed16"
         TSigned{..} | typeWidth <= 32
                       -> "__Signed32"
         TSigned{..} | typeWidth <= 64
                       -> "__Signed64"
                     | otherwise
                       -> "__BigInt"
         TTuple{..}    -> fbTupleName typeTupArgs
         TUser{..} | typeHasUniqueConstructor x
                       -> fbStructName typeName typeArgs
                   | otherwise
                       -> "__Table_" <> fbStructName typeName typeArgs
         TOpaque{typeArgs = [elemType], ..} | elem typeName sET_TYPES
                       -> "__Table_Vec_" <> mkTypeIdentifier elemType
         TOpaque{typeArgs = [keyType, valType], ..} | typeName == mAP_TYPE
                       -> typeTableName $ tOpaque "std.Vec" [tTuple [keyType, valType]]
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
         t | typeIsScalar t
                     -> tdecl
         TString{}   -> empty
         TBit{}      -> empty
         TSigned{}   -> empty
         TInt{}      -> empty
         TTuple{..}  -> tupleFlatBufferSchema typeTupArgs
         t@TUser{..} ->
             -- For structs, only generate a union if the struct has more than one
             -- constructor; otherwise, create a table whose name matches the name of the
             -- struct (not the name of the constructor).
             vcat constructors $+$
             opt_union
             where
             tstruct = typ' ?d t
             constructors = map (constructorFlatbufSchema typeArgs) $ typeCons tstruct
             opt_union = if typeHasUniqueConstructor t
                            then empty
                            else "union" <+> fbStructName typeName typeArgs <+> (braces $ commaSep $ map (fbConstructorName typeArgs) $ typeCons tstruct) $$
                                 "table" <+> typeTableName t $$
                                 (braces' $ "v:" <+> fbType t <> ";")
         _ -> tdecl
    where
    tdecl= "table" <+> typeTableName x $+$
           (braces' $ "v:" <+> fbType x <> ";")

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
         TUser{..}              -> fbStructName typeName typeArgs
         TOpaque{typeArgs = [elemType], ..} | elem typeName sET_TYPES
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
mkRelId Relation{..} = pp $ legalize relName

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

{- Functions to work with the FlatBuffers-generated Java API. -}

jFBCallConstructor :: (?prog_name::String) => Doc -> [Doc] -> Doc
jFBCallConstructor table [] =
    "((java.util.function.Supplier<Integer>) (() -> " $$
    (braces' $ jFBPackage <> "." <> table <> ".start" <> table <> "(this.fbbuilder);"            $$
              "return Integer.valueOf(" <+> jFBPackage <> "." <> table <> ".end" <> table <> "(this.fbbuilder));") <> ")).get()"
jFBCallConstructor table args =
    jFBPackage <> "." <> table <> ".create" <> table <>
           (parens $ commaSep $ "this.fbbuilder" : args)

{- Java convenience API. -}

-- Java types used in the FlafBuffers-generated _serialization_ API
-- (primitive types are passed as is, tables are passed as offsets)
jFBWriteType :: (WithType a, ?d::DatalogProgram) => Bool -> a -> Doc
jFBWriteType nested x =
    case typeNormalizeForFlatBuf x of
        TBool{}            -> "boolean"
        TInt{}             -> "int"
        TString{}          -> "int"
        -- FlatBuffers currently have what I think is a bug, where 'uint' types
        -- are represented differently in array and non-array context on the
        -- write path only:
        -- (https://github.com/google/flatbuffers/issues/5513)
        TBit{..} | nested && typeWidth <= 8
                           -> "byte"
        TBit{..} | nested && typeWidth <= 16
                           -> "short"
        TBit{..} | nested && typeWidth <= 32
                           -> "int"
        TBit{..} | nested && typeWidth <= 64
                           -> "long"
        -- Not in array context:
        TBit{..} | typeWidth <= 16
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
                                 else jFBWriteType True elemType <> "[]"
        TOpaque{typeArgs = [_,_],..} | typeName == mAP_TYPE
                           -> if nested
                                 then "int"
                                 else "int []"
        t'                 -> error $ "FlatBuffer.jFBWriteType: unsupported type " ++ show t'

-- Java types used in the FlafBuffers-generated _de-serialization_ API
jFBReadType :: (WithType a, ?d::DatalogProgram, ?prog_name::String) => a -> Doc
jFBReadType x =
    case typeNormalizeForFlatBuf x of
        TBool{}            -> "boolean"
        TString{}          -> "String"
        TBit{..} | typeWidth <= 16
                           -> "int"
        TBit{..} | typeWidth <= 64
                           -> "long"
        TSigned{..} | typeWidth <= 32
                           -> "int"
        TSigned{..} | typeWidth <= 64
                           -> "long"
        t                  -> jFBPackage <> "." <> typeTableName t

data RW = Read | Write deriving (Eq)

-- Types used in the convenience API.
jConvType :: (WithType a, ?d::DatalogProgram) => RW -> a -> Doc
jConvType rw x =
    case typeNormalizeForFlatBuf x of
        TBool{}            -> "boolean"
        TInt{}             -> "java.math.BigInteger"
        TString{}          -> "String"
        TBit{..} | typeWidth <= 16
                           -> "int"
        TBit{..} | typeWidth <= 64
                           -> "long"
        TBit{..}           -> "java.math.BigInteger"
        TSigned{..} | typeWidth <= 8
                           -> "byte"
        TSigned{..} | typeWidth <= 16
                           -> "short"
        TSigned{..} | typeWidth <= 32
                           -> "int"
        TSigned{..} | typeWidth <= 64
                           -> "long"
        TSigned{..}        -> "java.math.BigInteger"
        TTuple{..} | rw == Read
                           -> fbTupleName typeTupArgs <> "Reader"
                   | rw == Write
                           -> fbTupleName typeTupArgs <> "Writer"
        TUser{..}  | rw == Read
                           -> fbStructName typeName typeArgs <> "Reader"
                   | rw == Write
                           -> fbStructName typeName typeArgs <> "Writer"
        TOpaque{typeArgs = [elemType], ..} | elem typeName sET_TYPES
                           -> "java.util.List<" <> jConvObjType True rw elemType <> ">"
        TOpaque{typeArgs = [keyType,valType],..} | typeName == mAP_TYPE
                           -> "java.util.Map<" <> jConvObjType False rw keyType <> "," <+> jConvObjType False rw valType <> ">"
        t'                 -> error $ "FlatBuffer.jConvType: unsupported type " ++ show t'


jConvTypeW :: (WithType a, ?d::DatalogProgram) => a -> Doc
jConvTypeW = jConvType Write

jConvTypeR :: (WithType a, ?d::DatalogProgram) => a -> Doc
jConvTypeR = jConvType Read

-- Like jConvType, but returns Java object types instead of scalars for
-- primitive types.
jConvObjType :: (WithType a, ?d::DatalogProgram) => Bool -> RW -> a -> Doc
jConvObjType in_array_context rw x =
    case typeNormalizeForFlatBuf x of
         TBool{}            -> "Boolean"
         -- FlatBuffers currently have what I think is a bug, where 'uint' types
         -- are represented differently in array and non-array context on the
         -- write path only:
         -- (https://github.com/google/flatbuffers/issues/5513)
         TBit{..} | rw == Write && in_array_context && typeWidth <= 8
                            -> "Byte"
         TBit{..} | rw == Write && in_array_context && typeWidth <= 16
                            -> "Short"
         TBit{..} | rw == Write && in_array_context && typeWidth <= 32
                            -> "Integer"
         TBit{..} | rw == Write && in_array_context && typeWidth <= 64
                            -> "Long"
         TBit{..} | typeWidth <= 16
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
         _                  -> jConvType rw x


jConvObjTypeW :: (WithType a, ?d::DatalogProgram) => a -> Doc
jConvObjTypeW = jConvObjType False Write

jConvObjTypeR :: (WithType a, ?d::DatalogProgram) => a -> Doc
jConvObjTypeR = jConvObjType False Read

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
        FBField{..} | typeIsVector t
                -> jFBPackage <> "." <> fbctxTable <> ".create" <> pp (capitalize fbctxField) <> "Vector(this.fbbuilder," <> e' <> ")"
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
            ot@TOpaque{typeArgs = [_], ..}
                               -> "this.create_" <> mkTypeIdentifier ot <> parens e
            ot@TOpaque{typeArgs = [_,_], ..} | typeName == mAP_TYPE
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
jConvCreateTable :: (?d::DatalogProgram, ?prog_name::String) => Type -> Maybe Constructor -> Doc
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

parserClass :: (?prog_name::String) => String
parserClass = ?prog_name ++ "UpdateParser"

relEnum :: (?prog_name::String) => String
relEnum = ?prog_name ++ "Relation"

-- Create builder class with methods to create FlatBuffer-backed instances of DDlog
-- types.
mkJavaRelEnum :: (?d::DatalogProgram, ?prog_name::String) => (FilePath, Doc)
mkJavaRelEnum = ("ddlog" </> ?prog_name </> relEnum <.> "java",
    "// Automatically generated by the DDlog compiler."                 $$
    "package ddlog." <> pp ?prog_name <> ";"                            $$
    "public final class" <+> pp relEnum                                 $$
    (braces' $ vcat rels))
    where
    rels = map (\rel -> "public static final int" <+> mkRelId rel <+>
                        "=" <+> pp (relIdentifier ?d rel) <> ";")
               progIORelations

-- Create builder class with methods to create FlatBuffer-backed instances of DDlog
-- types.
mkJavaBuilder :: (?d::DatalogProgram, ?prog_name::String) => (FilePath, Doc)
mkJavaBuilder = ("ddlog" </> ?prog_name </> builderClass <.> "java",
    "// Automatically generated by the DDlog compiler."                 $$
    "package ddlog." <> pp ?prog_name <> ";"                            $$
    "import ddlogapi.DDlogAPI;"                                         $$
    "import ddlogapi.DDlogException;"                                   $$
    "import com.google.flatbuffers.*;"                                  $$
    "public class" <+> pp builderClass                                  $$
    (braces' $ "private FlatBufferBuilder fbbuilder;"                   $$
               "private java.util.Vector<Integer> commands;"            $$
               "private boolean finished;"                              $$
               "public" <+> pp builderClass <> "() {"                   $$
               "    this.fbbuilder = new FlatBufferBuilder();"          $$
               "    this.commands = new java.util.Vector<Integer>();"   $$
               "    this.finished = false;"                             $$
               "}"                                                      $$
               "public void applyUpdates(DDlogAPI hddlog)"              $$
               "    throws DDlogException {"                            $$
               "    if (this.finished) {"                               $$
               "        throw new IllegalStateException(\"applyUpdates() can only be invoked once for a" <+>
                                    pp builderClass <+> "instance.\");" $$
               "    }"                                                  $$
               "    int[] cmds = new int[this.commands.size()];"        $$
               "    for(int i = 0; i < cmds.length; i++)"               $$
               "        cmds[i] = this.commands.get(i);"                $$
               "    int cmdvec =" <+> jFBCallConstructor "__Commands"
                    [jFBPackage <> ".__Commands.createCommandsVector(this.fbbuilder,cmds)"] <> ";"
                                                                        $$
               "    this.fbbuilder.finish(cmdvec);"                     $$
               "    this.finished = true;"                              $$
               "    hddlog.applyUpdatesFromFlatBuf(this.fbbuilder.dataBuffer());" $$
               "}"                                                      $$
               (vcat $ map mk_command_constructors
                     $ filter ((== RelInput) . relRole)
                     $ M.elems $ progRelations ?d)                      $$
               (vcat $ map mk_type_factory progTypesToSerialize)))
    where
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
                                TStruct{..} -> map (\a -> jConvTypeW a <+> pp (name a)) $ consArgs $ typeCons !! 0
                                TTuple{..}  -> mapIdx (\a i -> jConvTypeW a <+> "a" <> pp i) typeTupArgs
                                _           -> [jConvTypeW relType <+> "v"] in
                "public void" <+> lcmd <> "_" <> mkRelId rel <> "(" <> commaSep args <> ")" $$
                (braces' $ "int cmd =" <+> jFBCallConstructor "__Command"
                                           [ jFBPackage <> ".__CommandKind." <> pp cmd
                                           , (pp $ relIdentifier ?d rel)
                                           , jFBPackage <> ".__Value." <> typeTableName relType
                                           , jConvCreateTable relType Nothing] <> ";"  $$
                           "this.commands.add(Integer.valueOf(cmd));")
           else -- Relation type is a struct with multiple constructors
                vcat $
                map (\c@Constructor{..} ->
                     "public void" <+> lcmd <> "_" <> mkRelId rel <> "_" <> (pp $ legalize $ name c) <>
                         "(" <> (commaSep $ map (\a -> jConvTypeW a <+> pp (name a)) consArgs) <> ")" $$
                     (braces' $ "int cmd =" <+> jFBCallConstructor "__Command"
                                                [ jFBPackage <> ".__CommandKind." <> pp cmd
                                                , (pp $ relIdentifier ?d rel)
                                                , jFBPackage <> ".__Value." <> typeTableName relType
                                                , jConvCreateTable relType (Just c)] <> ";"  $$
                                "this.commands.add(Integer.valueOf(cmd));"))
                    $ typeCons $ typ' ?d $ typeNormalizeForFlatBuf relType

    -- Create constructor methods
    mk_type_factory t@TUser{..} | typeHasUniqueConstructor t =
        -- Unique constructor: generate a single 'create_XXX' method with the
        -- name that matches type name, not constructor name.
        "public" <+> tname <+> "create_" <> typeTableName t <> "(" <> commaSep (map (\arg -> jConvTypeW arg <+> pp (name arg)) consArgs) <> ")" $$
        (braces' $ "return new" <+> tname <> "(" <>
                   (jFBCallConstructor (typeTableName t) $ map (\a -> jConv2FBType (FBField (fbType t) (name a)) (pp $ name a) (typ a)) consArgs) <> ");")
                                | otherwise =
        -- Multiple constructors: create method per constructor with method name
        -- matching constructor name.
        vcat (map (mk_cons_factory t) $ typeCons tstruct)
        where
        tname = jConvTypeW t
        tstruct = typ' ?d t
        Constructor{..} = head $ typeCons tstruct

    mk_type_factory t@TTuple{..}  =
        "public" <+> jConvTypeW t <+> "create_" <> typeTableName t <> "(" <> commaSep (mapIdx (\a i -> jConvTypeW a <+> "a" <> pp i) typeTupArgs) <> ")" $$
        (braces' $ "return new" <+> jConvTypeW t <> "(" <>
                   (jFBCallConstructor (typeTableName t) $ mapIdx (\a i -> jConv2FBType (FBField (fbType t) ("a" ++ show i)) ("a" <> pp i) a) typeTupArgs) <> ");")

    mk_type_factory t@TOpaque{typeArgs = [elemType], ..} | elem typeName sET_TYPES =
        -- Generate a private method to convert vector of 'elemType'
        -- into vector of offsets (the method is only used by code generated by
        -- 'jConv2FBType')
        "private" <+> jFBWriteType False t <+> "create_" <> mkTypeIdentifier t <> (parens $ jConvTypeW t <+> "v")    $$
        (braces' $ jFBWriteType False t <+> "res = new" <+> jFBWriteType True elemType <> "[v.size()];"             $$
                   "for (int __i = 0; __i < v.size(); __i++)"                                                       $$
                   (braces' $ "res[__i] =" <+> jConv2FBType FBArray "v.get(__i)" elemType <> ";")                   $$
                   "return res;")

    mk_type_factory t@TOpaque{typeArgs = [keyType, valType], ..} | typeName == mAP_TYPE =
        -- Generate a private method to Map<keyType,valType> into vector of offsets
        "private" <+> jFBWriteType False t <+> "create_" <> mkTypeIdentifier t <> (parens $ jConvTypeW t <+> "v")                $$
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
        tentry = "java.util.Map.Entry<" <> jConvObjTypeW keyType <> "," <+> jConvObjTypeW valType <> ">"
        entry_table = typeTableName $ tTuple [keyType, valType]

    mk_type_factory _ = empty

    mk_cons_factory t c@Constructor{..} =
        "public" <+> tname <+> "create_" <> cname <> "(" <> commaSep (map (\arg -> jConvTypeW arg <+> pp (name arg)) consArgs) <> ")" $$
        (braces' $ "return new" <+> tname <> "(" <>
                   jFBPackage <> "." <> fbtname <> "." <> cname <> "," <+>
                   (jFBCallConstructor cname $ map (\a -> jConv2FBType (FBField cname (name a)) (pp $ name a) (typ a)) consArgs) <> ");")
        where
        tname = jConvTypeW t
        cname = fbConstructorName (typeArgs t) c
        fbtname = fbType t


-- Create parser class with methods to extract updates from FlatBuffer
mkJavaParser :: (?d::DatalogProgram, ?prog_name::String) => (FilePath, Doc)
mkJavaParser = ("ddlog" </> ?prog_name </> parserClass <.> "java",
    "// Automatically generated by the DDlog compiler."                                                                 $$
    "package ddlog." <> pp ?prog_name <> ";"                                                                            $$
    "import ddlogapi.DDlogAPI;"                                                                                         $$
    "import ddlogapi.DDlogCommand;"                                                                                     $$
    "import ddlogapi.DDlogException;"                                                                                   $$
    "import com.google.flatbuffers.*;"                                                                                  $$
    "public class" <+> pp parserClass                                                                                   $$
    (braces' $ "public" <+> pp parserClass <> "(java.nio.ByteBuffer bb) {"                                              $$
               "    this.commands =" <+> jFBPackage <> ".__Commands.getRootAs__Commands(bb);"                           $$
               "}"                                                                                                      $$
               "private" <+> jFBPackage <> ".__Commands commands;"                                                      $$
               "public static byte[] byteBufferToArray(java.nio.ByteBuffer buf) {"                                      $$
               "    byte[] arr = new byte[buf.remaining()];"                                                            $$
               "    buf.get(arr);"                                                                                      $$
               "    return arr;"                                                                                        $$
               "}"                                                                                                      $$
               "public int numCommands() {"                                                                             $$
               "    return this.commands.commandsLength();"                                                             $$
               "}"                                                                                                      $$
               "public CommandReader command(int i){"                                                                   $$
               "    if (i >= this.numCommands() || i < 0) {"                                                            $$
               "        throw new IndexOutOfBoundsException(\"Command index \" + i + \" out of bounds\");"              $$
               "    }"                                                                                                  $$
               "    return new CommandReader(this.commands.commands(i));"                                               $$
               "}"                                                                                                      $$
               "public static void transactionCommitDumpChanges(DDlogAPI hddlog,"                                       $$
               "        java.util.function.Consumer<DDlogCommand<Object>> callback) throws DDlogException{"             $$
               "    DDlogAPI.FlatBufDescr fb = new DDlogAPI.FlatBufDescr();"                                            $$
               "    hddlog.transactionCommitDumpChangesToFlatbuf(fb);"                                                  $$
               "    try {"                                                                                              $$
               "        " <> pp parserClass <+> "parser = new" <+> pp parserClass <> "(fb.buf);"                        $$
               "        int ncmds = parser.numCommands();"                                                              $$
               "        for (int i = 0; i < ncmds; i++) {"                                                              $$
               "            callback.accept(parser.command(i));"                                                        $$
               "        }"                                                                                              $$
               "    } finally { hddlog.flatbufFree(fb); }"                                                              $$
               "}"))

mkCommandReader :: (?d::DatalogProgram, ?prog_name::String) => (FilePath, Doc)
mkCommandReader = ("ddlog" </> ?prog_name </> "CommandReader" <.> "java",
    "// Automatically generated by the DDlog compiler."                                                         $$
    "package ddlog." <> pp ?prog_name <> ";"                                                                    $$
    "import com.google.flatbuffers.*;"                                                                          $$
    "import ddlogapi.DDlogCommand;"                                                                             $$
    "public final class CommandReader implements DDlogCommand<Object>"                                          $$
    (braces' $ "protected CommandReader (" <> jFBPackage <> ".__Command inner) { this.inner = inner; }"         $$
               "private" <+> jFBPackage <> ".__Command inner;"                                                  $$
               "public final Kind kind(){"                                                                      $$
               "    if (inner.kind() ==" <+> jFBPackage <> ".__CommandKind.Insert) {"                           $$
               "        return DDlogCommand.Kind.Insert;"                                                       $$
               "    } if (inner.kind() ==" <+> jFBPackage <> ".__CommandKind.Delete) {"                         $$
               "        return DDlogCommand.Kind.DeleteVal;"                                                    $$
               "    } else {"                                                                                   $$
               "        throw new IllegalArgumentException(\"Unsupported command kind\" + inner.kind());"       $$
               "    }"                                                                                          $$
               "}"                                                                                              $$
               "public final int relid() { return (int)this.inner.relid(); }"                                   $$
               "public final Object value() {"                                                                  $$
               "    switch (this.relid()) {"                                                                    $$
               (nest' $ nest' $ vcat cases)                                                                     $$
               "        default: throw new IllegalArgumentException(\"Invalid relation id\" + this.relid());"   $$
               "    }"                                                                                          $$
               "}"))
    where
    cases = map (\rel -> let t = relType rel
                             jt = jFBPackage <> "." <> typeTableName t in
                         "case" <+> pp (relIdentifier ?d rel) <> ": {"                                      $$
                         "    " <> jt <+> "val =" <+> parens jt <> "this.inner.val(new" <+> jt <> "());"    $$
                         "    return (Object)" <+> jReadField 0 FBUnion "val" t <> ";"                      $$
                         "}")
                progIORelations

typeFlatbufJavaWriter :: (?d::DatalogProgram, ?prog_name::String) => Type -> Maybe (FilePath, Doc)
typeFlatbufJavaWriter t@TUser{..} = Just ( "ddlog" </> ?prog_name </> (render class_name) <.> "java"
                                         , "// Automatically generated by the DDlog compiler."  $$
                                           "package ddlog." <> pp ?prog_name <> ";"             $$
                                           code)
    where
    class_name = jConvTypeW t
    type_type = if length (typeCons $ typ' ?d t) <= 127
                   then "byte"
                   else "short"

    code | typeHasUniqueConstructor t =
        "public final class" <+> class_name $$
        (braces' $ "protected" <+> class_name <> "(int offset) { this.offset = offset; }" $$
                   "protected int offset;")

         | otherwise =
        "public final class" <+> class_name $$
        (braces' $ "protected" <+> class_name <> "(" <> type_type <+> "type, int offset) { this.type = type; this.offset = offset; }" $$
                   "protected" <+> type_type <+> "type;"                                                                              $$
                   "protected int offset;")

typeFlatbufJavaWriter t@TTuple{..} = Just ( "ddlog" </> ?prog_name </> (render class_name) <.> "java"
                                         , "package ddlog." <> pp ?prog_name <> ";" $$
                                           code)
    where
    class_name = jConvTypeW t
    code = "public final class" <+> class_name $$
           (braces' $ "protected" <+> class_name <> "(int offset) { this.offset = offset; }" $$
                      "protected int offset;")

typeFlatbufJavaWriter _ = Nothing

typeFlatbufJavaReader :: (?d::DatalogProgram, ?prog_name::String) => Type -> Maybe (FilePath, Doc)
typeFlatbufJavaReader t@TUser{..} = Just ( "ddlog" </> ?prog_name </> (render class_name) <.> "java"
                                         , "// Automatically generated by the DDlog compiler."  $$
                                           "package ddlog." <> pp ?prog_name <> ";"             $$
                                           "import com.google.flatbuffers.*;"                   $$
                                           code)
    where
    class_name = jConvTypeR t
    fb_class_name = jFBPackage <> "." <> fbStructName typeName typeArgs
    t' = typ' ?d t
    type_type = if length (typeCons t') <= 127
                   then "byte"
                   else "short"

    code | typeHasUniqueConstructor t =
        "public final class" <+> class_name $$
        (braces' $ "protected" <+> class_name <> "(" <> fb_class_name <+> "inner) { this.inner = inner; }" $$
                   "private" <+> fb_class_name <+> "inner;"                                                $$
                   (accessors $ head $ typeCons t'))

         | otherwise =
        "public class" <+> class_name $$
        (braces' $
            "protected static" <+> class_name <+> "init(" <> type_type <+> "type, java.util.function.UnaryOperator<Table> inner)" $$
            (braces' $
                "switch (type)" $$
                (braces' $ vcat
                 $ map (\c -> let cname = fbConstructorName typeArgs c
                                  fb_cname = jFBPackage <> "." <> cname in
                              "case" <+> fb_class_name <> "." <> cname <> ": return new" <+>
                              pp (legalize $ name c) <> "((" <> fb_cname <> ") inner.apply(new" <+> fb_cname <> "()));")
                 $ typeCons t')                                            $$
                 ("throw new RuntimeException(\"Invalid type \" + type + \" in" <+> class_name <> "\");")) $$
        (vcat $ map subclass $ typeCons t'))

    subclass :: Constructor -> Doc
    subclass c =
        "public static final class" <+> pp (legalize $ name c) <+> "extends" <+> class_name $$
        (braces' $ "protected" <+> pp (legalize $ name c) <> "(" <> fb_cons_name <+> "inner) { this.inner = inner; }" $$
                   "private" <+> fb_cons_name <+> "inner;"                                            $$
                   (accessors c))
        where fb_cons_name = jFBPackage <> "." <> fbConstructorName typeArgs c

    accessors :: Constructor -> Doc
    accessors Constructor{..} = vcat $ map accessor consArgs

    accessor :: Field -> Doc
    accessor f =
        "public" <+> jConvTypeR f <+> pp (name f) <+> "()" $$
        (braces' $ "return" <+> jReadField 0 (FBField class_name (name f)) "this.inner" (typ f) <> ";")

typeFlatbufJavaReader tt@TTuple{..} = Just ( "ddlog" </> ?prog_name </> (render class_name) <.> "java"
                                           , "// Automatically generated by the DDlog compiler."    $$
                                             "package ddlog." <> pp ?prog_name <> ";"               $$
                                             "import com.google.flatbuffers.*;"                     $$
                                             code)
    where
    class_name = jConvTypeR tt
    fb_class_name = jFBPackage <> "." <> fbTupleName typeTupArgs
    code = "public final class" <+> class_name $$
           (braces' $ "protected" <+> class_name <> "(" <> fb_class_name <+> "inner) { this.inner = inner; }" $$
                      "private" <+> fb_class_name <+> "inner;"                                                $$
                      accessors)
    accessors = vcat $ mapIdx accessor typeTupArgs

    accessor :: Type -> Int -> Doc
    accessor t i =
        "public" <+> jConvTypeR t <+> "a" <> pp i <+> "()" $$
        (braces' $ "return" <+> jReadField 0 (FBField class_name ("a" ++ show i)) "this.inner" t <> ";")

typeFlatbufJavaReader _ = Nothing

-- Extract value from context.
-- Java expression 'e' contains a FlatBuffers-native Java type that represents
-- 'fbctx' context.  Convert it to the corresponding convenience type.
jReadField :: (?d::DatalogProgram, ?prog_name::String) => Int -> FBCtx -> Doc -> Type -> Doc
jReadField nesting fbctx e t =
    case fbctx of
         -- Anything that's not a table must be wrapped in a table to be stored
         -- in a union
         FBUnion     | typeIsTable t
                     -> do_read e
                     | otherwise
                     -> jReadField (nesting+1) (FBField (typeTableName t) "v") e t
         -- Anything that's not a union or array can be stored in an array; unions and arrays must be
         -- wrapped in a table first.
         FBArray     | typeHasUniqueConstructor t && not (typeIsVector t)
                     -> do_read e
                     | otherwise
                     -> jReadField (nesting+1) (FBField (typeTableName t) "v") e t
         -- Vectors are accessed via a pair of methods: `e.<field>Size` and
         -- `e.<field>(i)`
         FBField{..} | typeIsVector t
                     -> do_read (e <> "." <> pp fbctxField)
                     | not (typeHasUniqueConstructor t)
                     -> do_read (e <> "." <> jAccessorName fbctxField <> "Type()," <+>
                                 "(Table t) ->" <+> e <> "." <> jAccessorName fbctxField <> "(t)")
                     | otherwise
                     -> do_read (e <> "." <> jAccessorName fbctxField <> "()")
    where
    do_read e' = case typeNormalizeForFlatBuf t of
                      TBool{}            -> e'
                      TInt{}             -> bigint
                      TString{}          -> e'
                      TBit{..} | typeWidth <= 64
                                         -> (parens $ jConvTypeR t) <> e'
                      TBit{..}           -> biguint
                      TSigned{..} | typeWidth <= 64
                                         -> (parens $ jConvTypeR t) <> e'
                      TSigned{..}        -> bigint
                      TTuple{..}         -> "new" <+> jConvTypeR t <> parens e'
                      TUser{..} | typeHasUniqueConstructor t
                                         -> "new" <+> jConvTypeR t <> parens e'
                                | otherwise
                                         -> jConvTypeR t <> ".init" <> parens e'
                      TOpaque{typeArgs = [elemType], ..} | elem typeName sET_TYPES ->
                          let elem_type = jConvObjTypeR elemType
                              ltype = "java.util.List<" <> elem_type <> ">"
                              -- Add nesting depth to local variables inside lambda so that they don't
                              -- clash with variables from containing scopes.
                              vlen = "len" <> pp nesting
                              vlst = "lst" <> pp nesting
                              vi   = "i"   <> pp nesting
                              _vi  = "_i"   <> pp nesting
                              vx   = "x"   <> pp nesting in
                          "((java.util.function.Supplier<" <> ltype <> ">) (() ->"                                          $$
                          (braces' $ "int" <+> vlen <+> "=" <+> e' <> "Length();"                                           $$
                                     ltype <+> vlst <+> "= new java.util.ArrayList<" <> elem_type <> ">(" <> vlen <> ");"   $$
                                     "for (int" <+> vi <+> "= 0;" <+> vi <+> "<" <+> vlen <> ";" <+> vi <> "++)"            $$
                                     (braces' $
                                        jFBReadType elemType <+> vx <+> "=" <+> e' <> parens vi <> ";" $$
                                        vlst <> ".add(" <> jReadField (nesting+1) FBArray vx elemType <> ");")  $$
                                     "return" <+> vlst <> ";") <> ")).get()"

                      TOpaque{typeArgs = [keyType,valType], ..} | typeName == mAP_TYPE ->
                          let ktype = jConvObjTypeR keyType
                              vtype = jConvObjTypeR valType
                              mtype = "java.util.Map<" <> ktype <> "," <+> vtype <> ">"
                              ttable = typeTableName $ tTuple [keyType, valType]
                              vmap = "map" <> pp nesting
                              vi   = "i"   <> pp nesting 
                              _vi   = "_i"   <> pp nesting 
                          in
                          "((java.util.function.Supplier<" <> mtype <> ">) (() -> "         $$
                          (braces' $ mtype <+> vmap <+> "= new java.util.HashMap<" <> ktype <> "," <+> vtype <> ">();"  $$
                                     "for (int" <+> vi <+> "= 0;" <+> vi <+> "<" <+> e' <> "Length();" <+> vi <> "++)"  $$
                                     (braces' $
                                        -- To prevent Java from complaining that
                                        -- vi is not final in case it ends up
                                        -- being used in a lambda.
                                        "int" <+> _vi <+> "=" <+> vi <> ";" $$
                                        vmap <> ".put(" <>
                                        (jReadField (nesting+1) (FBField ttable "a0") (e' <> parens _vi) keyType) <> "," <+>
                                        (jReadField (nesting+1) (FBField ttable "a1") (e' <> parens _vi) valType) <> ");") $$
                                     "return" <+> vmap <> ";") <> ")).get()"
                      t'                 -> error $ "FlatBuffer.jReadField: unsupported type " ++ show t'
        where
        bigint = "new java.math.BigInteger(" <> e' <> ".sign() ? 1 : -1," <+>
                    pp parserClass <> ".byteBufferToArray(" <> e' <> ".bytesAsByteBuffer()))"
        biguint = "new java.math.BigInteger(" <> pp parserClass <> ".byteBufferToArray(" <> e' <> ".bytesAsByteBuffer()))"

{- Rust bindings -}

rustValueFromFlatbuf :: (?d::DatalogProgram, ?cfg::R.CompilerConfig) => Doc
rustValueFromFlatbuf =
    "impl Value {"                                                                                  $$
    "    fn from_flatbuf(relid: RelId, v: fbrt::Table) -> Response<Self> {"                         $$
    "        match relid {"                                                                         $$
    (nest' $ nest' $ nest' $ vcat enums)                                                            $$
    "            _ => Err(format!(\"Value::from_flatbuf: invalid relation id {}\", relid))"         $$
    "        }"                                                                                     $$
    "    }"                                                                                         $$
    "}"                                                                                             $$
    "impl <'b> ToFlatBuffer<'b> for Value {"                                                        $$
    "    type Target = (fb::__Value, fbrt::WIPOffset<fbrt::UnionWIPOffset>);"                       $$
    "    fn to_flatbuf(&self, fbb: &mut fbrt::FlatBufferBuilder<'b>) -> Self::Target {"             $$
    "        match self {"                                                                          $$
    (nest' $ nest' $ nest' $ vcat to_enums)                                                         $$
    "            val => panic!(\"Value::to_flatbuf: invalid value {}\", val)"                       $$
    "        }"                                                                                     $$
    "    }"                                                                                         $$
    "}"
    where
    enums = map (\rel@Relation{..} ->
                 pp (relIdentifier ?d rel) <+> "=> Ok(" <>
                     R.mkValue ?d ("<" <> R.mkType relType <> ">::from_flatbuf(fb::" <> typeTableName relType <> "::init_from_table(v))?")
                               relType <> "),")
                progIORelations
    to_enums = map (\t ->
                    "Value::" <> R.mkValConstructorName ?d t <> "(v) => {"                                   $$
                    "    (fb::__Value::" <> typeTableName t <> ", v.to_flatbuf_table(fbb).as_union_value())" $$
                    "},")
                   $ nubBy (\t1 t2 -> R.mkValConstructorName ?d t1 == R.mkValConstructorName ?d t2)
                   $ map relType progIORelations

-- Deserialize struct with unique constructor.  Such structs are stored in
-- tables.
rustTypeFromFlatbuf :: (?d::DatalogProgram) => Type -> Doc
rustTypeFromFlatbuf t@TUser{..} | typeHasUniqueConstructor t =
    "impl <'a> FromFlatBuffer<fb::" <> tname <> "<'a>> for" <+> rtype <+> "{"                       $$
    "    fn from_flatbuf(v: fb::" <> tname <> "<'a>) -> Response<Self> {"                           $$
    "        Ok(" <> R.rname typeName <> (braces $ commaSep from_args) <> ")"                       $$
    "    }"                                                                                         $$
    "}"                                                                                             $$
    "impl <'b> ToFlatBuffer<'b> for" <+> rtype <+> "{"                                              $$
    "    type Target = fbrt::WIPOffset<fb::" <> tname <> "<'b>>;"                                   $$
    "    fn to_flatbuf(&self, fbb: &mut fbrt::FlatBufferBuilder<'b>) -> Self::Target {"             $$
    (nest' $ nest' $ vcat to_args)                                                                  $$
    "        fb::" <> tname <> "::create(fbb, &fb::" <> tname <> "Args{"                            $$
    (nest' $ nest' $ nest' $ vcommaSep arg_names)                                                   $$
    "        })"                                                                                    $$
    "    }"                                                                                         $$
    "}"                                                                                             $$
    "impl <'b> ToFlatBufferTable<'b> for" <+> rtype <+> "{"                                         $$
    "   type Target = fb::" <> tname <> "<'b>;"                                                     $$
    "   fn to_flatbuf_table(&self, fbb: &mut fbrt::FlatBufferBuilder<'b>) -> fbrt::WIPOffset<Self::Target> {" $$
    "       self.to_flatbuf(fbb)"                                                                   $$
    "   }"                                                                                          $$
    "}"                                                                                             $$
    "impl <'b> ToFlatBufferVectorElement<'b> for" <+> rtype <+> "{"                                 $$
    "    type Target = fbrt::WIPOffset<fb::" <> tname <> "<'b>>;"                                   $$
    "    fn to_flatbuf_vector_element(&self, fbb: &mut fbrt::FlatBufferBuilder<'b>) -> Self::Target {"   $$
    "        self.to_flatbuf(fbb)"                                                                  $$
    "    }"                                                                                         $$
    "}"
    where
    rtype = R.mkType t
    tname = typeTableName t
    tstruct = typ' ?d t
    arg_names = map (\a -> let n = pp $ name a in
                           if typeHasUniqueConstructor a
                              then n
                              else n <> "_type," <+> n)
                    $ consArgs $ head $ typeCons tstruct
    from_args = map (\a -> pp (name a) <> ": <" <> R.mkType a <> ">::from_flatbuf(" <> extract_field rtype a <> ")?")
                    $ consArgs $ head $ typeCons tstruct
    to_args = map (\a -> serialize_field "self." a (pp $ name a))
                  $ consArgs $ head $ typeCons tstruct

-- Deserialize struct with multiple constructors.  Such structs are stored in
-- unions and can be additionally wrapped in a table if the struct occurs inside
-- the Value union or vector.  We therefore generate to FromFlatBuffer
-- implementations: one to deserialize the struct from union, represented as a
-- (type, table) pair, and one to deserialize it from a table.
rustTypeFromFlatbuf t@TUser{..} =
    "impl <'a> FromFlatBuffer<(" <> fbstruct <> ", fbrt::Table<'a>)> for" <+> rtype <+> "{"                 $$
    "    fn from_flatbuf(v: (" <> fbstruct <> ", fbrt::Table<'a>)) -> Response<Self> {"                     $$
    "        match v.0 {"                                                                                   $$
    (nest' $ nest' $ nest' $ vcat cons)                                                                     $$
    "            _ => Err(\"" <> pp typeName <> "::from_flatbuf: invalid value type\".to_string())"         $$
    "        }"                                                                                             $$
    "    }"                                                                                                 $$
    "}"                                                                                                     $$
    "impl <'a> FromFlatBuffer<fb::" <> tname <> "<'a>> for" <+> rtype <+> "{"                               $$
    "    fn from_flatbuf(v: fb::" <> tname <> "<'a>) -> Response<Self> {"                                   $$
    "        let v_type = v.v_type();"                                                                      $$
    "        let v_table = v.v().ok_or_else(||format!(\"" <> rtype <> "::from_flatbuf: invalid buffer: failed to extract nested table\"))?;" $$
    "        <" <> rtype <> ">::from_flatbuf((v_type,v_table))"                                             $$
    "    }"                                                                                                 $$
    "}"                                                                                                     $$
    "impl <'b> ToFlatBuffer<'b> for" <+> rtype <+> "{"                                                      $$
    "    type Target = (" <> fbstruct <> ", fbrt::WIPOffset<fbrt::UnionWIPOffset>);"                        $$
    "    fn to_flatbuf(&self, fbb: &mut fbrt::FlatBufferBuilder<'b>) -> Self::Target {"                     $$
    "        match self {"                                                                                  $$
    (nest' $ nest' $ nest' $ vcommaSep to_cons)                                                             $$
    "        }"                                                                                             $$
    "    }"                                                                                                 $$
    "}"                                                                                                     $$
    "impl <'b> ToFlatBufferTable<'b> for" <+> rtype <+> "{"                                                 $$
    "    type Target = fb::" <+> tname <> "<'b>;"                                                           $$
    "    fn to_flatbuf_table(&self, fbb: &mut fbrt::FlatBufferBuilder<'b>) -> fbrt::WIPOffset<Self::Target> {"    $$
    "        let (v_type, v) = self.to_flatbuf(fbb);"                                                       $$
    "        let v = Some(v);"                                                                              $$
    "        fb::" <> tname <> "::create(fbb, &fb::" <> tname <> "Args{v_type, v})"                         $$
    "    }"                                                                                                 $$
    "}"                                                                                                     $$
    "impl <'b> ToFlatBufferVectorElement<'b> for" <+> rtype <+> "{"                                         $$
    "    type Target = fbrt::WIPOffset<fb::" <> tname <> "<'b>>;"                                           $$
    "    fn to_flatbuf_vector_element(&self, fbb: &mut fbrt::FlatBufferBuilder<'b>) -> Self::Target {"      $$
    "       self.to_flatbuf_table(fbb)"                                                                     $$
    "    }"                                                                                                 $$
    "}"
    where
    rtype = R.mkType t
    tname = typeTableName t
    fbstruct = "fb::" <> fbStructName typeName typeArgs
    tstruct = typ' ?d t
    cons = map (\c -> let fbcname = fbConstructorName typeArgs c
                          cname = R.rname typeName <> "::" <> R.rname (name c)
                          args = map (\a -> pp (name a) <> ": <" <> R.mkType a <> ">::from_flatbuf(" <> extract_field cname a <> ")?")
                                     $ consArgs c
                      in if null args
                            then fbstruct <> "::" <> fbcname <+> "=> {"                                $$
                                 "    Ok(" <> cname <> ")"                                             $$
                                 "},"
                            else fbstruct <> "::" <> fbcname <+> "=> {"                                $$
                                 "    let v = fb::" <> fbcname <> "::init_from_table(v.1);"            $$
                                 "    Ok(" <> cname <> (braces $ commaSep args) <> ")"                 $$
                                 "},")
               $ typeCons tstruct
    to_cons = map (\c -> let fbcname = fbConstructorName typeArgs c
                             cname = R.rname typeName <> "::" <> R.rname (name c)
                             args = map (\a -> serialize_field "" a (pp $ name a)) $ consArgs c
                             arg_names = map (\a -> let n = pp $ name a in
                                               if typeHasUniqueConstructor a
                                                  then n
                                                  else n <> "_type," <+> n)
                                             $ consArgs c
                             tab = "let __tab = fb::" <> fbcname <> "::create(fbb, &fb::" <> fbcname <> "Args{"   $$
                                   (nest' $ vcommaSep arg_names)                                                  $$
                                   "});"
                         in cname <> "{"<> (commaSep $ map (pp . name) $ consArgs c) <> "} => {"        $$
                            (nest' $ vcat args)                                                         $$
                            nest' tab                                                                   $$
                            "    (" <> fbstruct <> "::" <> fbcname <> ", __tab.as_union_value())"       $$
                            "}")
              $ typeCons tstruct

-- Deserialize Tuples.
rustTypeFromFlatbuf t@TTuple{..} =
    "impl <'a> FromFlatBuffer<fb::" <> tname <> "<'a>> for" <+> rtype <+> "{"                       $$
    "    fn from_flatbuf(v: fb::" <> tname <> "<'a>) -> Response<Self> {"                           $$
    "        Ok((" <> commaSep from_args <> "))"                                                    $$
    "    }"                                                                                         $$
    "}"                                                                                             $$
    "impl <'b> ToFlatBuffer<'b> for" <+> rtype <+> "{"                                              $$
    "    type Target = fbrt::WIPOffset<fb::" <> tname <> "<'b>>;"                                   $$
    "    fn to_flatbuf(&self, fbb: &mut fbrt::FlatBufferBuilder<'b>) -> Self::Target {"             $$
    (nest' $ nest' $ vcat to_args)                                                                  $$
    "        fb::" <> tname <> "::create(fbb, &fb::" <> tname <> "Args{"                            $$
    (nest' $ nest' $ nest' $ vcommaSep arg_names)                                                   $$
    "        })"                                                                                    $$
    "    }"                                                                                         $$
    "}"                                                                                             $$
    "impl <'b> ToFlatBufferTable<'b> for" <+> rtype <+> "{"                                         $$
    "   type Target = fb::" <> tname <> "<'b>;"                                                     $$
    "   fn to_flatbuf_table(&self, fbb: &mut fbrt::FlatBufferBuilder<'b>) -> fbrt::WIPOffset<Self::Target> {" $$
    "       self.to_flatbuf(fbb)"                                                                   $$
    "   }"                                                                                          $$
    "}"                                                                                             $$
    "impl <'b> ToFlatBufferVectorElement<'b> for" <+> rtype <+> "{"                                 $$
    "    type Target = fbrt::WIPOffset<fb::" <> tname <> "<'b>>;"                                   $$
    "    fn to_flatbuf_vector_element(&self, fbb: &mut fbrt::FlatBufferBuilder<'b>) -> Self::Target {"   $$
    "        self.to_flatbuf(fbb)"                                                                  $$
    "    }"                                                                                         $$
    "}"
    where
    tname = typeTableName t
    rtype = R.mkType t
    arg_names = mapIdx (\a i -> let n = pp $ "a" ++ show i in
                           if typeHasUniqueConstructor a
                              then n
                              else n <> "_type," <+> n)
                       typeTupArgs
    from_args = mapIdx (\a i -> "<" <> R.mkType a <> ">::from_flatbuf(" <> extract_field rtype (Field nopos ("a" ++ show i) a) <> ")?")
                typeTupArgs
    to_args = mapIdx (\a i -> serialize_field "self." (Field nopos (show i) a) ("a" <> pp i))
                     typeTupArgs

-- Container types (vectors, sets, maps).  We implement 'FromFlatBuffer<fb::Vector>'
-- and 'ToFlatBuffer<>' for containers in their corresponding libraries.  Here we
-- additionally generate 'FromFlatBuffer<fb::>' for wrapper tables,
-- 'ToFlatBufferVectorElement<>', and 'ToFlatBufferTable<>'.
rustTypeFromFlatbuf t@TOpaque{} =
    "impl <'a> FromFlatBuffer<fb::" <> tname <> "<'a>> for" <+> rtype <+> "{"                               $$
    "    fn from_flatbuf(v: fb::" <> tname <> "<'a>) -> Response<Self> {"                                   $$
    "        let vec = v.v().ok_or_else(||format!(\"" <> rtype <> "::from_flatbuf: invalid buffer: failed to extract nested vector\"))?;"       $$
    "        <" <> rtype <> ">::from_flatbuf(vec)"                                                          $$
    "    }"                                                                                                 $$
    "}"                                                                                                     $$
    "impl <'b> ToFlatBufferTable<'b> for" <+> rtype <+> "{"                                                 $$
    "    type Target = fb::" <+> tname <> "<'b>;"                                                           $$
    "    fn to_flatbuf_table(&self, fbb: &mut fbrt::FlatBufferBuilder<'b>) -> fbrt::WIPOffset<Self::Target> {"    $$
    "        let v = self.to_flatbuf(fbb);"                                                                 $$
    "        let v = Some(v);"                                                                              $$
    "        fb::" <> tname <> "::create(fbb, &fb::" <> tname <> "Args{v})"                                 $$
    "    }"                                                                                                 $$
    "}"                                                                                                     $$
    "impl <'b> ToFlatBufferVectorElement<'b> for" <+> rtype <+> "{"                                         $$
    "    type Target = fbrt::WIPOffset<fb::" <> tname <> "<'b>>;"                                           $$
    "    fn to_flatbuf_vector_element(&self, fbb: &mut fbrt::FlatBufferBuilder<'b>) -> Self::Target {"      $$
    "       self.to_flatbuf_table(fbb)"                                                                     $$
    "    }"                                                                                                 $$
    "}"
    where
    rtype = R.mkType t
    tname = typeTableName t

-- For builtin types ('bool', 'bit<>', 'signed', 'string', 'bigint'),
-- 'FromFlatBuffer<>' is implemented in the 'flatbuf.rs' template.
-- For library types (containers, 'Ref<>', etc), 'FromFlatBuffer<>' is
-- implemented in their corresponding library modules.
rustTypeFromFlatbuf t | typeIsScalar t =
    "impl <'a> FromFlatBuffer<fb::" <> tname <> "<'a>> for" <+> rtype <+> "{"                               $$
    "    fn from_flatbuf(v: fb::" <> tname <> "<'a>) -> Response<Self> {"                                   $$
    "        let v =" <+> extract_field "v" (Field nopos "v" t) <> ";"                                      $$
    "        <" <> rtype <> ">::from_flatbuf(v)"                                                            $$
    "    }"                                                                                                 $$
    "}"                                                                                                     $$
    "impl <'b> ToFlatBufferTable<'b> for" <+> rtype <+> "{"                                                 $$
    "    type Target = fb::" <+> tname <> "<'b>;"                                                           $$
    "    fn to_flatbuf_table(&self, fbb: &mut fbrt::FlatBufferBuilder<'b>) -> fbrt::WIPOffset<Self::Target> {"    $$
    "        let v = self.to_flatbuf(fbb);"                                                                 $$
    "        fb::" <> tname <> "::create(fbb, &fb::" <> tname <> "Args{v})"                                 $$
    "    }"                                                                                                 $$
    "}"
    where
    rtype = R.mkType t
    tname = typeTableName t

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

serialize_field :: (?d::DatalogProgram) => Doc -> Field -> Doc -> Doc
serialize_field prefix f to_name | typeIsScalar f =
    "let" <+> to_name <+> "=" <+> from_name <> ".to_flatbuf(fbb);"
                                 | typeHasUniqueConstructor f =
    "let" <+> to_name <+> "= Some(" <> from_name <> ".to_flatbuf(fbb));"
                                 | otherwise =
    "let (" <> to_name <> "_type," <+> to_name <> ") =" <+> from_name <> ".to_flatbuf(fbb);" $$
    "let" <+> to_name <+> "= Some(" <> to_name <> ");"
    where from_name = prefix <> (pp $ name f)
