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

{-# LANGUAGE RecordWildCards, FlexibleContexts, LambdaCase, OverloadedStrings #-}

module Language.DifferentialDatalog.FlatBuffer(
    flatBufferValidate,
    compileFlatBufferSchema)
where

-- FIXME: support `DeleteKey` and `Modify` commands.  The former require collecting
-- primary key types; the latter requires adding support for type-erased value
-- representation (aka `Record`).

import qualified Data.Set as S
import qualified Data.Map as M
import Data.List
import Data.Char
import Control.Monad.State
import Text.PrettyPrint
import Control.Monad.Except
import Prelude hiding((<>))

import Language.DifferentialDatalog.Syntax
import Language.DifferentialDatalog.Type
import Language.DifferentialDatalog.PP
import Language.DifferentialDatalog.Util
import Language.DifferentialDatalog.Pos
import Language.DifferentialDatalog.Name

-- | Checks that we are able to generate FlatBuffer schema for all types used in
-- program's input and output relations.
--
-- This function must be invoked after the normal DDlog validation has
-- succeeded.
--
-- If this function succeeds, then `compileFlatBufferSchema` must be guaranteed
-- to succeed.
flatBufferValidate :: (MonadError String me) => DatalogProgram -> me ()
flatBufferValidate d =
    mapM_ (\case
            t@TOpaque{..} ->
                check (elem typeName $ [rEF_TYPE, mAP_TYPE, iNTERNED_TYPE] ++ sET_TYPES) (pos t) $
                    "Cannot generate FlatBuffer schema for extern type " ++ show t
            _ -> return ())
          $ progTypesToSerialize d

-- | Generate FlatBuffer schema for DDlog program.
compileFlatBufferSchema :: DatalogProgram -> Doc
compileFlatBufferSchema d =
    "table __BigUint { bytes: [uint8]; }"                                       $$
    "table __BigInt { sign: bool; bytes: [uint8]; }"                            $$
    ""                                                                          $$
    "// Program type declarations"                                              $$
    (vcat $ intersperse "" types)                                               $$
    ""                                                                          $$
    (if null rel_types then empty else "// Tables for progam relations whose types do not have their own tables")   $$
    (vcat $ intersperse "" rel_types)                                           $$
    ""                                                                          $$
    "// Relation identifiers"                                                   $$
    "enum __RelId:" <+> relenum_type                                            $$
    (braces' $ vcommaSep $ map mkRelId rels)                                    $$
    ""                                                                          $$
    "// Union of all program relation types"                                    $$
    "union __Value"                                                             $$
    (braces' $ vcommaSep $ nub $ map (mkTypeIdentifier d) rels)                 $$
    ""                                                                          $$
    "// DDlog commands"                                                         $$
    "enum __CommandKind: uint8 { __Insert, __Delete }"                          $$
    "table __Command {"                                                         $$
    "   kind: __CommandKind;"                                                   $$
    "   relid: __RelId;"                                                        $$
    "   val:  __Value;"                                                         $$
    "}"
    where
    rels = progIORelations d
    -- Schema for all program types visible from outside
    types = map (typeFlatbufSchema d) $ progTypesToSerialize d
    -- Every relation type must have its own table, so we can form 'union Value'
    -- out of those.  If the relation is of a struct or tuple type, we use the
    -- table that already exists for this type.  Otherwise, we create a new table.
    rel_types = map (\t -> ("table" <+> mkTypeIdentifier d t) $$
                           (braces' $ "v:" <+> mkType d t <> ";"))
                    $ nub
                    $ filter (not . typeHasTable d)
                    $ map (typeNormalizeForFlatBuf d)
                    rels
    relenum_type | length rels <= 256    = "uint8"
                 | length rels <= 65536  = "uint16"
                 | otherwise             = "uint32"

typeHasTable :: (WithType a) => DatalogProgram -> a -> Bool
typeHasTable d x =
    isStruct d x                    ||
    isTuple d x                     ||
    mkType d (typ x) == "__BigInt"  ||
    mkType d (typ x) == "__BigUint"

-- Types for which serialization logic must be generated.
progTypesToSerialize :: DatalogProgram -> [Type]
progTypesToSerialize d =
    nub 
    $ concatMap (relTypesToSerialize d)
    $ progIORelations d

progIORelations :: DatalogProgram -> [Relation]
progIORelations d =
    filter (\rel -> elem (relRole rel) [RelInput, RelOutput])
           $ M.elems $ progRelations d

-- Types used in relation declaration (possibly, recursively), for which serialization
-- logic must be generated.
relTypesToSerialize :: DatalogProgram -> Relation -> [Type]
relTypesToSerialize d Relation{..} = execState (typeSubtypes d relType) []

-- Types that occur in type expression `t`, for which serialization logic must be
-- generated, including:
-- * the type itself, if it's not a primitive or extern type
-- * struct fields, if it's a struct type
-- * tuple fields, if it's a tuple type
-- * type arguments for generic types
typeSubtypes :: DatalogProgram -> Type -> State [Type] ()
typeSubtypes d t =
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
                         mapM_ (typeSubtypes d) typeTupArgs
        TUser{}    -> do addtype t''
                         mapM_ (mapM_ (typeSubtypes d . typ) . consArgs)
                               $ typeCons $ typ' d t''
        TOpaque{..}   -> do mapM_ (typeSubtypes d) typeArgs
                            -- Maps are encoded as arrays of (key,value)
                            -- tuples; we must therefore generate an
                            -- appropriate tuple type
                            when (typeName == mAP_TYPE)
                                 $ addtype $ tTuple [typeArgs !! 0, typeArgs !! 1]
        _             -> error $ "typeSubtypes: Unexpected type " ++ show t
    where
    -- Prepend 'x' to the list so that dependency declarations are generated before
    -- types that depend on them.
    addtype x = modify (nub . (x:))
    t'' = typeNormalizeForFlatBuf d t

-- Generate FlatBuffer schema for the type.
-- Assumes the type is normalized.
typeFlatbufSchema :: DatalogProgram -> Type -> Doc
typeFlatbufSchema d TTuple{..} =
    tupleFlatBufferSchema d typeTupArgs
typeFlatbufSchema d t@TUser{..} =
    -- For structs, only generate a union if the struct has more than one
    -- constructor; otherwise, create a table whose name matches the name of the
    -- struct (not the name of the constructor).
    vcat constructors $+$
    union
    where
    TStruct{..} = typ' d t
    constructors = map (constructorFlatbufSchema d typeArgs) typeCons
    union = if length typeCons == 1
               then empty
               else "union" <+> mkStructName d typeName typeArgs <+> (braces $ commaSep $ map (mkConstructorName d typeArgs) typeCons)
typeFlatbufSchema _ t = error $ "typeFlatbufSchema: unexpected type " ++ show t

-- Generate FlatBuffer "table" declaration for a constructor
-- Constructor arguments are _not_ be normalized.
constructorFlatbufSchema :: DatalogProgram -> [Type] -> Constructor -> Doc
constructorFlatbufSchema d targs c@Constructor{..} =
    "table" <+> mkConstructorName d targs c $+$
    (braces' $ vcat fields)
    where
    fields = map (\a -> pp (name a) <> ":" <+> mkType d (typ a) <> ";") consArgs

-- Generate FlatBuffer "table" declaration for a tuple type
-- Assumes 'types' are normalized
tupleFlatBufferSchema :: DatalogProgram -> [Type] -> Doc
tupleFlatBufferSchema d types =
    "table" <+> mkTupleName d types $+$
    (braces' $ vcat fields)
    where
    fields = mapIdx (\t i -> "a" <> pp i <> ":" <+> mkType d t <> ";") types

-- Map DDlog types to FlatBuffer types.
-- 't' may _not_ be normalized
mkType :: DatalogProgram -> Type -> Doc
mkType d t =
    case typeNormalizeForFlatBuf d t of
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
        TTuple{..}         -> mkTupleName d typeTupArgs
        TUser{..}          -> mkStructName d typeName typeArgs
        TOpaque{typeArgs = [elemType], ..} | elem typeName sET_TYPES
                           -> "[" <> mkType d elemType <> "]"
        TOpaque{typeArgs = [keyType,valType],..} | typeName == mAP_TYPE
                           -> "[" <> mkTupleName d [keyType, valType] <> "]"
        t'                 -> error $ "FlatBuffer.mkType: unsupported type " ++ show t'

-- Convert type into a valid FlatBuf identifier.
mkTypeIdentifier :: (WithType a) => DatalogProgram -> a -> Doc
mkTypeIdentifier d x = pp $ legalize $ render $ mkType d $ typ x

mkTupleName :: DatalogProgram -> [Type] -> Doc
mkTupleName d ts =
    "Tuple" <> pp (length ts) <> fields
    where
    fields = hcat $ map (("__" <>) . mkTypeIdentifier d) ts

mkStructName :: DatalogProgram -> String -> [Type] -> Doc
mkStructName d n args =
    pp (legalize n) <> targs
    where
    targs = hcat $ map (("__" <>) . mkTypeIdentifier d) args

mkConstructorName :: DatalogProgram -> [Type] -> Constructor -> Doc
mkConstructorName d args c | consIsUnique d (name c) = mkStructName d (name $ consType d $ name c) args
                           | otherwise               = pp (legalize $ name c) <> targs
    where
    targs = hcat $ map (("__" <>) . mkTypeIdentifier d) args

mkRelId :: Relation -> Doc
mkRelId Relation{..} = "Rel_" <> (pp $ legalize relName)

-- convert a DDlog identifier (possibly including namespaces) into a Java identifier
legalize :: String -> String
legalize n = map legalizeChar n

-- convert characters that are illegal in Java identifiers into underscores
legalizeChar :: Char -> Char
legalizeChar c = if isAlphaNum c then c else '_'

-- Like 'Type.typeNormalize', but also unwraps Ref<> and IObj<> types, which are
-- transparent to FlatBuffers.
typeNormalizeForFlatBuf :: (WithType a) => DatalogProgram -> a -> Type
typeNormalizeForFlatBuf d x =
    case typeNormalize d x of
         TOpaque{typeArgs = [innerType],..} | elem typeName [rEF_TYPE,  iNTERNED_TYPE] 
                                            -> typeNormalizeForFlatBuf d innerType
         t'                                 -> t'
