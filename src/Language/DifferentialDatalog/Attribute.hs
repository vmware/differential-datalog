{-
Copyright (c) 2020 VMware, Inc.
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

-- Functions for working with attributes specified for various DDlog program
-- entities using annotations.

{-# LANGUAGE RecordWildCards, FlexibleContexts #-}
module Language.DifferentialDatalog.Attribute where

import Data.Maybe
import Data.List
import Control.Monad.Except

import Language.DifferentialDatalog.Name
import Language.DifferentialDatalog.NS
import Language.DifferentialDatalog.Pos
import Language.DifferentialDatalog.Syntax
import {-# SOURCE #-} Language.DifferentialDatalog.Type
import Language.DifferentialDatalog.Util
import {-# SOURCE #-} Language.DifferentialDatalog.Function

progValidateAttributes :: (MonadError String me) => DatalogProgram -> me ()
progValidateAttributes d = do
    mapM_ (typedefValidateAttrs d) $ progTypedefs d
    mapM_ (indexValidateAttrs d) $ progIndexes d
    mapM_ (funcValidateAttrs d) $ progFunctions d

typedefValidateAttrs :: (MonadError String me) => DatalogProgram -> TypeDef -> me ()
typedefValidateAttrs d tdef@TypeDef{..} = do
    uniqNames ("Multiple definitions of attribute " ++) tdefAttrs
    mapM_ (typedefValidateAttr d tdef) tdefAttrs
    _ <- tdefCheckSizeAttr tdef
    _ <- checkRustAttrs tdefAttrs
    maybe (return ()) (typeValidateAttrs d) tdefType
    return ()

typedefValidateAttr :: (MonadError String me) => DatalogProgram -> TypeDef -> Attribute -> me ()
typedefValidateAttr _ TypeDef{..} attr = do
    case name attr of
         "size" -> do
            check (isNothing tdefType) (pos attr)
                $ "Only extern types can have a \"size\" attribute"
         "rust" -> do
            check (isJust tdefType) (pos attr)
                $ "Extern types cannot have a \"rust\" attribute"
         n -> err (pos attr) $ "Unknown attribute " ++ n

typeValidateAttrs :: (MonadError String me) => DatalogProgram -> Type -> me ()
typeValidateAttrs d struct@TStruct{..} =
    mapM_ (consValidateAttrs d struct) typeCons
typeValidateAttrs _ _ = return ()

consValidateAttrs :: (MonadError String me) => DatalogProgram -> Type -> Constructor -> me ()
consValidateAttrs d struct cons@Constructor{..} = do
    mapM_ (consValidateAttr d struct cons) consAttrs
    _ <- checkRustAttrs consAttrs
    mapM_ (fieldValidateAttrs d) consArgs
    return ()

consValidateAttr :: (MonadError String me) => DatalogProgram -> Type -> Constructor -> Attribute -> me ()
consValidateAttr _ struct Constructor{..} attr = do
    let TStruct{..} = struct
    case name attr of
         "rust" -> check (length typeCons > 1) (pos attr) $ "Per-constructor 'rust' attributes are only supported for types with multiple constructors"
         n -> err (pos attr) $ "Unknown attribute " ++ n

indexValidateAttrs :: (MonadError String me) => DatalogProgram -> Index -> me ()
indexValidateAttrs d Index{..} = mapM_ (fieldValidateAttrs d) idxVars

fieldValidateAttrs :: (MonadError String me) => DatalogProgram -> Field -> me ()
fieldValidateAttrs d field@Field{..} = do
    mapM_ (fieldValidateAttr d) fieldAttrs
    _ <- checkRustAttrs fieldAttrs
    _ <- fieldCheckDeserializeArrayAttr d field
    return ()

fieldValidateAttr :: (MonadError String me) => DatalogProgram -> Attribute -> me ()
fieldValidateAttr _ attr = do
    case name attr of
         "rust" -> return ()
         "deserialize_from_array" -> return ()
         n -> err (pos attr) $ "Unknown attribute " ++ n

funcValidateAttrs :: (MonadError String me) => DatalogProgram -> Function -> me ()
funcValidateAttrs d f@Function{..} = do
    uniqNames ("Multiple definitions of attribute " ++) funcAttrs
    mapM_ (funcValidateAttr d) funcAttrs
    _ <- checkSideEffectAttr f
    _ <- checkReturnByRefAttr f
    return ()

funcValidateAttr :: (MonadError String me) => DatalogProgram -> Attribute -> me ()
funcValidateAttr _ attr = do
    case name attr of
         "has_side_effects" -> return ()
         "return_by_ref" -> return ()
         n -> err (pos attr) $ "Unknown attribute " ++ n

{- 'size' attribute: Gives DDlog a hint about the size of an extern data type in bytes. -}

tdefCheckSizeAttr :: (MonadError String me) => TypeDef -> me (Maybe Int)
tdefCheckSizeAttr TypeDef{..} =
    case filter ((== "size") . name) tdefAttrs of
         []                   -> return Nothing
         [Attribute{attrVal = E (EInt _ nbytes)}] | nbytes <= toInteger (maxBound::Int)
                              -> return $ Just $ fromInteger nbytes
         [Attribute{..}] -> err attrPos $ "Invalid 'size' attribute: size must be an integer between 0 and " ++ show (maxBound::Int)
         _                    -> err tdefPos $ "Multiple 'size' attributes are not allowed"

tdefGetSizeAttr :: TypeDef -> Maybe Int
tdefGetSizeAttr tdef =
    case tdefCheckSizeAttr tdef of
         Left e   -> error e
         Right sz -> sz

{- 'rust' attribute is transferred directly to the generated Rust code. -}

checkRustAttrs :: (MonadError String me) => [Attribute] -> me [String]
checkRustAttrs attrs =
    mapM (\Attribute{..} ->
            case attrVal of
                 E (EString _ str) -> return str
                 _ -> err attrPos $ "Invalid 'rust' attribute: the value of the attribute must be a string literal, e.g., #[rust=\"serde(tag = \\\"type\\\"\")]")
         $ filter ((== "rust") . name) attrs

getRustAttrs :: [Attribute] -> [String]
getRustAttrs attrs =
    case checkRustAttrs attrs of
         Left e   -> error e
         Right as -> as

{- 'deserialize_from_array' attribute: specifies key function to be used to
   deserialize array into a map. -}

fieldCheckDeserializeArrayAttr :: (MonadError String me) => DatalogProgram -> Field -> me (Maybe String)
fieldCheckDeserializeArrayAttr d field@Field{..} =
    case filter ((== "deserialize_from_array") . name) fieldAttrs of
         []                   -> return Nothing
         [attr@Attribute{attrVal = E (EApply _ fname _)}] -> do
             check (isMap d field) (pos attr) $ "'deserialize_from_array' attribute is only applicable to fields of type 'Map<>'."
             let TOpaque _ _ [ktype, vtype] = typ' d field
             kfunc@Function{..} <- checkFunc (pos attr) d fname
             let nargs = length funcArgs
             check (nargs == 1) (pos attr)
                 $ "Key function '" ++ fname ++ "' must take one argument of type '" ++ show (fieldType) ++ "', but it takes " ++ show nargs ++ "arguments."
             _ <- funcTypeArgSubsts d (pos attr) kfunc [vtype, ktype]
             return $ Just fname
         [Attribute{..}] -> err attrPos
             $ "Invalid 'deserialize_from_array' attribute value '" ++ show attrVal ++ "': the value of the attribute must be the name of the key function, e.g.: \"deserialize_from_array=key_func()\"."
         _               -> err (pos field) $ "Multiple 'deserialize_from_array' attributes are not allowed."

fieldGetDeserializeArrayAttr :: DatalogProgram -> Field -> Maybe String
fieldGetDeserializeArrayAttr d field =
    case fieldCheckDeserializeArrayAttr d field of
         Left e      -> error e
         Right fname -> fname

{- 'has_side_effects' attribute: labels functions with side effects, e.g.,
   logging functions. -}

checkSideEffectAttr :: (MonadError String me) => Function -> me Bool
checkSideEffectAttr Function{..} =
    case find ((== "has_side_effects") . name) funcAttrs of
         Nothing -> return False
         Just attr -> do check (isNothing funcDef) (pos attr) "'has_side_effects' attribute is supported for extern functions only"
                         check (attrVal attr == eTrue) (pos attr)
                            "The value of 'has_side_effects' attribute must be 'true' or empty"
                         return True

funcGetSideEffectAttr :: Function -> Bool
funcGetSideEffectAttr f =
    case checkSideEffectAttr f of
         Left e -> error e
         Right has_side_effects -> has_side_effects 

checkReturnByRefAttr :: (MonadError String me) => Function -> me Bool
checkReturnByRefAttr Function{..} =
    case find ((== "return_by_ref") . name) funcAttrs of
         Nothing -> return False
         Just attr -> do check (isNothing funcDef) (pos attr) "'return_by_ref' attribute is supported for extern functions only"
                         check (attrVal attr == eTrue) (pos attr)
                            "The value of 'return_by_ref' attribute must be 'true' or empty"
                         return True

funcGetReturnByRefAttr :: Function -> Bool
funcGetReturnByRefAttr f =
    case checkReturnByRefAttr f of
         Left e -> error e
         Right return_by_ref -> return_by_ref
