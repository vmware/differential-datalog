{-
Copyright (c) 2020-2021 VMware, Inc.
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
import {-# SOURCE #-} Language.DifferentialDatalog.Function
import {-# SOURCE #-} Language.DifferentialDatalog.Validate
import Language.DifferentialDatalog.Rust
import Language.DifferentialDatalog.Error

progValidateAttributes :: (MonadError String me) => DatalogProgram -> me ()
progValidateAttributes d = do
    mapM_ (typedefValidateAttrs d) $ progTypedefs d
    mapM_ (indexValidateAttrs d) $ progIndexes d
    mapM_ (mapM_ (funcValidateAttrs d)) $ progFunctions d

typedefValidateAttrs :: (MonadError String me) => DatalogProgram -> TypeDef -> me ()
typedefValidateAttrs d tdef@TypeDef{..} = do
    uniqNames (Just d) ("Multiple definitions of attribute " ++) tdefAttrs
    mapM_ (typedefValidateAttr d tdef) tdefAttrs
    _ <- tdefCheckSizeAttr d tdef
    _ <- tdefCheckCustomSerdeAttr d tdef
    _ <- tdefCheckCustomFromRecord d tdef
    _ <- tdefCheckSharedRefAttr d tdef
    _ <- tdefCheckDynAllocAttr d tdef
    _ <- tdefCheckAliasAttr d tdef
    _ <- checkRustAttrs d tdefAttrs
    _ <- tdefCheckIterableAttr d tdef
    maybe (return ()) (typeValidateAttrs d) tdefType
    return ()

typedefValidateAttr :: (MonadError String me) => DatalogProgram -> TypeDef -> Attribute -> me ()
typedefValidateAttr d tdef@TypeDef{..} attr = do
    case name attr of
         "size" -> do
            check d (tdefIsExtern tdef) (pos attr)
                $ "Only extern types can have a 'size' attribute."
         "rust" -> do
            check d (not $ tdefIsExtern tdef) (pos attr)
                $ "Extern types cannot have a 'rust' attribute."
         "custom_serde" -> do
            check d (not $ tdefIsExtern tdef) (pos attr)
                $ "Extern types cannot have a 'custom_serde' attribute."
            let t = fromJust tdefType
            check d (isStruct d t) (pos attr)
                $ "'custom_serde' attribute cannot be applied to type aliases."
         "custom_from_record" -> do
            check d (not $ tdefIsExtern tdef) (pos attr)
                $ "Extern types cannot have a 'custom_from_record' attribute."
            let t = fromJust tdefType
            check d (isStruct d t) (pos attr)
                $ "'custom_from_record' attribute cannot be applied to type aliases."
         "shared_ref" -> do
            check d (tdefIsExtern tdef) (pos attr)
                $ "'sharef_ref' attribute is only applicable to extern types."
            check d (length tdefArgs == 1) (pos attr)
                $ "Types annotated with 'shared_ref' must have exactly one type argument, e.g., \"Ref<'T>\"."
         "dyn_alloc" -> do
            check d (tdefIsExtern tdef) (pos attr)
                $ "'dyn_alloc' attribute is only applicable to extern types."
         "alias" -> do
             case tdefType of
                  Just TUser{} -> err d (pos attr) "The 'alias' attribute does not apply to struct types."
                  Nothing -> err d (pos attr) "The 'alias' attribute does not apply to extern types."
                  _ -> return ()
         "iterate_by_ref" -> do
            check d (tdefIsExtern tdef) (pos attr)
                $ "Only extern types can have a 'iterate_by_ref' attribute."
         "iterate_by_val" -> do
            check d (tdefIsExtern tdef) (pos attr)
                $ "Only extern types can have a 'iterate_by_val' attribute."
         n -> err d (pos attr) $ "Unknown attribute " ++ n

typeValidateAttrs :: (MonadError String me) => DatalogProgram -> Type -> me ()
typeValidateAttrs d struct@TStruct{..} =
    mapM_ (consValidateAttrs d struct) typeCons
typeValidateAttrs _ _ = return ()

consValidateAttrs :: (MonadError String me) => DatalogProgram -> Type -> Constructor -> me ()
consValidateAttrs d struct cons@Constructor{..} = do
    mapM_ (consValidateAttr d struct cons) consAttrs
    _ <- checkRustAttrs d consAttrs
    mapM_ (fieldValidateAttrs d) consArgs
    return ()

consValidateAttr :: (MonadError String me) => DatalogProgram -> Type -> Constructor -> Attribute -> me ()
consValidateAttr d struct Constructor{} attr = do
    let TStruct{..} = struct
    case name attr of
         "rust" -> check d (length typeCons > 1) (pos attr) $ "Per-constructor 'rust' attributes are only supported for types with multiple constructors"
         n -> err d (pos attr) $ "Unknown attribute " ++ n

indexValidateAttrs :: (MonadError String me) => DatalogProgram -> Index -> me ()
indexValidateAttrs d Index{..} = mapM_ (fieldValidateAttrs d) idxVars

fieldValidateAttrs :: (MonadError String me) => DatalogProgram -> Field -> me ()
fieldValidateAttrs d field@Field{..} = do
    mapM_ (fieldValidateAttr d) fieldAttrs
    _ <- checkRustAttrs d fieldAttrs
    _ <- fieldCheckDeserializeArrayAttr d field
    return ()

fieldValidateAttr :: (MonadError String me) => DatalogProgram -> Attribute -> me ()
fieldValidateAttr d attr = do
    case name attr of
         "rust" -> return ()
         "deserialize_from_array" -> return ()
         n -> err d (pos attr) $ "Unknown attribute " ++ n

funcValidateAttrs :: (MonadError String me) => DatalogProgram -> Function -> me ()
funcValidateAttrs d f@Function{..} = do
    uniqNames (Just d) ("Multiple definitions of attribute " ++) funcAttrs
    mapM_ (funcValidateAttr d) funcAttrs
    _ <- checkSideEffectAttr d f
    _ <- checkReturnByRefAttr d f
    return ()

funcValidateAttr :: (MonadError String me) => DatalogProgram -> Attribute -> me ()
funcValidateAttr d attr = do
    case name attr of
         "has_side_effects" -> return ()
         "return_by_ref" -> return ()
         n -> err d (pos attr) $ "Unknown attribute " ++ n

{- 'size' attribute: Gives DDlog a hint about the size of an extern data type in bytes. -}

tdefCheckSizeAttr :: (MonadError String me) => DatalogProgram -> TypeDef -> me (Maybe Int)
tdefCheckSizeAttr d TypeDef{..} =
    case filter ((== "size") . name) tdefAttrs of
         []                   -> return Nothing
         [Attribute{attrVal = E (EInt _ nbytes)}] | nbytes <= toInteger (maxBound::Int)
                              -> return $ Just $ fromInteger nbytes
         [Attribute{..}] -> err d attrPos $ "Invalid 'size' attribute: size must be an integer between 0 and " ++ show (maxBound::Int)
         _                    -> err d tdefPos $ "Multiple 'size' attributes are not allowed"

{- 'custom_serde' attribute: Tells DDlog not to generate `Serialize` and
 - `Deserialize` implementations for a type.  The user must write their own
 - implementations in Rust. -}
tdefCheckCustomSerdeAttr :: (MonadError String me) => DatalogProgram -> TypeDef -> me Bool
tdefCheckCustomSerdeAttr d TypeDef{..} =
    case find ((== "custom_serde") . name) tdefAttrs of
         Nothing   -> return False
         Just attr -> do check d (attrVal attr == eTrue) (pos attr)
                               "The value of 'custom_serde' attribute must be 'true' or empty"
                         return True

tdefGetCustomSerdeAttr :: DatalogProgram -> TypeDef -> Bool
tdefGetCustomSerdeAttr d tdef = 
    case tdefCheckCustomSerdeAttr d tdef of
         Left e  -> error e
         Right b -> b

{- 'custom_from_record' attribute: Tells DDlog not to generate `FromRecord`
 - implementation for a type.  The user must write their own implementations in
 - Rust. -}
tdefCheckCustomFromRecord :: (MonadError String me) => DatalogProgram -> TypeDef -> me Bool
tdefCheckCustomFromRecord d TypeDef{..} =
    case find ((== "custom_from_record") . name) tdefAttrs of
         Nothing   -> return False
         Just attr -> do check d (attrVal attr == eTrue) (pos attr)
                               "The value of 'custom_from_record' attribute must be 'true' or empty"
                         return True

tdefGetCustomFromRecord :: DatalogProgram -> TypeDef -> Bool
tdefGetCustomFromRecord d tdef = 
    case tdefCheckCustomFromRecord d tdef of
         Left e  -> error e
         Right b -> b

{- 'shared_ref' attribute: Tells DDlog that the type is a shared reference in
 - the style of `Ref` and `Intern`. -}
tdefCheckSharedRefAttr :: (MonadError String me) => DatalogProgram -> TypeDef -> me Bool
tdefCheckSharedRefAttr d TypeDef{..} =
    case find ((== "shared_ref") . name) tdefAttrs of
         Nothing   -> return False
         Just attr -> do check d (attrVal attr == eTrue) (pos attr)
                               "The value of 'sharef_ref' attribute must be 'true' or empty"
                         return True

tdefGetSharedRefAttr :: DatalogProgram -> TypeDef -> Bool
tdefGetSharedRefAttr d tdef =
    case tdefCheckSharedRefAttr d tdef of
         Left e  -> error e
         Right b -> b

{- 'dyn_alloc' attribute: Tells DDlog that the type is a shared reference in
 - the style of `Ref` and `Intern`. -}
tdefCheckDynAllocAttr :: (MonadError String me) => DatalogProgram -> TypeDef -> me Bool
tdefCheckDynAllocAttr d TypeDef{..} =
    case find ((== "dyn_alloc") . name) tdefAttrs of
         Nothing   -> return False
         Just attr -> do check d (attrVal attr == eTrue) (pos attr)
                               "The value of 'dyn_alloc' attribute must be 'true' or empty"
                         return True

tdefGetDynAllocAttr :: DatalogProgram -> TypeDef -> Bool
tdefGetDynAllocAttr d tdef =
    case tdefCheckDynAllocAttr d tdef of
         Left e  -> error e
         Right b -> b

{- 'alias' attribute: Tells DDlog not to generate Rust declaration for the type
 - and instead replace all occurrences of the type with its definition. -}
tdefCheckAliasAttr :: (MonadError String me) => DatalogProgram -> TypeDef -> me Bool
tdefCheckAliasAttr d TypeDef{..} =        
    case find ((== "alias") . name) tdefAttrs of
         Nothing   -> return False
         Just attr -> do
             check d (attrVal attr == eTrue) (pos attr)
                   "The value of 'alias' attribute must be 'true' or empty"
             return True

tdefGetAliasAttr :: DatalogProgram -> TypeDef -> Bool
tdefGetAliasAttr d tdef =
    case tdefCheckAliasAttr d tdef of
         Left e  -> error e
         Right b -> b

{- 'rust' attribute is transferred directly to the generated Rust code. -}

checkRustAttrs :: (MonadError String me) => DatalogProgram -> [Attribute] -> me [String]
checkRustAttrs d attrs =
    mapM (\Attribute{..} ->
            case attrVal of
                 E (EString _ str) -> return str
                 _ -> err d attrPos $ "Invalid 'rust' attribute: the value of the attribute must be a string literal, e.g., #[rust=\"serde(tag = \\\"type\\\"\")]")
         $ filter ((== "rust") . name) attrs

getRustAttrs :: DatalogProgram -> [Attribute] -> [String]
getRustAttrs d attrs =
    case checkRustAttrs d attrs of
         Left e   -> error e
         Right as -> as

{- 'deserialize_from_array' attribute: specifies key function to be used to
   deserialize array into a map. -}

fieldCheckDeserializeArrayAttr :: (MonadError String me) => DatalogProgram -> Field -> me (Maybe String)
fieldCheckDeserializeArrayAttr d field@Field{..} =
    case filter ((== "deserialize_from_array") . name) fieldAttrs of
         []                   -> return Nothing
         [attr@Attribute{attrVal = E (EApply _ (E (EFunc _ [fname])) _)}] -> do
             check d (isMap d field) (pos attr) $ "'deserialize_from_array' attribute is only applicable to fields of type 'Map<>'."
             let TOpaque _ _ [ktype, vtype] = typ' d field
             (kfunc, _) <- checkFunc (pos attr) d fname [vtype] ktype
             _ <- funcTypeArgSubsts d (pos attr) kfunc [vtype] (Just ktype)
             return $ Just fname
         [Attribute{..}] -> err d attrPos
             $ "Invalid 'deserialize_from_array' attribute value '" ++ show attrVal ++ "': the value of the attribute must be the name of the key function, e.g.: \"deserialize_from_array=key_func()\"."
         _               -> err d (pos field) $ "Multiple 'deserialize_from_array' attributes are not allowed."

fieldGetDeserializeArrayAttr :: DatalogProgram -> Field -> Maybe String
fieldGetDeserializeArrayAttr d field =
    case fieldCheckDeserializeArrayAttr d field of
         Left e      -> error e
         Right fname -> fname

{- 'has_side_effects' attribute: labels functions with side effects, e.g.,
   logging functions. -}

checkSideEffectAttr :: (MonadError String me) => DatalogProgram -> Function -> me Bool
checkSideEffectAttr d Function{..} =
    case find ((== "has_side_effects") . name) funcAttrs of
         Nothing -> return False
         Just attr -> do check d (isNothing funcDef) (pos attr) "'has_side_effects' attribute is supported for extern functions only"
                         check d (attrVal attr == eTrue) (pos attr)
                            "The value of 'has_side_effects' attribute must be 'true' or empty"
                         return True

funcGetSideEffectAttr :: DatalogProgram -> Function -> Bool
funcGetSideEffectAttr d f =
    case checkSideEffectAttr d f of
         Left e -> error e
         Right has_side_effects -> has_side_effects

checkReturnByRefAttr :: (MonadError String me) => DatalogProgram -> Function -> me Bool
checkReturnByRefAttr d Function{..} =
    case find ((== "return_by_ref") . name) funcAttrs of
         Nothing -> return False
         Just attr -> do check d (isNothing funcDef) (pos attr) "'return_by_ref' attribute is supported for extern functions only"
                         check d (attrVal attr == eTrue) (pos attr)
                            "The value of 'return_by_ref' attribute must be 'true' or empty"
                         return True

funcGetReturnByRefAttr :: DatalogProgram -> Function -> Bool
funcGetReturnByRefAttr d f =
    case checkReturnByRefAttr d f of
         Left e -> error e
         Right return_by_ref -> return_by_ref

{- 'iterate_by_val', 'iterate_by_ref' attributes: Tell DDlog that the type can be iterated over and thus
   can be used in a for-loop or a 'FlatMap' operator.  The first form indicates tha the 'iter()' method
   returns a by-value iterator, the second form indicates that 'iter()' returns a by-reference iterator. -}
tdefCheckIterableAttr :: (MonadError String me) => DatalogProgram -> TypeDef -> me (Maybe (Type, EKind))
tdefCheckIterableAttr d TypeDef{..} = do
    by_ref <- case find ((== "iterate_by_ref") . name) tdefAttrs of
                   Nothing   -> return Nothing
                   Just attr -> case attrVal attr of
                                     E (ETyped _ (E EVar{..}) t) -> do
                                         typeValidate d tdefArgs t
                                         return $ Just t
                                     _ -> err d (pos $ attrVal attr)
                                                $ "The 'iterate_by_ref' attribute must have the following syntax: '#[iterate_by_ref=iter:<type>]', " ++
                                                  "where <type> is the type of each element when iterating over the collection of type '" ++ tdefName ++ "'"
    by_val <- case find ((== "iterate_by_val") . name) tdefAttrs of
                   Nothing   -> return Nothing
                   Just attr -> case attrVal attr of
                                     E (ETyped _ (E EVar{..}) t) -> do
                                         check d (isNothing by_ref) (pos attr) $ "Conflicting attributes 'iterate_by_val' and 'iterate_by_ref' specified for type '" ++ tdefName ++ "'"
                                         typeValidate d tdefArgs t
                                         return $ Just t
                                     _ -> err d (pos $ attrVal attr)
                                                $ "The 'iterate_by_val' attribute must have the following syntax: '#[iterate_by_val=iter:<type>]', " ++
                                                  "where <type> is the type of each element when iterating over the collection of type '" ++ tdefName ++ "'"
    case (by_ref, by_val) of
         (Nothing, Nothing) -> return Nothing
         (Just t, Nothing)  -> return $ Just (t, EReference)
         (Nothing, Just t)  -> return $ Just (t, EVal)
         _                  -> error "tdefCheckIterableAttr: unreachable"

tdefGetIterableAttr :: DatalogProgram -> TypeDef -> Maybe (Type, EKind)
tdefGetIterableAttr d tdef =
    case tdefCheckIterableAttr d tdef of
         Left e  -> error e
         Right b -> b
