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

{-# LANGUAGE RecordWildCards, FlexibleContexts #-}

module Language.DifferentialDatalog.Function(
    funcTypeArgSubsts,
    funcGroupArgTypes,
    funcIsPure
) where

import Control.Monad.Except
import Data.List
import qualified Data.Map as M

import Language.DifferentialDatalog.Pos
import Language.DifferentialDatalog.Syntax
import Language.DifferentialDatalog.Attribute
import {-# SOURCE #-} Language.DifferentialDatalog.Expr
import {-# SOURCE #-} Language.DifferentialDatalog.Type

funcTypeArgSubsts :: (MonadError String me) => DatalogProgram -> Pos -> Function -> [Type] -> me (M.Map String Type)
funcTypeArgSubsts d p f@Function{..} argtypes =
    unifyTypes d p ("in call to " ++ funcShowProto f) (zip (map typ funcArgs ++ [funcType]) argtypes)

-- | Functions that take an argument of type `Group<>` are treated in a special
-- way in Compile.hs. This function returns the list of `Group` types passed as
-- arguments to the function.
funcGroupArgTypes :: DatalogProgram -> Function -> [Type]
funcGroupArgTypes d Function{..} =
    nub $ filter (isGroup d) $ map typ funcArgs

funcIsPure :: DatalogProgram -> Function -> Bool
funcIsPure d f =
    (funcGetSideEffectAttr d f == False) &&
    (maybe True (exprIsPure d) $ funcDef f)
