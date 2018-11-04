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

{-# LANGUAGE FlexibleContexts, RecordWildCards, OverloadedStrings, LambdaCase #-}

-- This file is incorrectly named; it does not have anything to do with syntax.
-- In fact, this file contains the definition of the program intermediate representation:
-- the data structures used to represent the datalog program.
-- Each data structure must implement several instances:
-- PP: pretty-printing
-- Eq: equality testing (in general it is recursive and it ignores the position)
-- Show: conversion to string; in general it just calls pp
-- WithPos: manipulating position information

module Language.DifferentialDatalog.Syntax (
        Type(..),
        typeTypeVars,
        tBool,
        tInt,
        tString,
        tBit,
        tStruct,
        tTuple,
        tUser,
        tVar,
        tOpaque,
        structFields,
        structGetField,
        structFieldGuarded,
        Field(..),
        TypeDef(..),
        Constructor(..),
        consType,
        consIsUnique,
        KeyExpr(..),
        RelationRole(..),
        Relation(..),
        RuleRHS(..),
        rhsIsLiteral,
        Atom(..),
        Rule(..),
        ExprNode(..),
        Expr(..),
        ENode,
        enode,
        eVar,
        eApply,
        eField,
        eBool,
        eTrue,
        eFalse,
        eInt,
        eString,
        eBit,
        eStruct,
        eTuple,
        eSlice,
        eMatch,
        eVarDecl,
        eSeq,
        eITE,
        eSet,
        eBinOp,
        eUnOp,
        eNot,
        ePHolder,
        eTyped,
        Function(..),
        funcShowProto,
        funcTypeVars,
        ModuleName(..),
        Import(..),
        DatalogProgram(..),
        emptyDatalogProgram,
        progStructs,
        progConstructors,
        progAddRel,
        progAddRules,
        progAddTypedef,
        ECtx(..),
        ctxParent)
where

import Text.PrettyPrint
import Data.Maybe
import Data.List
import Data.String.Utils
import qualified Data.Map as M

import Language.DifferentialDatalog.Pos
import Language.DifferentialDatalog.Ops
import Language.DifferentialDatalog.Name
import Language.DifferentialDatalog.PP

data Field = Field { fieldPos  :: Pos
                   , fieldName :: String
                   , fieldType :: Type
                   }

instance Eq Field where
    (==) (Field _ n1 t1) (Field _ n2 t2) = n1 == n2 && t1 == t2

instance Ord Field where
    compare (Field _ n1 t1) (Field _ n2 t2) = compare (n1, t1) (n2, t2)

instance WithPos Field where
    pos = fieldPos
    atPos f p = f{fieldPos = p}

instance WithName Field where
    name = fieldName
    setName f n = f { fieldName = n }

instance PP Field where
    pp (Field _ n t) = pp n <> ":" <+> pp t

instance Show Field where
    show = render . pp

data Type = TBool     {typePos :: Pos}
          | TInt      {typePos :: Pos}
          | TString   {typePos :: Pos}
          | TBit      {typePos :: Pos, typeWidth :: Int}
          | TStruct   {typePos :: Pos, typeCons :: [Constructor]}
          | TTuple    {typePos :: Pos, typeTupArgs :: [Type]}
          | TUser     {typePos :: Pos, typeName :: String, typeArgs :: [Type]}
          | TVar      {typePos :: Pos, tvarName :: String}
          | TOpaque   {typePos :: Pos, typeName :: String, typeArgs :: [Type]}

tBool      = TBool     nopos
tInt       = TInt      nopos
tString    = TString   nopos
tBit       = TBit      nopos
tStruct    = TStruct   nopos
tTuple [t] = t
tTuple ts  = TTuple    nopos ts
tUser      = TUser     nopos
tVar       = TVar      nopos
tOpaque    = TOpaque   nopos

structGetField :: Type -> String -> Field
structGetField t f = fromJust $ find ((==f) . name) $ structFields t

structFields :: Type -> [Field]
structFields (TStruct _ cs) = nub $ concatMap consArgs cs
structFields t              = error $ "structFields " ++ show t

-- True iff the field is not defined in some constructors
structFieldGuarded :: Type -> String -> Bool
structFieldGuarded (TStruct _ cs) f = any (isNothing . find ((==f) . name) . consArgs) cs
structFieldGuarded t              _ = error $ "structFieldGuarded " ++ show t

{-
-- All constructors that contain the field
structFieldConstructors :: [Constructor] -> String -> [Constructor]
structFieldConstructors cs f = filter (isJust . find ((==f) . name) . consArgs) cs

structTypeDef :: Refine -> Type -> TypeDef
structTypeDef r TStruct{..} = consType r $ name $ head typeCons
structTypeDef _ t           = error $ "structTypeDef " ++ show t
-}

instance Eq Type where
    (==) TBool{}            TBool{}             = True
    (==) TInt{}             TInt{}              = True
    (==) TString{}          TString{}           = True
    (==) (TBit _ w1)        (TBit _ w2)         = w1 == w2
    (==) (TStruct _ cs1)    (TStruct _ cs2)     = cs1 == cs2
    (==) (TTuple _ ts1)     (TTuple _ ts2)      = ts1 == ts2
    (==) (TUser _ n1 as1)   (TUser _ n2 as2)    = n1 == n2 && as1 == as2
    (==) (TVar _ v1)        (TVar _ v2)         = v1 == v2
    (==) (TOpaque _ t1 as1) (TOpaque _ t2 as2)  = t1 == t2 && as1 == as2
    (==) _                  _                   = False

-- assign rank to constructors; used in the implementation of Ord
trank :: Type -> Int
trank TBool  {} = 0
trank TInt   {} = 1
trank TString{} = 2
trank TBit   {} = 3
trank TStruct{} = 4
trank TTuple {} = 5
trank TUser  {} = 6
trank TVar   {} = 7
trank TOpaque{} = 8

instance Ord Type where
    compare TBool{}            TBool{}             = EQ
    compare TInt{}             TInt{}              = EQ
    compare TString{}          TString{}           = EQ
    compare (TBit _ w1)        (TBit _ w2)         = compare w1 w2
    compare (TStruct _ cs1)    (TStruct _ cs2)     = compare cs1 cs2
    compare (TTuple _ ts1)     (TTuple _ ts2)      = compare ts1 ts2
    compare (TUser _ n1 as1)   (TUser _ n2 as2)    = compare (n1, as1) (n2, as2)
    compare (TVar _ v1)        (TVar _ v2)         = compare v1 v2
    compare (TOpaque _ t1 as1) (TOpaque _ t2 as2)  = compare (t1, as1) (t2, as2)
    compare t1                 t2                  = compare (trank t1) (trank t2)


instance WithPos Type where
    pos = typePos
    atPos t p = t{typePos = p}

instance PP Type where
    pp (TBool _)        = "bool"
    pp (TInt _)         = "bigint"
    pp (TString _)      = "string"
    pp (TBit _ w)       = "bit<" <> pp w <> ">"
    pp (TStruct _ cons) = hcat $ punctuate (" | ") $ map pp cons
    pp (TTuple _ as)    = parens $ hsep $ punctuate comma $ map pp as
    pp (TUser _ n as)   = pp n <>
                          if null as
                             then empty
                             else "<" <> (hcat $ punctuate comma $ map pp as) <> ">"
    pp (TVar _ v)       = "'" <> pp v
    pp (TOpaque _ t as) = pp t <>
                          if null as
                             then empty
                             else "<" <> (hcat $ punctuate comma $ map pp as) <> ">"

instance Show Type where
    show = render . pp


-- | Type variables used in type declaration in the order they appear in the declaration
typeTypeVars :: Type -> [String]
typeTypeVars TBool{}     = []
typeTypeVars TInt{}      = []
typeTypeVars TString{}   = []
typeTypeVars TBit{}      = []
typeTypeVars TStruct{..} = nub $ concatMap (typeTypeVars . fieldType)
                               $ concatMap consArgs typeCons
typeTypeVars TTuple{..}  = nub $ concatMap typeTypeVars typeTupArgs
typeTypeVars TUser{..}   = nub $ concatMap typeTypeVars typeArgs
typeTypeVars TVar{..}    = [tvarName]
typeTypeVars TOpaque{..} = nub $ concatMap typeTypeVars typeArgs

data TypeDef = TypeDef { tdefPos  :: Pos
                       , tdefName :: String
                       , tdefArgs :: [String]
                       , tdefType :: Maybe Type
                       }

instance WithPos TypeDef where
    pos = tdefPos
    atPos t p = t{tdefPos = p}

instance WithName TypeDef where
    name = tdefName
    setName d n = d{tdefName = n}

instance PP TypeDef where
    pp TypeDef{..} | isJust tdefType
                   = "typedef" <+> pp tdefName <>
                     (if null tdefArgs
                         then empty
                         else "<" <> (hcat $ punctuate comma $ map (("'" <>) . pp) tdefArgs) <> ">") <+>
                     "=" <+> (pp $ fromJust tdefType)
    pp TypeDef{..} = "extern type" <+> pp tdefName <>
                     (if null tdefArgs
                         then empty
                         else "<" <> (hcat $ punctuate comma $ map (("'" <>) . pp) tdefArgs) <> ">")

instance Show TypeDef where
    show = render . pp

instance Eq TypeDef where
    (==) t1 t2 = name t1 == name t2 && tdefType t1 == tdefType t2

data Constructor = Constructor { consPos :: Pos
                               , consName :: String
                               , consArgs :: [Field]
                               }

instance Eq Constructor where
    (==) (Constructor _ n1 as1) (Constructor _ n2 as2) = n1 == n2 && as1 == as2

instance Ord Constructor where
    compare (Constructor _ n1 as1) (Constructor _ n2 as2) = compare (n1, as1) (n2, as2)

instance WithName Constructor where
    name = consName
    setName c n = c{consName = n}

instance WithPos Constructor where
    pos = consPos
    atPos c p = c{consPos = p}

instance PP Constructor where
    pp Constructor{..} = pp consName <> (braces $ hsep $ punctuate comma $ map pp consArgs)

instance Show Constructor where
    show = render . pp

consType :: DatalogProgram -> String -> TypeDef
consType d c =
    fromJust
    $ find (\td -> case tdefType td of
                        Just (TStruct _ cs) -> any ((==c) . name) cs
                        _                   -> False)
    $ progTypedefs d

-- | 'True' iff c is the unique constructor of its type
consIsUnique :: DatalogProgram -> String -> Bool
consIsUnique d c = (length $ typeCons $ fromJust $ tdefType $ consType d c) == 1

data KeyExpr = KeyExpr { keyPos  :: Pos
                       , keyVar  :: String
                       , keyExpr :: Expr
                       }

instance Eq KeyExpr where
    (==) (KeyExpr _ v1 e1) (KeyExpr _ v2 e2) = v1 == v2 && e1 == e2

instance WithPos KeyExpr where
    pos = keyPos
    atPos k p = k{keyPos = p}

instance PP KeyExpr where
    pp KeyExpr{..} = "(" <> pp keyVar <> ")" <+> pp keyExpr

instance Show KeyExpr where
    show = render . pp

data RelationRole = RelInput 
                  | RelOutput
                  | RelInternal
                  deriving(Eq, Ord)


instance PP RelationRole where
    pp RelInput    = "input"
    pp RelOutput   = "output"
    pp RelInternal = empty

instance Show RelationRole where
    show = render . pp

data Relation = Relation { relPos         :: Pos
                         , relRole        :: RelationRole
                         , relName        :: String
                         , relType        :: Type
                         , relPrimaryKey  :: Maybe KeyExpr
                         }

instance Eq Relation where
    (==) (Relation _ r1 n1 t1 k1) (Relation _ r2 n2 t2 k2) = r1 == r2 && n1 == n2 && t1 == t2 && k1 == k2

instance WithPos Relation where
    pos = relPos
    atPos r p = r{relPos = p}

instance WithName Relation where
    name = relName
    setName r n = r{relName = n}

instance PP Relation where
    pp Relation{..} = pp relRole <+>
                      "relation" <+> pp relName <+> "[" <> pp relType <> "]" <+> pkey
        where pkey = maybe empty (("primary key" <+>) . pp) relPrimaryKey

instance Show Relation where
    show = render . pp

data Atom = Atom { atomPos      :: Pos
                 , atomRelation :: String
                 , atomVal      :: Expr
                 }

instance WithName Atom where
    name = atomRelation
    setName a n = a {atomRelation = n}

instance Eq Atom where
    (==) (Atom _ r1 v1) (Atom _ r2 v2) = r1 == r2 && v1 == v2

instance WithPos Atom where
    pos = atomPos
    atPos a p = a{atomPos = p}

instance PP Atom where
    pp (Atom _ rel (E (EStruct _ cons as))) | rel == cons
                = pp rel <> (parens $ hsep $ punctuate comma $
                             map (\(n,e) -> (if null n then empty else ("." <> pp n <> "=")) <> pp e) as)
    pp Atom{..} = pp atomRelation <> "[" <> pp atomVal <> "]"

instance Show Atom where
    show = render . pp

-- The RHS of a rule consists of relational atoms with
-- positive/negative polarity, Boolean conditions, aggregation and
-- disaggregation (flatmap) operations.  The last two must occur after
-- all atoms.
data RuleRHS = RHSLiteral   {rhsPolarity:: Bool, rhsAtom :: Atom}
             | RHSCondition {rhsExpr :: Expr}
             | RHSAggregate {rhsGroupBy :: [String], rhsVar :: String, rhsAggFunc :: String, rhsAggExpr :: Expr}
             | RHSFlatMap   {rhsVar :: String, rhsMapExpr :: Expr}
             deriving (Eq)

instance PP RuleRHS where
    pp (RHSLiteral True a)    = pp a
    pp (RHSLiteral False a)   = "not" <+> pp a
    pp (RHSCondition c)       = pp c
    pp (RHSAggregate g v f e) = "Aggregate" <> "(" <>
                                (parens $ vcat $ punctuate comma $ map pp g) <> comma <+>
                                pp v <+> "=" <+> pp f <> (parens $ pp e) <> ")"
    pp (RHSFlatMap v e)       = "var" <+> pp v <+> "=" <+> "FlatMap" <> (parens $ pp e)

instance Show RuleRHS where
    show = render . pp

rhsIsLiteral :: RuleRHS -> Bool
rhsIsLiteral RHSLiteral{} = True
rhsIsLiteral _            = False

data Rule = Rule { rulePos :: Pos
                 , ruleLHS :: [Atom]
                 , ruleRHS :: [RuleRHS]
                 }

instance Eq Rule where
    (==) (Rule _ lhs1 rhs1) (Rule _ lhs2 rhs2) =
        lhs1 == lhs2 && rhs1 == rhs2

instance WithPos Rule where
    pos = rulePos
    atPos r p = r{rulePos = p}

instance PP Rule where
    pp Rule{..} = (vcat $ punctuate "," $ map pp ruleLHS) <+>
                  (if null ruleRHS
                      then empty
                      else ":-" <+> (hsep $ punctuate comma $ map pp ruleRHS)) <> "."

instance Show Rule where
    show = render . pp

data ExprNode e = EVar          {exprPos :: Pos, exprVar :: String}
                | EApply        {exprPos :: Pos, exprFunc :: String, exprArgs :: [e]}
                | EField        {exprPos :: Pos, exprStruct :: e, exprField :: String}
                | EBool         {exprPos :: Pos, exprBVal :: Bool}
                | EInt          {exprPos :: Pos, exprIVal :: Integer}
                | EString       {exprPos :: Pos, exprString :: String}
                | EBit          {exprPos :: Pos, exprWidth :: Int, exprIVal :: Integer}
                | EStruct       {exprPos :: Pos, exprConstructor :: String, exprStructFields :: [(String, e)]}
                | ETuple        {exprPos :: Pos, exprTupleFields :: [e]}
                | ESlice        {exprPos :: Pos, exprOp :: e, exprH :: Int, exprL :: Int}
                | EMatch        {exprPos :: Pos, exprMatchExpr :: e, exprCases :: [(e, e)]}
                | EVarDecl      {exprPos :: Pos, exprVName :: String}
                | ESeq          {exprPos :: Pos, exprLeft :: e, exprRight :: e}
                | EITE          {exprPos :: Pos, exprCond :: e, exprThen :: e, exprElse :: e}
                | ESet          {exprPos :: Pos, exprLVal :: e, exprRVal :: e}
                | EBinOp        {exprPos :: Pos, exprBOp :: BOp, exprLeft :: e, exprRight :: e}
                | EUnOp         {exprPos :: Pos, exprUOp :: UOp, exprOp :: e}
                | EPHolder      {exprPos :: Pos}
                | ETyped        {exprPos :: Pos, exprExpr :: e, exprTSpec :: Type}

instance Eq e => Eq (ExprNode e) where
    (==) (EVar _ v1)              (EVar _ v2)                = v1 == v2
    (==) (EApply _ f1 as1)        (EApply _ f2 as2)          = f1 == f2 && as1 == as2
    (==) (EField _ s1 f1)         (EField _ s2 f2)           = s1 == s2 && f1 == f2
    (==) (EBool _ b1)             (EBool _ b2)               = b1 == b2
    (==) (EInt _ i1)              (EInt _ i2)                = i1 == i2
    (==) (EString _ s1)           (EString _ s2)             = s1 == s2
    (==) (EBit _ w1 i1)           (EBit _ w2 i2)             = w1 == w2 && i1 == i2
    (==) (EStruct _ c1 fs1)       (EStruct _ c2 fs2)         = c1 == c2 && fs1 == fs2
    (==) (ETuple _ fs1)           (ETuple _ fs2)             = fs1 == fs2
    (==) (ESlice _ e1 h1 l1)      (ESlice _ e2 h2 l2)        = e1 == e2 && h1 == h2 && l1 == l2
    (==) (EMatch _ e1 cs1)        (EMatch _ e2 cs2)          = e1 == e2 && cs1 == cs2
    (==) (EVarDecl _ v1)          (EVarDecl _ v2)            = v1 == v2
    (==) (ESeq _ l1 r1)           (ESeq _ l2 r2)             = l1 == l2 && r1 == r2
    (==) (EITE _ i1 t1 e1)        (EITE _ i2 t2 e2)          = i1 == i2 && t1 == t2 && e1 == e2
    (==) (ESet _ l1 r1)           (ESet _ l2 r2)             = l1 == l2 && r1 == r2
    (==) (EBinOp _ o1 l1 r1)      (EBinOp _ o2 l2 r2)        = o1 == o2 && l1 == l2 && r1 == r2
    (==) (EUnOp _ o1 e1)          (EUnOp _ o2 e2)            = o1 == o2 && e1 == e2
    (==) (EPHolder _)             (EPHolder _)               = True
    (==) (ETyped _ e1 t1)         (ETyped _ e2 t2)           = e1 == e2 && t1 == t2
    (==) _                        _                          = False

instance WithPos (ExprNode e) where
    pos = exprPos
    atPos e p = e{exprPos = p}

instance PP e => PP (ExprNode e) where
    pp (EVar _ v)            = pp v
    pp (EApply _ f as)       = pp f <> (parens $ hsep $ punctuate comma $ map pp as)
    pp (EField _ s f)        = pp s <> char '.' <> pp f
    pp (EBool _ True)        = "true"
    pp (EBool _ False)       = "false"
    pp (EInt _ v)            = pp v
    pp (EString _ s)         = pp (show s)
    pp (EBit _ w v)          = pp w <> "'d" <> pp v
    pp (EStruct _ s fs)      = pp s <> (braces $ hsep $ punctuate comma
                                        $ map (\(n,e) -> (if null n then empty else ("." <> pp n <> "=")) <> pp e) fs)
    pp (ETuple _ fs)         = parens $ hsep $ punctuate comma $ map pp fs
    pp (ESlice _ e h l)      = pp e <> (brackets $ pp h <> colon <> pp l)
    pp (EMatch _ e cs)       = "match" <+> parens (pp e) <+> "{"
                               $$
                               (nest' $ vcat
                                      $ punctuate comma
                                      $ (map (\(c,v) -> pp c <+> "->" <+> pp v) cs))
                               $$
                               "}"
    pp (EVarDecl _ v)        = "var" <+> pp v
    pp (ESeq _ l r)          = parens $ (pp l <> semi) $$ pp r
    pp (EITE _ c t e)        = ("if" <+> pp c <+> lbrace)
                               $$
                               (nest' $ pp t)
                               $$
                               rbrace <+> (("else" <+> lbrace) $$ (nest' $ pp e) $$ rbrace)
    pp (ESet _ l r)          = pp l <+> "=" <+> pp r
    pp (EBinOp _ op e1 e2)   = parens $ pp e1 <+> pp op <+> pp e2
    pp (EUnOp _ op e)        = parens $ pp op <+> pp e
    pp (EPHolder _)          = "_"
    pp (ETyped _ e t)        = parens $ pp e <> ":" <+> pp t

instance PP e => Show (ExprNode e) where
    show = render . pp

type ENode = ExprNode Expr

newtype Expr = E ENode
enode :: Expr -> ExprNode Expr
enode (E n) = n

instance Eq Expr where
    (==) (E e1) (E e2) = e1 == e2

instance PP Expr where
    pp (E n) = pp n

instance Show Expr where
    show (E n) = show n

instance WithPos Expr where
    pos (E n) = pos n
    atPos (E n) p = E $ atPos n p

eVar v              = E $ EVar      nopos v
eApply f as         = E $ EApply    nopos f as
eField e f          = E $ EField    nopos e f
eBool b             = E $ EBool     nopos b
eTrue               = eBool True
eFalse              = eBool False
eInt i              = E $ EInt      nopos i
eString s           = E $ EString   nopos s
eBit w v            = E $ EBit      nopos w v
eStruct c as        = E $ EStruct   nopos c as
eTuple [a]          = a
eTuple as           = E $ ETuple    nopos as
eSlice e h l        = E $ ESlice    nopos e h l
eMatch e cs         = E $ EMatch    nopos e cs
eVarDecl v          = E $ EVarDecl  nopos v
eSeq l r            = E $ ESeq      nopos l r
eITE i t e          = E $ EITE      nopos i t e
eSet l r            = E $ ESet      nopos l r
eBinOp op l r       = E $ EBinOp    nopos op l r
eUnOp op e          = E $ EUnOp     nopos op e
eNot e              = eUnOp Not e
ePHolder            = E $ EPHolder  nopos
eTyped e t          = E $ ETyped    nopos e t

data Function = Function { funcPos   :: Pos
                         , funcName  :: String
                         , funcArgs  :: [Field]
                         , funcType  :: Type
                         , funcDef   :: Maybe Expr
                         }

instance Eq Function where
    (==) (Function _ n1 as1 t1 d1) (Function _ n2 as2 t2 d2) =
        n1 == n2 && as1 == as2 && t1 == t2 && d1 == d2

instance WithPos Function where
    pos = funcPos
    atPos f p = f{funcPos = p}

instance WithName Function where
    name = funcName
    setName f n = f{funcName = n}

instance PP Function where
    pp Function{..} = (maybe "extern" (\_ -> empty) funcDef) <+>
                      ("function" <+> pp funcName
                       <+> (parens $ hsep $ punctuate comma $ map pp funcArgs)
                       <> colon <+> pp funcType
                       <+> (maybe empty (\_ -> "=") funcDef))
                      $$
                       (maybe empty (nest' . pp) funcDef)

instance Show Function where
    show = render . pp

funcShowProto :: Function -> String
funcShowProto Function{..} = render $
    "function" <+> pp funcName
    <+> (parens $ hsep $ punctuate comma $ map pp funcArgs)
    <> colon <+> pp funcType

-- | Type variables used in function declaration in the order they appear in the declaration
funcTypeVars :: Function -> [String]
funcTypeVars = nub . concatMap (typeTypeVars . fieldType) . funcArgs

data ModuleName = ModuleName {modulePath :: [String]}
                  deriving (Eq, Ord)

instance PP ModuleName where
    pp (ModuleName p) = hcat $ punctuate "." $ map pp p

instance Show ModuleName where
    show = render . pp

-- | Import statement
data Import = Import { importPos    :: Pos
                     , importModule :: ModuleName
                     , importAlias  :: ModuleName 
                     }

instance WithPos Import where
    pos = importPos
    atPos i p = i { importPos = p }

instance PP Import where
    pp Import{..} = "import" <+> pp importModule <+>
                    (if null (modulePath importAlias) then empty else "as" <+> pp importAlias)

instance Show Import where
    show = render . pp

instance Eq Import where
    (==) (Import _ p1 a1) (Import _ p2 a2) = p1 == p2 && a1 == a2

data DatalogProgram = DatalogProgram { progImports   :: [Import]
                                     , progTypedefs  :: M.Map String TypeDef
                                     , progFunctions :: M.Map String Function
                                     , progRelations :: M.Map String Relation
                                     , progRules     :: [Rule]
                                     }
                      deriving (Eq)

instance PP DatalogProgram where
    pp DatalogProgram{..} = vcat $ punctuate "" $
                            ((map pp progImports)
                             ++
                             (map pp $ M.elems progTypedefs)
                             ++
                             (map pp $ M.elems progFunctions)
                             ++
                             (map pp $ M.elems progRelations)
                             ++
                             (map pp progRules))

instance Show DatalogProgram where
    show = render . pp

progStructs :: DatalogProgram -> M.Map String TypeDef
progStructs DatalogProgram{..} =
    M.filter ((\case
                Just TStruct{} -> True
                _              -> False) . tdefType)
             progTypedefs

progConstructors :: DatalogProgram -> [Constructor]
progConstructors = concatMap (typeCons . fromJust . tdefType) . M.elems . progStructs

emptyDatalogProgram :: DatalogProgram
emptyDatalogProgram = DatalogProgram { progImports    = []
                                     , progTypedefs   = M.empty
                                     , progFunctions  = M.empty
                                     , progRelations  = M.empty
                                     , progRules      = [] }

progAddTypedef :: TypeDef -> DatalogProgram -> DatalogProgram
progAddTypedef tdef prog = prog{progTypedefs = M.insert (name tdef) tdef (progTypedefs prog)}

progAddRules :: [Rule] -> DatalogProgram -> DatalogProgram
progAddRules rules prog = prog{progRules = rules ++ (progRules prog)}

progAddRel :: Relation -> DatalogProgram -> DatalogProgram
progAddRel rel prog = prog{progRelations = M.insert (name rel) rel (progRelations prog)}

-- | Expression's syntactic context determines the kinds of
-- expressions that can appear at this location in the Datalog program,
-- expected type of the expression, and variables visible withing the
-- given scope.
--
-- Below, 'X' indicates the position of the expression addressed by
-- context.
--
-- Most 'ECtx' constructors take reference to parent expression
-- ('ctxParExpr') and parent context ('ctxPar').
data ECtx = -- | Top-level context. Serves as the root of the context hierarchy.
            -- Expressions cannot appear directly in this context.
            CtxTop
            -- | Function definition: 'function f(...) = {X}'
          | CtxFunc           {ctxFunc::Function}
            -- | Argument to an atom in the left-hand side of a rule:
            -- 'Rel1(.f1=X,.f2=y,.f3=x) :- ...'.
            --           ^
            --           \- context points here
            -- 'ctxAtomIdx' is the index of the LHS atom where the
            -- expression appears
            --
            -- 'ctxField' is the field within the atom.
            -- Note: preprocessing ensures that field names are always
            -- present.
          | CtxRuleL          {ctxRule::Rule, ctxAtomIdx::Int}
            -- | Argument to a right-hand-side atom
          | CtxRuleRAtom      {ctxRule::Rule, ctxAtomIdx::Int}
            -- | Filter or assignment expression the RHS of a rule
          | CtxRuleRCond      {ctxRule::Rule, ctxIdx::Int}
            -- | FlatMap clause in the RHS of a rule
          | CtxRuleRFlatMap   {ctxRule::Rule, ctxIdx::Int}
            -- | Aggregate clause in the RHS of a rule
          | CtxRuleRAggregate {ctxRule::Rule, ctxIdx::Int}
            -- | Key expression
          | CtxKey            {ctxRelation::Relation}
            -- | Argument passed to a function
          | CtxApply          {ctxParExpr::ENode, ctxPar::ECtx, ctxIdx::Int}
            -- | Field expression: 'X.f'
          | CtxField          {ctxParExpr::ENode, ctxPar::ECtx}
            -- | Argument passed to a type constructor: 'Cons(X, y, z)'
          | CtxStruct         {ctxParExpr::ENode, ctxPar::ECtx, ctxArg::String}
            -- | Argument passed to a tuple expression: '(X, y, z)'
          | CtxTuple          {ctxParExpr::ENode, ctxPar::ECtx, ctxIdx::Int}
            -- | Bit slice: 'X[h:l]'
          | CtxSlice          {ctxParExpr::ENode, ctxPar::ECtx}
            -- | Argument of a match expression: 'match (X) {...}'
          | CtxMatchExpr      {ctxParExpr::ENode, ctxPar::ECtx}
            -- | Match pattern: 'match (...) {X: e1, ...}'
          | CtxMatchPat       {ctxParExpr::ENode, ctxPar::ECtx, ctxIdx::Int}
            -- | Value returned by a match clause: 'match (...) {p1: X, ...}'
          | CtxMatchVal       {ctxParExpr::ENode, ctxPar::ECtx, ctxIdx::Int}
            -- | First expression in a sequence 'X; y'
          | CtxSeq1           {ctxParExpr::ENode, ctxPar::ECtx}
            -- | Second expression in a sequence 'y; X'
          | CtxSeq2           {ctxParExpr::ENode, ctxPar::ECtx}
            -- | 'if (X) ... else ...'
          | CtxITEIf          {ctxParExpr::ENode, ctxPar::ECtx}
            -- | 'if (cond) X else ...'
          | CtxITEThen        {ctxParExpr::ENode, ctxPar::ECtx}
            -- | 'if (cond) ... else X'
          | CtxITEElse        {ctxParExpr::ENode, ctxPar::ECtx}
            -- | Left-hand side of an assignment: 'X = y'
          | CtxSetL           {ctxParExpr::ENode, ctxPar::ECtx}
            -- | Righ-hand side of an assignment: 'y = X'
          | CtxSetR           {ctxParExpr::ENode, ctxPar::ECtx}
            -- | First operand of a binary operator: 'X op y'
          | CtxBinOpL         {ctxParExpr::ENode, ctxPar::ECtx}
            -- | Second operand of a binary operator: 'y op X'
          | CtxBinOpR         {ctxParExpr::ENode, ctxPar::ECtx}
            -- | Operand of a unary operator: 'op X'
          | CtxUnOp           {ctxParExpr::ENode, ctxPar::ECtx}
            -- | Argument of a typed expression 'X: t'
          | CtxTyped          {ctxParExpr::ENode, ctxPar::ECtx}

instance PP ECtx where
    pp CtxTop        = "CtxTop"
    pp ctx = (pp $ ctxParent ctx) $$ ctx'
        where
        epar = short $ ctxParExpr ctx
        rule = short $ pp $ ctxRule ctx
        rel  = short $ pp $ ctxRelation ctx
        mlen = 100
        short :: (PP a) => a -> Doc
        short = pp . (\x -> if length x < mlen then x else take (mlen - 3) x ++ "...") . replace "\n" " " . render . pp
        ctx' = case ctx of
                    CtxRuleL{..}          -> "CtxRuleL          " <+> rule <+> pp ctxAtomIdx
                    CtxRuleRAtom{..}      -> "CtxRuleRAtom      " <+> rule <+> pp ctxAtomIdx
                    CtxRuleRCond{..}      -> "CtxRuleRCond      " <+> rule <+> pp ctxIdx
                    CtxRuleRFlatMap{..}   -> "CtxRuleRFlatMap   " <+> rule <+> pp ctxIdx
                    CtxRuleRAggregate{..} -> "CtxRuleRAggregate " <+> rule <+> pp ctxIdx
                    CtxKey{..}            -> "CtxKey            " <+> rel
                    CtxFunc{..}           -> "CtxFunc           " <+> (pp $ name ctxFunc)
                    CtxApply{..}          -> "CtxApply          " <+> epar <+> pp ctxIdx
                    CtxField{..}          -> "CtxField          " <+> epar
                    CtxStruct{..}         -> "CtxStruct         " <+> epar <+> pp ctxArg
                    CtxTuple{..}          -> "CtxTuple          " <+> epar <+> pp ctxIdx
                    CtxSlice{..}          -> "CtxSlice          " <+> epar
                    CtxMatchExpr{..}      -> "CtxMatchExpr      " <+> epar
                    CtxMatchPat{..}       -> "CtxMatchPat       " <+> epar <+> pp ctxIdx
                    CtxMatchVal{..}       -> "CtxMatchVal       " <+> epar <+> pp ctxIdx
                    CtxSeq1{..}           -> "CtxSeq1           " <+> epar
                    CtxSeq2{..}           -> "CtxSeq2           " <+> epar
                    CtxITEIf{..}          -> "CtxITEIf          " <+> epar
                    CtxITEThen{..}        -> "CtxITEThen        " <+> epar
                    CtxITEElse{..}        -> "CtxITEElse        " <+> epar
                    CtxSetL{..}           -> "CtxSetL           " <+> epar
                    CtxSetR{..}           -> "CtxSetR           " <+> epar
                    CtxBinOpL{..}         -> "CtxBinOpL         " <+> epar
                    CtxBinOpR{..}         -> "CtxBinOpR         " <+> epar
                    CtxUnOp{..}           -> "CtxUnOp           " <+> epar
                    CtxTyped{..}          -> "CtxTyped          " <+> epar
                    CtxTop                -> error "pp CtxTop"

instance Show ECtx where
    show = render . pp

ctxParent :: ECtx -> ECtx
ctxParent CtxRuleL{}          = CtxTop
ctxParent CtxRuleRAtom{}      = CtxTop
ctxParent CtxRuleRCond{}      = CtxTop
ctxParent CtxRuleRFlatMap{}   = CtxTop
ctxParent CtxRuleRAggregate{} = CtxTop
ctxParent CtxKey{}            = CtxTop
ctxParent CtxFunc{}           = CtxTop
ctxParent ctx                 = ctxPar ctx
