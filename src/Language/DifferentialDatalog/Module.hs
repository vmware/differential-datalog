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
Module     : Module
Description: DDlog's module system implemented as syntactic sugar over core syntax.
-}

module Language.DifferentialDatalog.Module(
    parseDatalogProgram)

import Language.DifferentialDatalog.Parse
import Language.DifferentialDatalog.Name

type ModuleName = [String]

data DatalogModule = DatalogModule {
    moduleName :: ModuleName,
    moduleDefs :: DatalogProgram
}

-- | Parse a datalog program along with all its imports; returns a "flat"
-- program without imports.
--
-- 'roots' is the list of directories to search for imports
--
-- if 'insert_preamble' is true, prepends the content of
-- 'datalogPreamble' to the file before parsing it.
parseDatalogProgram :: [FilePath] -> Bool -> FilePath -> IO DatalogProgram
parseDatalogProgram roots insert_preamble fname = do
    fdata <- readFile fname
    prog <- parseDatalogString insert_preamble fdata fname
    let main_mod = DatalogModule [] prog
    imports <- evalStateT (parseImports roots prog) []
    flattenNamespace $ main_mod : imports

flattenNamespace :: [DatalogModule] -> IO DatalogProgram
flattenNamespace mods = do
    mods' <- case map flattenNamespace1 mods of
                  Left e   -> errorWithoutStackTrace e
                  Right ms -> mods
    return $ mergeModules mods'

flattenNamespace1 :: (MonadError String me) => DatalogModule -> me DatalogProgram
flattenNamespace1 mod@DatalogModule{..} = do
    types' <- M.fromList <$>
              mapM ((\tdef -> (name tdef, tdef)) <$> tdefFlatten mod) 
              $ M.values $ progTypedefs moduleDefs
    funcs' <- M.fromList <$>
              mapM ((\f -> (name f, f)) <$> funcFlatten mod) 
              $ M.values $ progFunctions moduleDefs
    rels' <- M.fromList <$>
             map ((\r -> (name r, r)) <$> relFlatten mod) 
             $ M.values $ progRelations moduleDefs
    rules' <- mapM (ruleFlatten mod) $ progRules moduleDefs
    stats' <- mapM (statFlatten mod) $ progStatements moduleDefs
    return $ DatalogProgram {
        progTypedefs   = types'
        progFunctions  = funcs'
        progRelations  = rels'
        progRules      = rules'
        progStatements = stats'
    }


tdefFlatten :: (MonadError String me) => DatalogModule -> TypeDef -> me TypeDef
tdefFlatten mod tdef@TypeDef{..} = do
    let n = flattenDeclName mod tdefName
    t <- typeFlatten mod tdefType
    return $ tdef {
        tdefName = n,
        tdefType = t
    }

funcFlatten :: (MonadError String me) => DatalogModule -> Function -> me Function
funcFlatten mod fun@Function{..} = do
    let n = flattenDeclName mod funcName
    a <- map (fieldFlatten mod) funcArgs
    t <- typeFlatten mod funcType
    d <- fmap (exprFlatten mod) funcDef
    return $ fun {
        funcName = n,
        funcArgs = a,
        funcType = t,
        funcDef  = d
    }

relFlatten :: (MonadError String me) => DatalogModule -> Relation -> me Relation
relFlatten mod rel@Relation{..} = do
    let n = flattenDeclName mod relName
    t <- typeFlatten mod relType
    return $ rel {
        relName = n,
        relType = t
    }

ruleFlatten :: (MonadError String me) => DatalogModule -> Rule -> me Rule
ruleFlatten mod rule@Rule{..} = do
    l <- mapM (atomFlatten mod)    ruleLHS
    r <- mapM (ruleRHSFlatten mod) ruleRHS
    return $ rule {
        ruleLHS = l,
        ruleRHS = r
    }

statFlatten :: (MonadError String me) => DatalogModule -> Statement -> me Statement
statFlatten mod stat@ForStatement{..} = do
    n <- flattenName mod (pos stat) forRelName
    c <- maybe (return Nothing) (exprFlatten mod) forCondition
    f <- statFlatten mod forStatement
    return $ stat {
        forRelName   = n,
        forCondition = c,
        forStatement = f
    }
statFlatten mod stat@IfStatement{..} = do
    c <- exprFlatten mod ifCondition
    s <- statFlatten mod ifStatement
    e <- maybe (return Nothing) (statFlatten mod) elseStatement
    return $ stat {
        ifCondition   = c,
        ifStatement   = s,
        elseStatement = e
    }
statFlatten mod stat@MatchStatement{..} = do
    e <- exprFlatten mod matchExpr
    c <- mapM (\(e,s) -> (,) <$> exprFlatten mod e <*> statFlatten mod s) cases
    return $ stat {
        matchExpr = e,
        cases     = c
    }
statFlatten mod stat@VarStatement{..} = do
    v <- mapM (\assn@Assignment{..} -> do
                  t <- fmap (typeFlatten mod) typeAssign
                  r <- exprFlatten mod rightAssign
                  return $ assn {
                      typeAssign  = t,
                      rightAssign = r
                  }) varList
    s <- statFlatten mod varStatement
    return $ stat {
        varList      = v
        varStatement = s
    }
statFlatten mod stat@InsertStatement{..} = do
    a <- atomFlatten mod insertAtom
    return $ stat { insertAtom = a }
statFlatten mod stat@BlockStatement{..} = do
    l <- mapM (statFlatten mod) seqList
    return $ stat { seqList = l }
statFlatten mod stat@EmptyStatement{} = return stat

flattenDeclName :: DatalogModule -> String -> String
flattenDeclName mod n = intercalate "." (moduleName ++ n)

flattenName :: (MonadError String me) => DatalogModule -> Pos -> String -> me String
flattenName mod pos n =
    case namePrefix n of
         Nothing   -> return $ intercalate "." (moduleName ++ n)
         Just pref -> case find ((==pref) . importAlias) $ progImports moduleDefs of
                           Nothing  -> err pos $ "Unknown module " ++ pref ++ ".  Did you forget to import it?"
                           Just imp -> return $ intercalate "." (importModule imp ++ n)

typeFlatten :: (MonadError String me) => DatalogModule -> Type -> me Type
typeFlatten mod t = do
    case t of
         TStruct{..} -> do c = mapM (constructorFlatten mod) typeCons
                           return $ t { typeCons = c }
         TTuple{..}  -> do a = mapM (typeFlatten mod) typeTupArgs
                           return $ t { typeTupArgs = a }
         TUser{..}   -> do n <- flattenName mod (pos t) timeName
                           a <- mapM (typeFlatten mod) typeArgs
                           return $ t { typeName = n,
                                        typeArgs = a }
         TOpaque{}   -> do n <- flattenName mod (pos t) typeName
                           a <- mapM (typeFlatten mod) typeArgs
                           return $ t { typeName = flattenName mod (pos t) typeName,
                                        typeArgs = map (typeFlatten mod) typeArgs }
         _           -> return t

constructorFlatten :: (MonadError String me) => DatalogModule -> Constructor -> me Constructor
constructorFlatten mod c@Constructor{..} = do
    n <- flattenName mod (pos c) consName
    a <- mapM (fieldFlatten mod) consArgs
    return $ Constructor { consName = n
                         , consArgs = a }

fieldFlatten :: (MonadError String me) => DatalogModule -> Field -> me Field
fieldFlatten mod f@Field{..} = do
    t <- typeFlatten mod fieldType
    return $ f { fieldType = t }

atomFlatten :: (MonadError String me) => DatalogModule -> Atom -> me Atom
atomFlatten mod a@Atom{..} = do
    r <- flattenName mod (pos a) atomRelation
    v <- exprFlatten mod atomVal
    return $ a { atomRelation = r
               , atomVal      = v }

ruleRHSFlatten :: (MonadError String me) => DatalogModule -> RuleRHS -> me RuleRHS
ruleRHSFlatten mod rhs@RHSLiteral{..}   = do 
    a <- atomFlatten mod rhsAtom
    return $ rhs { rhsAtom = a }
ruleRHSFlatten mod rhs@RHSCondition{..} = do
    r <- exprFlatten mod rhsExpr
    return $ rhs { rhsExpr = r }
ruleRHSFlatten mod rhs@RHSAggregate{..} = do
    a <- exprFlatten mod rhsAggExpr
    return $ rhs { rhsAggExpr = a }
ruleRHSFlatten mod rhs@RHSFlatMap{..}   = do
    e <- exprFlatten mod rhsMapExpr
    return $ rhs { rhsMapExpr = e }

exprFlatten :: (MonadError String me) => DatalogModule -> Expr -> me Expr
exprFlatten mod e = exprFoldM (exprFlatten' mod) e

exprFlatten' :: (MonadError String me) => DatalogModule -> ENode -> me Expr
exprFlatten' mod e@EApply{..} = do
    f <- flattenName mod (pos e) exprFunc
    return $ e { exprFunc :: String }
exprFlatten' mod e@EStruct{..} = do
    c <- flattenName mod (pos e) exprConstructor
    return $ e { exprConstructor = c }
exprFlatten' mod e@ETyped{..} = do
    t <- typeFlatten mod exprTSpec
    return $ e { exprTSpec = t }
exprFlatten' _   e = return e 

mergeModules :: [DatalogModule] -> DatalogProgram
mergeModules mods =
    DatalogProgram {
        progTypedefs   = M.unions $ map (progTypedefs  . moduleDefs) mods,
        progFunctions  = M.unions $ map (progFunctions . moduleDefs) mods,
        progRelations  = M.unions $ map (progRelations . moduleDefs) mods,
        progRules      = concatMap (progRules . moduleDefs)          mods,
        progStatements = concatMap (progStatements . moduleDeds)     mods
    }

parseImports :: [FilePath] -> DatalogProgram -> StateT [ModuleName] (IO [DatalogModule])
parseImports roots prog = concat <$> 
    mapM (\imp@Import{..} -> do 
           exists <- gets $ elem importPath
           if exists
              then return []
              else parseImport roots imp)
         $ progImports prog

parseImport :: [FilePath] -> Import -> StateT [ModuleName] (IO [DatalogModule])
parseImport roots Import{..} = do
    modify $ \imports -> if elem importPath imports then imports else (importPath : imports)
    prog <- lift $ do fname <- findModule roots importPath
                      fdata <- readFile fname
                      parseDatalogString False fdata fname
    let mod = DatalogModule importPath prog
    imports <- parseImports prog
    return $ mod : imports

findModule :: [FilePath] -> ModuleName -> IO FilePath
findModule roots mod = do
    let fpath = addExtension (joinPath mod) ".dl"
    let modpath = intercalate "." mod
    let candidates = map (\r -> joinPath [r, path]) roots
    mods <- filterM isFile candidates
    case mods of
         [mod] -> return mod
         []    -> errorWithoutStackTrace $
                     "Module " ++ modpath ++ " imported by " ++ ++ 
                     " not found.Paths searched:\n" ++
                     (intercalate "\n" candidates)
         _     -> errorWithoutStackTrace $ 
                    "Found multiple candidates for module " ++ modpath ++ " imported by " ++ ++ ":\n" ++
                    (intercalate "\n" candidates)
