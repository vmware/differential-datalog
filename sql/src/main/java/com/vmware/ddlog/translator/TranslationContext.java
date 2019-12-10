/*
 * Copyright (c) 2019 VMware Inc. All Rights Reserved.
 * SPDX-License-Identifier: MIT
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the "Software"), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice (including the next paragraph) shall be included in all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 */

package com.vmware.ddlog.translator;

import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.Node;
import com.facebook.presto.sql.tree.NodeLocation;
import com.vmware.ddlog.ir.*;

import javax.annotation.Nullable;
import java.util.*;

/**
 * This class is used to maintain information during the translation from SQL to DDlog.
 */
class TranslationContext {
    private final DDlogProgram program;
    private final HashMap<String, DDlogRelation> relations;
    private final ExpressionTranslationVisitor etv;
    /**
     * If true we resolve identifiers to scopes, not to values within a scope.
     */
    private boolean searchScopeName;
    private final List<Scope> translationScope;

    private final HashMap<String, DDlogEBinOp.BOp> arithmeticFunctions;
    private final HashMap<String, DDlogEBinOp.BOp> stringFunctions;
    private final HashMap<String, DDlogEBinOp.BOp> booleanFunctions;

    TranslationContext() {
        this.program = new DDlogProgram();
        this.program.imports.add(new DDlogImport("sql", ""));
        this.program.imports.add(new DDlogImport("sqlop", ""));
        this.relations = new HashMap<String, DDlogRelation>();
        this.etv = new ExpressionTranslationVisitor();
        this.translationScope = new ArrayList<Scope>();
        this.searchScopeName = false;
        this.arithmeticFunctions = new HashMap<String, DDlogEBinOp.BOp>();
        this.stringFunctions = new HashMap<String, DDlogEBinOp.BOp>();
        this.booleanFunctions = new HashMap<String, DDlogEBinOp.BOp>();
        this.initializeMaps();
    }

    public DDlogExpression operationCall(DDlogEBinOp.BOp op, DDlogExpression left, DDlogExpression right) {
        return this.etv.operationCall(op, left, right, this);
    }

    String getFunction(DDlogEBinOp.BOp op, DDlogType ltype, @Nullable DDlogType rtype) {
        HashMap<String, DDlogEBinOp.BOp> map;
        if (ltype.as(DDlogTBool.class) != null) {
            map = this.booleanFunctions;
        } else if (ltype.as(DDlogTInt.class) != null ||
            ltype.as(DDlogTBit.class) != null ||
            ltype.as(DDlogTSigned.class) != null) {
            map = this.arithmeticFunctions;
        } else {
            map = this.stringFunctions;
        }

        String suffixl = ltype.mayBeNull ? "N" : "R";
        String suffixr = rtype == null ? "" : (rtype.mayBeNull ? "N" : "R");
        for (String k: map.keySet()) {
            if (map.get(k).equals(op)) {
                return k + "_" + suffixl + suffixr;
            }
        }
        throw new RuntimeException("Could not find `" + op + "` for type " + ltype.toString());
    }

    private void initializeMaps() {
        this.arithmeticFunctions.put("a_eq", DDlogEBinOp.BOp.Eq);
        this.arithmeticFunctions.put("a_neq", DDlogEBinOp.BOp.Neq);
        this.arithmeticFunctions.put("a_lt", DDlogEBinOp.BOp.Lt);
        this.arithmeticFunctions.put("a_gt", DDlogEBinOp.BOp.Gt);
        this.arithmeticFunctions.put("a_lte", DDlogEBinOp.BOp.Lte);
        this.arithmeticFunctions.put("a_gte", DDlogEBinOp.BOp.Gte);
        this.arithmeticFunctions.put("a_plus", DDlogEBinOp.BOp.Plus);
        this.arithmeticFunctions.put("a_minus", DDlogEBinOp.BOp.Minus);
        this.arithmeticFunctions.put("a_mod", DDlogEBinOp.BOp.Mod);
        this.arithmeticFunctions.put("a_times", DDlogEBinOp.BOp.Times);
        this.arithmeticFunctions.put("a_div", DDlogEBinOp.BOp.Div);
        // The following don't work for 64 bits
        // this.arithmeticFunctions.put("a_shiftr", DDlogEBinOp.BOp.ShiftR);
        // this.arithmeticFunctions.put("a_shiftl", DDlogEBinOp.BOp.ShiftL);
        this.arithmeticFunctions.put("a_band", DDlogEBinOp.BOp.BAnd);
        this.arithmeticFunctions.put("a_bor", DDlogEBinOp.BOp.BOr);
        this.arithmeticFunctions.put("a_bxor", DDlogEBinOp.BOp.BXor);

        this.stringFunctions.put("s_concat", DDlogEBinOp.BOp.Concat);
        this.stringFunctions.put("s_eq", DDlogEBinOp.BOp.Eq);
        this.stringFunctions.put("s_neq", DDlogEBinOp.BOp.Neq);

        this.booleanFunctions.put("b_eq", DDlogEBinOp.BOp.Eq);
        this.booleanFunctions.put("b_neq", DDlogEBinOp.BOp.Neq);
        this.booleanFunctions.put("b_and", DDlogEBinOp.BOp.And);
        this.booleanFunctions.put("b_or", DDlogEBinOp.BOp.Or);
        this.booleanFunctions.put("b_impl", DDlogEBinOp.BOp.Impl);
    }

    /**
     * Generates a library with many functions that operate on Option values.
     */
    DDlogProgram generateLibrary() {
        DDlogProgram result = new DDlogProgram();
        for (HashMap<String, DDlogEBinOp.BOp> h: Arrays.asList(
            this.arithmeticFunctions, this.booleanFunctions, this.stringFunctions)) {
            for (String f : h.keySet()) {
                DDlogEBinOp.BOp op = h.get(f);
                for (int i = 0; i < 4; i++) {
                    String function = f + "_";
                    DDlogType leftType;
                    DDlogType rightType;
                    DDlogType raw;
                    DDlogType withNull;
                    if (h.equals(this.stringFunctions)) {
                        raw = DDlogTString.instance;
                    } else if (op.isBoolean()) {
                        raw = DDlogTBool.instance;
                    } else {
                        raw = new DDlogTSigned(64, false);
                    }
                    withNull = raw.setMayBeNull(true);
                    DDlogExpression leftMatch = new DDlogEVarDecl("l", raw);
                    DDlogExpression rightMatch = new DDlogEVarDecl("r", raw);
                    if ((i & 1) == 1) {
                        function += "N";
                        leftType = withNull;
                        leftMatch = new DDlogEStruct("Some", Collections.singletonList(
                            new DDlogEStruct.FieldValue("x",
                                leftMatch)), leftType);
                    } else {
                        function += "R";
                        leftType = raw;
                    }
                    if ((i & 2) == 2) {
                        function += "N";
                        rightType = withNull;
                        rightMatch = new DDlogEStruct("Some", Collections.singletonList(
                            new DDlogEStruct.FieldValue("x", rightMatch)), rightType);
                    } else {
                        function += "R";
                        rightType = raw;
                    }
                    /*
                    function add(left: Option<bigint>, right: Option<bigint>): Option<bigint> =
                    match ((left, right)) {
                        (Some{a}, Some{b}) -> Some{a + b},
                        (_, _)             -> None
                    }
                    */
                    DDlogFuncArg left = new DDlogFuncArg("left", false, leftType);
                    DDlogFuncArg right = new DDlogFuncArg("right", false, rightType);
                    DDlogType type = DDlogType.reduceType(leftType, rightType);
                    if (op.isComparison()) {
                        type = DDlogTBool.instance.setMayBeNull(type.mayBeNull);
                    }
                    DDlogExpression def;
                    if (i == 0) {
                        def = new DDlogEBinOp(op,
                            new DDlogEVar("left", raw), new DDlogEVar("right", raw));
                    } else {
                        def = new DDlogEMatch(
                            new DDlogETuple(
                                new DDlogEVar("left", leftType),
                                new DDlogEVar("right", rightType)),
                            Arrays.asList(new DDlogEMatch.Case(
                                    new DDlogETuple(leftMatch, rightMatch),
                                    new DDlogEStruct("Some", Collections.singletonList(
                                        new DDlogEStruct.FieldValue("x",
                                            new DDlogEBinOp(op,
                                                new DDlogEVar("l", raw), new DDlogEVar("r", raw)))
                                    ), type)),
                                new DDlogEMatch.Case(
                                    new DDlogETuple(
                                        new DDlogEPHolder(),
                                        new DDlogEPHolder()),
                                    new DDlogEStruct("None", Collections.emptyList(), type)))
                        );
                    }
                    DDlogFunction func = new DDlogFunction(
                        function, Arrays.asList(left, right), type, def);
                    result.functions.add(func);
                }
            }
        }
        return result;
    }

    static String location(Node node) {
        if (!node.getLocation().isPresent())
            return "";
        NodeLocation location = node.getLocation().get();
        return "line: " + location.getLineNumber() + " column: " + location.getColumnNumber();
    }

    void warning(String message, Node node) {
        System.err.println(message + ": " + node.toString() + " " + location(node));
    }

    @Nullable
    DDlogType resolveTypeDef(DDlogTUser type) {
        for (DDlogTypeDef t: this.program.typedefs) {
            if (t.getName().equals(type.getName()))
                return t.getType();
        }
        return null;
    }

    DDlogType resolveType(DDlogType type) {
        if (type instanceof DDlogTUser) {
            DDlogTUser tu = (DDlogTUser)type;
            DDlogType result = this.resolveTypeDef(tu);
            if (result == null)
                throw new RuntimeException("Cannot resolve type " + tu.getName());
            return result;
        }
        return type;
    }

    DDlogTUser createTypedef(DDlogTStruct type) {
        DDlogTypeDef tdef = new DDlogTypeDef(type.getName(), type);
        this.add(tdef);
        return new DDlogTUser(tdef.getName(), type.mayBeNull);
    }

    @Nullable
    DDlogExpression lookupIdentifier(String identifier) {
        for (int i = 0; i < this.translationScope.size(); i++) {
            // Look starting from the end.
            int index = this.translationScope.size() - 1 - i;
            Scope scope = this.translationScope.get(index);
            if (this.searchScopeName) {
                if (identifier.equals(scope.scopeName))
                    return new DDlogScope(scope);
            } else {
                @Nullable
                DDlogExpression expr = scope.lookupColumn(identifier, this);
                if (expr != null) return expr;
            }
        }
        return null;
    }

    void enterScope(Scope scope) {
        this.translationScope.add(scope);
    }

    void searchScope(boolean yes) {
        this.searchScopeName = yes;
    }

    private void exitAllScopes() {
        this.translationScope.clear();
    }

    void exitScope() {
        this.translationScope.remove(this.translationScope.size() - 1);
    }

    DDlogExpression translateExpression(Expression expr) {
        return this.etv.process(expr, this);
    }

    void add(DDlogRelation relation) {
        this.program.relations.add(relation);
        this.relations.put(relation.getName(), relation);
    }

    private int globalCounter = 0;
    @SuppressWarnings("SameParameterValue")
    String freshGlobalName(String prefix) {
        // TODO: maintain a real symbol table
        return prefix + this.globalCounter++;
    }

    private int localCounter = 0;
    String freshLocalName(String prefix) {
        // TODO: maintain a real symbol table
        return prefix + this.localCounter++;
    }

    void beginTranslation() {
        this.localCounter = 0;
    }

    void endTranslation() {
        this.exitAllScopes();
    }

    void add(DDlogRule rule) {
        this.program.rules.add(rule);
    }

    void add(DDlogTypeDef tdef) {
        this.program.typedefs.add(tdef);
    }

    @Nullable
    DDlogRelation getRelation(String name) {
        return this.relations.get(name);
    }

    DDlogProgram getProgram() {
        return this.program;
    }
}
