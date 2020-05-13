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
    private final HashMap<String, DDlogRelationDeclaration> relations;
    private final ExpressionTranslationVisitor etv;
    /**
     * If true we resolve identifiers to scopes, not to values within a scope.
     */
    private boolean searchScopeName;
    private final List<Scope> translationScope;
    /**
     * If a Node (representing a SQL expression) is mapped to an expression
     * then the expression translation will return directly the expression
     * instead of performing the translation proper.
     */
    private final HashMap<Node, DDlogExpression> substitutions;
    /**
     * Global to the whole program.
     */
    public SymbolTable globalSymbols;
    /**
     * Local to a query.
     */
    @Nullable
    private SymbolTable localSymbols;

    // True if the view that is being compiled should produce an output relation.
    public boolean viewIsOutput;

    TranslationContext() {
        this.viewIsOutput = true;
        this.substitutions = new HashMap<Node, DDlogExpression>();
        this.program = new DDlogProgram();
        this.program.imports.add(new DDlogImport("fp", ""));
        this.program.imports.add(new DDlogImport("time", ""));
        this.program.imports.add(new DDlogImport("sql", ""));
        this.program.imports.add(new DDlogImport("sqlop", ""));
        this.relations = new HashMap<String, DDlogRelationDeclaration>();
        this.etv = new ExpressionTranslationVisitor();
        this.translationScope = new ArrayList<Scope>();
        this.searchScopeName = false;
        this.localSymbols = null;
        this.globalSymbols = new SymbolTable();
    }

    public DDlogExpression operationCall(Node node, DDlogEBinOp.BOp op, DDlogExpression left, DDlogExpression right) {
        return ExpressionTranslationVisitor.operationCall(node, op, left, right);
    }

    static String location(Node node) {
        if (!node.getLocation().isPresent())
            return "";
        NodeLocation location = node.getLocation().get();
        return "line: " + location.getLineNumber() + " column: " + location.getColumnNumber();
    }

    public void addSubstitution(Node node, DDlogExpression expression) {
        DDlogExpression e = this.substitutions.get(node);
        if (e != null)
            System.out.println("Changing substitution of " + node + " from " + e + " to " + expression);
        this.substitutions.put(node, expression);
    }

    public void removeSubstitution(Node node) {
        this.substitutions.remove(node);
    }

    public void clearSubstitutions() {
        this.substitutions.clear();
    }

    @Nullable
    public DDlogExpression getSubstitution(Node node) {
        return this.substitutions.get(node);
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
                tu.error("Cannot resolve type " + tu.getName());
            assert result != null;
            return result;
        }
        return type;
    }

    DDlogTUser createTypedef(@Nullable Node node, DDlogTStruct type) {
        DDlogTypeDef tdef = new DDlogTypeDef(node, type.getName(), type);
        this.add(tdef);
        return new DDlogTUser(node, tdef.getName(), type.mayBeNull);
    }

    @Nullable
    DDlogExpression lookupIdentifier(String identifier) {
        for (int i = this.translationScope.size() - 1; i >= 0; i--) {
            // Look starting from the end.
            Scope scope = this.translationScope.get(i);
            if (this.searchScopeName) {
                if (identifier.equals(scope.scopeName))
                    return new DDlogScope(scope);
            } else {
                @Nullable
                DDlogExpression expr = scope.lookupColumn(scope.node, identifier, this);
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

    public Iterable<Scope> allScopes() { return this.translationScope; }

    DDlogExpression translateExpression(Expression expr) {
        return this.etv.process(expr, this);
    }

    void add(DDlogRelationDeclaration relation) {
        this.program.relations.add(relation);
        this.relations.put(relation.getName(), relation);
    }

    @SuppressWarnings("SameParameterValue")
    String freshGlobalName(String prefix) {
        return this.globalSymbols.freshName(prefix);
    }

    String freshLocalName(String prefix) {
        assert this.localSymbols != null;
        return this.localSymbols.freshName(prefix);
    }

    void beginTranslation() {
        this.localSymbols = new SymbolTable();
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
    DDlogRelationDeclaration getRelation(String name) {
        return this.relations.get(name);
    }

    DDlogProgram getProgram() {
        return this.program;
    }
}
