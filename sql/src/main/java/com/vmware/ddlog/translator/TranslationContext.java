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
import com.vmware.ddlog.ir.*;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

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

    TranslationContext() {
        this.program = new DDlogProgram();
        this.program.imports.add(new DDlogImport(Arrays.asList("sql"),
                new ArrayList<String>()));
        this.relations = new HashMap<String, DDlogRelation>();
        this.etv = new ExpressionTranslationVisitor();
        this.translationScope = new ArrayList<Scope>();
        this.searchScopeName = false;
    }

    @Nullable
    DDlogType resolveTypeDef(DDlogTUser type) {
        for (DDlogTypeDef t: this.program.typedefs) {
            if (t.getName().equals(type.getName()))
                return t.getType();
        }
        return null;
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
