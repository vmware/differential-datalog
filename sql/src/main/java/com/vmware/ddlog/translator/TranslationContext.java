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

    static class Scope {
        final String rowVariable;
        final DDlogType type;

        Scope(String rowVariable, DDlogType type) {
            this.rowVariable = rowVariable;
            this.type = type;
        }

        /**
         * Lookup the specified identifier as a column name.
         * @param identifier  Identifier to look up.
         * @return            null if the identifier is not a field name.
         *                    A DDlogField expression of the row variable if the field is present.
         */
        @Nullable
        DDlogExpression lookupColumn(String identifier, TranslationContext context) {
            DDlogType type = this.type;
            while (type instanceof DDlogTUser) {
                type = context.resolveTypeDef((DDlogTUser)type);
            }
            if (!(type instanceof DDlogTStruct))
                return null;
            DDlogTStruct ts = (DDlogTStruct)type;
            for (DDlogField f: ts.getFields()) {
                if (identifier.equals(f.getName())) {
                    DDlogEVar var = new DDlogEVar(this.rowVariable, type);
                    return new DDlogEField(var, identifier, f.getType());
                }
            }
            return null;
        }
    }

    @Nullable
    private DDlogType resolveTypeDef(DDlogTUser type) {
        for (DDlogTypeDef t: this.program.typedefs) {
            if (t.getName().equals(type.getName()))
                return t.getType();
        }
        return null;
    }

    private final List<Scope> translationScope;

    TranslationContext() {
        this.program = new DDlogProgram();
        this.program.imports.add(new DDlogImport(Arrays.asList("sql"),
                new ArrayList<String>()));
        this.relations = new HashMap<String, DDlogRelation>();
        this.etv = new ExpressionTranslationVisitor();
        this.translationScope = new ArrayList<Scope>();
    }

    @Nullable
    public DDlogExpression lookupColumn(String identifier) {
        for (int i = 0; i < this.translationScope.size(); i++) {
            // Look starting from the end.
            int index = this.translationScope.size() - 1 - i;
            Scope scope = this.translationScope.get(index);
            @Nullable
            DDlogExpression expr = scope.lookupColumn(identifier, this);
            if (expr != null) return expr;
        }
        return null;
    }

    void enterScope(Scope scope) {
        this.translationScope.add(scope);
    }

    private void exitAllScopes() {
        this.translationScope.clear();
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
