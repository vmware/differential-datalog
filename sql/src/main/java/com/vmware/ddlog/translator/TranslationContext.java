/*
 * Copyright (c) 2021 VMware, Inc.
 * SPDX-License-Identifier: MIT
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 *
 */

package com.vmware.ddlog.translator;

import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.Node;
import com.facebook.presto.sql.tree.NodeLocation;
import com.facebook.presto.sql.tree.SingleColumn;
import com.vmware.ddlog.ir.*;
import com.vmware.ddlog.util.Linq;
import com.vmware.ddlog.util.Utilities;

import javax.annotation.Nullable;
import java.util.*;

/**
 * This class is used to maintain information during the translation from SQL to DDlog.
 */
class TranslationContext {
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
    private final TranslationState translationState;
    // True if the view that is being compiled should produce an output relation.
    public boolean viewIsOutput;

    private TranslationContext(@Nullable TranslationState state) {
        this.viewIsOutput = true;
        this.substitutions = new HashMap<Node, DDlogExpression>();
        this.translationScope = new ArrayList<Scope>();
        this.searchScopeName = false;
        if (state != null)
            this.translationState = state;
        else
            this.translationState = new TranslationState();
    }

    public boolean contains(Scope scope) {
        for (Scope s: this.translationScope)
            if (s.id == scope.id)
                return true;
        return false;
    }

    public TranslationContext() {
        this(null);
    }

    @SuppressWarnings("MethodDoesntCallSuperMethod")
    public TranslationContext clone() {
        TranslationContext result = new TranslationContext(this.translationState);
        Utilities.copyMap(result.substitutions, this.substitutions);
        result.viewIsOutput = this.viewIsOutput;
        result.translationScope.addAll(this.translationScope);
        return result;
    }

    public void mergeWith(TranslationContext other) {
        if (this.translationState != other.translationState)
            throw new RuntimeException("Merging contexts with different state");
        for (Scope s: other.translationScope)
            if (!this.contains(s))
                this.translationScope.add(s);
        for (Map.Entry<Node, DDlogExpression> e: other.substitutions.entrySet())
            this.addSubstitution(e.getKey(), e.getValue());
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

    @SuppressWarnings("unused")
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

    public String columnName(SingleColumn sc) {
        String name;
        if (sc.getAlias().isPresent()) {
            name = sc.getAlias().get().getValue().toLowerCase();
        } else {
            ExpressionColumnName ecn = new ExpressionColumnName();
            name = ecn.process(sc.getExpression());
            if (name == null)
                name = this.freshLocalName("col");
        }
        return name;
    }

    /**
     * Given a set of struct types with the same number of fields,
     * find their "meet": a type where
     * all fields are defined by reduceType on the corresponding fields.
     */
    public DDlogType meet(List<DDlogType> types) {
        if (types.size() == 0)
            throw new RuntimeException("No types to meet");
        if (types.size() == 1)
            return types.get(0);
        types = Linq.map(types, this::resolveType);
        DDlogType type0 = types.get(0);
        DDlogTStruct str = type0.to(DDlogTStruct.class);
        List<DDlogField> fields = new ArrayList<DDlogField>(str.getFields());
        boolean changed = false;
        for (int i = 1; i < types.size(); i++) {
            DDlogTStruct stri = types.get(i).to(DDlogTStruct.class);
            List<DDlogField> striFields = stri.getFields();
            if (striFields.size() != fields.size())
                type0.error("Incompatible types: " + type0 + " and " + stri);
            for (int j = 0; j < fields.size(); j++) {
                DDlogField fj = fields.get(j);
                if (!fj.getName().equals(striFields.get(j).getName()))
                    type0.error("Incompatible types: " + type0 + " and " + stri);
                DDlogType t0j = fields.get(j).getType();
                DDlogType tij = striFields.get(j).getType();
                DDlogType red = DDlogType.reduceType(t0j, tij);
                if (!t0j.same(red)) {
                    changed = true;
                    fields.set(j, new DDlogField(fj.getNode(), fj.getName(), red));
                }
            }
        }
        if (changed)
            return this.createStruct(type0.getNode(), fields, str.getName());
        return type0;
    }

    public DDlogType meet(DDlogType... types) {
        return meet(Linq.list(types));
    }

    /**
     * Creates:
     * - a struct type with the specified fields.
     * - a typedef to refer to the struct
     * - a TUser that names the typedef; this is returned.
     * @param node    SQL node.
     * @param fields  Fields of the created struct.
     * @param suggestedName  If type does not exist it is created with this name.
     */
    public DDlogTUser createStruct(
            @Nullable Node node, List<DDlogField> fields, String suggestedName) {
        for (DDlogTypeDef td: this.getProgram().typedefs) {
            DDlogType type = td.getType();
            if (type == null)
                // extern type.
                continue;
            if (type.is(DDlogTStruct.class)) {
                DDlogTStruct strct = type.to(DDlogTStruct.class);
                if (strct.getFields().size() != fields.size())
                    continue;
                boolean different = false;
                for (int i = 0; i < strct.getFields().size(); i++) {
                    DDlogField f = strct.getFields().get(i);
                    DDlogField e = fields.get(i);
                    if (!f.same(e)) {
                        different = true;
                        break;
                    }
                }
                if (!different)
                    return new DDlogTUser(node, td.getName(), type.mayBeNull);
            }
        }

        String typeName = this.freshGlobalName(DDlogType.typeName(suggestedName));
        DDlogTStruct type = new DDlogTStruct(node, typeName, fields);
        return this.createTypedef(node, type);
    }

    DDlogType resolveType(DDlogType type) {
        if (type instanceof DDlogTUser) {
            DDlogTUser tu = (DDlogTUser)type;
            DDlogType result = this.translationState.resolveTypeDef(tu);
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

    public void exitAllScopes() {
        this.translationScope.clear();
    }

    void exitScope() {
        this.translationScope.remove(this.translationScope.size() - 1);
    }

    DDlogExpression translateExpression(Expression expr) {
        return this.translationState.etv.process(expr, this);
    }

    void add(DDlogRelationDeclaration relation) {
        this.translationState.add(relation);
    }

    void add(DDlogIndexDeclaration index) {
        this.translationState.add(index);
    }

    String freshRelationName(String prefix) {
        return this.freshGlobalName(DDlogRelationDeclaration.relationName(prefix));
    }

    String freshGlobalName(String prefix) {
        return this.translationState.freshGlobalName(prefix);
    }

    String freshLocalName(String prefix) {
        return this.translationState.freshLocalName(prefix);
    }

    void beginTranslation() {
        this.translationState.beginTranslation();
        this.viewIsOutput = true;
    }

    void endTranslation() {
        this.exitAllScopes();
    }

    void add(DDlogRule rule) {
        this.translationState.add(rule);
    }

    void add(DDlogTypeDef tdef) {
        this.translationState.add(tdef);
    }

    @Nullable
    DDlogRelationDeclaration getRelation(String name) {
        return this.translationState.getRelation(name);
    }

    DDlogProgram getProgram() {
        return this.translationState.getProgram();
    }

    void reserveGlobalName(String name) {
        this.translationState.globalSymbols.addName(name);
    }
}
