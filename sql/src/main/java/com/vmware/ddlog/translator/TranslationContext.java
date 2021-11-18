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
import com.vmware.ddlog.translator.environment.EnvHandle;
import com.vmware.ddlog.util.Linq;
import com.vmware.ddlog.util.Utilities;

import javax.annotation.Nullable;
import java.util.*;

/**
 * This class is used to maintain information during the translation from SQL to DDlog.
 */
class TranslationContext {
    /**
     * If a Node (representing a SQL expression) is mapped to an expression
     * then the expression translation will return directly the expression
     * instead of performing the translation proper.
     */
    private final HashMap<Node, DDlogExpression> substitutions;
    private final CompilerState compilerState;
    // True if the view that is being compiled should produce an output relation.
    public boolean viewIsOutput;
    public EnvHandle environment;

    private TranslationContext(@Nullable CompilerState state) {
        this.viewIsOutput = true;
        this.environment = new EnvHandle();
        this.substitutions = new HashMap<Node, DDlogExpression>();
        if (state != null)
            this.compilerState = state;
        else
            this.compilerState = new CompilerState();
    }

    public TranslationContext() {
        this(null);
    }

    // Note: does not clone the environment.
    public TranslationContext cloneCtxt() {
        TranslationContext result = new TranslationContext(this.compilerState);
        Utilities.copyMap(result.substitutions, this.substitutions);
        result.viewIsOutput = this.viewIsOutput;
        return result;
    }

    public void mergeWith(TranslationContext other) {
        if (this.compilerState != other.compilerState)
            throw new RuntimeException("Merging contexts with different state");
        for (Map.Entry<Node, DDlogExpression> e: other.substitutions.entrySet())
            this.addSubstitution(e.getKey(), e.getValue());
        this.relationAlias.addAll(other.relationAlias);
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

    @Nullable
    public String originalColumnName(SingleColumn sc) {
        ColumnNameVisitor ecn = new ColumnNameVisitor();
        return ecn.process(sc.getExpression());
    }

    public String columnName(SingleColumn sc) {
        String name;
        if (sc.getAlias().isPresent()) {
            name = sc.getAlias().get().getValue().toLowerCase();
        } else {
            name = this.originalColumnName(sc);
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
                if (!t0j.equals(red)) {
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
                    if (!f.equals(e)) {
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
            DDlogType result = this.compilerState.resolveTypeDef(tu);
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

    DDlogExpression translateExpression(Expression expr) {
        return this.compilerState.etv.process(expr, this);
    }

    void add(DDlogRelationDeclaration relation) {
        this.compilerState.add(relation);
    }

    void add(DDlogIndexDeclaration index) {
        this.compilerState.add(index);
    }

    RelationName freshRelationName(String suggested) {
        String name = this.popAlias(suggested);
        String legalName = name;
        if (!RelationName.isValid(name))
            legalName = RelationName.makeRelationName(name);
        return new RelationName(this.freshGlobalName(legalName), name, null);
    }

    String freshGlobalName(String prefix) {
        return this.compilerState.freshGlobalName(prefix);
    }

    String freshLocalName(String prefix) {
        return this.compilerState.freshLocalName(prefix);
    }

    void beginTranslation() {
        this.compilerState.beginTranslation();
        this.viewIsOutput = true;
    }

    void endTranslation() {
        this.environment.exitAllScopes();
    }

    void add(DDlogRule rule) {
        this.compilerState.add(rule);
    }

    void add(DDlogTypeDef tdef) {
        this.compilerState.add(tdef);
    }

    @Nullable
    DDlogRelationDeclaration getRelation(RelationName name) {
        return this.compilerState.getRelation(name);
    }

    DDlogProgram getProgram() {
        return this.compilerState.getProgram();
    }

    void reserveGlobalName(String name) {
        this.compilerState.globalSymbols.addName(name);
    }

    // Keep track of relations renamed explicitly by the user
    /**
     *     Stack aliases encountered while parsing the current query
     */
    List<String> relationAlias = new ArrayList<>();
    @Nullable
    String lastRelationName = null;

    public void pushAlias(String name) {
        this.relationAlias.add(name);
    }

    public String popAlias(String suggested) {
        if (this.relationAlias.size() == 0)
            return suggested;
        this.lastRelationName = this.relationAlias.remove(this.relationAlias.size() - 1);
        return this.lastRelationName;
    }

    @Nullable
    public String lastRelationName() {
        // Selecting from a relation creates an anonymous relation which can be referred to
        // by the same name as the select source.
        return this.lastRelationName;
    }

    @Override
    public String toString() {
        return this.environment.toString();
    }

    public void exitAllScopes() {
        this.environment.exitAllScopes();
        this.relationAlias.clear();
        this.lastRelationName = null;
    }
}
