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

import com.facebook.presto.sql.tree.*;
import com.vmware.ddlog.ir.*;

import java.util.ArrayList;
import java.util.List;

class TranslationVisitor extends AstVisitor<DDlogIRNode, TranslationContext> {
    private static String convertQualifiedName(QualifiedName name) {
        return String.join(".", name.getParts());
    }

    @Override
    protected DDlogIRNode visitCreateTable(CreateTable node, TranslationContext context) {
        String name = convertQualifiedName(node.getName());

        List<TableElement> elements = node.getElements();
        List<DDlogField> fields = new ArrayList<DDlogField>();
        for (TableElement te: elements) {
            DDlogIRNode field = this.process(te, context);
            if (field instanceof DDlogField)
                fields.add((DDlogField)field);
            else
                throw new TranslationException("Unexpected table element", te);
        }
        DDlogConstructor cons = new DDlogConstructor(name, fields);
        DDlogTStruct type = new DDlogTStruct(cons);
        DDlogRelation rel = new DDlogRelation(
                DDlogRelation.RelationRole.RelInput, name, type);
        context.add(rel);
        return rel;
    }

    @Override
    protected DDlogIRNode visitQuery(Query query, TranslationContext context) {
        if (query.getLimit().isPresent())
            throw new TranslationException("LIMIT clauses not supported", query);
        if (query.getOrderBy().isPresent())
            throw new TranslationException("ORDER BY clauses not supported", query);
        if (query.getWith().isPresent())
            throw new TranslationException("WITH clauses not supported", query);
        return this.process(query.getQueryBody(), context);
    }

    @Override
    protected DDlogIRNode visitSelect(Select select, TranslationContext context) {
        if (!select.isDistinct())
            throw new TranslationException("Only SELECT DISTINCT currently supported", select);
        List<SelectItem> items = select.getSelectItems();
        for (SelectItem i: items) {

        }
        return null;  // TODO
    }

    @Override
    protected DDlogIRNode visitTable(Table table, TranslationContext context) {
        String name = convertQualifiedName(table.getName());
        DDlogRelation node = context.getRelation(name);
        if (node == null)
            throw new TranslationException("Could not find relation", table);
        String var = context.freshName("v");
        return new DDlogTempRelation(var, node.getType(), node, null);
    }

    @Override
    protected DDlogIRNode visitQuerySpecification(QuerySpecification spec, TranslationContext context) {
        if (spec.getLimit().isPresent())
            throw new TranslationException("LIMIT clauses not supported", spec);
        if (spec.getOrderBy().isPresent())
            throw new TranslationException("ORDER BY clauses not supported", spec);
        if (!spec.getFrom().isPresent())
            throw new TranslationException("FROM clause is required", spec);
        DDlogIRNode source = this.process(spec.getFrom().get(), context);
        if (!(source instanceof DDlogTempRelation))
            throw new RuntimeException("Translating query specification did not produce a relation");
        DDlogTempRelation relation = (DDlogTempRelation)source;
        if (spec.getWhere().isPresent()) {
            Expression expr = spec.getWhere().get();
            DDlogExpression ddexpr = context.translateExpression(expr);
            relation = new DDlogTempRelation(relation.getRowVariable(), relation.getType(),
                    relation.getSource(), ddexpr);
        }
        return relation;
    }

    @Override
    protected DDlogIRNode visitUnion(Union union, TranslationContext context) {
        for (Relation r: union.getRelations()) {
            DDlogIRNode rel = this.process(r, context);
            String tmp = context.freshName("tmp");
            DDlogType type = null;  // TODO
            DDlogRelation tmprel = new DDlogRelation(
                    DDlogRelation.RelationRole.RelInternal, tmp, type);
            context.add(tmprel);
        }
        return null;  // TODO
    }

    @Override
    protected DDlogIRNode visitCreateView(CreateView view, TranslationContext context) {
        String name = convertQualifiedName(view.getName());
        DDlogIRNode query = process(view.getQuery(), context);
        if (!(query instanceof DDlogTempRelation))
            throw new TranslationException("Unexpected query translation", view);

        DDlogTempRelation rel = (DDlogTempRelation)query;
        DDlogRelation out = new DDlogRelation(
                DDlogRelation.RelationRole.RelOutput, name, rel.getType());

        DDlogExpression varExpr = new DDlogEVar(rel.getRowVariable());
        DDlogAtom rhsatom = new DDlogAtom(rel.getSource().getName(), varExpr);
        List<DDlogRuleRHS> rhs = new ArrayList<DDlogRuleRHS>();
        DDlogRHSLiteral lit = new DDlogRHSLiteral(true, rhsatom);
        rhs.add(lit);
        if (rel.getCondition() != null) {
            DDlogRHSCondition cond = new DDlogRHSCondition(rel.getCondition());
            rhs.add(cond);
        }
        DDlogAtom lhs = new DDlogAtom(name, varExpr);
        DDlogRule rule = new DDlogRule(lhs, rhs);
        context.add(out);
        return rule;
    }

    @Override
    protected DDlogIRNode visitColumnDefinition(ColumnDefinition definition, TranslationContext context) {
        String name = definition.getName().getValue();
        String type = definition.getType();
        DDlogType ddtype = createType(type);
        return new DDlogField(name, ddtype);
    }

    private static DDlogType createType(String sqltype) {
        if (sqltype.equals("boolean"))
            return DDlogTBool.instance;
        else if (sqltype.equals("integer"))
            return new DDlogTSigned(64);
        else if (sqltype.startsWith("varchar"))
            return new DDlogTString();
        throw new RuntimeException("SQL type not yet implemented: " + sqltype);
    }
}
