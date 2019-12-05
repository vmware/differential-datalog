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
import com.vmware.ddlog.util.Linq;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

class TranslationVisitor extends AstVisitor<DDlogIRNode, TranslationContext> {
    static String convertQualifiedName(QualifiedName name) {
        return String.join(".", name.getParts());
    }

    @Override
    protected DDlogIRNode visitCreateTable(CreateTable node, TranslationContext context) {
        String name = convertQualifiedName(node.getName());

        List<TableElement> elements = node.getElements();
        List<DDlogField> fields = new ArrayList<DDlogField>();
        for (TableElement te: elements) {
            DDlogIRNode field = this.process(te, context);
            fields.add(field.as(DDlogField.class, null));
        }
        DDlogTStruct type = new DDlogTStruct(DDlogType.typeName(name), fields);
        DDlogTUser tuser = context.createTypedef(type);
        DDlogRelation rel = new DDlogRelation(
                DDlogRelation.RelationRole.RelInput, name, tuser);
        context.add(rel);
        return rel;
    }

    @Override
    protected DDlogIRNode visitTableSubquery(TableSubquery query, TranslationContext context) {
        DDlogIRNode subquery = this.process(query.getQuery(), context);
        RelationRHS relation = subquery.as(RelationRHS.class, null);

        String relName = context.freshGlobalName("tmp");
        DDlogRelation rel = new DDlogRelation(
                DDlogRelation.RelationRole.RelInternal, relName, relation.getType());
        context.add(rel);
        String lhsVar = context.freshLocalName("v");
        DDlogExpression varExpr = new DDlogEVar(lhsVar, relation.getType());
        List<DDlogRuleRHS> rhs = relation.getDefinitions();
        DDlogESet set = new DDlogESet(new DDlogEVarDecl(lhsVar, relation.getType()),
                relation.getRowVariable(false));
        rhs.add(new DDlogRHSCondition(set));
        DDlogAtom lhs = new DDlogAtom(relName, varExpr);
        DDlogRule rule = new DDlogRule(lhs, rhs);
        context.add(rule);
        RelationRHS result = new RelationRHS(lhsVar, relation.getType());
        result.addDefinition(new DDlogRHSLiteral(true, new DDlogAtom(rel.getName(), varExpr)));
        Scope scope = new Scope(relName, lhsVar, rel.getType());
        context.enterScope(scope);
        return result;
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
    protected DDlogIRNode visitAliasedRelation(
            AliasedRelation relation, TranslationContext context) {
        DDlogIRNode rel = this.process(relation.getRelation(), context);
        String name = relation.getAlias().getValue();
        RelationRHS rrhs = rel.as(RelationRHS.class, null);
        Scope scope = new Scope(name, rrhs.getVarName(), rrhs.getType());
        context.enterScope(scope);
        return rrhs;
    }

    @Override
    protected DDlogIRNode visitTable(Table table, TranslationContext context) {
        String name = convertQualifiedName(table.getName());
        DDlogRelation relation = context.getRelation(name);
        if (relation == null)
            throw new TranslationException("Could not find relation", table);
        String var = context.freshLocalName("v");
        DDlogType type = relation.getType();
        Scope scope = new Scope(name, var, type);
        context.enterScope(scope);
        RelationRHS result = new RelationRHS(var, type);
        result.addDefinition(new DDlogRHSLiteral(
                true, new DDlogAtom(relation.getName(), result.getRowVariable(false))));
        return result;
    }

    private DDlogIRNode processSelect(RelationRHS relation, Select select, TranslationContext context) {
        if (!select.isDistinct())
            throw new TranslationException("Only SELECT DISTINCT currently supported", select);

        // Special case for SELECT *
        List<SelectItem> items = select.getSelectItems();
        if (items.size() == 1) {
            SelectItem single = items.get(0);
            if (single instanceof AllColumns) {
                AllColumns all = (AllColumns)single;
                if (!all.getPrefix().isPresent())
                    return relation;
            }
        }
        String outRelName = context.freshGlobalName("tmp");
        List<DDlogField> typeList = new ArrayList<DDlogField>();
        List<DDlogEStruct.FieldValue> exprList = new ArrayList<DDlogEStruct.FieldValue>();
        for (SelectItem s: items) {
            if (s instanceof AllColumns) {
                AllColumns ac = (AllColumns)s;
                // TODO
            } else {
                SingleColumn sc = (SingleColumn)s;
                String name;
                if (sc.getAlias().isPresent())
                    name = sc.getAlias().get().getValue();
                else {
                    ExpressionColumnName ecn = new ExpressionColumnName();
                    name = ecn.process(sc.getExpression());
                    if (name == null)
                        name = context.freshLocalName("col");
                }
                DDlogExpression expr = context.translateExpression(sc.getExpression());
                typeList.add(new DDlogField(name, expr.getType()));
                exprList.add(new DDlogEStruct.FieldValue(name, expr));
            }
        }
        String newTypeName = DDlogType.typeName(outRelName);
        DDlogTStruct type = new DDlogTStruct(newTypeName, typeList);
        DDlogType tuser = context.createTypedef(type);
        DDlogExpression project = new DDlogEStruct(newTypeName, exprList, tuser);
        DDlogRelation outRel = new DDlogRelation(DDlogRelation.RelationRole.RelInternal, outRelName, tuser);
        context.add(outRel);

        String var = context.freshLocalName("v");
        RelationRHS result = new RelationRHS(var, tuser);
        for (DDlogRuleRHS rhs: relation.getDefinitions())
            result.addDefinition(rhs);
        DDlogExpression assignProject = new DDlogESet(
                result.getRowVariable(true),
                project);
        return result.addDefinition(assignProject);
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
        if (source == null)
            throw new TranslationException("Not yet handled", spec);
        RelationRHS relation = source.as(RelationRHS.class, null);
        if (spec.getWhere().isPresent()) {
            Expression expr = spec.getWhere().get();
            DDlogExpression ddexpr = context.translateExpression(expr);
            ddexpr = ExpressionTranslationVisitor.unwrapBool(ddexpr);
            relation = relation.addDefinition(ddexpr);
        }

        Select select = spec.getSelect();
        return this.processSelect(relation, select, context);
    }

    @Override
    protected DDlogIRNode visitCreateView(CreateView view, TranslationContext context) {
        String name = convertQualifiedName(view.getName());
        DDlogIRNode query = this.process(view.getQuery(), context);
        RelationRHS rel = query.as(RelationRHS.class, null);
        DDlogRelation out = new DDlogRelation(
                DDlogRelation.RelationRole.RelOutput, name, rel.getType());

        String outVarName = context.freshLocalName("v");
        DDlogExpression outRowVarDecl = new DDlogEVarDecl(outVarName, rel.getType());
        DDlogExpression inRowVar = rel.getRowVariable(false);
        List<DDlogRuleRHS> rhs = rel.getDefinitions();
        DDlogESet set = new DDlogESet(outRowVarDecl, inRowVar);
        rhs.add(new DDlogRHSCondition(set));
        DDlogAtom lhs = new DDlogAtom(name, new DDlogEVar(outVarName, out.getType()));
        DDlogRule rule = new DDlogRule(lhs, rhs);
        context.add(out);
        context.add(rule);
        return rule;
    }

    /**
     * Generate a fresh name that is not in the set.
     * @param prefix  Prefix used to generate the name.
     * @param used    Set of names that cannot be used.
     * @return        The generated name.  The name is added to the used set.
     */
    public static String freshName(String prefix, Set<String> used) {
        if (!used.contains(prefix)) {
            used.add(prefix);
            return prefix;
        }
        for (int i = 0; ; i++) {
            String candidate = prefix + i;
            if (!used.contains(candidate)) {
                used.add(candidate);
                return candidate;
            }
        }
    }

    @Override
    public DDlogIRNode visitJoin(Join join, TranslationContext context) {
        DDlogIRNode left = this.process(join.getLeft(), context);
        DDlogIRNode right = this.process(join.getRight(), context);
        RelationRHS lrel = left.as(RelationRHS.class, null);
        RelationRHS rrel = right.as(RelationRHS.class, null);
        String var = context.freshLocalName("v");
        DDlogType ltype = context.resolveType(lrel.getType());
        DDlogType rtype = context.resolveType(rrel.getType());
        DDlogTStruct lst = ltype.as(DDlogTStruct.class, null);
        DDlogTStruct rst = rtype.as(DDlogTStruct.class, null);
        List<DDlogRuleRHS> rules = new ArrayList<DDlogRuleRHS>();
        rules.addAll(lrel.getDefinitions());
        rules.addAll(rrel.getDefinitions());

        Set<String> joinColumns = new HashSet<String>();
        switch (join.getType()) {
            case INNER:
                if (join.getCriteria().isPresent()) {
                    JoinCriteria c = join.getCriteria().get();
                    if (c instanceof JoinOn) {
                        JoinOn on = (JoinOn)c;
                        DDlogExpression onE = context.translateExpression(on.getExpression());
                        rules.add(new DDlogRHSCondition(ExpressionTranslationVisitor.unwrapBool(onE)));
                    } else if (c instanceof JoinUsing) {
                        JoinUsing using = (JoinUsing)c;
                        joinColumns = new HashSet<String>(Linq.map(using.getColumns(), Identifier::getValue));
                    } else if (c instanceof NaturalJoin) {
                        joinColumns = new HashSet<String>(Linq.map(lst.getFields(), DDlogField::getName));
                        HashSet<String> rightCols = new HashSet<String>(Linq.map(rst.getFields(), DDlogField::getName));
                        joinColumns.retainAll(rightCols);
                    } else {
                        throw new TranslationException("Unexpected join", join);
                    }
                }
                break;
            case CROSS:
            case IMPLICIT:
                // Nothing more to do
                break;
            case LEFT:
            case RIGHT:
            case FULL:
                // TODO
            default:
                throw new TranslationException("Unexpected join type", join);
        }

        DDlogExpression condition = new DDlogEBool(true);
        for (String col: joinColumns) {
            DDlogExpression e = context.operationCall(DDlogEBinOp.BOp.Eq,
                    new DDlogEField(lrel.getRowVariable(false), col, lst.getFieldType(col)),
                    new DDlogEField(rrel.getRowVariable(false), col, rst.getFieldType(col)));
            condition = context.operationCall(DDlogEBinOp.BOp.And, condition, e);
        }
        rules.add(new DDlogRHSCondition(ExpressionTranslationVisitor.unwrapBool(condition)));

        // For the result we take all fields from the left and right but we skip
        // the joinColumn fields from the right.
        String tmp = DDlogType.typeName(context.freshGlobalName("tmp"));
        List<DDlogField> tfields = new ArrayList<DDlogField>();
        List<DDlogEStruct.FieldValue> fields = new ArrayList<DDlogEStruct.FieldValue>();

        Set<String> names = new HashSet<String>();
        for (DDlogField f: lst.getFields()) {
            tfields.add(f);
            names.add(f.getName());
            fields.add(new DDlogEStruct.FieldValue(f.getName(),
                    new DDlogEField(lrel.getRowVariable(false), f.getName(), f.getType())));
        }
        for (DDlogField f: rst.getFields()) {
            if (joinColumns.contains(f.getName()))
                continue;
            // We may need to rename this field if it is already present
            String name = freshName(f.getName(), names);
            DDlogField field = new DDlogField(name, f.getType());
            tfields.add(field);
            fields.add(new DDlogEStruct.FieldValue(field.getName(),
                    new DDlogEField(rrel.getRowVariable(false), f.getName(), f.getType())));
        }
        DDlogTStruct type = new DDlogTStruct(tmp, tfields);
        DDlogTUser tuser = context.createTypedef(type);
        DDlogExpression e = new DDlogESet(
                new DDlogEVarDecl(var, type),
                new DDlogEStruct(tmp, fields, type));
        rules.add(new DDlogRHSCondition(e));

        RelationRHS result = new RelationRHS(var, tuser);
        for (DDlogRuleRHS r: rules)
            result.addDefinition(r);
        return result;
    }

    @Override
    protected DDlogIRNode visitColumnDefinition(ColumnDefinition definition, TranslationContext context) {
        String name = definition.getName().getValue();
        String type = definition.getType();
        DDlogType ddtype = createType(type, definition.isNullable());
        return new DDlogField(name, ddtype);
    }

    private static DDlogType createType(String sqltype, boolean mayBeNull) {
        if (sqltype.equals("boolean")) {
            if (mayBeNull)
                return DDlogTBool.instanceWNull;
            return DDlogTBool.instance;
        } else if (sqltype.equals("integer")) {
            return new DDlogTSigned(64, mayBeNull);
        } else if (sqltype.startsWith("varchar")) {
            if (mayBeNull)
                return DDlogTString.instanceWNull;
            return DDlogTString.instance;
        } else if (sqltype.equals("bigint")) {
            if (mayBeNull)
                return DDlogTInt.instanceWNull;
            return DDlogTInt.instance;
        }
        throw new RuntimeException("SQL type not yet implemented: " + sqltype);
    }
}
