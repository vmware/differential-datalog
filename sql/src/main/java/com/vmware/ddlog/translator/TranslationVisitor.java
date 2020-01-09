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
import com.vmware.ddlog.util.Ternary;

import javax.annotation.Nullable;
import java.util.*;

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

    private boolean isAggregate(Select select, TranslationContext context) {
        boolean foundAggregate = false;
        boolean foundNonAggregate = false;

        List<SelectItem> items = select.getSelectItems();
        for (SelectItem s: items) {
            if (s instanceof AllColumns) {
                foundNonAggregate = true;
            } else {
                SingleColumn sc = (SingleColumn)s;
                AggregateVisitor visitor = new AggregateVisitor();
                if (visitor.process(sc.getExpression(), context) == Ternary.Yes)
                    foundAggregate = true;
                else
                    foundNonAggregate = true;
            }
        }
        if (foundAggregate && foundNonAggregate)
            throw new TranslationException("SELECT with a mix of aggregates and non-aggregates.", select);
        return foundAggregate;
    }

    // Convert a select without aggregation.
    private DDlogIRNode processSimpleSelect(RelationRHS relation, Select select, TranslationContext context) {
        String outRelName = context.freshGlobalName("tmp");
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

        List<DDlogField> typeList = new ArrayList<DDlogField>();
        List<DDlogEStruct.FieldValue> exprList = new ArrayList<DDlogEStruct.FieldValue>();
        for (SelectItem s : items) {
            if (s instanceof SingleColumn) {
                SingleColumn sc = (SingleColumn) s;
                String name;
                if (sc.getAlias().isPresent())
                    name = sc.getAlias().get().getValue();
                else {
                    ExpressionColumnName ecn = new ExpressionColumnName();
                    name = ecn.process(sc.getExpression());
                    if (name == null)
                        name = context.freshLocalName("col");
                }

                Expression expression = sc.getExpression();
                DDlogExpression expr = context.translateExpression(expression);
                typeList.add(new DDlogField(name, expr.getType()));
                exprList.add(new DDlogEStruct.FieldValue(name, expr));
            } else {
                throw new TranslationException("Not yet implemented", s);
            }
        }

        String newTypeName = DDlogType.typeName(outRelName);
        DDlogTStruct type = new DDlogTStruct(newTypeName, typeList);
        DDlogType tuser = context.createTypedef(type);
        String var = context.freshLocalName("v");
        RelationRHS result = new RelationRHS(var, tuser);
        for (DDlogRuleRHS rhs: relation.getDefinitions())
            result.addDefinition(rhs);
        DDlogExpression project = new DDlogEStruct(newTypeName, exprList, tuser);
        DDlogExpression assignProject = new DDlogESet(
                result.getRowVariable(true),
                project);
        result = result.addDefinition(assignProject);
        DDlogRelation outRel = new DDlogRelation(DDlogRelation.RelationRole.RelInternal, outRelName, tuser);
        context.add(outRel);
        return result;
    }

    private DDlogIRNode processSelect(RelationRHS relation, Select select, TranslationContext context) {
        boolean isAgg = this.isAggregate(select, context);
        if (!select.isDistinct() && !isAgg)
            throw new TranslationException("Only SELECT DISTINCT currently supported", select);
        if (isAgg)
            return this.processSelectAggregate(relation, select, context);
        return this.processSimpleSelect(relation, select, context);
    }

    private DDlogType intermediateType(String aggregate, DDlogType aggregatedType) {
        switch (aggregate) {
            case "count":
            case "sum":
                return DDlogTSigned.signed64;
            case "min":
            case "max":
                return aggregatedType;
            case "avg":
                return new DDlogTTuple(
                        DDlogTSigned.signed64,  // sum
                        DDlogTSigned.signed64   // count
                );
            default:
                throw new RuntimeException("Unexpected aggregate: " + aggregate);
        }
    }

    private DDlogExpression aggregateIncrement(String aggregate, DDlogType resultType, DDlogEVar variable,
                                               DDlogExpression increment) {
        DDlogType incType = increment.getType();
        String funcName = "agg_" + aggregate + "_" + (incType.mayBeNull ? "N" : "R");
        return new DDlogEApply(funcName, resultType, variable, increment);
    }

    private DDlogExpression aggregateInitializer(String aggregate, DDlogType dataType,
                                                 @Nullable DDlogExpression initial) {
        switch (aggregate) {
            case "count":
                if (dataType.mayBeNull)
                    return new DDlogEApply("isNullAsInt", DDlogTSigned.signed64, initial);
                return new DDlogESigned(1);
            case "sum":
                return new DDlogESigned(0);
            case "min":
            case "max":
                assert initial != null;
                return initial;
            case "avg":
                DDlogExpression inc;
                DDlogExpression ct;
                if (dataType.mayBeNull) {
                    inc = new DDlogEApply("unwrapNull", DDlogTSigned.signed64, initial);
                    ct = new DDlogEApply("isNullAsInt", DDlogTSigned.signed64, initial);
                } else {
                    inc = initial;
                    ct = new DDlogESigned(1);
                }
                return new DDlogETuple(ct, inc);
            default:
                throw new RuntimeException("Unexpected aggregate: " + aggregate);
        }
    }

    private DDlogExpression aggregateComplete(String aggregate, DDlogExpression value) {
        if (!aggregate.equals("avg"))
            return value;
        // Average is obtained by dividing the sum by the count
        return new DDlogEApply("avg",
                DDlogTSigned.signed64, // TODO: should be double
                new DDlogETupField(value, 1), new DDlogETupField(value, 0));
    }

    private DDlogExpression zero(DDlogType type) {
        if (type instanceof DDlogTSigned)
            return new DDlogESigned(0);
        else if (type instanceof DDlogTString)
            return new DDlogEString("");
        else if (type instanceof DDlogTTuple)
            // average
            return new DDlogETuple(new DDlogESigned(0), new DDlogESigned(0));
        else
            throw new RuntimeException("Unexpected aggregate type: " + type);
    }

    private DDlogIRNode processSelectAggregate(RelationRHS relation, Select select, TranslationContext context) {
        String outRelName = context.freshGlobalName("tmp");
        DDlogExpression functionBody;
        DDlogExpression loopBody = null;
        String paramName = context.freshLocalName("g");

        // We will generate a custom function to perform the aggregation.
        // The parameter of the function is a Group<T> where T is
        // a tuple with all relations that are in scope.
        List<DDlogType> tupleFields = new ArrayList<DDlogType>();
        List<DDlogEVar> tupleVars = new ArrayList<DDlogEVar>();
        for (Scope s: context.allScopes()) {
            tupleFields.add(s.type);
            tupleVars.add(new DDlogEVar(s.rowVariable, s.type));
        }

        DDlogTTuple tuple = new DDlogTTuple(tupleFields.toArray(new DDlogType[0]));
        String iter = context.freshLocalName("i");  // loop iteration variable
        DDlogEVar iterVar = new DDlogEVar(iter, tuple);
        // The loop iteration variable will have the type tuple
        int index = 0;
        for (Scope s: context.allScopes()) {
            DDlogEVarDecl decl = new DDlogEVarDecl(s.rowVariable, s.type);
            DDlogExpression project;
            if (tuple.size() > 1) {
                project = new DDlogETupField(iterVar, index++);
            } else {
                project = iterVar;  // tuples with 1 element are not really tuples
            }
            DDlogESet set = new DDlogESet(decl, project);
            loopBody = DDlogESeq.seq(loopBody, set);
        }

        String agg = context.freshGlobalName("agg");
        DDlogTUser paramType = new DDlogTUser("Group", false, tuple);
        DDlogFuncArg param = new DDlogFuncArg(paramName, false, paramType);
        DDlogETuple callArg = new DDlogETuple(tupleVars.toArray(new DDlogExpression[0]));
        DDlogExpression init = new DDlogEBool(true);
        String first = context.freshLocalName("first");
        DDlogEVarDecl firstDecl = new DDlogEVarDecl(first, DDlogTBool.instance);
        DDlogEVar firstVar = new DDlogEVar(first, firstDecl.getType());
        functionBody = new DDlogESet(firstDecl, init);

        List<DDlogField> typeList = new ArrayList<DDlogField>();
        List<DDlogEStruct.FieldValue> exprList = new ArrayList<DDlogEStruct.FieldValue>();
        List<SelectItem> items = select.getSelectItems();
        for (SelectItem s : items) {
            if (s instanceof SingleColumn) {
                SingleColumn sc = (SingleColumn) s;
                String name;
                if (sc.getAlias().isPresent())
                    name = sc.getAlias().get().getValue();
                else {
                    ExpressionColumnName ecn = new ExpressionColumnName();
                    name = ecn.process(sc.getExpression());
                    if (name == null)
                        name = context.freshLocalName("col");
                }

                Expression expression = sc.getExpression();
                AggregateVisitor aggv = new AggregateVisitor();
                aggv.process(expression, context);
                AggregateVisitor.Decomposition decomposition = aggv.result;

                // For each aggregation function in the decomposition we generate a temporary
                for (FunctionCall f: decomposition.aggregateNodes) {
                    String aggregateFunction = f.getName().toString();
                    DDlogExpression increment;
                    if (f.getArguments().size() == 1) {
                        increment = context.translateExpression(f.getArguments().get(0));
                        // Save the result of incrementing in a variable so
                        // it is not evaluated twice
                        String incrVar = context.freshLocalName("incr");
                        DDlogEVarDecl incrVarDecl = new DDlogEVarDecl(incrVar, increment.getType());
                        DDlogESet set = new DDlogESet(incrVarDecl, increment);
                        loopBody = DDlogESeq.seq(loopBody, set);
                        increment = new DDlogEVar(incrVar, increment.getType());
                    } else if (f.getArguments().size() == 0) {
                        // This is the translation of COUNT(*)
                        increment = new DDlogESigned(1);
                    } else {
                        throw new TranslationException("Unexpected aggregate", f);
                    }
                    DDlogExpression unused = context.translateExpression(f);
                    DDlogType aggregatedType = unused.getType();
                    DDlogType intermediateType = this.intermediateType(aggregateFunction, aggregatedType);
                    String varName = context.freshLocalName(aggregateFunction);
                    DDlogExpression varDef = new DDlogEVarDecl(varName, intermediateType);
                    // Replace all occurrences of f with varName when translating later.
                    context.addSubstitution(f, this.aggregateComplete(
                            aggregateFunction, new DDlogEVar(varName, intermediateType)));
                    functionBody = DDlogESeq.seq(functionBody, new DDlogESet(varDef, this.zero(intermediateType)));
                    DDlogEVar tmpVar = new DDlogEVar(varName, unused.getType());
                    DDlogExpression cond = new DDlogEITE(firstVar,
                            this.aggregateInitializer(aggregateFunction, increment.getType(), increment),
                            this.aggregateIncrement(aggregateFunction, intermediateType, tmpVar, increment));
                    DDlogExpression newVal = new DDlogESet(tmpVar, cond);
                    loopBody = DDlogESeq.seq(loopBody, newVal);
                }
                DDlogExpression expr = context.translateExpression(expression);
                typeList.add(new DDlogField(name, expr.getType()));
                exprList.add(new DDlogEStruct.FieldValue(name, expr));
                context.clearSubstitutions();
            } else {
                throw new TranslationException("Not yet implemented", s);
            }
        }

        loopBody = DDlogESeq.seq(loopBody, new DDlogESet(firstVar, new DDlogEBool(false)));
        String newTypeName = DDlogType.typeName(outRelName);
        DDlogTStruct type = new DDlogTStruct(newTypeName, typeList);
        DDlogType tuser = context.createTypedef(type);
        String var = context.freshLocalName("v");
        RelationRHS result = new RelationRHS(var, tuser);
        for (DDlogRuleRHS rhs: relation.getDefinitions())
            result.addDefinition(rhs);

        assert loopBody != null;
        DDlogEFor forLoop = new DDlogEFor(iter, new DDlogEVar(paramName, paramType), loopBody);
        functionBody = DDlogESeq.seq(functionBody, forLoop);
        DDlogRHSAggregate aggregate = new DDlogRHSAggregate(result.getVarName(), agg, callArg);
        result.addDefinition(aggregate);
        DDlogExpression project = new DDlogEStruct(newTypeName, exprList, tuser);
        functionBody = DDlogESeq.seq(functionBody, project);
        DDlogFunction func = new DDlogFunction(agg, tuser, functionBody, param);
        context.getProgram().functions.add(func);

        DDlogRelation outRel = new DDlogRelation(DDlogRelation.RelationRole.RelInternal, outRelName, tuser);
        context.add(outRel);
        return result;
    }

    @Override
    protected DDlogIRNode visitQuerySpecification(QuerySpecification spec, TranslationContext context) {
        if (spec.getLimit().isPresent())
            throw new TranslationException("LIMIT clauses not supported", spec);
        if (spec.getOrderBy().isPresent())
            throw new TranslationException("ORDER BY clauses not supported", spec);
        if (!spec.getFrom().isPresent())
            throw new TranslationException("FROM clause is required", spec);
        if (spec.getGroupBy().isPresent())
            throw new TranslationException("Not yet handled", spec);
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
                throw new TranslationException("Not yet implemented", join);
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
        DDlogType type = null;
        if (sqltype.equals("boolean")) {
            type = DDlogTBool.instance;
        } else if (sqltype.equals("integer")) {
            type = DDlogTSigned.signed64;
        } else if (sqltype.startsWith("varchar")) {
            type = DDlogTString.instance;
        } else if (sqltype.equals("bigint")) {
            type = DDlogTInt.instance;
        }
        if (type == null)
            throw new RuntimeException("SQL type not yet implemented: " + sqltype);
        return type.setMayBeNull(mayBeNull);
    }
}
