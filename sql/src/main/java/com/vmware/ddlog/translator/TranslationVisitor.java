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
    static class GroupByInfo {
        /**
         * Original SQL expression.
         */
        public final Expression groupBy;
        /**
         * DDlog translation.
         */
        public final DDlogExpression translation;
        /**
         * Variable name created for aggregation function.
         */
        public String varName;
        @Nullable
        public String fieldName;

        public GroupByInfo(Expression e, DDlogExpression translation, String varName) {
            this.groupBy = e;
            this.translation = translation;
            this.varName = varName;
            this.fieldName = null;
        }

        public DDlogExpression getVariable() {
            return new DDlogEVar(this.varName, this.translation.getType());
        }
    }

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
        String typeName = context.freshGlobalName(DDlogType.typeName(name));
        DDlogTStruct type = new DDlogTStruct(typeName, fields);
        DDlogTUser tuser = context.createTypedef(type);
        String relName = DDlogRelation.relationName(name);
        context.globalSymbols.addName(relName);
        DDlogRelation rel = new DDlogRelation(
                DDlogRelation.RelationRole.RelInput, relName, tuser);
        context.add(rel);
        return rel;
    }

    @Override
    protected DDlogIRNode visitTableSubquery(TableSubquery query, TranslationContext context) {
        DDlogIRNode subquery = this.process(query.getQuery(), context);
        RelationRHS relation = subquery.as(RelationRHS.class, null);

        String relName = context.freshGlobalName(DDlogRelation.relationName("tmp"));
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
        DDlogRelation relation = context.getRelation(DDlogRelation.relationName(name));
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

    /**
     * Check if the select expression requires an aggregate.
     * @param select   Select computed.
     * @param groupBy  Expressions that are grouped-by.
     * @param context  Translation context.
     * @return         True if the select requires aggregation.  It should when groupBy is not empty.
     */
    private boolean isAggregate(Select select, List<GroupByInfo> groupBy, TranslationContext context) {
        boolean foundAggregate = false;
        boolean foundNonAggregate = false;

        List<SelectItem> items = select.getSelectItems();
        AggregateVisitor visitor = new AggregateVisitor(groupBy);
        for (SelectItem s: items) {
            if (s instanceof AllColumns) {
                foundNonAggregate = true;
            } else {
                SingleColumn sc = (SingleColumn)s;
                Expression e = sc.getExpression();
                if (visitor.process(e, context) == Ternary.Yes) {
                    //System.out.println("Aggregate: " + e);
                    foundAggregate = true;
                } else {
                    //System.out.println("Nonaggregate: " + e);
                    foundNonAggregate = true;
                }
            }
        }
        //System.out.println("-----");
        if (foundAggregate && foundNonAggregate)
            throw new TranslationException("SELECT with a mix of aggregates and non-aggregates.", select);
        return foundAggregate;
    }

    /**
     * Convert a select without aggregation.
     */
    private DDlogIRNode processSimpleSelect(RelationRHS relation, Select select,
                                            TranslationContext context) {
        String outRelName = context.freshGlobalName(DDlogRelation.relationName("tmp"));
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

        String newTypeName = context.freshGlobalName(DDlogType.typeName(outRelName));
        DDlogTStruct type = new DDlogTStruct(newTypeName, typeList);
        DDlogType tuser = context.createTypedef(type);
        String var = context.freshLocalName("v");
        RelationRHS result = new RelationRHS(var, tuser);
        for (DDlogRuleRHS rhs: relation.getDefinitions())
            result.addDefinition(rhs);
        DDlogExpression project = new DDlogEStruct(newTypeName, tuser, exprList);
        DDlogExpression assignProject = new DDlogESet(
                result.getRowVariable(true),
                project);
        result = result.addDefinition(assignProject);
        DDlogRelation outRel = new DDlogRelation(DDlogRelation.RelationRole.RelInternal, outRelName, tuser);
        context.add(outRel);
        return result;
    }

    private DDlogIRNode processSelect(RelationRHS relation, Select select,
                                      List<GroupByInfo> groupBy,
                                      @Nullable Expression having,
                                      TranslationContext context) {
        boolean isAgg = this.isAggregate(select, groupBy, context);
        if (!select.isDistinct() && !isAgg)
            throw new TranslationException("Only SELECT DISTINCT currently supported", select);
        if (isAgg)
            return this.processSelectAggregate(relation, select, groupBy, having, context);
        if (groupBy.size() > 0)
            throw new TranslationException("Select without aggregation with GROUP BY", select);
        return this.processSimpleSelect(relation, select, context);
    }

    /**
     * The type of an intermediate expression produced by an aggregate.
     * @param aggregate       SQL aggregate function name.
     * @param aggregatedType  The type of the result produced.
     * @return                The DDlog type.
     */
    private DDlogType intermediateType(String aggregate, DDlogType aggregatedType) {
        switch (aggregate) {
            case "count":
                return DDlogTSigned.signed64.setMayBeNull(aggregatedType.mayBeNull);
            case "sum_distinct":
            case "count_distinct":
                return new DDlogTUser("Set", false, aggregatedType);
            case "any":
            case "some":
            case "every":
            case "sum":
                return aggregatedType;
            case "min":
            case "max":
                return new DDlogTTuple(
                        DDlogTBool.instance, // first
                        aggregatedType       // value
                );
            case "avg":
            case "avg_distinct":
                return new DDlogTTuple(
                        aggregatedType,  // sum
                        aggregatedType   // count
                ).setMayBeNull(aggregatedType.mayBeNull);
            default:
                throw new RuntimeException("Unexpected aggregate: " + aggregate);
        }
    }

    /**
     * An expression that increments the current aggregate value.
     * @param aggregate    SQL aggregate function name.
     * @param resultType   Type of result produced by increment.
     * @param variable     Variable that is incremented.
     * @param increment    Increment value.
     */
    private DDlogExpression aggregateIncrement(String aggregate, DDlogType resultType, DDlogEVar variable,
                                               DDlogExpression increment) {
        DDlogType incType = increment.getType();
        switch (aggregate) {
            case "count_distinct":
            case "sum_distinct":
                String insertFunc = increment.getType().mayBeNull ? "insert_non_null" : "set_insert";
                return new DDlogEApply(insertFunc, DDlogTTuple.emptyTupleType, variable, increment);
            case "sum":
            case "avg":
                aggregate += "_" + (incType.is(DDlogTSigned.class) ? "signed" : "int");
                break;
            default:
                break;
        }
        String funcName = "agg_" + aggregate + "_" + (incType.mayBeNull ? "N" : "R");
        DDlogExpression add = new DDlogEApply(funcName, resultType, variable, increment);
        return new DDlogESet(variable, add);
    }

    /**
     * An expression that initializes an aggregate.
     * @param f          SQL IR function - for reporting error message.
     * @param aggregate  SQL aggregate function name
     * @param dataType   Type of the data that is being aggregated (not the type of the aggregate).
     */
    private DDlogExpression aggregateInitializer(FunctionCall f, String aggregate, DDlogType dataType) {
        boolean isSigned = dataType instanceof DDlogTSigned;
        boolean isInt = dataType instanceof DDlogTInt;
        DDlogExpression zero = isSigned ? new DDlogESigned(0) : new DDlogEInt(0);
        DDlogExpression none = dataType.getNone();

        switch (aggregate) {
            case "any":
            case "some":
                return new DDlogEBool(false);
            case "every":
                return new DDlogEBool(true);
            case "sum":
                if (dataType.mayBeNull)
                    return none;
                if (!(isSigned || isInt))
                    throw new TranslationException("sum not supported for type " + dataType.toString(), f);
                return zero;
            case "count":
                if (dataType.mayBeNull)
                    return none;
                return new DDlogESigned(0);
            case "min":
            case "max":
                if (dataType.mayBeNull)
                    return none;
                DDlogExpression t = new DDlogEBool(true);
                if (dataType instanceof DDlogTString)
                    return new DDlogETuple(t, new DDlogEString(""));
                assert (isSigned || isInt);
                return new DDlogETuple(t, zero);
            case "avg":
                DDlogETuple zt = new DDlogETuple(zero, zero);
                if (dataType.mayBeNull)
                    return zt.getType().getNone();
                return zt;
            case "count_distinct":
            case "sum_distinct":
                DDlogType setType = new DDlogTUser("Set", false, dataType.setMayBeNull(false));
                return new DDlogEApply("set_empty", setType);
            default:
                throw new RuntimeException("Unexpected aggregate: " + aggregate);
        }
    }

    private DDlogExpression aggregateComplete(String aggregate, DDlogExpression value) {
        switch (aggregate) {
            case "avg": {
                String suffix;
                DDlogType argType = value.getType();
                DDlogTTuple tuple = argType.as(DDlogTTuple.class, "Expected a tuple");
                DDlogType sumType = tuple.component(0);
                if (argType.mayBeNull) {
                    suffix = "N";
                } else {
                    suffix = "R";
                }
                String type = sumType.is(DDlogTSigned.class) ? "signed" : "int";
                return new DDlogEApply("avg_" + type + "_" + suffix, sumType, value);
            }
            case "min":
            case "max":
                return new DDlogETupField(value, 1);
            case "count_distinct": {
                DDlogTUser set = value.getType().as(DDlogTUser.class, "expected a set");
                return new DDlogEAs(
                        new DDlogEApply("set_size", set.getTypeArg(0), value),
                        DDlogTSigned.signed64);
            }
            case "sum_distinct": {
                DDlogTUser set = value.getType().as(DDlogTUser.class, "expected a set");
                DDlogType elemType = set.getTypeArg(0);
                String type = elemType.is(DDlogTSigned.class) ? "signed" : "int";
                return new DDlogEApply("set_" + type + "_sum", elemType, value);
            }
            default:
                return value;
        }
    }

    /**
     * This class bundles state that is constructed while translating
     * Selects.  This makes it easy to pass this state around to
     * helper functions.
     */
    static class SelectTranslationState {
        /**
         * SQL Expressions that show up in the Select statement.
         */
        List<Expression> selectExpressions = new ArrayList<Expression>();
        /**
         * List of expressions that we are grouping by.
         */
        List<GroupByInfo> groupBy;
        /**
         * List of fields of the resulting struct produced by the
         * aggregation function.
         */
        List<DDlogField> resultTypeFields = new ArrayList<DDlogField>();
        /**
         * Type of input to aggregation function.
         */
        List<DDlogField> functionTypeFields = new ArrayList<DDlogField>();
        /**
         * Fields returned by the aggregation function.
         */
        List<DDlogEStruct.FieldValue> functionResultFields = new ArrayList<DDlogEStruct.FieldValue>();
        /**
         * Loop in the aggregation function.
         */
        @Nullable
        DDlogExpression loopBody;
        /**
         * Aggregation function body.
         */
        @Nullable
        DDlogExpression functionBody;

        public SelectTranslationState(List<GroupByInfo> groupBy) {
            this.loopBody = null;
            this.functionBody = null;
            this.groupBy = groupBy;
        }

        public void addLoopStatement(DDlogExpression expr) {
            this.loopBody = DDlogESeq.seq(this.loopBody, expr);
        }

        public void addFunctionStatement(DDlogExpression expr) {
            this.functionBody = DDlogESeq.seq(this.functionBody, expr);
        }
    }

    private void
    processSelectExpression(Expression expression,
                            boolean inHaving,
                            @Nullable String name,
                            SelectTranslationState state,
                            TranslationContext context) {
        if (name == null)
            name = context.freshLocalName("col");
        if (!inHaving)
            state.selectExpressions.add(expression);
        boolean found = false;
        for (GroupByInfo a: state.groupBy) {
            if (expression.equals(a.groupBy)) {
                state.resultTypeFields.add(new DDlogField(name, a.translation.getType()));
                a.fieldName = name;
                found = true;
                break;
            }
        }
        if (found)
            return;

        AggregateVisitor aggv = new AggregateVisitor(state.groupBy);
        aggv.process(expression, context);
        AggregateVisitor.Decomposition decomposition = aggv.decomposition;

        // For each aggregation function in the decomposition we generate a temporary
        for (FunctionCall f: decomposition.aggregateNodes) {
            if (context.getSubstitution(f) != null)
                // We already have a translation for this expression
                continue;

            String aggregateFunction = ExpressionTranslationVisitor.functionName(f);
            DDlogExpression increment;
            if (f.getArguments().size() == 1) {
                increment = context.translateExpression(f.getArguments().get(0));
                String incrVar = context.freshLocalName("incr");
                DDlogEVarDecl incrVarDecl = new DDlogEVarDecl(incrVar, increment.getType());
                DDlogESet set = new DDlogESet(incrVarDecl, increment);
                state.addLoopStatement(set);
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
            String aggVarName = context.freshLocalName(aggregateFunction);
            DDlogExpression aggVarDef = new DDlogEVarDecl(aggVarName, intermediateType);
            // Replace all occurrences of f with varName when translating later.
            context.addSubstitution(f, this.aggregateComplete(
                    aggregateFunction, new DDlogEVar(aggVarName, intermediateType)));
            state.addFunctionStatement(new DDlogESet(
                    aggVarDef, this.aggregateInitializer(f, aggregateFunction, increment.getType()),
                    true));
            DDlogEVar aggVar = new DDlogEVar(aggVarName, aggregatedType);
            DDlogExpression inc = this.aggregateIncrement(
                    aggregateFunction, intermediateType, aggVar, increment);
            state.addLoopStatement(inc);
        }
        DDlogExpression expr = context.translateExpression(expression);
        DDlogField field = new DDlogField(name, expr.getType());
        state.functionTypeFields.add(field);
        if (!inHaving) {
            state.resultTypeFields.add(field);
        }
        state.functionResultFields.add(new DDlogEStruct.FieldValue(name, expr));
    }

    private DDlogIRNode processSelectAggregate(RelationRHS relation, Select select,
                                               List<GroupByInfo> groupBy,
                                               @Nullable
                                               Expression having,
                                               TranslationContext context) {
        /*
            General structure of an aggregation in DDlog is:

            R[v1] :- R[v], ..., var gb1 = ..., var aggResult = Aggregate( (gb1, gb2, ...), agg(v)), var v1 = v.

            where

            function agg(g: Group< (Tk1, Tk2), (ValueTuple) >: <AggregateType> =
            (var gb1, var gb2, ...) = group_key(g);
            <initialize_aggregate>
            for (i in g) {
                var v = i;
                <increment aggregate>
             }
             result = <complete aggregate>(gb1, gb2, v)
         */
        SelectTranslationState state = new SelectTranslationState(groupBy);
        String outRelName = context.freshGlobalName(DDlogRelation.relationName("tmp"));
        String paramName = context.freshLocalName("g");

        // We will generate a custom function to perform the aggregation.
        // The parameter of the function is a Group<K, T> where K is the key type
        // and T is a tuple with all relations that are in scope.
        List<DDlogType> keyFields = Linq.map(groupBy, g -> g.translation.getType());
        DDlogTTuple keyType = new DDlogTTuple(keyFields);
        if (keyFields.size() > 0) {
            List<DDlogExpression> keyVars =
                    Linq.map(groupBy, g -> new DDlogEVarDecl(g.varName, g.translation.getType()));
            DDlogESet getKeys = new DDlogESet(new DDlogETuple(keyVars),
                    new DDlogEApply("group_key", keyType, new DDlogEVar("g", keyType)));
            state.addFunctionStatement(getKeys);
        }

        List<DDlogType> tupleFields = new ArrayList<DDlogType>();
        List<DDlogEVar> tupleVars = new ArrayList<DDlogEVar>();
        for (Scope s: context.allScopes()) {
            tupleFields.add(s.type);
            tupleVars.add(new DDlogEVar(s.rowVariable, s.type));
        }
        for (GroupByInfo g: groupBy)
            context.addSubstitution(g.groupBy, g.getVariable());

        DDlogTTuple tuple = new DDlogTTuple(tupleFields);
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
            state.addLoopStatement(set);
        }

        String agg = context.freshGlobalName("agg");
        DDlogTUser paramType = new DDlogTUser("Group", false, keyType, tuple);
        DDlogFuncArg param = new DDlogFuncArg(paramName, false, paramType);
        DDlogETuple callArg = new DDlogETuple(tupleVars.toArray(new DDlogExpression[0]));

        List<SelectItem> items = select.getSelectItems();
        for (SelectItem s : items) {
            if (!(s instanceof SingleColumn)) {
                throw new TranslationException("Not yet implemented", s);
            }

            SingleColumn sc = (SingleColumn) s;
            String name;
            if (sc.getAlias().isPresent())
                name = sc.getAlias().get().getValue();
            else {
                ExpressionColumnName ecn = new ExpressionColumnName();
                name = ecn.process(sc.getExpression());
            }

            Expression expression = sc.getExpression();
            this.processSelectExpression(expression, false, name, state, context);
        }

        // The HAVING clause can also require new aggregates besides the ones present
        // in the SELECT, so we treat it as an additional expression in SELECT
        if (having != null && context.getSubstitution(having) == null)
            this.processSelectExpression(having, true, null, state, context);

        context.clearSubstitutions();
        String newTypeName = context.freshGlobalName(DDlogType.typeName(outRelName));
        DDlogTStruct resultType = new DDlogTStruct(newTypeName, state.resultTypeFields);
        DDlogTUser tUserResult = context.createTypedef(resultType);

        DDlogTUser tUserFunction;
        String funcTypeName;
        if (state.resultTypeFields.size() == state.functionTypeFields.size()) {
            funcTypeName = newTypeName;
            tUserFunction = tUserResult;
        } else {
            funcTypeName = context.freshGlobalName(DDlogType.typeName(agg));
            DDlogTStruct functionType = new DDlogTStruct(funcTypeName, state.functionTypeFields);
            tUserFunction = context.createTypedef(functionType);
        }
        String var = context.freshLocalName("v");
        RelationRHS result = new RelationRHS(var, tUserResult);
        for (DDlogRuleRHS rhs: relation.getDefinitions())
            result.addDefinition(rhs);

        // For each expression that we group by add a new temporary variable
        List<String> groupByVars = new ArrayList<String>();
        for (GroupByInfo g : groupBy) {
            DDlogESet groupByVarDef = new DDlogESet(
                    new DDlogEVarDecl(g.varName, g.translation.getType()), g.translation);
            result.addDefinition(groupByVarDef);
            groupByVars.add(g.varName);
            context.addSubstitution(g.groupBy, g.getVariable());
        }

        assert state.loopBody != null;
        DDlogEFor forLoop = new DDlogEFor(iter, new DDlogEVar(paramName, paramType), state.loopBody);
        state.addFunctionStatement(forLoop);
        String[] vars = groupByVars.toArray(new String[0]);
        String aggregateVarName = context.freshLocalName("aggResult");
        DDlogRHSAggregate aggregate = new DDlogRHSAggregate(aggregateVarName, agg, callArg, vars);
        result.addDefinition(aggregate);
        DDlogEStruct project = new DDlogEStruct(funcTypeName, tUserResult, state.functionResultFields);
        state.addFunctionStatement(project);
        DDlogFunction func = new DDlogFunction(agg, tUserFunction, state.functionBody, param);
        context.getProgram().functions.add(func);

        DDlogRelation outRel = new DDlogRelation(DDlogRelation.RelationRole.RelInternal, outRelName, tUserResult);
        context.add(outRel);

        DDlogExpression copy;
        if (groupBy.size() != 0) {
            // The final result copies all the aggregated groupby fields and all fields computed by the function
            List<DDlogEStruct.FieldValue> fields = new ArrayList<DDlogEStruct.FieldValue>();
            for (GroupByInfo gr : groupBy) {
                if (gr.fieldName != null)
                    fields.add(new DDlogEStruct.FieldValue(gr.fieldName,
                            new DDlogEVar(gr.varName, gr.translation.getType())));
            }

            int selectIndex = 0;
            for (DDlogEStruct.FieldValue field: project.fields) {
                DDlogEVar aggVar = new DDlogEVar(aggregateVarName, tUserFunction);
                DDlogEField projField = new DDlogEField(aggVar, field.getName(), field.getValue().getType());
                if (selectIndex < state.selectExpressions.size()) {
                    fields.add(new DDlogEStruct.FieldValue(field.getName(), projField));
                    Expression selExpression = state.selectExpressions.get(selectIndex);
                    context.addSubstitution(selExpression, projField);
                } else {
                    // This field must have been produced by the HAVING expression
                    assert having != null;
                    assert selectIndex == state.selectExpressions.size();
                    context.addSubstitution(having, projField);
                }
                selectIndex++;
            }
            DDlogExpression resultFields = new DDlogEStruct(tUserResult.getName(), tUserResult, fields);
            copy = new DDlogESet(result.getRowVariable(true), resultFields);
        } else {
            copy = new DDlogESet(result.getRowVariable(true), new DDlogEVar(aggregateVarName, tUserFunction));
        }

        result.addDefinition(copy);
        if (having != null) {
            DDlogExpression hav = context.translateExpression(having);
            result.addDefinition(hav);
        }
        context.clearSubstitutions();
        return result;
    }

    public void processGroupBy(GroupBy group, TranslationContext context, List<GroupByInfo> result) {
        for (GroupingElement ge: group.getGroupingElements()) {
            if (ge instanceof SimpleGroupBy) {
                SimpleGroupBy sgb = (SimpleGroupBy)ge;
                for (Expression e: sgb.getExpressions()) {
                    DDlogExpression g = context.translateExpression(e);
                    String gb = context.freshLocalName("gb");
                    GroupByInfo gr = new GroupByInfo(e, g, gb);
                    result.add(gr);
                }
            } else {
                throw new TranslationException("Not yet supported", group);
            }
        }
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
        List<GroupByInfo> groupBy = new ArrayList<GroupByInfo>();
        if (spec.getGroupBy().isPresent()) {
            GroupBy gb = spec.getGroupBy().get();
            this.processGroupBy(gb, context, groupBy);
        }

        @Nullable Expression having = null;
        if (spec.getHaving().isPresent())
            having = spec.getHaving().get();

        Select select = spec.getSelect();
        return this.processSelect(relation, select, groupBy, having, context);
    }

    @Override
    protected DDlogIRNode visitCreateView(CreateView view, TranslationContext context) {
        String name = convertQualifiedName(view.getName());
        DDlogIRNode query = this.process(view.getQuery(), context);
        RelationRHS rel = query.as(RelationRHS.class, null);
        String relName = DDlogRelation.relationName(name);
        context.globalSymbols.addName(relName);
        DDlogRelation out = new DDlogRelation(
                DDlogRelation.RelationRole.RelOutput, relName, rel.getType());

        String outVarName = context.freshLocalName("v");
        DDlogExpression outRowVarDecl = new DDlogEVarDecl(outVarName, rel.getType());
        DDlogExpression inRowVar = rel.getRowVariable(false);
        List<DDlogRuleRHS> rhs = rel.getDefinitions();
        DDlogESet set = new DDlogESet(outRowVarDecl, inRowVar);
        rhs.add(new DDlogRHSCondition(set));
        DDlogAtom lhs = new DDlogAtom(relName, new DDlogEVar(outVarName, out.getType()));
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
        String tmp = context.freshGlobalName(DDlogType.typeName("tmp"));
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
                new DDlogEStruct(tmp, type, fields));
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
        DDlogType ddtype = SqlSemantics.createType(type, definition.isNullable());
        return new DDlogField(name, ddtype);
    }
}
