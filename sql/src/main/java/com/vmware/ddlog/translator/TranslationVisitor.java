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

import com.facebook.presto.sql.SqlFormatter;
import com.facebook.presto.sql.tree.*;
import com.vmware.ddlog.ir.*;
import com.vmware.ddlog.translator.environment.RelationEnvironent;
import com.vmware.ddlog.util.Linq;
import com.vmware.ddlog.util.Ternary;
import com.vmware.ddlog.util.Utilities;

import javax.annotation.Nullable;
import java.util.*;

import static com.facebook.presto.sql.tree.Join.Type.LEFT;

class TranslationVisitor extends AstVisitor<DDlogIRNode, TranslationContext> {
    static boolean debug = false;

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
        /*
         * Each field that we group on could be used multiple times.
         * Consider SELECT column2 as a, column2 as b FROM t GROUP BY column2
         */
        public List<String> fieldNames;

        public GroupByInfo(Expression e, DDlogExpression translation, String varName) {
            this.groupBy = e;
            this.translation = translation;
            this.varName = varName;
            this.fieldNames = new ArrayList<String>();
        }

        public DDlogExpression getVariable() {
            return new DDlogEVar(this.groupBy, this.varName, this.translation.getType());
        }

        @Override
        public String toString() {
            return "GroupBy: " + this.groupBy + " as " + this.varName;
        }
    }

    static String toString(Node node) {
        return SqlFormatter.formatSql(node, Optional.empty()).replace("\n", " ");
    }

    public static void setDebug(boolean debug) {
        TranslationVisitor.debug = debug;
    }

    @Override
    protected DDlogIRNode visitCreateTable(CreateTable node, TranslationContext context) {
        String name = Utilities.convertQualifiedName(node.getName());
        RelationName relName = context.freshRelationName(name);
        List<TableElement> elements = node.getElements();
        List<DDlogField> fields = new ArrayList<DDlogField>();
        List<DDlogField> keyColumns = new ArrayList<DDlogField>();
        for (TableElement te: elements) {
            DDlogIRNode f = this.process(te, context);
            DDlogField field = f.to(DDlogField.class);
            fields.add(field);
            if (te instanceof ColumnDefinition) {
                ColumnDefinition cd = (ColumnDefinition)te;
                if (Linq.any(cd.getProperties(), p -> p.getName().getValue().equals("primary_key")))
                    keyColumns.add(field);
            }
        }
        DDlogTUser tuser = context.createStruct(node, fields, relName.name);
        DDlogRelationDeclaration rel = new DDlogRelationDeclaration(
                node, DDlogRelationDeclaration.Role.Input, relName, tuser);
        if (keyColumns.size() > 0) {
            // This type name will not appear in the generated program
            rel = rel.setPrimaryKey(keyColumns, context.freshLocalName("TKey"));
        }
        context.add(rel, name);
        return rel;
    }

    // The rules we synthesize have a relatively fixed syntax Rule[variable] :- ...
    // This function extracts the variable on the on the lhs of the rule.
    protected static DDlogEVar getRuleVar(DDlogRule rule) {
        return rule.head.val.to(DDlogEVar.class);
    }

    /**
     * Translate a query that produces a table.
     * @param query   Query to translate
     * @param context Translation context.
     * @return        A RuleBody whose row variable represents all rows in the table query.
     */
    @Override
    protected DDlogIRNode visitTableSubquery(TableSubquery query, TranslationContext context) {
        DDlogIRNode subquery = this.process(query.getQuery(), context);
        RuleBody body = subquery.to(RuleBody.class);
        RelationName relName = context.freshRelationName("tmp");
        DDlogRule rule = context.createRule(query, null, relName, body, DDlogRelationDeclaration.Role.Internal);
        String lhsVar = getRuleVar(rule).var;
        RuleBody result = new RuleBody(query, lhsVar, body.getType());
        result.addDefinition(new BodyTermLiteral(query, true, new DDlogAtom(query, relName, rule.head.val)));
        DDlogTStruct type = context.resolveType(rule.head.val.getType()).to(DDlogTStruct.class);
        RelationEnvironent scope = new RelationEnvironent(query, relName.name, lhsVar, type);
        context.environment.stack(scope);
        context.environment.renameDown(body.getVarName(), lhsVar);
        String lastRel = context.lastRelationName();
        if (lastRel != null)
            context.environment.renameDown(lastRel, lhsVar);
        context.clearSubstitutions();
        return result;
    }

    @Override
    protected DDlogIRNode visitQuery(Query query, TranslationContext context) {
        if (query.getOrderBy().isPresent())
            throw new TranslationException("ORDER BY clauses not supported", query);
        if (query.getWith().isPresent())
            throw new TranslationException("WITH clauses not supported", query);
        DDlogIRNode result = this.process(query.getQueryBody(), context);
        if (result == null)
            throw new TranslationException("Not yet implemented", query);
        return result;
    }

    @Override
    protected DDlogIRNode visitUnion(Union union, TranslationContext context) {
        if (!union.isDistinct())
            throw new TranslationException("UNION ALL not supported", union);

        List<RuleBody> convert = Linq.map(union.getRelations(), r -> this.process(r, context).to(RuleBody.class));
        List<DDlogType> types = Linq.map(convert, RuleBody::getType);
        DDlogType resultType = context.meet(types);
        DDlogRule rule = null;
        for (RuleBody rhs: convert) {
            rhs = this.convertType(rhs, resultType, context);
            if (rule == null) {
                RelationName ruleName = context.freshRelationName("union");
                rule = context.createRule(union, null, ruleName, rhs, DDlogRelationDeclaration.Role.Internal);
            } else {
                if (!rule.head.val.getType().same(rhs.getType()))
                    throw new TranslationException("Union between sets with different types: ", union);
                DDlogAtom lhs = new DDlogAtom(union, rule.head.relation, rule.head.val);
                List<RuleBodyTerm> definitions = rhs.getDefinitions();
                definitions.add(new RuleBodyVarDef(union, getRuleVar(rule).var, rhs.getRowVariable()));
                DDlogRule newRule = new DDlogRule(union, lhs, definitions);
                context.add(newRule);
            }
        }
        assert rule != null;
        RuleBody result = new RuleBody(union, getRuleVar(rule).var, rule.head.val.getType());
        result.addDefinition(new BodyTermLiteral(union, true, rule.head));
        return result;
    }

    /**
     * If rhs has the type given do nothing.
     * Otherwise the type may have some fields that are nullable, where the rhs has a non-nullable fields.
     * In this case return a new RelationRHS that has the same values as RHS
     * but where the suitable fields are wrapped in Some{}.
     * @param rhs   Right hand side describing a set of values.
     * @param type  The type desired for the RHS.
     * @return      A new right-hand side with the same values converted if necessary.
     *
     * For example, the type of relation may be T{.a: string, .b: bool} and
     * type may be T1{.a: Optional[string], .b: bool}.  In this case we generate
     * a new relation with type T1 and where we transform every value of type T
     * to a value of type T1.
     */
    RuleBody convertType(RuleBody rhs, DDlogType type, TranslationContext context) {
        DDlogType rhsType = context.resolveType(rhs.getType());
        if (rhsType.same(context.resolveType(type)))
            return rhs;

        RelationName relName = context.freshRelationName("source");
        DDlogTStruct rhsStr = rhsType.to(DDlogTStruct.class);
        DDlogRule rule = context.createRule(rhs.getNode(), null, relName, rhs, DDlogRelationDeclaration.Role.Internal);
        RuleBody result = new RuleBody(rhs.getNode(), rhs.getVarName(), type);
        result.addDefinition(new BodyTermLiteral(rhs.getNode(), true, rule.head));
        DDlogTStruct str = context.resolveType(type).to(DDlogTStruct.class);
        List<DDlogEStruct.FieldValue> fields = new ArrayList<DDlogEStruct.FieldValue>();
        for (DDlogField f: str.getFields()) {
            DDlogExpression extract = new DDlogEField(rhs.getNode(), getRuleVar(rule), f.getName(), f.getType());
            DDlogType origType = rhsStr.getFieldType(f.getName());
            DDlogType destType = f.getType();
            DDlogType.checkCompatible(destType, origType, false);
            if (destType.mayBeNull && !destType.same(origType))
                extract = ExpressionTranslationVisitor.wrapSome(extract, destType);
            DDlogEStruct.FieldValue field = new DDlogEStruct.FieldValue(f.getName(), extract);
            fields.add(field);
        }
        DDlogExpression expr = new DDlogEStruct(rhs.getNode(), str.getName(), str, fields);
        result.addDefinition(new DDlogESet(rhs.getNode(), result.getRowVariable(true), expr));
        return result;
    }

    @Override
    protected DDlogIRNode visitExcept(Except except, TranslationContext context) {
        RuleBody left = this.process(except.getLeft(), context).to(RuleBody.class);
        RuleBody right = this.process(except.getRight(), context).to(RuleBody.class);
        DDlogType type = context.meet(left.getType(), right.getType());
        left = this.convertType(left, type, context);
        right = this.convertType(right, type, context);

        RelationName relName = context.freshRelationName("except");
        DDlogRule rule = context.createRule(
                except.getRight(), null, relName, right, DDlogRelationDeclaration.Role.Internal);
        left.addDefinition(new RuleBodyVarDef(except, getRuleVar(rule).var, left.getRowVariable(false)));
        left.addDefinition(new BodyTermLiteral(except, false, rule.head));
        return left;
    }

    @Override
    protected DDlogIRNode visitIntersect(Intersect intersect, TranslationContext context) {
        List<RuleBody> convert = Linq.map(intersect.getRelations(), r -> this.process(r, context).to(RuleBody.class));
        List<DDlogType> types = Linq.map(convert, RuleBody::getType);
        DDlogType resultType = context.meet(types);

        RuleBody result = null;
        for (RuleBody rhs: convert) {
            rhs = this.convertType(rhs, resultType, context);
            RelationName ruleName = context.freshRelationName("intersect");
            DDlogRule rule = context.createRule(intersect, null, ruleName, rhs, DDlogRelationDeclaration.Role.Internal);
            if (result == null)
                result = new RuleBody(intersect, context.freshLocalName("v"), rule.head.val.getType());
            result.addDefinition(new BodyTermLiteral(
                    intersect, true, new DDlogAtom(rhs.getNode(), rule.head.relation, result.getRowVariable())));
        }
        assert result != null;
        return result;
    }

    @Override
    protected DDlogIRNode visitAliasedRelation(
            AliasedRelation relation, TranslationContext context) {
        String name = relation.getAlias().getValue();
        if (debug) System.out.println("Visting aliased relation " + relation);
        context.pushAlias(name);
        DDlogIRNode rel = this.process(relation.getRelation(), context);
        RuleBody rrhs = rel.to(RuleBody.class);
        context.environment.renameDown(name, rrhs.getVarName());
        RelationNameVisitor rnv = new RelationNameVisitor();
        String relationName = relation.getRelation().accept(rnv, null);
        if (debug) System.out.println("Returning from aliased relation " + relation);
        if (relationName != null && !name.equals(relationName))
            context.environment.delete(relationName);
        return rrhs;
    }

    @Override
    protected DDlogIRNode visitTable(Table table, TranslationContext context) {
        String qualifiedName = Utilities.convertQualifiedName(table.getName());
        RelationName name = new RelationName(RelationName.makeRelationName(qualifiedName), qualifiedName, table);
        DDlogRelationDeclaration relation = context.getRelation(name);
        if (relation == null)
            throw new TranslationException("Could not find relation `" + table.toString() + "'", table);
        String var = context.freshLocalName("v");
        DDlogType type = relation.getType();
        DDlogTStruct stype = context.resolveType(type).to(DDlogTStruct.class);
        RelationEnvironent scope = new RelationEnvironent(table, qualifiedName, var, stype);
        context.environment.stack(scope);
        RuleBody result = new RuleBody(table, var, type);
        result.addDefinition(new BodyTermLiteral(table,
                true, new DDlogAtom(table, relation.getName(), result.getRowVariable())));
        return result;
    }

    /**
     * Convert a select without aggregation.
     */
    private <T extends SelectItem> RuleBody processSimpleSelect(
            Node select, RuleBody inputRelation, List<T> selectArguments,
            TranslationContext context) {
        if (debug) System.out.println("Visiting simple select " + select);
        RelationName outRelName = context.freshRelationName("tmp");
        if (selectArguments.size() == 1) {
            // Special case for SELECT *
            SelectItem single = selectArguments.get(0);
            if (single instanceof AllColumns) {
                AllColumns all = (AllColumns)single;
                if (!all.getPrefix().isPresent())
                    return inputRelation;
            }
        }

        List<DDlogField> typeList = new ArrayList<DDlogField>();
        List<DDlogEStruct.FieldValue> exprList = new ArrayList<DDlogEStruct.FieldValue>();
        HashMap<String, String> aliases = new HashMap<>();
        for (SelectItem s : selectArguments) {
            if (s instanceof SingleColumn) {
                SingleColumn sc = (SingleColumn) s;
                String name = context.columnName(sc);
                String originalName = context.originalColumnName(sc);
                if (!name.equals(originalName)) {
                    aliases.put(name, originalName);
                }
                Expression expression = sc.getExpression();
                DDlogExpression expr = context.translateExpression(expression);
                DDlogType type = expr.getType();
                typeList.add(new DDlogField(sc, name, type));
                exprList.add(new DDlogEStruct.FieldValue(name, expr));
            } else if (s instanceof AllColumns) {
                // We can get all columns from the type of the
                // input relation
                DDlogType type = inputRelation.getType();
                type = context.resolveType(type);
                DDlogTStruct struct = type.to(DDlogTStruct.class);
                for (DDlogField f: struct.getFields()) {
                    typeList.add(f);
                    exprList.add(new DDlogEStruct.FieldValue(f.getName(),
                            new DDlogEField(inputRelation.getNode(), inputRelation.getRowVariable(),
                                    f.getName(), f.getType())));
                }
            } else {
                throw new TranslationException("Not yet implemented", s);
            }
        }

        DDlogTUser tuser = context.createStruct(select, typeList, outRelName.name);
        String var = context.freshLocalName("v");
        RuleBody result = new RuleBody(select, var, tuser);
        for (RuleBodyTerm rhs: inputRelation.getDefinitions())
            result.addDefinition(rhs);
        DDlogExpression project = new DDlogEStruct(select, tuser.name, tuser, exprList);
        DDlogExpression assignProject = new DDlogESet(
                select,
                result.getRowVariable(true),
                project);
        result = result.addDefinition(assignProject);
        if (!aliases.isEmpty())
            context.environment.renameDown(aliases);
        return result;
    }

    /**
     * The type of an intermediate expression produced by an aggregate.
     * @param aggregate       SQL aggregate function name.
     * @param aggregatedType  The type of the result produced.
     * @return                The DDlog type.
     */
    private DDlogType intermediateType(Node node, String aggregate, DDlogType aggregatedType) {
        switch (aggregate) {
            case "count":
                return DDlogTSigned.signed64;
            case "sum_distinct":
            case "count_distinct":
                return new DDlogTUser(node,"Set", false, aggregatedType);
            case "any":
            case "some":
            case "every":
                return aggregatedType;
            case "sum":
                return aggregatedType.toNumeric().aggregateType();
            case "array_agg":
            case "set_agg":
                return aggregatedType.to(DDlogTRef.class).getElementType();
            case "min":
            case "max":
                return new DDlogTTuple(node,
                        DDlogTBool.instance, // first
                        aggregatedType       // value
                );
            case "avg":
            case "avg_distinct":
                DDlogType interm = aggregatedType.toNumeric().aggregateType();
                return new DDlogTTuple(node,
                        interm,  // sum
                        DDlogTSigned.signed64.setMayBeNull(aggregatedType.mayBeNull)   // count
                ).setMayBeNull(aggregatedType.mayBeNull);
            default:
                throw new TranslationException("Unexpected aggregate", node);
        }
    }

    /**
     * An expression that increments the current aggregate value.
     * @param aggregate    SQL aggregate function name.
     * @param resultType   Type of result produced by increment.
     * @param variable     Variable that is incremented.
     * @param increment    Increment value.
     */
    private DDlogExpression aggregateIncrement(
            Node node,
            String aggregate, DDlogType resultType, DDlogEVar variable,
            DDlogExpression increment) {
        DDlogType incType = increment.getType();
        switch (aggregate) {
            case "count_distinct":
            case "sum_distinct":
                String insertFunc = increment.getType().mayBeNull ? "insert_non_null" : "set_insert";
                return new DDlogEApply(node, insertFunc, DDlogTTuple.emptyTupleType, variable, increment);
            case "avg":
            case "sum":
                IsNumericType num = incType.toNumeric();
                aggregate += "_" + num.simpleName();
                break;
            case "array_agg":
                return new DDlogEApply(node, "vec_push", resultType, variable, increment);
            case "set_agg":
                return new DDlogEApply(node, "insert", resultType, variable, increment);
            default:
                break;
        }
        String funcName = "agg_" + aggregate + "_" + (incType.mayBeNull ? "N" : "R");
        DDlogExpression add = new DDlogEApply(node, funcName, resultType, variable, increment);
        return new DDlogESet(node, variable, add);
    }

    /**
     * An expression that initializes an aggregate.
     * @param f          SQL IR function - for reporting error message.
     * @param aggregate  SQL aggregate function name
     * @param dataType   Type of the data that is being aggregated (not the type of the aggregate).
     */
    private DDlogExpression aggregateInitializer(FunctionCall f, String aggregate, DDlogType dataType) {
        DDlogExpression none = dataType.getNone(f);

        switch (aggregate) {
            case "any":
            case "some":
                return new DDlogEBool(f,false, dataType.mayBeNull);
            case "every":
                return new DDlogEBool(f,true, dataType.mayBeNull);
            case "sum": {
                if (dataType.mayBeNull)
                    return none;
                IsNumericType num = dataType.toNumeric();
                return num.zero();
            }
            case "count":
                return new DDlogESigned(f, 0);
            case "min":
            case "max": {
                if (dataType.mayBeNull)
                    return none;
                DDlogExpression t = new DDlogEBool(f, true);
                if (dataType instanceof DDlogTString
                    || dataType instanceof DDlogTIString)
                    return new DDlogETuple(f, t, new DDlogEIString(f,""));
                IsNumericType num = dataType.toNumeric();
                return new DDlogETuple(f, t, num.zero());
            }
            case "avg": {
                IsNumericType num = dataType.toNumeric().aggregateType().toNumeric();
                DDlogExpression result = new DDlogETuple(f, num.zero(), DDlogTSigned.signed64.zero());
                DDlogType resultType = result.getType().setMayBeNull(dataType.mayBeNull);
                return ExpressionTranslationVisitor.wrapSomeIfNeeded(result, resultType);
            }
            case "count_distinct":
            case "sum_distinct":
                DDlogType setType = new DDlogTUser(f, "Set", false, dataType.setMayBeNull(false));
                return new DDlogEApply(f, "set_empty", setType);
            case "array_agg": {
                DDlogType type = new DDlogTArray(f, dataType, false);
                return new DDlogEApply(f, "vec_empty", type);
            }
            case "set_agg": {
                DDlogType type = new DDlogTSet(f, dataType, false);
                return new DDlogEApply(f, "set_empty", type);
            }
            default:
                throw new TranslationException("Unexpected aggregate: " + aggregate, f);
        }
    }

    private DDlogExpression aggregateComplete(Node node, String aggregate, DDlogExpression value, DDlogType resultType) {
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
                IsNumericType num = sumType.toNumeric();
                DDlogExpression compute = new DDlogEApply(node, "avg_" + num.simpleName() + "_" + suffix, sumType, value);
                return DDlogEAs.cast(compute, resultType);
            }
            case "min":
            case "max":
                return new DDlogETupField(node, value, 1);
            case "count_distinct": {
                DDlogTUser set = value.getType().as(DDlogTUser.class, "expected a set");
                return DDlogEAs.cast(
                        new DDlogEApply(node, "set_size", new DDlogTBit(node, 32, false), value),
                        resultType);
            }
            case "sum_distinct": {
                DDlogTUser set = value.getType().as(DDlogTUser.class, "expected a set");
                DDlogType elemType = set.getTypeArg(0);
                IsNumericType num = elemType.toNumeric();
                DDlogExpression compute = new DDlogEApply(node, "set_" + num.simpleName() + "_sum", elemType, value);
                return DDlogEAs.cast(compute, resultType);
            }
            case "set_agg":
            case "array_agg":
                return DDlogTRef.ref_new(value);
            default:
                return DDlogEAs.cast(value, resultType);
        }
    }

    /**
     * This class bundles state that is constructed while translating
     * Selects.  This makes it easy to pass this state around to
     * helper functions.
     */
    static class SelectTranslationState {
        private final Select select;
        /**
         * SQL Expressions that show up in the Select statement.
         */
        List<Expression> selectExpressions = new ArrayList<Expression>();
        /**
         * List of expressions that we are grouping by.
         */
        List<GroupByInfo> groupBy;
        /**
         * Fields of the struct produced by the output relation.
         */
        List<DDlogField> resultTypeFields = new ArrayList<DDlogField>();
        /**
         * Type of input to aggregation function.
         */
        List<DDlogField> functionResultTypeFields = new ArrayList<DDlogField>();
        /**
         * Fields returned by the aggregation function.  This is the same as selectExpressions
         * with the exception of elements that are in groupBy.
         */
        List<DDlogEStruct.FieldValue> functionResultFields = new ArrayList<DDlogEStruct.FieldValue>();
        /**
         * Loop in the aggregation function.
         */
        @Nullable
        DDlogExpression loopBody;
        /**
         * For each field name in functionResultFields this holds the original expression
         * that produced it.
         */
        Map<String, Expression> resultFieldOrigin = new HashMap<String, Expression>();
        /**
         * Aggregation function body.
         */
        @Nullable
        DDlogExpression functionBody;

        public SelectTranslationState(Select select, List<GroupByInfo> groupBy) {
            this.select = select;
            this.loopBody = null;
            this.functionBody = null;
            this.groupBy = groupBy;
        }

        public void addLoopStatement(DDlogExpression expr) {
            this.loopBody = DDlogESeq.seq(select, this.loopBody, expr);
        }

        public void addFunctionStatement(DDlogExpression expr) {
            this.functionBody = DDlogESeq.seq(select, this.functionBody, expr);
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
                state.resultTypeFields.add(new DDlogField(a.groupBy, name, a.translation.getType()));
                a.fieldNames.add(name);
                found = true;
                // continue scanning, each field could be used multiple times.
            }
        }
        if (found)
            return;

        AggregateVisitor aggv = new AggregateVisitor(state.groupBy, true);
        Ternary aggTern = aggv.process(expression, context);
        if (aggTern == null) {
            throw new TranslationException("Unhandled node type in AggregateVisitor", expression);
        }

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
                DDlogEVarDecl incrVarDecl = new DDlogEVarDecl(f, incrVar, increment.getType());
                DDlogESet set = new DDlogESet(f, incrVarDecl, increment);
                state.addLoopStatement(set);
                increment = new DDlogEVar(f, incrVar, increment.getType());
            } else if (f.getArguments().size() == 0) {
                // This is the translation of COUNT(*)
                increment = new DDlogESigned(f, 1);
            } else {
                throw new TranslationException("Unexpected aggregate", f);
            }
            DDlogExpression unused = context.translateExpression(f);
            DDlogType aggregatedType = unused.getType();
            DDlogType intermediateType = this.intermediateType(f, aggregateFunction, aggregatedType);
            String aggVarName = context.freshLocalName(aggregateFunction);
            DDlogExpression aggVarDef = new DDlogEVarDecl(f, aggVarName, intermediateType);
            // Replace all occurrences of f with varName when translating later.
            DDlogExpression complete = this.aggregateComplete(f,
                    aggregateFunction, new DDlogEVar(f, aggVarName, intermediateType), aggregatedType);
            context.addSubstitution(f, complete);
            state.addFunctionStatement(new DDlogESet(f,
                    aggVarDef, this.aggregateInitializer(f, aggregateFunction, increment.getType()),
                    true));
            DDlogEVar aggVar = new DDlogEVar(f, aggVarName, aggregatedType);
            DDlogExpression inc = this.aggregateIncrement(f,
                    aggregateFunction, intermediateType, aggVar, increment);
            state.addLoopStatement(inc);
        }
        DDlogExpression expr = context.translateExpression(expression);
        DDlogField field = new DDlogField(expression, name, expr.getType());
        state.functionResultTypeFields.add(field);
        if (!inHaving) {
            state.resultTypeFields.add(field);
        }
        state.functionResultFields.add(new DDlogEStruct.FieldValue(name, expr));
        state.resultFieldOrigin.put(name, expression);
    }

    private <T extends SelectItem> RuleBody processSelectAggregate(
            Select select,
            RuleBody inputRelation,
            List<T> selectArguments,
            @Nullable GroupBy gby,
            List<GroupByInfo> groupBy,
            @Nullable Expression having,
            TranslationContext context) {
        /*
            General structure of an aggregation in DDlog is:

            R[v1] :- R[v], ..., var gb1 = ..., var groupByResult = (gb1, gb2, ...).group_by((v)), var aggResult = agg(groupByResult), var v1 = v.

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
        SelectTranslationState state = new SelectTranslationState(select, groupBy);
        RelationName outRelName = context.freshRelationName("tmp");
        String paramName = context.freshLocalName("g");

        // We will generate a custom function to perform the aggregation.
        // The parameter of the function is a Group<K, T> where K is the key type
        // and T is a tuple with all relations that are in scope.
        List<DDlogType> keyFields = Linq.map(groupBy, g -> g.translation.getType());
        DDlogTTuple keyType = new DDlogTTuple(select, keyFields);
        if (keyFields.size() > 0) {
            List<DDlogExpression> keyVars =
                    Linq.map(groupBy, g -> new DDlogEVarDecl(g.groupBy, g.varName, g.translation.getType()));
            DDlogESet getKeys = new DDlogESet(gby, new DDlogETuple(select, keyVars),
                    new DDlogEApply(gby,"group_key", keyType, new DDlogEVar(select,paramName, keyType)));
            state.addFunctionStatement(getKeys);
        }

        String agg = context.freshGlobalName("agg");
        List<DDlogType> tupleFields = new ArrayList<DDlogType>();
        List<DDlogEVar> tupleVars = new ArrayList<DDlogEVar>();
        // This is a bit conservative: we pass to the aggregation function
        // all variables that are currently "live".  The aggregation may
        // not use all of them.
        for (RuleBodyTerm term: inputRelation.getDefinitions()) {
            if (term.is(BodyTermLiteral.class)) {
                BodyTermLiteral lit = term.to(BodyTermLiteral.class);
                if (lit.polarity && lit.atom.val.is(DDlogEVar.class)) {
                    tupleFields.add(lit.atom.val.getType());
                    tupleVars.add(lit.atom.val.to(DDlogEVar.class));
                }
            } else if (term.is(RuleBodyVarDef.class)) {
                RuleBodyVarDef def = term.to(RuleBodyVarDef.class);
                tupleFields.add(def.getExprType());
                tupleVars.add(new DDlogEVar(def.getNode(), def.getVar(), def.getExprType()));
            }
        }
        for (GroupByInfo g: groupBy)
            context.addSubstitution(g.groupBy, g.getVariable());

        DDlogTTuple tuple = new DDlogTTuple(select, tupleFields);
        String iter = context.freshLocalName("i");  // loop iteration variable
        DDlogEVar iterVar = new DDlogEVar(select, iter, tuple);
        // The loop iteration variable will have the type tuple
        int index = 0;
        for (DDlogEVar s: tupleVars) {
            DDlogEVarDecl decl = new DDlogEVarDecl(gby, s.var, s.getType());
            DDlogExpression project;
            if (tuple.size() > 1) {
                project = new DDlogETupField(gby, iterVar, index++);
            } else {
                project = iterVar;  // tuples with 1 element are not really tuples
            }
            DDlogESet set = new DDlogESet(gby, decl, project);
            state.addLoopStatement(set);
        }

        DDlogTUser paramType = new DDlogTGroup(gby, keyType, tuple);
        DDlogFuncArg param = new DDlogFuncArg(gby, paramName, false, paramType);
        DDlogETuple callArg = new DDlogETuple(select, tupleVars.toArray(new DDlogExpression[0]));

        for (SelectItem s : selectArguments) {
            if (s instanceof SingleColumn) {
                SingleColumn sc = (SingleColumn) s;
                String name;
                if (sc.getAlias().isPresent())
                    name = sc.getAlias().get().getValue().toLowerCase();
                else {
                    ColumnNameVisitor ecn = new ColumnNameVisitor();
                    name = ecn.process(sc.getExpression());
                }
                Expression expression = sc.getExpression();
                this.processSelectExpression(expression, false, name, state, context);
            } else if (s instanceof AllColumns) {
                throw new TranslationException("'SELECT *' not compatible with group-by", s);
            } else {
                throw new TranslationException("Not yet implemented", s);
            }
        }

        // The HAVING clause can also require new aggregates besides the ones present
        // in the SELECT, so we treat it as an additional expression in SELECT
        if (having != null && context.getSubstitution(having) == null)
            this.processSelectExpression(having, true, null, state, context);

        context.clearSubstitutions();
        DDlogTUser tUserResult = context.createStruct(select, state.resultTypeFields, outRelName.name);
        DDlogTUser tUserFunction = context.createStruct(select, state.functionResultTypeFields, agg);
        String var = context.freshLocalName("v");
        RuleBody result = new RuleBody(select, var, tUserResult);
        for (RuleBodyTerm rhs: inputRelation.getDefinitions())
            result.addDefinition(rhs);

        // For each expression that we group by add a new temporary variable
        List<String> groupByVars = new ArrayList<String>();
        for (GroupByInfo g : groupBy) {
            DDlogESet groupByVarDef = new DDlogESet(g.groupBy,
                    new DDlogEVarDecl(g.groupBy, g.varName, g.translation.getType()), g.translation);
            result.addDefinition(groupByVarDef);
            groupByVars.add(g.varName);
            context.addSubstitution(g.groupBy, g.getVariable());
        }

        assert state.loopBody != null;

        DDlogEFor forLoop = new DDlogEFor(gby, new DDlogETuple(gby, Arrays.asList(new DDlogEVar(gby, iter, tuple), new DDlogEPHolder(gby))), new DDlogEVar(gby, paramName, paramType), state.loopBody);
        state.addFunctionStatement(forLoop);
        String[] vars = groupByVars.toArray(new String[0]);
        String aggregateVarName = context.freshLocalName("aggResult");
        String gbVarName = context.freshLocalName("groupResult");
        BodyTermGroupby gb = new BodyTermGroupby(gby, gbVarName, callArg, vars);
        result.addDefinition(gb);

        DDlogType gtype = new DDlogTGroup(gby, forLoop.getType(), tUserFunction);
        DDlogEStruct project = new DDlogEStruct(select, tUserFunction.name, tUserResult, state.functionResultFields);
        if (!state.functionResultTypeFields.isEmpty()) {
            DDlogExpression aggregate = new DDlogESet(select,
                    new DDlogEVarDecl(select, aggregateVarName, tUserFunction),
                    new DDlogEApply(select, agg, tUserFunction, new DDlogEVar(select, gbVarName, gtype)));
            result.addDefinition(aggregate);
            state.addFunctionStatement(project);
            DDlogFunction func = new DDlogFunction(select, agg, tUserFunction, state.functionBody, param);
            context.getProgram().functions.add(func);
        }

        DDlogRelationDeclaration outRel = new DDlogRelationDeclaration(
                select, DDlogRelationDeclaration.Role.Internal, outRelName, tUserResult);
        context.add(outRel, null);

        DDlogExpression copy;
        if (groupBy.size() != 0) {
            // Copy into the final tuple the groupBy fields necessary...
            List<DDlogEStruct.FieldValue> fields = new ArrayList<DDlogEStruct.FieldValue>();
            for (GroupByInfo gr : groupBy) {
                for (String fieldName: gr.fieldNames) {
                    fields.add(new DDlogEStruct.FieldValue(fieldName,
                            new DDlogEVar(gr.groupBy, gr.varName, gr.translation.getType())));
                }
            }

            // ... and the other fields computed by the aggregation function
            DDlogEVar aggVar = new DDlogEVar(gby, aggregateVarName, tUserFunction);
            for (DDlogEStruct.FieldValue field: project.fields) {
                DDlogEField projField = new DDlogEField(select, aggVar, field.getName(), field.getValue().getType());
                Expression original = state.resultFieldOrigin.get(field.getName());
                assert original != null;
                context.addSubstitution(original, projField);
                if (!original.equals(having))
                    fields.add(new DDlogEStruct.FieldValue(field.getName(), projField));
            }
            DDlogExpression resultFields = new DDlogEStruct(select, tUserResult.getName(), tUserResult, fields);
            copy = new DDlogESet(select, result.getRowVariable(true), resultFields);
        } else {
            copy = new DDlogESet(select, result.getRowVariable(true), new DDlogEVar(select, aggregateVarName, tUserFunction));
        }

        result.addDefinition(copy);
        if (having != null) {
            DDlogExpression hav = context.translateExpression(having);
            result.addDefinition(ExpressionTranslationVisitor.unwrapBool(hav));
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
        if (spec.getOrderBy().isPresent())
            throw new TranslationException("ORDER BY clauses not supported", spec);
        if (!spec.getFrom().isPresent())
            throw new TranslationException("FROM clause is required", spec);

        // We start by processing the from clause; the scope of the rest of the
        // query is influenced by the from clause.
        if (debug) System.out.println("Visiting FROM clause of " + spec);
        DDlogIRNode source = this.process(spec.getFrom().get(), context);
        if (source == null)
            throw new TranslationException("Not yet handled", spec);

        GroupBy gb = null;
        List<GroupByInfo> groupBy = new ArrayList<GroupByInfo>();
        if (spec.getGroupBy().isPresent()) {
            gb = spec.getGroupBy().get();
            this.processGroupBy(gb, context, groupBy);
        }

        // Analyze the structure of the select items to discover window computations
        // and aggregate calls.
        Select select = spec.getSelect();
        List<SelectItem> items = select.getSelectItems();

        // if (debug) System.out.println(toString(select));
        boolean foundAggregate = false;
        boolean foundNonAggregate = false;
        AggregateVisitor aggregateVisitor = new AggregateVisitor(groupBy);
        WindowVisitor windowVisitor = new WindowVisitor();
        List<SingleColumn> aggregateItems = new ArrayList<SingleColumn>();
        List<SingleColumn> nonAggregateItems = new ArrayList<SingleColumn>();
        List<SingleColumn> windowItems = new ArrayList<SingleColumn>();

        for (SelectItem s : items) {
            if (s instanceof AllColumns) {
                foundNonAggregate = true;
                DDlogType type = source.to(RuleBody.class).getType();
                DDlogTStruct struct = context.resolveType(type).to(DDlogTStruct.class);
                for (DDlogField f: struct.getFields()) {
                    Identifier id = new Identifier(f.getName());
                    SingleColumn sc = new SingleColumn(id, id);
                    nonAggregateItems.add(sc);
                }
            } else {
                SingleColumn sc = (SingleColumn)s;
                Expression e = sc.getExpression();
                if (windowVisitor.process(e, context) == Ternary.Yes) {
                    if (debug) System.out.println("Window: " + e);
                    windowItems.add(sc);
                } else {
                    Identifier id = new Identifier(context.columnName(sc));
                    sc = new SingleColumn(sc.getExpression(), id);
                    if (aggregateVisitor.process(e, context) == Ternary.Yes) {
                        //if (debug) System.out.println("Aggregate: " + e);
                        foundAggregate = true;
                        aggregateItems.add(sc);
                        windowVisitor.substitutions.add(sc.getExpression(), id);
                    } else {
                        //if (debug) System.out.println("Nonaggregate: " + e);
                        foundNonAggregate = true;
                        nonAggregateItems.add(sc);
                    }
                }
            }
        }

        if (debug) System.out.println("-----");
        if (foundAggregate && foundNonAggregate)
            throw new TranslationException("SELECT with a mix of aggregates and non-aggregates.", select);
        if (!select.isDistinct() && !foundAggregate)
            throw new TranslationException("Only SELECT DISTINCT currently supported", select);

        if (windowVisitor.windows.isEmpty()) {
            // No window computations: synthesize the query directly.
            RuleBody relation = source.to(RuleBody.class);
            if (spec.getWhere().isPresent()) {
                Expression expr = spec.getWhere().get();
                DDlogExpression ddexpr = context.translateExpression(expr);
                ddexpr = ExpressionTranslationVisitor.unwrapBool(ddexpr);
                relation = relation.addDefinition(ddexpr);
            }

            Expression having = null;
            if (spec.getHaving().isPresent())
                having = spec.getHaving().get();

            // If we have no window functions we can do everything in one step.
            RuleBody selectTranslation;
            if (foundAggregate) {
                selectTranslation = this.processSelectAggregate(
                        select, relation, items, gb, groupBy, having, context);
            } else {
                if (groupBy.size() > 0)
                    throw new TranslationException("Select without aggregation with GROUP BY", select);
                selectTranslation = this.processSimpleSelect(select, relation, items, context);
            }

            if (spec.getLimit().isPresent()) {
                String limit = spec.getLimit().get();
                int intLimit = Integer.parseInt(limit);
                RelationName relName = context.freshRelationName("limit");
                RuleBody rhs = selectTranslation.to(RuleBody.class);
                DDlogRule rule = context.createRule(spec, null, relName, rhs, DDlogRelationDeclaration.Role.Internal);

                String resultVar = context.freshLocalName("limited");
                RuleBody limited = new RuleBody(spec, resultVar, rhs.getType());
                limited.addDefinition(new BodyTermLiteral(spec, true, new DDlogAtom(spec, relName, rule.head.val)));
                // use a group-by with an empty key and the built-in limit aggregator
                String groupVar = context.freshLocalName("g");
                limited.addDefinition(new BodyTermGroupby(spec, groupVar, getRuleVar(rule)));
                DDlogType setType = new DDlogTUser(null, "Set", false, rhs.getType());

                String aggVar = context.freshLocalName("agg");
                limited.addDefinition(
                        new DDlogESet(spec, new DDlogEVarDecl(spec, aggVar, rhs.getType()),
                            new DDlogEApply(spec, "limit", rhs.getType(),
                                new DDlogEVar(spec, groupVar, setType),
                                new DDlogESigned(spec, intLimit))));
                limited.addDefinition(new BodyTermFlatMap(spec, resultVar, new DDlogEVar(spec, aggVar, rhs.getType())));
                selectTranslation = limited;
            }
            return selectTranslation;
        }

        return this.processWindows(spec, windowVisitor, windowItems, aggregateItems, nonAggregateItems, context);
    }

    private DDlogIRNode processWindows(QuerySpecification query,
                                       WindowVisitor windowVisitor,
                                       List<SingleColumn> windowItems,
                                       List<SingleColumn> aggregateItems,
                                       List<SingleColumn> nonAggregateItems,
                                       TranslationContext context) {
        List<SelectItem> inputItems = new ArrayList<SelectItem>();
        inputItems.addAll(windowVisitor.firstSelect);
        inputItems.addAll(Linq.flatMap(windowVisitor.windows, w -> w.groupOn));
        inputItems.addAll(aggregateItems);
        inputItems.addAll(nonAggregateItems);
        Select select = new Select(query.getSelect().isDistinct(), inputItems);
        QuerySpecification prepare = new QuerySpecification(
                select,
                query.getFrom(),
                query.getWhere(),
                query.getGroupBy(),
                query.getHaving(),
                query.getOrderBy(),
                query.getLimit());
        String overInputName = context.freshGlobalName("OverInput");
        QualifiedName overInput = QualifiedName.of(overInputName);
        Query q = new Query(Optional.empty(), prepare, Optional.empty(), Optional.empty());
        CreateView overInputView = new CreateView(overInput, q, false);
        if (debug) System.out.println(toString(overInputView));
        context.exitAllScopes();
        context.viewIsOutput = false;
        this.process(overInputView, context);
        Table overInputTable = new Table(overInput);

        context.exitAllScopes();
        Relation join = overInputTable;
        for (WindowVisitor.WindowAggregation w: windowVisitor.windows) {
            List<SelectItem> items = new ArrayList<SelectItem>();
            //noinspection OptionalGetWithoutIsPresent
            items.addAll(Linq.map(w.groupOn, s -> new SingleColumn(s.getAlias().get())));
            items.addAll(w.windowResult);
            Select selectI = new Select(false, items);
            String overName = context.freshGlobalName("Over");
            QualifiedName overQName = QualifiedName.of(overName);
            GroupBy groupBy = w.getGroupBy();
            Table overView = new Table(overQName);
            QuerySpecification window = new QuerySpecification(
                    selectI,
                    Optional.of(overInputTable),
                    Optional.empty(),
                    Optional.of(groupBy),
                    Optional.empty(),
                    Optional.empty(),
                    Optional.empty());
            Query qi = new Query(Optional.empty(), window, Optional.empty(), Optional.empty());
            CreateView over = new CreateView(overQName, qi, false);
            if (debug) System.out.println(toString(over));
            context.environment.exitAllScopes();
            context.viewIsOutput = false;
            this.process(over, context);
            JoinCriteria criteria = new NaturalJoin();
            join = new Join(Join.Type.INNER, join, overView, Optional.of(criteria));
        }

        List<SelectItem> finalItems = new ArrayList<SelectItem>();
        SubstitutionRewriter rewriter = new SubstitutionRewriter(windowVisitor.substitutions);
        ExpressionTreeRewriter<Void> subst = new ExpressionTreeRewriter<Void>(rewriter);
        ExpressionTreeRewriter<Void> dropTableExprRewriter = new ExpressionTreeRewriter<Void>(new ColumnContextEliminationRewriter());

        // Add columns to finalItems in the order that they appear in the original table.
        for (SelectItem originalColumn : query.getSelect().getSelectItems()) {
            if (originalColumn instanceof AllColumns) {
                if (query.getFrom().isPresent()) {
                    Table r = (Table) query.getFrom().get();
                    String tn = Utilities.convertQualifiedName(r.getName());
                    String name = RelationName.makeRelationName(tn);
                    RelationName rn = new RelationName(name, tn, r);
                    DDlogType tableType = context.resolveType(Objects.requireNonNull(context.getRelation(rn)).getType());
                    DDlogTStruct struct = tableType.to(DDlogTStruct.class);
                    for (DDlogField f : struct.getFields()) {
                        Identifier id = new Identifier(f.getName());
                        finalItems.add(new SingleColumn(id, id));
                    }
                }
            } else if (originalColumn instanceof SingleColumn) {
                SingleColumn scOrigCol = (SingleColumn) originalColumn;
                boolean found = false;
                for (SingleColumn sc: Utilities.concatenate(aggregateItems, windowItems)) {
                    // Aggregate or window items should be referenced by their name in the OverInput or Over tables
                    // created by this Visitor.
                    if (scOrigCol.getExpression().equals(sc.getExpression())) {
                        Expression repl = subst.rewrite(sc.getExpression(), null);
                        Expression repl1 = dropTableExprRewriter.rewrite(repl, null);
                        finalItems.add(new SingleColumn(repl1, sc.getAlias()));
                        found = true;
                        break;
                    }
                }
                if (!found) {
                    for (SingleColumn sc: nonAggregateItems) {
                        // Due to the OverInput / Over intermediate tables created by this Visitor, these
                        // nonaggregated columns now must be referenced by their aliases, if present.
                        if (scOrigCol.getExpression().equals(sc.getExpression())) {
                            Expression repl = subst.rewrite(sc.getExpression(), null);
                            Expression repl1 = dropTableExprRewriter.rewrite(repl, null);
                            finalItems.add(new SingleColumn(
                                    sc.getAlias().isPresent()? sc.getAlias().get() : repl1, sc.getAlias()));
                        }
                    }

                }
            } else {
                throw new TranslationException("Select items aren't of type SingleColumn or AllColumns", query);
            }
        }

        Select selectFinal = new Select(true, finalItems);
        QuerySpecification joins = new QuerySpecification(
                selectFinal,
                Optional.of(join),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty());
        if (debug) System.out.println(toString(joins));
        context.environment.exitAllScopes();
        context.viewIsOutput = false;
        return this.process(joins, context);
    }

    @Override
    protected DDlogIRNode visitCreateView(CreateView view, TranslationContext context) {
        if (debug) System.out.println("Executing " + view);
        String qn = Utilities.convertQualifiedName(view.getName());
        RelationName name = context.freshRelationName(qn);
        DDlogRelationDeclaration.Role role =
                context.viewIsOutput ? DDlogRelationDeclaration.Role.Output : DDlogRelationDeclaration.Role.Internal;
        DDlogIRNode query = this.process(view.getQuery(), context);
        if (query == null)
            throw new TranslationException("Not yet implemented", view);
        RuleBody rel = query.to(RuleBody.class);
        return context.createRule(view, Utilities.convertQualifiedName(view.getName()), name, rel, role);
    }

    /**
     * Given a rule body make sure that it is a relation definition.
     * Either it contains just a positive relation atom, or, if not,
     * define a new relation.
     * @param body  Rule body.
     */
    RelationName ensureRelation(RuleBody body, TranslationContext context) {
        List<RuleBodyTerm> defs = body.getDefinitions();
        if (defs.size() == 1) {
            RuleBodyTerm term = defs.get(0);
            if (term.is(BodyTermLiteral.class)) {
                BodyTermLiteral lit = term.to(BodyTermLiteral.class);
                if (lit.polarity)
                    return lit.atom.relation;
            }
        }

        RelationName rel = context.freshRelationName("tmp");
        context.createRule(body.getNode(), null, rel, body, DDlogRelationDeclaration.Role.Internal);
        return rel;
    }

    /**
     * Generate code for a join.
     */
    @Override
    public DDlogIRNode visitJoin(Join join, TranslationContext context) {
        /*
            Let us consider this example
            SELECT DISTINCT t1.column2, X.column1 FROM t1
            JOIN (SELECT DISTINCT column1 FROM t2) AS X
            ON t1.column1 = X.column1
         */
        // Process each relation in its own context.
        TranslationContext leftContext = context.cloneCtxt();
        TranslationContext rightContext = context.cloneCtxt();
        DDlogIRNode left = this.process(join.getLeft(), leftContext);
        DDlogIRNode right = this.process(join.getRight(), rightContext);
        RuleBody leftBody = left.to(RuleBody.class);
        RuleBody rightBody = right.to(RuleBody.class);
        context.mergeWith(leftContext);
        context.mergeWith(rightContext);
        DDlogTStruct ltype = context.resolveType(leftBody.getType()).to(DDlogTStruct.class);
        DDlogTStruct rtype = context.resolveType(rightBody.getType()).to(DDlogTStruct.class);
        // Terms of the result.
        List<RuleBodyTerm> terms = new ArrayList<RuleBodyTerm>();
        // We expect that each of the lrel and rrel is just a single RuleBodyTerm
        // For our example leftRelation should be T1.  
        RelationName leftRelation = this.ensureRelation(leftBody, context);
        // For our example rightRelation should be X.  
        RelationName rightRelation = this.ensureRelation(rightBody, context);

        // For a full join both of these are true
        boolean leftJoin = false;
        boolean rightJoin = false;

        // Detect which columns we perform the join on, and whether
        // this is an equijoin.  In our example this will detect that we join on
        // t1.column1 = X.column1, and this is an equijoin.
        JoinColumns joinMap = new JoinColumns(leftContext, rightContext);
        switch (join.getType()) {
            case FULL:
                // both left and right
                leftJoin = true;
                // fall through
            case RIGHT:
                rightJoin = true;
                // fall through
            case LEFT:
                // fall through
            case INNER:
                if (join.getType() == LEFT)
                    leftJoin = true;
                if (join.getCriteria().isPresent()) {
                    JoinCriteria c = join.getCriteria().get();
                    if (c instanceof JoinOn) {
                        // This is the only case which may not be an equijoin.
                        JoinOn on = (JoinOn)c;
                        joinMap.analyze(on.getExpression());
                    } else if (c instanceof JoinUsing) {
                        JoinUsing using = (JoinUsing)c;
                        using.getColumns().forEach(col -> joinMap.addColumnPair(col.getValue(), col.getValue()));
                    } else if (c instanceof NaturalJoin) {
                        HashSet<String> joinColumns = new HashSet<String>(Linq.map(ltype.getFields(), DDlogField::getName));
                        HashSet<String> rightCols = new HashSet<String>(Linq.map(rtype.getFields(), DDlogField::getName));
                        joinColumns.retainAll(rightCols);
                        joinColumns.forEach(col -> joinMap.addColumnPair(col, col));
                    } else {
                        throw new TranslationException("Unexpected join", join);
                    }
                }
                break;
            case CROSS:
            case IMPLICIT:
                // Nothing more to do
                break;
            default:
                throw new TranslationException("Unexpected join type", join);
        }

        String var = context.freshLocalName("v");
        if (!joinMap.isEquiJoin()) {
            // In this case we generate a Cartesian product followed by a filter.
            // The filter is generated directly from the join condition.
            // We use the terms we have already obtained and we compile the predicate.
            // This does not apply to our example, but the general shape is is:
            // T1[v0], X[v1], predicate(v0, v1), ...
            // left side of cartesian product
            terms.addAll(leftBody.getDefinitions());
            // right side of cartesian product
            terms.addAll(rightBody.getDefinitions());
            context.environment = leftContext.environment;
            context.environment.stack(rightContext.environment);
            // condition
            DDlogExpression onE = context.translateExpression(joinMap.getCondition());
            terms.add(new RuleBodyCondition(join, ExpressionTranslationVisitor.unwrapBool(onE)));
        }

        // Now we have to generate a new term that contains all the tuples produced by the join.
        // We generate a flat struct, which has all fields from the left and the right relations that are joined.
        // Unfortunately fields may have common names, and in that case we need to rename some of them.
        List<DDlogField> resultFields = new ArrayList<DDlogField>();
        List<DDlogEStruct.FieldValue> resultFieldValues = new ArrayList<DDlogEStruct.FieldValue>();

        // This will generate a DDlog struct value like this: TRt1{.column1 = column1, .column2 = column2, ... }
        // with one fresh variable for each column of the left relation.
        // However, for non-equi joins we use the variable generated above (v0) and we generate a struct like
        // TRt1{.column1 = v0.column1, .column2 = v0.column2, ... }.   This does not introduce any variables.
        SymbolTable usedFieldNames = new SymbolTable();
        List<DDlogEStruct.FieldValue> leftValues = new ArrayList<>();
        for (DDlogField f: ltype.getFields()) {
            String fieldName = f.getName();
            DDlogExpression value;

            if (joinMap.isEquiJoin()) {
                // For the left table all variables are fresh.
                // Let us assume that they look like freshColumn1, freshColumn2, etc.
                String varName = context.freshLocalName(f.getName());
                joinMap.setColumnVariable(f.getName(), varName, f.getType());
                // The wrapSome below is necessary to filter out null values.  Joins ignore nulls.
                // If the column is nullable the struct generated will look like:
                // TRt1{.column1 = Some{freshColumn1}, .column2 = Some{freshColumn2}, ... }
                value = new DDlogEVar(f.getNode(), varName, f.getType());
                if (joinMap.isLeftColumn(f.getName()))
                    value = ExpressionTranslationVisitor.wrapSomeIfNeeded(value, f.getType());
            } else { // non equi-join
                value =new DDlogEField(f.getNode(), leftBody.getRowVariable(), f.getName(), f.getType());
            }
            usedFieldNames.addName(fieldName);
            DDlogEStruct.FieldValue fieldValue = new DDlogEStruct.FieldValue(fieldName, value);
            leftValues.add(fieldValue);  // only needed for equijoins
            resultFieldValues.add(fieldValue);
            resultFields.add(f);
        }
        if (joinMap.isEquiJoin()) {
            // Generate the term that matches the relation:
            // RT1[ struct_from_above ], i.e.,
            // RT1[ TRt1{.column1 = freshColumn1, .column2 = freshColumn2, ... } ]
            DDlogEStruct leftValue = new DDlogEStruct(ltype.getNode(), ltype.getName(), ltype, leftValues);
            DDlogAtom atom = new DDlogAtom(join.getLeft(), leftRelation, leftValue);
            terms.add(new BodyTermLiteral(join, true, atom));
        }

        // Same process for the right relation.
        // The struct produced in our case will be TX{.column1 = freshColumn1}.  Note that this
        // reuses variable freshColumn1, introduced in the left relation, denoting a join.
        List<DDlogEStruct.FieldValue> rightValues = new ArrayList<>();
        // For each right column name the variable associated
        HashMap<String, String> rightColumnNameInResult = new HashMap<>();
        for (DDlogField f: rtype.getFields()) {
            // We may need to generate a fresh field name in the result structure that
            // has all columns from both tables,  if the left table already has a field with this name.
            String resultRightFieldName = usedFieldNames.freshName(f.getName());
            // Generate a variable for columns that are not being joined on.
            String rightVariable = context.freshLocalName(f.getName());
            DDlogExpression value;
            // True if the right column has a corresponding left column that it's equal to.
            boolean joinedOn = false;
            // True if the right column has the same type as the corresponding left column.
            boolean sameType = true;
            String rightColumnName = f.getName();

            if (joinMap.isEquiJoin()) {
                // Find the temporary variable that we generated that corresponds to this column in the
                // left table if any.  This would find 'freshColumn1' for X.column1
                String useVariable = joinMap.findLeftVariable(rightColumnName);
                // If the variable is not null, this column is being joined on.
                joinedOn = useVariable != null;
                if (joinedOn) {
                    DDlogType type = joinMap.findLeftType(f.getName());
                    if (!f.getType().same(type))
                        // This can happen because one type is nullable and the other is not.
                        // The we should keep both columns
                        sameType = false;
                } else {
                    // I we are not joining on this column use a fresh variable.
                    useVariable = rightVariable;
                }
                value = new DDlogEVar(f.getNode(), useVariable, f.getType());
                if (joinedOn) {
                    value = ExpressionTranslationVisitor.wrapSomeIfNeeded(value, f.getType());
                }
            } else {
                value = new DDlogEField(f.getNode(), rightBody.getRowVariable(), f.getName(), f.getType());
            }

            rightValues.add(new DDlogEStruct.FieldValue(f.getName(), value));  // only used for equijoins
            if (!joinedOn || !sameType) {
                rightColumnNameInResult.put(f.getName(), resultRightFieldName);
                DDlogField rightField = new DDlogField(f.getNode(), resultRightFieldName, f.getType());
                resultFields.add(rightField);
                resultFieldValues.add(new DDlogEStruct.FieldValue(resultRightFieldName, value));
            } else {
                // This field is already present identically in the left table, we don't
                // need to add it to the result at all.  But we need to rename accesses to this
                // field to the left column name.
                String leftColumn = joinMap.findLeftColumn(f.getName());
                rightColumnNameInResult.put(f.getName(), Objects.requireNonNull(leftColumn));
            }
        }
        if (joinMap.isEquiJoin()) {
            DDlogEStruct rightValue = new DDlogEStruct(rtype.getNode(), rtype.getName(), rtype, rightValues);
            DDlogAtom atom = new DDlogAtom(join.getLeft(), rightRelation, rightValue);
            terms.add(new BodyTermLiteral(join, true, atom));
        }

        DDlogTUser tuser = context.createStruct(join, resultFields, "tmp");
        // The term containing the result.
        terms.add(new RuleBodyVarDef(join, var, new DDlogEStruct(join, tuser.name, tuser, resultFieldValues)));
        RuleBody result = new RuleBody(join, var, tuser);
        for (RuleBodyTerm r: terms)
            result.addDefinition(r);

        leftContext.environment.renameUp(leftBody.getVarName(), var, new HashMap<>());
        rightContext.environment.renameUp(rightBody.getVarName(), var, rightColumnNameInResult);
        leftContext.environment.stack(rightContext.environment);
        context.environment = leftContext.environment;
        return result;

        /*
        String commonName = context.freshRelationName("common");
        DDlogRule common = createRule(join, commonName, commonRhs, DDlogRelationDeclaration.Role.Internal, context);

        if (leftJoin) {
            String leftName = context.freshRelationName("left");
            RuleBody leftBody = new RuleBody(join, lRel.getVarName(), lRel.getType());
            lRel.getDefinitions().forEach(leftBody::addDefinition);
            DDlogEStruct struct = new DDlogEStruct(join, ltype.getName(), lRel.getType());
            DDlogAtom atom = new DDlogAtom(join, commonName, struct);
            for (DDlogField field: ltype.getFields()) {

            }
            BodyTermLiteral neg = new BodyTermLiteral(join, false, atom);
            leftBody.addDefinition(neg);
            DDlogRule leftOnly = createRule(join, leftName, leftBody, DDlogRelationDeclaration.Role.Internal, context);
        }
        if (rightJoin) {
            // TODO
        }

        RuleBody result = new RuleBody(join, getRuleVar(common).var, common.head.val.getType());
        result.addDefinition(new BodyTermLiteral(join, true, common.head));
        return result;
         */
    }

    @Override
    protected DDlogIRNode visitColumnDefinition(ColumnDefinition definition, TranslationContext context) {
        String name = definition.getName().getValue();
        String type = definition.getType();
        DDlogType ddtype = SqlSemantics.createType(definition, type, definition.isNullable());
        return new DDlogField(definition, name, ddtype);
    }
}
