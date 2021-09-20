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
import com.vmware.ddlog.util.Linq;
import com.vmware.ddlog.util.Ternary;
import com.vmware.ddlog.util.Utilities;

import javax.annotation.Nullable;
import java.util.*;

import static com.facebook.presto.sql.tree.Join.Type.LEFT;

class TranslationVisitor extends AstVisitor<DDlogIRNode, TranslationContext> {
    static final boolean debug = false;

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

    static String convertQualifiedName(QualifiedName name) {
        return String.join(".", name.getParts());
    }

    @Override
    protected DDlogIRNode visitCreateTable(CreateTable node, TranslationContext context) {
        String name = convertQualifiedName(node.getName());

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
        DDlogTUser tuser = context.createStruct(node, fields, name);
        String relName = DDlogRelationDeclaration.relationName(name);
        context.reserveGlobalName(relName);
        DDlogRelationDeclaration rel = new DDlogRelationDeclaration(
                node, DDlogRelationDeclaration.Role.Input, relName, tuser);
        if (keyColumns.size() > 0) {
            // This type name will not appear in the generated program
            rel = rel.setPrimaryKey(keyColumns, context.freshLocalName("TKey"));
        }
        context.add(rel);
        return rel;
    }

    /**
     * Creates a rule and a declaration for the associated relation; adds them to the program.
     * @param node    SQL node that is being translated.
     * @param relName Name of the relation created.
     * @param rhs     Right-hand side that defines the tuples of the current relation.
     * @param role    Kind of relation created.
     * @param context Translation context; the rule and relation are added there.
     */
    protected DDlogRule createRule(@Nullable Node node, String relName, RelationRHS rhs,
                                   DDlogRelationDeclaration.Role role,
                                   TranslationContext context) {
        String outVarName = context.freshLocalName("v");
        DDlogExpression outRowVarDecl = new DDlogEVarDecl(node, outVarName, rhs.getType());
        DDlogRelationDeclaration relDecl = new DDlogRelationDeclaration(node, role, relName, rhs.getType());
        DDlogAtom lhs = new DDlogAtom(node, relName, new DDlogEVar(node, outVarName, relDecl.getType()));
        List<DDlogRuleRHS> definitions = rhs.getDefinitions();
        DDlogExpression inRowVar = rhs.getRowVariable();
        DDlogESet set = new DDlogESet(node, outRowVarDecl, inRowVar);
        definitions.add(new DDlogRHSCondition(node, set));
        DDlogRule rule = new DDlogRule(node, lhs, definitions);
        rule.addComment(new DDlogComment(node));
        context.add(relDecl);
        context.add(rule);
        return rule;
    }

    // The rules we synthesize have a relatively fixed syntax Rule[variable] :- ...
    // This function extracts the variable on the on the lhs of the rule.
    protected static DDlogEVar getRuleVar(DDlogRule rule) {
        return rule.lhs.val.to(DDlogEVar.class);
    }

    /**
     * Translate a query that produces a table.
     * @param query   Query to translate
     * @param context Translation context.
     * @return        A RelationRHS whose row variable represents all rows in the table query.
     */
    @Override
    protected DDlogIRNode visitTableSubquery(TableSubquery query, TranslationContext context) {
        DDlogIRNode subquery = this.process(query.getQuery(), context);
        RelationRHS rhs = subquery.to(RelationRHS.class);
        String relName = context.freshRelationName("tmp");
        DDlogRule rule = this.createRule(query, relName, rhs, DDlogRelationDeclaration.Role.Internal, context);
        String lhsVar = getRuleVar(rule).var;
        RelationRHS result = new RelationRHS(query, lhsVar, rhs.getType());
        result.addDefinition(new DDlogRHSLiteral(query, true, new DDlogAtom(query, relName, rule.lhs.val)));
        Scope scope = new Scope(query, relName, lhsVar, rule.lhs.val.getType());
        context.enterScope(scope);
        context.clearSubstitutions();
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
        DDlogIRNode result = this.process(query.getQueryBody(), context);
        if (result == null)
            throw new TranslationException("Not yet implemented", query);
        return result;
    }

    @Override
    protected DDlogIRNode visitUnion(Union union, TranslationContext context) {
        if (!union.isDistinct())
            throw new TranslationException("UNION ALL not supported", union);

        List<RelationRHS> convert = Linq.map(union.getRelations(), r -> this.process(r, context).to(RelationRHS.class));
        List<DDlogType> types = Linq.map(convert, RelationRHS::getType);
        DDlogType resultType = context.meet(types);
        DDlogRule rule = null;
        for (RelationRHS rhs: convert) {
            rhs = this.convertType(rhs, resultType, context);
            if (rule == null) {
                String ruleName = context.freshRelationName("union");
                rule = this.createRule(union, ruleName, rhs, DDlogRelationDeclaration.Role.Internal, context);
            } else {
                if (!rule.lhs.val.getType().same(rhs.getType()))
                    throw new TranslationException("Union between sets with different types: ", union);
                DDlogAtom lhs = new DDlogAtom(union, rule.lhs.relation, rule.lhs.val);
                List<DDlogRuleRHS> definitions = rhs.getDefinitions();
                DDlogESet set = new DDlogESet(union, getRuleVar(rule).createDeclaration(), rhs.getRowVariable());
                definitions.add(new DDlogRHSCondition(union, set));
                DDlogRule newRule = new DDlogRule(union, lhs, definitions);
                context.add(newRule);
            }
        }
        assert rule != null;
        RelationRHS result = new RelationRHS(union, getRuleVar(rule).var, rule.lhs.val.getType());
        result.addDefinition(new DDlogRHSLiteral(union, true, rule.lhs));
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
    RelationRHS convertType(RelationRHS rhs, DDlogType type, TranslationContext context) {
        DDlogType rhsType = context.resolveType(rhs.getType());
        if (rhsType.same(context.resolveType(type)))
            return rhs;

        String relName = context.freshRelationName("source");
        DDlogTStruct rhsStr = rhsType.to(DDlogTStruct.class);
        DDlogRule rule = this.createRule(rhs.getNode(), relName, rhs, DDlogRelationDeclaration.Role.Internal, context);
        RelationRHS result = new RelationRHS(rhs.getNode(), rhs.getVarName(), type);
        result.addDefinition(new DDlogRHSLiteral(rhs.getNode(), true, rule.lhs));
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
        RelationRHS left = this.process(except.getLeft(), context).to(RelationRHS.class);
        RelationRHS right = this.process(except.getRight(), context).to(RelationRHS.class);
        DDlogType type = context.meet(left.getType(), right.getType());
        left = this.convertType(left, type, context);
        right = this.convertType(right, type, context);

        String relName = context.freshRelationName("except");
        DDlogRule rule = this.createRule(
                except.getRight(), relName, right, DDlogRelationDeclaration.Role.Internal, context);
        left.addDefinition(new DDlogRHSCondition(
                except, new DDlogESet(except, getRuleVar(rule).createDeclaration(), left.getRowVariable(false))));
        left.addDefinition(new DDlogRHSLiteral(except, false, rule.lhs));
        return left;
    }

    @Override
    protected DDlogIRNode visitIntersect(Intersect intersect, TranslationContext context) {
        List<RelationRHS> convert = Linq.map(intersect.getRelations(), r -> this.process(r, context).to(RelationRHS.class));
        List<DDlogType> types = Linq.map(convert, RelationRHS::getType);
        DDlogType resultType = context.meet(types);

        RelationRHS result = null;
        for (RelationRHS rhs: convert) {
            rhs = this.convertType(rhs, resultType, context);
            String ruleName = context.freshRelationName("intersect");
            DDlogRule rule = this.createRule(intersect, ruleName, rhs, DDlogRelationDeclaration.Role.Internal, context);
            if (result == null)
                result = new RelationRHS(intersect, context.freshLocalName("v"), rule.lhs.val.getType());
            result.addDefinition(new DDlogRHSLiteral(
                    intersect, true, new DDlogAtom(rhs.getNode(), rule.lhs.relation, result.getRowVariable())));
        }
        assert result != null;
        return result;
    }

    @Override
    protected DDlogIRNode visitAliasedRelation(
            AliasedRelation relation, TranslationContext context) {
        DDlogIRNode rel = this.process(relation.getRelation(), context);
        String name = relation.getAlias().getValue();
        RelationRHS rrhs = rel.to(RelationRHS.class);
        context.exitScope();  // drop previous scope
        Scope scope = new Scope(relation, name, rrhs.getVarName(), rrhs.getType());
        context.enterScope(scope);
        return rrhs;
    }

    @Override
    protected DDlogIRNode visitTable(Table table, TranslationContext context) {
        String name = convertQualifiedName(table.getName());
        DDlogRelationDeclaration relation = context.getRelation(DDlogRelationDeclaration.relationName(name));
        if (relation == null)
            throw new TranslationException("Could not find relation", table);
        String var = context.freshLocalName("v");
        DDlogType type = relation.getType();
        Scope scope = new Scope(table, name, var, type);
        context.enterScope(scope);
        RelationRHS result = new RelationRHS(table, var, type);
        result.addDefinition(new DDlogRHSLiteral(table,
                true, new DDlogAtom(table, relation.getName(), result.getRowVariable())));
        return result;
    }

    /**
     * Convert a select without aggregation.
     */
    private <T extends SelectItem> RelationRHS processSimpleSelect(
            Node select, RelationRHS inputRelation, List<T> selectArguments,
            TranslationContext context) {
        String outRelName = context.freshRelationName("tmp");
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
        for (SelectItem s : selectArguments) {
            if (s instanceof SingleColumn) {
                SingleColumn sc = (SingleColumn) s;
                String name = context.columnName(sc);
                Expression expression = sc.getExpression();
                DDlogExpression expr = context.translateExpression(expression);
                typeList.add(new DDlogField(sc, name, expr.getType()));
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

        DDlogTUser tuser = context.createStruct(select, typeList, outRelName);
        String var = context.freshLocalName("v");
        RelationRHS result = new RelationRHS(select, var, tuser);
        for (DDlogRuleRHS rhs: inputRelation.getDefinitions())
            result.addDefinition(rhs);
        DDlogExpression project = new DDlogEStruct(select, tuser.name, tuser, exprList);
        DDlogExpression assignProject = new DDlogESet(
                select,
                result.getRowVariable(true),
                project);
        result = result.addDefinition(assignProject);
        DDlogRelationDeclaration outRel = new DDlogRelationDeclaration(select, DDlogRelationDeclaration.Role.Internal, outRelName, tuser);
        context.add(outRel);
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
                return DDlogTSigned.signed64.setMayBeNull(aggregatedType.mayBeNull);
            case "sum_distinct":
            case "count_distinct":
                return new DDlogTUser(node,"Set", false, aggregatedType);
            case "any":
            case "some":
            case "every":
            case "sum":
                return aggregatedType;
            case "min":
            case "max":
                return new DDlogTTuple(node,
                        DDlogTBool.instance, // first
                        aggregatedType       // value
                );
            case "avg":
            case "avg_distinct":
                return new DDlogTTuple(node,
                        aggregatedType,  // sum
                        aggregatedType   // count
                ).setMayBeNull(aggregatedType.mayBeNull);
            case "array_agg":
                return aggregatedType;
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
            case "sum":
            case "avg":
                IsNumericType num = incType.toNumeric();
                aggregate += "_" + num.simpleName();
                break;
            case "array_agg":
                return new DDlogEApply(node, "vec_push", resultType, variable, increment);
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
                if (dataType.mayBeNull)
                    return new DDlogENull(f, DDlogTSigned.signed64.setMayBeNull(true));
                return new DDlogESigned(f, 0);
            case "min":
            case "max": {
                if (dataType.mayBeNull)
                    return none;
                DDlogExpression t = new DDlogEBool(f, true);
                if (dataType instanceof DDlogTString)
                    return new DDlogETuple(f, t, new DDlogEString(f,""));
                IsNumericType num = dataType.toNumeric();
                return new DDlogETuple(f, t, num.zero());
            }
            case "avg": {
                IsNumericType num = dataType.toNumeric();
                DDlogETuple zt = new DDlogETuple(f, num.zero(), num.zero());
                if (dataType.mayBeNull)
                    return zt.getType().getNone(f);
                return zt;
            }
            case "count_distinct":
            case "sum_distinct":
                DDlogType setType = new DDlogTUser(f, "Set", false, dataType.setMayBeNull(false));
                return new DDlogEApply(f, "set_empty", setType);
            case "array_agg":
                DDlogType type = new DDlogTArray(f, dataType, false);
                return new DDlogEApply(f, "vec_empty", type);
            default:
                throw new TranslationException("Unexpected aggregate: " + aggregate, f);
        }
    }

    private DDlogExpression aggregateComplete(Node node, String aggregate, DDlogExpression value) {
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
                return new DDlogEApply(node, "avg_" + num.simpleName() + "_" + suffix, sumType, value);
            }
            case "min":
            case "max":
                return new DDlogETupField(node, value, 1);
            case "count_distinct": {
                DDlogTUser set = value.getType().as(DDlogTUser.class, "expected a set");
                return new DDlogEAs(node,
                        new DDlogEApply(node, "set_size", set.getTypeArg(0), value),
                        DDlogTSigned.signed64);
            }
            case "sum_distinct": {
                DDlogTUser set = value.getType().as(DDlogTUser.class, "expected a set");
                DDlogType elemType = set.getTypeArg(0);
                IsNumericType num = elemType.toNumeric();
                return new DDlogEApply(node, "set_" + num.simpleName() + "_sum", elemType, value);
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
            context.addSubstitution(f, this.aggregateComplete(f,
                    aggregateFunction, new DDlogEVar(f, aggVarName, intermediateType)));
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

    private <T extends SelectItem> RelationRHS processSelectAggregate(
            Select select,
            RelationRHS inputRelation,
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
        String outRelName = context.freshRelationName("tmp");
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
        for (DDlogRuleRHS rhs: inputRelation.getDefinitions()) {
            if (!rhs.is(DDlogRHSLiteral.class))
                continue;
            DDlogRHSLiteral lit = rhs.to(DDlogRHSLiteral.class);
            if (lit.atom.val.is(DDlogEVar.class)) {
                tupleFields.add(lit.atom.val.getType());
                tupleVars.add(lit.atom.val.to(DDlogEVar.class));
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
                    ExpressionColumnName ecn = new ExpressionColumnName();
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
        DDlogTUser tUserResult = context.createStruct(select, state.resultTypeFields, outRelName);
        DDlogTUser tUserFunction = context.createStruct(select, state.functionResultTypeFields, agg);
        String var = context.freshLocalName("v");
        RelationRHS result = new RelationRHS(select, var, tUserResult);
        for (DDlogRuleRHS rhs: inputRelation.getDefinitions())
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
        DDlogRHSGroupby gb = new DDlogRHSGroupby(gby, gbVarName, callArg, vars);
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

        DDlogRelationDeclaration outRel = new DDlogRelationDeclaration(select, DDlogRelationDeclaration.Role.Internal, outRelName, tUserResult);
        context.add(outRel);

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
        if (spec.getLimit().isPresent())
            throw new TranslationException("LIMIT clauses not supported", spec);
        if (spec.getOrderBy().isPresent())
            throw new TranslationException("ORDER BY clauses not supported", spec);
        if (!spec.getFrom().isPresent())
            throw new TranslationException("FROM clause is required", spec);

        // We start by processing the from clause; the scope of the rest of the
        // query is influenced by the from clause.
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
                DDlogType type = source.to(RelationRHS.class).getType();
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
            RelationRHS relation = source.to(RelationRHS.class);
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
            RelationRHS selectTranslation;
            if (foundAggregate) {
                selectTranslation = this.processSelectAggregate(
                        select, relation, items, gb, groupBy, having, context);
            } else {
                if (groupBy.size() > 0)
                    throw new TranslationException("Select without aggregation with GROUP BY", select);
                selectTranslation = this.processSimpleSelect(select, relation, items, context);
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
            context.exitAllScopes();
            context.viewIsOutput = false;
            this.process(over, context);
            JoinCriteria criteria = new NaturalJoin();
            join = new Join(Join.Type.INNER, join, overView, Optional.of(criteria));
        }

        List<SelectItem> finalItems = new ArrayList<SelectItem>();
        SubstitutionRewriter rewriter = new SubstitutionRewriter(windowVisitor.substitutions);
        ExpressionTreeRewriter<Void> subst = new ExpressionTreeRewriter<Void>(rewriter);
        ExpressionTreeRewriter<Void> dropTable = new ExpressionTreeRewriter<Void>(new ColumnContextEliminationRewriter());

        for (SingleColumn sc: Utilities.concatenate(aggregateItems, nonAggregateItems, windowItems)) {
            Expression repl = subst.rewrite(sc.getExpression(), null);
            Expression repl1 = dropTable.rewrite(repl, null);
            finalItems.add(new SingleColumn(repl1, sc.getAlias()));
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
        context.exitAllScopes();
        context.viewIsOutput = false;
        return this.process(joins, context);
    }

    @Override
    protected DDlogIRNode visitCreateView(CreateView view, TranslationContext context) {
        String name = convertQualifiedName(view.getName());
        DDlogRelationDeclaration.Role role =
                context.viewIsOutput ? DDlogRelationDeclaration.Role.Output : DDlogRelationDeclaration.Role.Internal;
        DDlogIRNode query = this.process(view.getQuery(), context);
        if (query == null)
            throw new TranslationException("Not yet implemented", view);
        RelationRHS rel = query.to(RelationRHS.class);
        String relName = DDlogRelationDeclaration.relationName(name);
        return this.createRule(view, relName, rel, role, context);
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
        TranslationContext rightContext = context.clone();
        DDlogIRNode left = this.process(join.getLeft(), context);
        // Process the right relation in its own context.
        DDlogIRNode right = this.process(join.getRight(), rightContext);
        context.mergeWith(rightContext);
        RelationRHS lrel = left.to(RelationRHS.class);
        RelationRHS rrel = right.to(RelationRHS.class);
        String var = context.freshLocalName("v");
        DDlogType ltype = context.resolveType(lrel.getType());
        DDlogType rtype = context.resolveType(rrel.getType());
        DDlogTStruct lst = ltype.to(DDlogTStruct.class);
        DDlogTStruct rst = rtype.to(DDlogTStruct.class);
        List<DDlogRuleRHS> rules = new ArrayList<DDlogRuleRHS>();
        rules.addAll(lrel.getDefinitions());
        rules.addAll(rrel.getDefinitions());

        boolean leftJoin = false;
        boolean rightJoin = false;

        Set<String> joinColumns = new HashSet<String>();
        switch (join.getType()) {
            /*
            case FULL:
                leftJoin = true;
                // fall through
            case RIGHT:
                rightJoin = true;
                // fall through
            case LEFT:
                // fall through
              */
            case INNER:
                if (join.getType() == LEFT)
                    leftJoin = true;
                if (join.getCriteria().isPresent()) {
                    JoinCriteria c = join.getCriteria().get();
                    if (c instanceof JoinOn) {
                        JoinOn on = (JoinOn)c;
                        DDlogExpression onE = context.translateExpression(on.getExpression());
                        rules.add(new DDlogRHSCondition(join, ExpressionTranslationVisitor.unwrapBool(onE)));
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
            default:
                throw new TranslationException("Unexpected join type", join);
        }

        DDlogExpression condition = new DDlogEBool(join, true);
        for (String col: joinColumns) {
            DDlogExpression e = context.operationCall(join, DDlogEBinOp.BOp.Eq,
                    new DDlogEField(join, lrel.getRowVariable(), col, lst.getFieldType(col)),
                    new DDlogEField(join, rrel.getRowVariable(), col, rst.getFieldType(col)));
            condition = context.operationCall(join, DDlogEBinOp.BOp.And, condition, e);
        }
        rules.add(new DDlogRHSCondition(join, ExpressionTranslationVisitor.unwrapBool(condition)));

        // For the result we take all fields from the left and right but we skip
        // the joinColumn fields from the right.
        List<DDlogField> tfields = new ArrayList<DDlogField>();
        List<DDlogEStruct.FieldValue> fields = new ArrayList<DDlogEStruct.FieldValue>();

        Set<String> names = new HashSet<String>();
        for (DDlogField f: lst.getFields()) {
            tfields.add(f);
            names.add(f.getName());
            fields.add(new DDlogEStruct.FieldValue(f.getName(),
                    new DDlogEField(join, lrel.getRowVariable(), f.getName(), f.getType())));
        }
        for (DDlogField f: rst.getFields()) {
            if (joinColumns.contains(f.getName()))
                continue;
            // We may need to rename this field if it is already present
            String name = freshName(f.getName(), names);
            DDlogField field = new DDlogField(f.getNode(), name, f.getType());
            tfields.add(field);
            fields.add(new DDlogEStruct.FieldValue(field.getName(),
                    new DDlogEField(f.getNode(), rrel.getRowVariable(), f.getName(), f.getType())));
        }

        DDlogTUser tuser = context.createStruct(join, tfields, "tmp");
        DDlogExpression e = new DDlogESet(join,
                new DDlogEVarDecl(join, var, tuser),
                new DDlogEStruct(join, tuser.name, tuser, fields));
        rules.add(new DDlogRHSCondition(join, e));

        RelationRHS result = new RelationRHS(join, var, tuser);
        for (DDlogRuleRHS r: rules)
            result.addDefinition(r);
        return result;
    }

    @Override
    protected DDlogIRNode visitColumnDefinition(ColumnDefinition definition, TranslationContext context) {
        String name = definition.getName().getValue();
        String type = definition.getType();
        DDlogType ddtype = SqlSemantics.createType(definition, type, definition.isNullable());
        return new DDlogField(definition, name, ddtype);
    }
}
