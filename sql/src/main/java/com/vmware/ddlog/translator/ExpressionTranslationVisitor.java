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

import com.facebook.presto.sql.tree.*;
import com.google.common.base.CharMatcher;
import com.google.common.base.Preconditions;
import com.vmware.ddlog.ir.*;
import com.vmware.ddlog.translator.environment.IEnvironment;
import com.vmware.ddlog.util.Linq;
import com.vmware.ddlog.util.Utilities;

import javax.annotation.Nullable;
import java.math.BigInteger;
import java.util.*;
import java.util.function.Function;

public class ExpressionTranslationVisitor extends AstVisitor<DDlogExpression, TranslationContext> {
    public static DDlogExpression checkHandled(@Nullable DDlogExpression expression, Node node) {
        if (expression == null)
            throw new TranslationException("Expression not handled", node);
        return expression;
    }

    private static DDlogExpression makeNull(Node node) {
        return new DDlogENull(node);
    }

    /**
     * If the expression is a NULL, set its type as indicated.
     */
    private static DDlogExpression fixNull(DDlogExpression expr, DDlogType type) {
        if (!expr.is(DDlogENull.class))
            return expr;
        if (!type.mayBeNull)
            expr.error("Type of NULL should be nullable " + type);
        if (type.is(DDlogTUnknown.class))
            expr.error("Cannot infer type for null");
        return new DDlogENull(expr.getNode(), type);
    }

    @Override
    protected DDlogExpression visitCurrentTime(CurrentTime node, TranslationContext context) {
        throw new TranslationException("DDlog programs are deterministic, and cannot use CurrentTime", node);
    }

    /**
     * expr has a non-nullable type; wrap it into the nullable version.
     * @param expr expression to wrap
     * @param type result type
     * @return Some{expr}
     */
    public static DDlogExpression wrapSome(DDlogExpression expr, DDlogType type) {
        return new DDlogEStruct(expr.getNode(), "Some", type,
                new DDlogEStruct.FieldValue("x", expr));
    }

    /**
     * If type is nullable wrap the expression in Some{}, else leave it unchanged.
     * @param expr  Expression to wrap.
     * @param type  Type of expression.
     * @return      A new expression that is never null.
     */
    public static DDlogExpression wrapSomeIfNeeded(DDlogExpression expr, DDlogType type) {
        if (type.mayBeNull)
            return wrapSome(expr, type);
        return expr;
    }

    public static DDlogExpression operationCall(Node node, DDlogEBinOp.BOp op, DDlogExpression left, DDlogExpression right) {
        String function = SqlSemantics.semantics.getFunction(node, op, left.getType(), right.getType());
        DDlogType type = DDlogType.reduceType(left.getType(), right.getType());
        DDlogType outputType = type;
        if (op.isComparison() || op.isBoolean()) {
            outputType = DDlogTBool.instance.setMayBeNull(type.mayBeNull);
        }
        if (function.endsWith("RR"))
            // optimize for the case of no nulls
            return new DDlogEBinOp(node, op, left, right);
        return new DDlogEApply(node, function, outputType, fixNull(left, type), fixNull(right, type));
    }

    @Override
    protected DDlogExpression visitDoubleLiteral(DoubleLiteral literal, TranslationContext context) {
        return new DDlogEDouble(literal, literal.getValue());
    }

    /**
     * Given an expression, if it is nullable unpack it and apply the wrapper then pack it again.
     * Otherwise just apply the wrapper.
     * @param original  Input expression.
     * @param destType  Type of output expression.
     * @param wrapper   A function that we need to apply to expression.
     * @return          A expression that has the function applied.
     */
    protected static DDlogExpression wrapInMatch(
            DDlogExpression original,
            DDlogType destType,
            Function<DDlogExpression, DDlogExpression> wrapper) {
        DDlogType oType = original.getType();
        if (!oType.mayBeNull)
            return wrapper.apply(original);
        List<DDlogEMatch.Case> cases = new ArrayList<DDlogEMatch.Case>();
        Node node = original.getNode();
        cases.add(new DDlogEMatch.Case(node, oType.getNone(node), destType.getNone(node)));
        DDlogExpression wrap = wrapper.apply(new DDlogEVar(node, "x", oType.setMayBeNull(false)));
        cases.add(new DDlogEMatch.Case(node,
                wrapSome(new DDlogEVarDecl(node, "x", oType), oType),
                wrapSome(wrap, destType)));
        return new DDlogEMatch(node, original, cases);
    }

    @Override
    protected DDlogExpression visitCast(Cast node, TranslationContext context) {
        DDlogExpression e = this.process(node.getExpression(), context);
        DDlogType eType = e.getType();
        DDlogType destType = SqlSemantics.createType(node, node.getType(), e.getType().mayBeNull);
        if (destType.is(DDlogTString.class)) {
            // convert to string
            if (eType.is(DDlogTString.class)) {
                return e;
            } else if (eType.is(DDlogTSigned.class) ||
                    eType.is(DDlogTBool.class) ||
                    eType.is(DDlogTFloat.class) ||
                    eType.is(DDlogTDouble.class) ||
                    eType.is(DDlogTUser.class)) {
                Function<DDlogExpression, DDlogExpression> wrapper =
                        ex -> new DDlogEString(node, "${" + ex.toString() + "}");
                return wrapInMatch(e, destType, wrapper);
            } else {
                throw new TranslationException("Unsupported cast to string", node);
            }
        } else if (destType.is(DDlogTFloat.class) || destType.is(DDlogTDouble.class)) {
            IsNumericType num = destType.toNumeric();
            if (eType.is(DDlogTString.class)) {
                String suffix = eType.is(DDlogTFloat.class) ? "f" : "d";
                // I am lying here, the result is actually Result<>,
                // but the unwrap below will remove it.
                Function<DDlogExpression, DDlogExpression> wrapper = ex -> {
                    DDlogExpression parse = new DDlogEApply(node,
                            "parse_" + suffix, destType.setMayBeNull(true), ex);
                    return new DDlogEApply(node,
                            "result_unwrap_or_default", destType, parse);
                };
                return wrapInMatch(e, destType, wrapper);
            } else if (eType.is(DDlogTBool.class)) {
                Function<DDlogExpression, DDlogExpression> wrapper = ex -> new DDlogEITE(node, ex, num.one(), num.zero());
                return wrapInMatch(e, destType, wrapper);
            } else {
                Function<DDlogExpression, DDlogExpression> wrapper = ex -> new DDlogEAs(node, ex, destType.setMayBeNull(false));
                return wrapInMatch(e, destType, wrapper);
            }
        } else if (destType.is(DDlogTSigned.class) || destType.is(DDlogTBit.class) || destType.is(DDlogTInt.class)) {
            IsNumericType num = destType.toNumeric();
            if (eType.is(DDlogTFloat.class) || eType.is(DDlogTDouble.class)) {
                String suffix = eType.is(DDlogTFloat.class) ? "f" : "d";
                Function<DDlogExpression, DDlogExpression> wrapper = ex -> {
                    DDlogExpression convertToInt = new DDlogEApply(node,
                            "int_from_" + suffix, DDlogTInt.instance.setMayBeNull(true), ex);
                    DDlogExpression unwrap = new DDlogEApply(node,
                            "option_unwrap_or_default", DDlogTInt.instance, convertToInt);
                    return new DDlogEAs(node, unwrap, destType.setMayBeNull(false));
                };
                return wrapInMatch(e, destType, wrapper);
            } else if (eType.is(DDlogTString.class)) {
                Function<DDlogExpression, DDlogExpression> wrapper = ex -> {
                    DDlogExpression parse = new DDlogEApply(node,
                            "parse_dec_i64", DDlogTSigned.signed64.setMayBeNull(true), ex);
                    return new DDlogEApply(node,
                            "option_unwrap_or_default", destType, parse);
                };
                return wrapInMatch(e, destType, wrapper);
            } else if (eType.is(DDlogTBool.class)) {
                Function<DDlogExpression, DDlogExpression> wrapper = ex -> new DDlogEITE(node, ex, num.one(), num.zero());
                return wrapInMatch(e, destType, wrapper);
            } else if (eType.is(IsNumericType.class)) {
                IBoundedNumericType eb = eType.as(IBoundedNumericType.class);
                IBoundedNumericType db = destType.as(IBoundedNumericType.class);
                if (eb != null && db != null) {
                    // Need to do two different casts
                    IBoundedNumericType intermediate = db.getWithWidth(eb.getWidth());
                    Function<DDlogExpression, DDlogExpression> wrapper = ex ->
                        new DDlogEAs(node, new DDlogEAs(node,
                            ex, intermediate.as(DDlogType.class, "Must be type")), destType.setMayBeNull(false));
                    return wrapInMatch(e, destType, wrapper);
                }
                Function<DDlogExpression, DDlogExpression> wrapper = ex -> new DDlogEAs(node, ex, destType);
                return wrapInMatch(e, destType, wrapper);
            }
        } else if (destType.is(DDlogTUser.class)) {
            DDlogTUser tu = destType.as(DDlogTUser.class, "Expected TUser");
            Function<DDlogExpression, DDlogExpression> wrapper = ex -> {
                DDlogType exType = ex.getType();
                // At least in MySQL integers are converted to dates as if they were strings...
                if (exType.is(DDlogTInt.class) || exType.is(DDlogTSigned.class) || exType.is(DDlogTBit.class)) {
                    ex = new DDlogEString(node, "${" + e.toString() + "}");
                    exType = ex.getType();
                }
                if (exType.is(DDlogTString.class)) {
                    String parseFunc;
                    switch (tu.getName()) {
                        case "Date":
                            parseFunc = "string2date";
                            break;
                        case "Time":
                            parseFunc = "string2time";
                            break;
                        case "DateTime":
                            parseFunc = "string2datetime";
                            break;
                        default:
                            throw new TranslationException("Unexpected destination type", node);
                    }
                    DDlogExpression parse = new DDlogEApply(node, parseFunc, destType.setMayBeNull(true), ex);
                    return new DDlogEApply(node,
                            "result_unwrap_or_default", destType, parse);
                }
                throw new TranslationException("Illegal cast", node);
            };
            return wrapInMatch(e, destType, wrapper);
        } else if (destType.is(DDlogTBool.class)) {
            if (eType.is(IsNumericType.class)) {
                DDlogExpression zero = eType.toNumeric().zero();
                Function<DDlogExpression, DDlogExpression> wrapper =
                        ex -> operationCall(node, DDlogEBinOp.BOp.Neq, ex, zero);
                return wrapInMatch(e, destType, wrapper);
            }
        }
        throw new TranslationException("Illegal cast", node);
    }

    @Override
    protected DDlogExpression visitNullLiteral(NullLiteral node, TranslationContext context) {
        return makeNull(node);
    }

    @Override
    protected DDlogExpression visitInListExpression(InListExpression node, TranslationContext context) {
        List<DDlogExpression> expressions = Linq.map(node.getValues(), n -> this.process(n, context));
        if (expressions.size() == 0)
            throw new TranslationException("Zero-sized list?", node);
        DDlogTArray type = new DDlogTArray(node, expressions.get(0).getType(), false);
        DDlogExpression result = new DDlogEApply(node, "vec_empty", type);
        for (DDlogExpression e: expressions)
            result = new DDlogEApply(node, "vec_push_imm", type, result, e);
        return result;
    }

    @Override
    protected DDlogExpression visitInPredicate(InPredicate node, TranslationContext context) {
        DDlogExpression value = this.process(node.getValue(), context);
        DDlogExpression list = this.process(node.getValueList(), context);
        if (list.is(DDlogEInRelation.class)) {
            DDlogEInRelation src = list.to(DDlogEInRelation.class);
            return new DDlogEInRelation(src.getNode(), value, src.relationName, src.getType());
        } else {
            return new DDlogEApply(node, "vec_contains", DDlogTBool.instance, list, value);
        }
    }

    @Override
    public DDlogExpression process(Node node, @Nullable TranslationContext context) {
        assert context != null;
        DDlogExpression subst = context.getSubstitution(node);
        if (subst != null)
            return subst;
        DDlogExpression translated = super.process(node, context);
        return checkHandled(translated, node);
    }

    @Override
    protected DDlogExpression visitSubqueryExpression(SubqueryExpression node, TranslationContext context) {
        DDlogIRNode query = context.cloneCtxt().translateQuery(node.getQuery());
        RuleBody rhs = query.to(RuleBody.class);
        RelationName relName = context.freshRelationName("sub");
        DDlogRule rule = context.createRule(node, null, relName, rhs, DDlogRelationDeclaration.Role.Internal);
        return new DDlogEInRelation(node, null, rule.head.relation.name, rhs.getType());
    }

    @Override
    protected DDlogExpression visitArithmeticBinary(
            ArithmeticBinaryExpression node, TranslationContext context) {
        DDlogExpression left = this.process(node.getLeft(), context);
        DDlogExpression right = this.process(node.getRight(), context);

        DDlogEBinOp.BOp op;
        switch (node.getOperator()) {
            case ADD:
                op = DDlogEBinOp.BOp.Plus;
                break;
            case SUBTRACT:
                op = DDlogEBinOp.BOp.Minus;
                break;
            case MULTIPLY:
                op = DDlogEBinOp.BOp.Times;
                break;
            case DIVIDE:
                op = DDlogEBinOp.BOp.Div;
                break;
            case MODULUS:
                op = DDlogEBinOp.BOp.Mod;
                break;
            default:
                throw new TranslationException("Unexpected node", node);
        }
        return operationCall(node, op, left, right);
    }

    /**
     * From an expression that produced Option[bool] extract
     * just a bool
     */
    static DDlogExpression unwrapBool(DDlogExpression expr) {
        if (expr.getType().mayBeNull)
            return new DDlogEApply(expr.getNode(), "unwrapBool", DDlogTBool.instance,
                    fixNull(expr, DDlogTBool.instance.setMayBeNull(true)));
        return expr;
    }

    @Override
    protected DDlogExpression visitBetweenPredicate(
            BetweenPredicate node, TranslationContext context) {
        DDlogExpression max = this.process(node.getMax(), context);
        DDlogExpression value = this.process(node.getValue(), context);
        DDlogExpression min = this.process(node.getMin(), context);
        DDlogExpression left = operationCall(node, DDlogEBinOp.BOp.Lte, min, value);
        DDlogExpression right = operationCall(node, DDlogEBinOp.BOp.Lte, value, max);
        return operationCall(node, DDlogEBinOp.BOp.And, left, right);
    }

    @Override
    protected DDlogExpression visitExtract(Extract node, TranslationContext context) {
        DDlogExpression from = this.process(node.getExpression(), context);
        Extract.Field field = node.getField();
        return new DDlogEApply(node, "sql_extract_" + field.toString().toLowerCase(),
                DDlogTSigned.signed64, from);
    }

    @Override
    protected DDlogExpression visitCoalesceExpression(
            CoalesceExpression node, TranslationContext context) {
        // Find first non-null expression
        for (Expression operand : node.getOperands()) {
            DDlogExpression e = this.process(operand, context);
        }
        // TODO: define a datatype for lists
        throw new UnsupportedOperationException();
    }

    @Override
    protected DDlogExpression visitDereferenceExpression(
            DereferenceExpression node, TranslationContext context) {
        Expression expression = node.getBase();
        if (!(expression instanceof Identifier))
            throw new TranslationException("Unexpected DereferenceExpression", node);
        Identifier id = (Identifier)expression;
        IEnvironment env = context.environment.lookupRelation(id.getValue());
        if (env == null)
            throw new TranslationException("Could not resolve relation " +
                    Utilities.singleQuote(id.toString()), id);
        context.environment.save();
        context.environment.set(env);
        DDlogExpression result = this.process(node.getField(), context);
        context.environment.restore();
        return result;
    }

    @Override
    protected DDlogExpression visitIdentifier(Identifier id, TranslationContext context) {
        DDlogExpression expr = context.environment.lookupIdentifier(id.getValue());
        if (expr == null)
            throw new TranslationException("Could not resolve identifier " +
                    Utilities.singleQuote(id.toString()), id);
        return expr;
    }

    @Override
    protected DDlogExpression visitComparisonExpression(
            ComparisonExpression node, TranslationContext context) {
        DDlogExpression left = this.process(node.getLeft(), context);
        DDlogExpression right = this.process(node.getRight(), context);
        DDlogEBinOp.BOp op;
        switch (node.getOperator()) {
            case EQUAL:
                op = DDlogEBinOp.BOp.Eq;
                break;
            case NOT_EQUAL:
            case IS_DISTINCT_FROM:
                op = DDlogEBinOp.BOp.Neq;
                break;
            case LESS_THAN:
                op = DDlogEBinOp.BOp.Lt;
                break;
            case LESS_THAN_OR_EQUAL:
                op = DDlogEBinOp.BOp.Lte;
                break;
            case GREATER_THAN:
                op = DDlogEBinOp.BOp.Gt;
                break;
            case GREATER_THAN_OR_EQUAL:
                op = DDlogEBinOp.BOp.Gte;
                break;
            default:
                throw new TranslationException("Unexpected node: ", node);
        }
        return operationCall(node, op, left, right);
    }

    @Override
    protected DDlogExpression visitIsNullPredicate(
            IsNullPredicate node, TranslationContext context) {
        DDlogExpression arg = this.process(node.getValue(), context);
        if (!arg.getType().mayBeNull) {
            context.warning("isNull can never be true", node);
            return new DDlogEBool(node, false);
        }
        return new DDlogEApply(node, "is_null", DDlogTBool.instance,
                fixNull(arg, DDlogTBool.instance.setMayBeNull(true)));
    }

    @Override
    protected DDlogExpression visitIsNotNullPredicate(IsNotNullPredicate node, TranslationContext context) {
        DDlogExpression arg = this.process(node.getValue(), context);
        if (!arg.getType().mayBeNull) {
            context.warning("isNotNull can never be false", node);
            return new DDlogEBool(node, true);
        }
        DDlogExpression isNull = new DDlogEApply(node, "is_null", DDlogTBool.instance,
                fixNull(arg, DDlogTBool.instance.setMayBeNull(true)));
        return new DDlogEUnOp(node, DDlogEUnOp.UOp.Not, isNull);
    }

    @Override
    protected DDlogExpression visitLogicalBinaryExpression(LogicalBinaryExpression node, TranslationContext context) {
        DDlogExpression left = this.process(node.getLeft(), context);
        DDlogExpression right = this.process(node.getRight(), context);
        if (left.is(DDlogEInRelation.class))
            return new DDlogEComma(node, left, right);
        DDlogEBinOp.BOp op;
        switch (node.getOperator()) {
            case AND:
                op = DDlogEBinOp.BOp.And;
                break;
            case OR:
                op = DDlogEBinOp.BOp.Or;
                break;
            default:
                throw new TranslationException("Unexpected node: ", node);
        }
        return operationCall(node, op, left, right);
    }

    @Override
    protected DDlogExpression visitArithmeticUnary(
            ArithmeticUnaryExpression node, TranslationContext context) {
        DDlogExpression value = this.process(node.getValue(), context);
        switch (node.getSign()) {
            case PLUS:
                return value;
            case MINUS:
                return new DDlogEUnOp(node, DDlogEUnOp.UOp.UMinus, value);
            default:
                throw new TranslationException("Unexpected node: ", node);
        }
    }

    private static DDlogType functionResultType(Node node, String function, List<DDlogExpression> args) {
        switch (function) {
            case "any":
            case "some":
            case "every":
            case "substr":
            case "min":
            case "max":
            case "avg":
            case "avg_distinct":
            case "sum":
            case "sum_distinct":
            case "abs":
                if (args.size() == 0)
                    throw new TranslationException("No arguments for aggregate?", node);
                return args.get(0).getType();
            case "count":
            case "count_distinct":
            case "array_length":
                if (args.size() == 0)
                    return DDlogTSigned.signed64;
                return DDlogTSigned.signed64.setMayBeNull(args.get(0).getType().mayBeNull);
            case "concat":
                boolean mayBeNull = Linq.any(args, a -> a.getType().mayBeNull);
                return DDlogTString.instance.setMayBeNull(mayBeNull);
            case "array_agg":
                if (args.size() != 1)
                    throw new TranslationException("Expected exactly 1 argument for aggregate", node);
                DDlogExpression arg = args.get(0);
                return new DDlogTArray(node, arg.getType(), false);
            case "array_contains":
                return DDlogTBool.instance;
            default:
                throw new UnsupportedOperationException(function);
        }
    }

    /**
     * The following aggregate functions compute the same result when using with
     * DISTINCT and without.
     */
    private static final Set<String> sameAsDistinct = Utilities.makeSet("min", "max", "some", "any", "every");
    public static String functionName(FunctionCall fc) {
        String name = Utilities.convertQualifiedName(fc.getName());
        if (fc.isDistinct() && !sameAsDistinct.contains(name))
            name += "_distinct";
        return name;
    }

    /**
     * The following functions can have any number of arguments in SQL.
     */
    private static final Set<String> varargs = Utilities.makeSet("concat");

    @Override
    protected DDlogExpression visitFunctionCall(FunctionCall node, TranslationContext context) {
        /*
        We ignore these; they are handled by our callers properly.
        if (node.getWindow().isPresent())
            throw new TranslationException("Not yet supported", node);
        if (node.getOrderBy().isPresent())
            throw new TranslationException("Not yet supported", node);
        if (node.getFilter().isPresent())
            throw new TranslationException("Not yet supported", node);
        */
        String name = functionName(node);
        List<Expression> arguments = node.getArguments();
        if (name.toLowerCase().equals("concat")) {
            // Cast each argument to a string
            arguments = Linq.map(arguments, a -> new Cast(a, "varchar"));
        }
        List<DDlogExpression> args = Linq.map(arguments, a -> this.process(a, context));
        DDlogType type = functionResultType(node, name, args);
        boolean someNull = Linq.any(args, a -> a.getType().mayBeNull);
        String useName = "sql_" + name;
        if (someNull)
            useName += "_N";
        if (varargs.contains(name)) {
            if (arguments.size() == 0)
                throw new TranslationException("Function does not have arguments", node);
            DDlogExpression result = args.get(0);
            for (int i = 1; i < args.size(); i++)
                result = new DDlogEApply(node, useName, type, result, args.get(i));
            return result;
        }
        return new DDlogEApply(node, useName, type, args.toArray(new DDlogExpression[0]));
    }

    @Override
    protected DDlogExpression visitNotExpression(NotExpression node, TranslationContext context) {
        DDlogExpression value = this.process(node.getValue(), context);
        if (value.getType().mayBeNull)
            return new DDlogEApply(node, "b_not_N", value.getType(),
                    fixNull(value, DDlogTBool.instance.setMayBeNull(true)));
        return new DDlogEUnOp(node, DDlogEUnOp.UOp.Not, value);
    }

    protected DDlogExpression caseExpression(
            Node node,
            List<DDlogExpression> comparisons,
            List<DDlogExpression> results,
            @Nullable
            DDlogExpression defaultValue) {
        assert comparisons.size() == results.size();
        // Compute result type
        List<DDlogType> types = Linq.map(results, DDlogExpression::getType);
        DDlogType resultType = DDlogType.reduceType(types);
        if (defaultValue != null)
            resultType = DDlogType.reduceType(defaultValue.getType(), resultType);
        else
            resultType = resultType.setMayBeNull(true);

        if (defaultValue != null) {
            if (defaultValue.getType().mayBeNull != resultType.mayBeNull) {
                defaultValue = wrapSome(defaultValue, resultType);
            } else {
                defaultValue = fixNull(defaultValue, resultType);
            }
        }
        DDlogExpression current = defaultValue;
        for (int i = 0; i < comparisons.size(); i++) {
            DDlogExpression label = comparisons.get(i);
            DDlogExpression result = results.get(i);
            if (result.getType().mayBeNull != resultType.mayBeNull) {
                result = wrapSome(result, resultType);
            } else {
                result = fixNull(result, resultType);
            }
            label = unwrapBool(label);
            current = new DDlogEITE(node, label, result, current);
        }
        if (current == null)
            throw new NullPointerException();
        return current;
    }

    @SuppressWarnings("DuplicatedCode")
    @Override
    protected DDlogExpression visitSearchedCaseExpression(SearchedCaseExpression expression, TranslationContext context) {
        DDlogExpression defaultValue = makeNull(expression);
        Optional<Expression> def = expression.getDefaultValue();
        if (def.isPresent())
            defaultValue = this.process(def.get(), context);
        List<WhenClause> whens = new ArrayList<WhenClause>(expression.getWhenClauses());
        Collections.reverse(whens);
        List<DDlogExpression> labels = Linq.map(whens, w -> this.process(w.getOperand(), context));
        List<DDlogExpression> results = Linq.map(whens, w -> this.process(w.getResult(), context));
        return caseExpression(expression, labels, results, defaultValue);
    }

    @SuppressWarnings("DuplicatedCode")
    @Override
    protected DDlogExpression visitSimpleCaseExpression(SimpleCaseExpression expression, TranslationContext context) {
        DDlogExpression op = this.process(expression.getOperand(), context);
        @Nullable
        DDlogExpression defaultValue = null;
        Optional<Expression> def = expression.getDefaultValue();
        if (def.isPresent())
            defaultValue = this.process(def.get(), context);
        List<WhenClause> whens = new ArrayList<WhenClause>(expression.getWhenClauses());
        Collections.reverse(whens);
        List<DDlogExpression> cases = Linq.map(whens, w -> this.process(w.getOperand(), context));
        List<DDlogExpression> results = Linq.map(whens, w -> this.process(w.getResult(), context));
        List<DDlogExpression> comparisons = Linq.map(cases, c -> operationCall(expression, DDlogEBinOp.BOp.Eq, op, c));
        return caseExpression(expression, comparisons, results, defaultValue);
    }

    @Override
    protected DDlogExpression visitIfExpression(IfExpression node, TranslationContext context) {
        DDlogExpression condition = this.process(node.getCondition(), context);
        DDlogExpression trueValue = this.process(node.getTrueValue(), context);
        DDlogExpression falseValue = null;
        if (node.getFalseValue().isPresent())
            falseValue = this.process(node.getFalseValue().get(), context);
        condition = unwrapBool(condition);
        return new DDlogEITE(node, condition, trueValue, falseValue);
    }

    @Override
    protected DDlogExpression visitDecimalLiteral(DecimalLiteral node, TranslationContext context) {
        return new DDlogEInt(node, new BigInteger(node.getValue(), 10));
    }

    @Override
    protected DDlogExpression visitLongLiteral(LongLiteral node, TranslationContext context) {
        return new DDlogESigned(node, 64, BigInteger.valueOf(node.getValue()));
    }

    @Override
    protected DDlogExpression visitBooleanLiteral(BooleanLiteral node, TranslationContext context) {
        return new DDlogEBool(node, node.getValue());
    }

    private static boolean isAsciiPrintable(int codePoint) {
        return codePoint < 0x7F && codePoint >= 0x20;
    }

    private static String formatStringLiteral(String s) {
        // This is taken from com.facebook.presto.sql.ExpressionFormatter, because
        // unfortunately it is not public.
        s = s.replace("'", "''");
        if (CharMatcher.inRange((char) 0x20, (char) 0x7E).matchesAllOf(s)) {
            return "'" + s + "'";
        }

        StringBuilder builder = new StringBuilder();
        builder.append("U&'");
        PrimitiveIterator.OfInt iterator = s.codePoints().iterator();
        while (iterator.hasNext()) {
            int codePoint = iterator.nextInt();
            Preconditions.checkArgument(codePoint >= 0,
                    "Invalid UTF-8 encoding in characters: %s", s);
            if (isAsciiPrintable(codePoint)) {
                char ch = (char) codePoint;
                if (ch == '\\') {
                    builder.append(ch);
                }
                builder.append(ch);
            }
            else if (codePoint <= 0xFFFF) {
                builder.append('\\');
                builder.append(String.format("%04X", codePoint));
            }
            else {
                builder.append("\\+");
                builder.append(String.format("%06X", codePoint));
            }
        }
        builder.append("'");
        return builder.toString();
    }

    @Override
    protected DDlogExpression visitStringLiteral(StringLiteral node, TranslationContext context) {
        String s = formatStringLiteral(node.getValue());
        return new DDlogEString(node, s);
    }
}
