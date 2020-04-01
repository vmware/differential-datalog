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
import com.google.common.base.CharMatcher;
import com.google.common.base.Preconditions;
import com.vmware.ddlog.ir.*;
import com.vmware.ddlog.util.Linq;
import com.vmware.ddlog.util.Utilities;

import javax.annotation.Nullable;
import java.math.BigInteger;
import java.util.*;

public class ExpressionTranslationVisitor extends AstVisitor<DDlogExpression, TranslationContext> {
    public static DDlogExpression checkHandled(@Nullable DDlogExpression expression, Node node) {
        if (expression == null)
            throw new TranslationException("Expression not handled", node);
        return expression;
    }

    private static DDlogExpression makeNull() {
        return new DDlogENull();
    }

    /**
     * If the expression is a NULL, set its type as indicated.
     */
    private static DDlogExpression fixNull(DDlogExpression expr, DDlogType type) {
        if (!expr.is(DDlogENull.class))
            return expr;
        if (!type.mayBeNull)
            throw new RuntimeException("Type of NULL should be nullable " + type);
        if (type.is(DDlogTUnknown.class))
            throw new RuntimeException("Cannot infer type for null");
        return new DDlogENull(type);
    }

    /**
     * expr has a non-nullable type; wrap it into the nullable version.
     * @param expr expression to wrap
     * @param type result type
     * @return Some{expr}
     */
    public static DDlogExpression wrapSome(DDlogExpression expr, DDlogType type) {
        return new DDlogEStruct("Some", type,
                new DDlogEStruct.FieldValue("x", expr));
    }

    public static DDlogExpression operationCall(DDlogEBinOp.BOp op, DDlogExpression left, DDlogExpression right) {
        String function = SqlSemantics.semantics.getFunction(op, left.getType(), right.getType());
        DDlogType type = DDlogType.reduceType(left.getType(), right.getType());
        if (op.isComparison() || op.isBoolean()) {
            type = DDlogTBool.instance.setMayBeNull(type.mayBeNull);
        }
        if (function.endsWith("RR"))
            // optimize for the case of no nulls
            return new DDlogEBinOp(op, left, right);
        return new DDlogEApply(function, type, fixNull(left, type), fixNull(right, type));
    }

    @Override
    protected DDlogExpression visitCast(Cast node, TranslationContext context) {
        DDlogExpression e = this.process(node.getExpression(), context);
        DDlogType type = SqlSemantics.createType(node.getType(), e.getType().mayBeNull);
        return new DDlogEAs(e, type);
    }

    @Override
    protected DDlogExpression visitNullLiteral(NullLiteral node, TranslationContext context) {
        return makeNull();
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
        return operationCall(op, left, right);
    }

    /**
     * From an expression that produced Option[bool] extract
     * just a bool
     */
    static DDlogExpression unwrapBool(DDlogExpression expr) {
        if (expr.getType().mayBeNull)
            return new DDlogEApply("unwrapBool", DDlogTBool.instance,
                    fixNull(expr, DDlogTBool.instance.setMayBeNull(true)));
        return expr;
    }

    @Override
    protected DDlogExpression visitBetweenPredicate(
            BetweenPredicate node, TranslationContext context) {
        DDlogExpression max = this.process(node.getMax(), context);
        DDlogExpression value = this.process(node.getValue(), context);
        DDlogExpression min = this.process(node.getMin(), context);
        DDlogExpression left = operationCall(DDlogEBinOp.BOp.Lte, min, value);
        DDlogExpression right = operationCall(DDlogEBinOp.BOp.Lte, value, max);
        return operationCall(DDlogEBinOp.BOp.And, left, right);
    }

    @Override
    protected DDlogExpression visitExtract(Extract node, TranslationContext context) {
        // TODO: define a datatype for dates
        throw new UnsupportedOperationException();
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
    protected DDlogExpression visitIdentifier(Identifier id, TranslationContext context) {
        DDlogExpression expr = context.lookupIdentifier(id.getValue());
        if (expr == null)
            throw new TranslationException("Could not resolve identifier", id);
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
                throw new RuntimeException("Unexpected node: " + node);
        }
        return operationCall(op, left, right);
    }

    @Override
    protected DDlogExpression visitIsNullPredicate(
            IsNullPredicate node, TranslationContext context) {
        DDlogExpression arg = this.process(node.getValue(), context);
        if (!arg.getType().mayBeNull) {
            context.warning("isNull can never be true", node);
            return new DDlogEBool(false);
        }
        return new DDlogEApply("is_null", DDlogTBool.instance,
                fixNull(arg, DDlogTBool.instance.setMayBeNull(true)));
    }

    @Override
    protected DDlogExpression visitIsNotNullPredicate(IsNotNullPredicate node, TranslationContext context) {
        DDlogExpression arg = this.process(node.getValue(), context);
        if (!arg.getType().mayBeNull) {
            context.warning("isNotNull can never be false", node);
            return new DDlogEBool(true);
        }
        DDlogExpression isNull = new DDlogEApply("isNull", DDlogTBool.instance,
                fixNull(arg, DDlogTBool.instance.setMayBeNull(true)));
        return new DDlogEUnOp(DDlogEUnOp.UOp.BNeg, isNull);
    }

    @Override
    protected DDlogExpression visitLogicalBinaryExpression(LogicalBinaryExpression node, TranslationContext context) {
        DDlogExpression left = this.process(node.getLeft(), context);
        DDlogExpression right = this.process(node.getRight(), context);
        DDlogEBinOp.BOp op;
        switch (node.getOperator()) {
            case AND:
                op = DDlogEBinOp.BOp.And;
                break;
            case OR:
                op = DDlogEBinOp.BOp.Or;
                break;
            default:
                throw new RuntimeException("Unexpected node: " + node);
        }
        return operationCall(op, left, right);
    }

    @Override
    protected DDlogExpression visitArithmeticUnary(
            ArithmeticUnaryExpression node, TranslationContext context) {
        DDlogExpression value = this.process(node.getValue(), context);
        switch (node.getSign()) {
            case PLUS:
                return value;
            case MINUS:
                return new DDlogEUnOp(DDlogEUnOp.UOp.UMinus, value);
            default:
                throw new RuntimeException("Unexpected node: " + node);
        }
    }

    @Override
    protected DDlogExpression visitDereferenceExpression(
            DereferenceExpression node, TranslationContext context) {
         context.searchScope(true);
         DDlogExpression scope = this.process(node.getBase(), context);
         context.searchScope(false);
         DDlogScope ddscope = scope.as(DDlogScope.class, null);
         context.enterScope(ddscope.getScope());  // we look up the next identifier in this scope.
         DDlogExpression result = this.process(node.getField(), context);
         context.exitScope();
         return result;
    }

    private static DDlogType functionResultType(String function, List<DDlogExpression> args) {
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
                    throw new RuntimeException("No arguments for aggregate?");
                return args.get(0).getType();
            case "count":
            case "count_distinct":
                if (args.size() == 0)
                    return DDlogTSigned.signed64;
                return DDlogTSigned.signed64.setMayBeNull(args.get(0).getType().mayBeNull);
            default:
                throw new UnsupportedOperationException(function);
        }
    }

    /**
     * The following aggregate functions compute the same result when using with
     * DISTINCT and without.
     */
    private static Set<String> sameAsDistinct = Utilities.makeSet("min", "max", "some", "any", "every");
    public static String functionName(FunctionCall fc) {
        String name = TranslationVisitor.convertQualifiedName(fc.getName());
        if (fc.isDistinct() && !sameAsDistinct.contains(name))
            name += "_distinct";
        return name;
    }

    @Override
    protected DDlogExpression visitFunctionCall(FunctionCall node, TranslationContext context) {
        if (node.getWindow().isPresent())
            throw new TranslationException("Not yet supported", node);
        if (node.getOrderBy().isPresent())
            throw new TranslationException("Not yet supported", node);
        if (node.getFilter().isPresent())
            throw new TranslationException("Not yet supported", node);
        String name = functionName(node);
        List<DDlogExpression> args = Linq.map(node.getArguments(), a -> this.process(a, context));
        DDlogType type = functionResultType(name, args);
        boolean someNull = Linq.any(args, a -> a.getType().mayBeNull);
        if (someNull)
            name += "_N";
        return new DDlogEApply(name, type, args.toArray(new DDlogExpression[0]));
    }

    @Override
    protected DDlogExpression visitNotExpression(NotExpression node, TranslationContext context) {
        DDlogExpression value = this.process(node.getValue(), context);
        if (value.getType().mayBeNull)
            return new DDlogEApply("b_not_N", value.getType(),
                    fixNull(value, DDlogTBool.instance.setMayBeNull(true)));
        return new DDlogEUnOp(DDlogEUnOp.UOp.Not, value);
    }

    protected DDlogExpression caseExpression(
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
            current = new DDlogEITE(label, result, current);
        }
        if (current == null)
            throw new NullPointerException();
        return current;
    }

    @Override
    protected DDlogExpression visitSearchedCaseExpression(SearchedCaseExpression expression, TranslationContext context) {
        DDlogExpression defaultValue = makeNull();
        Optional<Expression> def = expression.getDefaultValue();
        if (def.isPresent())
            defaultValue = this.process(def.get(), context);
        List<WhenClause> whens = new ArrayList<WhenClause>(expression.getWhenClauses());
        Collections.reverse(whens);
        List<DDlogExpression> labels = Linq.map(whens, w -> this.process(w.getOperand(), context));
        List<DDlogExpression> results = Linq.map(whens, w -> this.process(w.getResult(), context));
        return caseExpression(labels, results, defaultValue);
    }

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
        List<DDlogExpression> comparisons = Linq.map(cases, c -> operationCall(DDlogEBinOp.BOp.Eq, op, c));
        return caseExpression(comparisons, results, defaultValue);
    }

    @Override
    protected DDlogExpression visitIfExpression(IfExpression node, TranslationContext context) {
        DDlogExpression condition = this.process(node.getCondition(), context);
        DDlogExpression trueValue = this.process(node.getTrueValue(), context);
        DDlogExpression falseValue = null;
        if (node.getFalseValue().isPresent())
            falseValue = this.process(node.getFalseValue().get(), context);
        condition = unwrapBool(condition);
        return new DDlogEITE(condition, trueValue, falseValue);
    }

    @Override
    protected DDlogExpression visitDecimalLiteral(DecimalLiteral node, TranslationContext context) {
        return new DDlogEInt(new BigInteger(node.getValue(), 10));
    }

    @Override
    protected DDlogExpression visitLongLiteral(LongLiteral node, TranslationContext context) {
        return new DDlogESigned(64, BigInteger.valueOf(node.getValue()));
    }

    @Override
    protected DDlogExpression visitBooleanLiteral(BooleanLiteral node, TranslationContext context) {
        return new DDlogEBool(node.getValue());
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
        return new DDlogEString(s);
    }
}
