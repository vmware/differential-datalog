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

import javax.annotation.Nullable;
import java.math.BigInteger;
import java.util.*;

public class ExpressionTranslationVisitor extends AstVisitor<DDlogExpression, TranslationContext> {
    public DDlogExpression operationCall(DDlogEBinOp.BOp op, DDlogExpression left, DDlogExpression right) {
        String function = SqlSemantics.semantics.getFunction(op, left.getType(), right.getType());
        DDlogType type = DDlogType.reduceType(left.getType(), right.getType());
        if (op.isComparison() || op.isBoolean()) {
            type = DDlogTBool.instance.setMayBeNull(type.mayBeNull);
        }
        if (function.endsWith("RR"))
            // optimize for the case of no nulls
            return new DDlogEBinOp(op, left, right);
        return new DDlogEApply(function, type, left, right);
    }

    @Override
    public DDlogExpression process(Node node, @Nullable TranslationContext context) {
        assert context != null;
        DDlogExpression subst = context.getSubstitution(node);
        if (subst != null)
            return subst;
        return super.process(node, context);
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
        return this.operationCall(op, left, right);
    }

    /**
     * From an expression that produced Option[bool] extract
     * just a bool
     */
    static DDlogExpression unwrapBool(DDlogExpression expr) {
        if (expr.getType().mayBeNull)
            return new DDlogEApply("unwrapBool", DDlogTBool.instance, expr);
        return expr;
    }

    @Override
    protected DDlogExpression visitBetweenPredicate(
            BetweenPredicate node, TranslationContext context) {
        DDlogExpression max = this.process(node.getMax(), context);
        DDlogExpression value = this.process(node.getValue(), context);
        DDlogExpression min = this.process(node.getMin(), context);
        DDlogExpression left = this.operationCall(DDlogEBinOp.BOp.Lte, min, value);
        DDlogExpression right = this.operationCall(DDlogEBinOp.BOp.Lte, value, max);
        return this.operationCall(DDlogEBinOp.BOp.And, left, right);
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
        return this.operationCall(op, left, right);
    }

    @Override
    protected DDlogExpression visitIsNullPredicate(
            IsNullPredicate node, TranslationContext context) {
        DDlogExpression arg = this.process(node.getValue(), context);
        if (!arg.getType().mayBeNull) {
            context.warning("isNull can never be true", node);
            return new DDlogEBool(false);
        }
        return new DDlogEApply("isNull", DDlogTBool.instance, arg);
    }

    @Override
    protected DDlogExpression visitIsNotNullPredicate(IsNotNullPredicate node, TranslationContext context) {
        DDlogExpression arg = this.process(node.getValue(), context);
        if (!arg.getType().mayBeNull) {
            context.warning("isNotNull can never be true", node);
            return new DDlogEBool(true);
        }
        DDlogExpression isNull = new DDlogEApply("isNull", DDlogTBool.instance, arg);
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
        return this.operationCall(op, left, right);
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

    @SuppressWarnings("unused")
    private DDlogType functionResultType(String function, List<DDlogExpression> args, TranslationContext context) {
        switch (function) {
            case "substr":
                return args.get(0).getType();
            case "min":
            case "max":
            case "avg":
            case "sum":
            case "abs":
                if (args.size() == 0)
                    throw new RuntimeException("No arguments for aggregate?");
                return args.get(0).getType();
            case "count":
                return DDlogTSigned.signed64;
            default:
                throw new UnsupportedOperationException(function);
        }
    }

    @Override
    protected DDlogExpression visitFunctionCall(FunctionCall node, TranslationContext context) {
        if (node.getWindow().isPresent())
            throw new TranslationException("Not yet supported", node);
        if (node.getOrderBy().isPresent())
            throw new TranslationException("Not yet supported", node);
        if (node.getFilter().isPresent())
            throw new TranslationException("Not yet supported", node);
        if (node.isDistinct())
            throw new TranslationException("Not yet supported", node);
        String name = TranslationVisitor.convertQualifiedName(node.getName());
        List<DDlogExpression> args = Linq.map(node.getArguments(), a -> this.process(a, context));
        DDlogType type = this.functionResultType(name, args, context);
        boolean someNull = Linq.any(args, a -> a.getType().mayBeNull);
        if (someNull)
            name += "_N";
        return new DDlogEApply(name, type, args.toArray(new DDlogExpression[0]));
    }

    @Override
    protected DDlogExpression visitNotExpression(NotExpression node, TranslationContext context) {
        DDlogExpression value = this.process(node.getValue(), context);
        return new DDlogEUnOp(DDlogEUnOp.UOp.Not, value);
    }

    private DDlogExpression makeNull() {
        return new DDlogEStruct("None", new ArrayList<DDlogEStruct.FieldValue>(), new
                DDlogTUser("Option", false));
    }

    @Override
    protected DDlogExpression visitSearchedCaseExpression(SearchedCaseExpression expression, TranslationContext context) {
        DDlogExpression current = this.makeNull();
        Optional<Expression> def = expression.getDefaultValue();
        if (def.isPresent())
            current = this.process(def.get(), context);
        List<WhenClause> whens = new ArrayList<WhenClause>(expression.getWhenClauses());
        Collections.reverse(whens);
        for (WhenClause w: whens) {
            DDlogExpression label = this.process(w.getOperand(), context);
            DDlogExpression result = this.process(w.getResult(), context);
            label = unwrapBool(label);
            current = new DDlogEITE(label, result, current);
        }
        return current;
    }

    @Override
    protected DDlogExpression visitSimpleCaseExpression(SimpleCaseExpression expression, TranslationContext context) {
        DDlogExpression op = this.process(expression.getOperand(), context);
        @Nullable
        DDlogExpression current = null;
        Optional<Expression> def = expression.getDefaultValue();
        if (def.isPresent())
            current = this.process(def.get(), context);
        List<WhenClause> whens = new ArrayList<WhenClause>(expression.getWhenClauses());
        Collections.reverse(whens);
        for (WhenClause w: whens) {
            DDlogExpression compared = this.process(w.getOperand(), context);
            DDlogExpression result = this.process(w.getResult(), context);
            DDlogExpression eq = this.operationCall(DDlogEBinOp.BOp.Eq, op, compared);
            eq = unwrapBool(eq);
            current = new DDlogEITE(eq, result, current);
        }
        if (current == null)
            throw new NullPointerException();
        return current;
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
