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

import javax.annotation.Nullable;
import java.math.BigInteger;
import java.util.*;

public class ExpressionTranslationVisitor extends AstVisitor<DDlogExpression, TranslationContext> {
    @Override
    protected DDlogExpression visitArithmeticBinary(ArithmeticBinaryExpression node, TranslationContext context) {
        DDlogExpression left = process(node.getLeft(), context);
        DDlogExpression right = process(node.getRight(), context);
        if (!left.type.equals(right.type)) {
            throw new RuntimeException("Types do not match: " + left.type + " vs " + right.type);
        }
        switch (node.getOperator()) {
            case ADD:
                return new DDlogEBinOp(DDlogEBinOp.BOp.Plus, left, right);
            case SUBTRACT:
                return new DDlogEBinOp(DDlogEBinOp.BOp.Minus, left, right);
            case MULTIPLY:
                return new DDlogEBinOp(DDlogEBinOp.BOp.Times, left, right);
            case DIVIDE:
                return new DDlogEBinOp(DDlogEBinOp.BOp.Div, left, right);
            case MODULUS:
                return new DDlogEBinOp(DDlogEBinOp.BOp.Mod, left, right);
        }
        throw new RuntimeException("Unexpected node: " + node);
    }

    @Override
    protected DDlogExpression visitBetweenPredicate(BetweenPredicate node, TranslationContext context) {
        DDlogExpression max = process(node.getMax(), context);
        DDlogExpression value = process(node.getValue(), context);
        DDlogExpression min = process(node.getMin(), context);
        DDlogExpression left = new DDlogEBinOp(DDlogEBinOp.BOp.Lte, min, value);
        DDlogExpression right = new DDlogEBinOp(DDlogEBinOp.BOp.Lte, value, max);
        return new DDlogEBinOp(DDlogEBinOp.BOp.BAnd, left, right);
    }

    @Override
    protected DDlogExpression visitExtract(Extract node, TranslationContext context) {
        // TODO: define a datatype for dates
        throw new UnsupportedOperationException();
    }

    @Override
    protected DDlogExpression visitCoalesceExpression(CoalesceExpression node, TranslationContext context) {
        // Find first non-null expression
        for (Expression operand : node.getOperands()) {
            DDlogExpression e = process(operand, context);
        }
        // TODO: define a datatype for lists
        throw new UnsupportedOperationException();
    }

    @Override
    protected DDlogExpression visitIdentifier(Identifier id, TranslationContext context) {
        return new DDlogEVar(id.getValue());
    }

    @Override
    protected DDlogExpression visitComparisonExpression(ComparisonExpression node, TranslationContext context) {
        DDlogExpression left = process(node.getLeft(), context);
        DDlogExpression right = process(node.getRight(), context);
        switch (node.getOperator()) {
            case EQUAL:
                return new DDlogEBinOp(DDlogEBinOp.BOp.Eq, left, right);
            case NOT_EQUAL:
            case IS_DISTINCT_FROM:
                return new DDlogEBinOp(DDlogEBinOp.BOp.Neq, left, right);
            case LESS_THAN:
                return new DDlogEBinOp(DDlogEBinOp.BOp.Lt, left, right);
            case LESS_THAN_OR_EQUAL:
                return new DDlogEBinOp(DDlogEBinOp.BOp.Lte, left, right);
            case GREATER_THAN:
                return new DDlogEBinOp(DDlogEBinOp.BOp.Gt, left, right);
            case GREATER_THAN_OR_EQUAL:
                return new DDlogEBinOp(DDlogEBinOp.BOp.Gte, left, right);
            default:
                throw new RuntimeException("Unexpected node: " + node);
        }
    }

    @Override
    protected DDlogExpression visitLogicalBinaryExpression(LogicalBinaryExpression node, TranslationContext context) {
        DDlogExpression left = process(node.getLeft(), context);
        DDlogExpression right = process(node.getRight(), context);
        switch (node.getOperator()) {
            case AND:
                return new DDlogEBinOp(DDlogEBinOp.BOp.And, left, right);
            case OR:
                return new DDlogEBinOp(DDlogEBinOp.BOp.Or, left, right);
            default:
                throw new RuntimeException("Unexpected node: " + node);
        }
    }

    @Override
    protected DDlogExpression visitArithmeticUnary(ArithmeticUnaryExpression node, TranslationContext context) {
        DDlogExpression value = process(node.getValue(), context);
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
    protected DDlogExpression visitNotExpression(NotExpression node, TranslationContext context) {
        DDlogExpression value = process(node.getValue(), context);
        return new DDlogEUnOp(DDlogEUnOp.UOp.Not, value);
    }

    @Override
    protected DDlogExpression visitSimpleCaseExpression(SimpleCaseExpression expression, TranslationContext context) {
        DDlogExpression op = process(expression.getOperand());
        @Nullable
        DDlogExpression current = null;
        Optional<Expression> def = expression.getDefaultValue();
        if (def.isPresent())
            current = process(def.get(), context);
        List<WhenClause> whens = new ArrayList<WhenClause>(expression.getWhenClauses());
        Collections.reverse(whens);
        for (WhenClause w: whens) {
            DDlogExpression compared = process(w.getOperand(), context);
            DDlogExpression result = process(w.getResult(), context);
            current = new DDlogEITE(new DDlogEBinOp(DDlogEBinOp.BOp.Eq, op, compared), result, current);
        }
        if (current == null)
            throw new NullPointerException();
        return current;
    }

    @Override
    protected DDlogExpression visitIfExpression(IfExpression node, TranslationContext context) {
        DDlogExpression condition = process(node.getCondition(), context);
        DDlogExpression trueValue = process(node.getTrueValue(), context);
        DDlogExpression falseValue = null;
        if (node.getFalseValue().isPresent())
            falseValue = process(node.getFalseValue().get(), context);
        return new DDlogEITE(condition, trueValue, falseValue);
    }

    @Override
    protected DDlogExpression visitDecimalLiteral(DecimalLiteral node, TranslationContext context) {
        return new DDlogEInt(new BigInteger(node.getValue(), 10));
    }

    @Override
    protected DDlogExpression visitLongLiteral(LongLiteral node, TranslationContext context) {
        return new DDlogEBit(64, BigInteger.valueOf(node.getValue()));
    }

    @Override
    protected DDlogExpression visitBooleanLiteral(BooleanLiteral node, TranslationContext context) {
        return new DDlogEBool(node.getValue());
    }

    private static boolean isAsciiPrintable(int codePoint)
    {
        if (codePoint >= 0x7F || codePoint < 0x20) {
            return false;
        }
        return true;
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
            Preconditions.checkArgument(codePoint >= 0, "Invalid UTF-8 encoding in characters: %s", s);
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
