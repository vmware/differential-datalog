package com.vmware.ddlog.translator;

import com.facebook.presto.sql.tree.*;
import com.vmware.ddlog.util.Ternary;

import javax.annotation.Nullable;

/**
 * Abstract base class for decomposing expressions containing aggregates into subexpressions.
 */
public abstract class AggregationVisitorBase extends AstVisitor<Ternary, TranslationContext> {
    @Override
    protected abstract Ternary visitFunctionCall(FunctionCall fc, TranslationContext context);

    public Ternary combine(Node node, @Nullable Ternary left, @Nullable Ternary right) {
        if (left == null || right == null)
            throw new TranslationException("Not yet implemented", node);
        if (left == Ternary.Maybe)
            return right;
        if (right == Ternary.Maybe)
            return left;
        if (left != right)
            this.error(node);
        return left;
    }

    @Override
    protected Ternary visitCast(Cast node, TranslationContext context) {
        return this.process(node.getExpression(), context);
    }

    @Override
    protected Ternary visitArithmeticBinary(ArithmeticBinaryExpression node, TranslationContext context) {
        Ternary lb = this.process(node.getLeft(), context);
        Ternary rb = this.process(node.getRight(), context);
        return this.combine(node, lb, rb);
    }

    @Override
    protected Ternary visitDereferenceExpression(DereferenceExpression node, TranslationContext context) {
        return Ternary.Maybe;
    }

    @Override
    protected Ternary visitNotExpression(NotExpression node, TranslationContext context) {
        return this.process(node.getValue(), context);
    }

    @Override
    protected Ternary visitBetweenPredicate(BetweenPredicate node, TranslationContext context) {
        Ternary value = this.process(node.getValue(), context);
        Ternary min = this.process(node.getMin(), context);
        Ternary max = this.process(node.getMax(), context);
        return this.combine(node, this.combine(node, value, min), max);
    }

    @Override
    protected Ternary visitIdentifier(Identifier node, TranslationContext context) {
        return Ternary.No;
    }

    @Override
    protected Ternary visitLiteral(Literal node, TranslationContext context) {
        return Ternary.Maybe;
    }

    private void error(Node node) {
        throw new TranslationException("Operation between aggregated and non-aggregated values", node);
    }

    @Override
    protected Ternary visitIfExpression(IfExpression node, TranslationContext context) {
        Ternary c = this.process(node.getCondition(), context);
        Ternary th = this.process(node.getTrueValue(), context);
        Ternary e = node.getFalseValue().isPresent() ? Ternary.Maybe :
                this.process(node.getFalseValue().get(), context);
        return this.combine(node, this.combine(node, c, th), e);
    }

    @Override
    protected Ternary visitComparisonExpression(ComparisonExpression node, TranslationContext context) {
        Ternary lb = this.process(node.getLeft(), context);
        Ternary rb = this.process(node.getRight(), context);
        return this.combine(node, lb, rb);
    }

    @Override
    protected Ternary visitLogicalBinaryExpression(LogicalBinaryExpression node, TranslationContext context) {
        Ternary lb = this.process(node.getLeft(), context);
        Ternary rb = this.process(node.getRight(), context);
        return this.combine(node, lb, rb);
    }

    @Override
    protected Ternary visitSimpleCaseExpression(SimpleCaseExpression node, TranslationContext context) {
        Ternary c = this.process(node.getOperand(), context);
        if (c == null)
            throw new TranslationException("Not supported: ", node.getOperand());
        for (WhenClause e: node.getWhenClauses()) {
            Ternary o = this.process(e.getOperand(), context);
            if (o == null)
                throw new TranslationException("Not supported: ", node.getOperand());
            Ternary v = this.process(e.getResult(), context);
            if (v == null)
                throw new TranslationException("Not supported: ", node.getOperand());
            Ternary s = this.combine(e, o, v);
            c = this.combine(node, c, s);
        }
        if (node.getDefaultValue().isPresent()) {
            Ternary v = this.process(node.getDefaultValue().get(), context);
            if (v == null)
                throw new TranslationException("Not supported: ", node.getOperand());
            c = this.combine(node, v, c);
        }
        return c;
    }

    @Override
    protected Ternary visitSearchedCaseExpression(SearchedCaseExpression node, TranslationContext context) {
        Ternary c = Ternary.Maybe;
        for (WhenClause e: node.getWhenClauses()) {
            Ternary o = this.process(e.getOperand(), context);
            Ternary v = this.process(e.getResult(), context);
            Ternary s = this.combine(e, o, v);
            c = this.combine(node, c, s);
        }
        return c;
    }
}
