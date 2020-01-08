package com.vmware.ddlog.translator;

import com.facebook.presto.sql.tree.*;
import com.vmware.ddlog.util.Ternary;

import java.util.ArrayList;
import java.util.List;

/**
 * This visitor computes the AggregateDecomposition of an expression.
 * It returns 'true' if the expression is the result of an aggregation.
 */
public class AggregateVisitor
        extends AstVisitor<Ternary, TranslationContext> {
    /**
     * This class represents the decomposition of an expression that contains
     * aggregates into multiple expressions.  Consider this example:
     *
     * select max(salaries) + min(abs(salaries)) from employees
     *
     * Consider the expression tree
     * + ( functionCall(max, salaries), functionCall(min, functionCall(min, salaries)))
     * The Aggregate decomposition will have aggregates pointing to
     * the functionCall(max, ...) and functionCall(min, ...) tree nodes.
     */
    public static class Decomposition {
        public final List<FunctionCall> aggregateNodes;

        Decomposition() {
            this.aggregateNodes = new ArrayList<FunctionCall>();
        }

        void addNode(FunctionCall node) {
            this.aggregateNodes.add(node);
        }

        public boolean isAggregate() {
            return !this.aggregateNodes.isEmpty();
        }
    }

    Decomposition result;

    public AggregateVisitor() {
        result = new Decomposition();
    }

    @Override
    protected Ternary visitFunctionCall(FunctionCall node, TranslationContext context) {
        String name = TranslationVisitor.convertQualifiedName(node.getName());
        Ternary result = Ternary.Maybe;
        boolean isAggregate = SqlSemantics.semantics.isAggregateFunction(name);
        for (Expression e: node.getArguments()) {
            Ternary arg = this.process(e, context);
            if (isAggregate && arg == Ternary.Yes)
                throw new TranslationException("Nested aggregation", node);
            if (arg != Ternary.Maybe)
                result = arg;
        }
        this.result.addNode(node);
        if (isAggregate)
            return Ternary.Yes;
        return result;
    }

    public Ternary combine(Node node, Ternary left, Ternary right) {
        if (left == Ternary.Maybe)
            return right;
        if (right == Ternary.Maybe)
            return left;
        if (left != right)
            this.error(node);
        return left;
    }

    @Override
    protected Ternary visitArithmeticBinary(ArithmeticBinaryExpression node, TranslationContext context) {
        Ternary lb = this.process(node.getLeft(), context);
        Ternary rb = this.process(node.getRight(), context);
        return this.combine(node, lb, rb);
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
        for (WhenClause e: node.getWhenClauses()) {
            Ternary o = this.process(e.getOperand(), context);
            Ternary v = this.process(e.getResult(), context);
            Ternary s = this.combine(e, o, v);
            c = this.combine(node, c, s);
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
