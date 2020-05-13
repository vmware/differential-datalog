package com.vmware.ddlog.translator;

import com.facebook.presto.sql.tree.*;
import com.vmware.ddlog.util.Ternary;

import java.util.ArrayList;
import java.util.List;

/**
 * This visitor computes the Decomposition of an expression.
 * It returns 'true' if the expression is the result of an aggregation.
 */
public class AggregateVisitor
        extends AggregationVisitorBase {
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

    Decomposition decomposition;
    private final List<TranslationVisitor.GroupByInfo> aggregates;
    boolean ignoreWindows;

    /**
     * Create a visitor that analyzes an expression to see whether it requires aggregation.
     * @param aggregates   Columns that are already being aggregated.
     */
    public AggregateVisitor(List<TranslationVisitor.GroupByInfo> aggregates) {
        this(aggregates, false);
    }

    /**
     * Create a visitor that analyzes an expression to see whether it requires aggregation.
     * @param aggregates   Columns that are already being aggregated.
     * @param ignoreWindows   If set we ignore window functions in aggregates.
     */
    public AggregateVisitor(List<TranslationVisitor.GroupByInfo> aggregates, boolean ignoreWindows) {
        this.ignoreWindows = ignoreWindows;
        this.decomposition = new Decomposition();
        this.aggregates = aggregates;
    }

    @Override
    protected Ternary visitFunctionCall(FunctionCall fc, TranslationContext context) {
        if (!ignoreWindows && fc.getWindow().isPresent())
            throw new TranslationException("Window functions not allowed in aggregates", fc);
        if (this.isGroupedBy(fc)) {
            this.decomposition.addNode(fc);
            return Ternary.Yes;
        }
        String name = TranslationVisitor.convertQualifiedName(fc.getName());
        Ternary result = Ternary.Maybe;
        boolean isAggregate = SqlSemantics.semantics.isAggregateFunction(name);
        if (isAggregate) {
            this.decomposition.addNode(fc);
        }
        for (Expression e: fc.getArguments()) {
            Ternary arg = this.process(e, context);
            if (isAggregate && arg == Ternary.Yes)
                throw new TranslationException("Nested aggregation", fc);
            result = this.combine(fc, result, arg);
        }
        if (isAggregate)
            return Ternary.Yes;
        return result;
    }

    public boolean isGroupedBy(Expression e) {
        for (TranslationVisitor.GroupByInfo a: this.aggregates) {
            if (e.equals(a.groupBy))
                return true;
        }
        return false;
    }

    @Override
    protected Ternary visitExpression(Expression node, TranslationContext context) {
        if (this.isGroupedBy(node))
            return Ternary.Yes;
        return super.visitExpression(node, context);
    }

    @Override
    protected Ternary visitCast(Cast node, TranslationContext context) {
        if (this.isGroupedBy(node))
            return Ternary.Yes;
        return super.visitCast(node, context);
    }

    @Override
    protected Ternary visitArithmeticBinary(ArithmeticBinaryExpression node, TranslationContext context) {
        if (this.isGroupedBy(node))
            return Ternary.Yes;
        return super.visitArithmeticBinary(node, context);
    }

    @Override
    protected Ternary visitNotExpression(NotExpression node, TranslationContext context) {
        if (this.isGroupedBy(node))
            return Ternary.Yes;
        return super.visitNotExpression(node, context);
    }

    @Override
    protected Ternary visitBetweenPredicate(BetweenPredicate node, TranslationContext context) {
        if (this.isGroupedBy(node))
            return Ternary.Yes;
        return super.visitBetweenPredicate(node, context);
    }

    @Override
    protected Ternary visitIdentifier(Identifier node, TranslationContext context) {
        if (this.isGroupedBy(node))
            return Ternary.Yes;
        return Ternary.No;
    }

    @Override
    protected Ternary visitLiteral(Literal node, TranslationContext context) {
        if (this.isGroupedBy(node))
            return Ternary.Yes;
        return Ternary.Maybe;
    }

    @Override
    protected Ternary visitIfExpression(IfExpression node, TranslationContext context) {
        if (this.isGroupedBy(node))
            return Ternary.Yes;
        return super.visitIfExpression(node, context);
    }

    @Override
    protected Ternary visitComparisonExpression(ComparisonExpression node, TranslationContext context) {
        if (this.isGroupedBy(node))
            return Ternary.Yes;
        return super.visitComparisonExpression(node, context);
    }

    @Override
    protected Ternary visitLogicalBinaryExpression(LogicalBinaryExpression node, TranslationContext context) {
        if (this.isGroupedBy(node))
            return Ternary.Yes;
        return super.visitLogicalBinaryExpression(node, context);
    }

    @Override
    protected Ternary visitDereferenceExpression(DereferenceExpression node, TranslationContext context) {
        if (this.isGroupedBy(node))
            return Ternary.Yes;
        return Ternary.Maybe;
    }

    @Override
    protected Ternary visitSimpleCaseExpression(SimpleCaseExpression node, TranslationContext context) {
        if (this.isGroupedBy(node))
            return Ternary.Yes;
        return super.visitSimpleCaseExpression(node, context);
    }

    @Override
    protected Ternary visitSearchedCaseExpression(SearchedCaseExpression node, TranslationContext context) {
        if (this.isGroupedBy(node))
            return Ternary.Yes;
        return super.visitSearchedCaseExpression(node, context);
    }
}
