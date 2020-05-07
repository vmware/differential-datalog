package com.vmware.ddlog.translator;

import com.facebook.presto.sql.tree.*;
import com.vmware.ddlog.ir.DDlogExpression;
import com.vmware.ddlog.util.Ternary;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;

/**
 * This visitor computes the AggregateDecomposition of an expression that
 * may contain aggregation functions over windows.
 * See for example https://www.postgresql.org/docs/9.1/tutorial-window.html
 * for a documentation of SQL Window functions.
 * It returns 'true' if the expression does contain window function aggregations.
 */
public class WindowVisitor
        extends AstVisitor<Ternary, TranslationContext> {

    public WindowVisitor() {
        this.groupOn = new ArrayList<TranslationVisitor.GroupByInfo>();
        aggregatedItems = new ArrayList<SelectItem>();
        inputItems = new ArrayList<SelectItem>();
    }

    /**
     * Let us consider the following query:
     * select 3 + count(column1) over (partition by column2) from t1
     *
     * This is decomposed as follows:
     * - groupByInfo: column2
     * - aggregatedItems: 3 + count(tmp) <<< cut at aggregation functions in over
     * - inputItems: column1 as tmp <<< argument to any aggregation function in over
     */
    public final List<TranslationVisitor.GroupByInfo> groupOn;
    public final List<SelectItem> aggregatedItems;
    public final List<SelectItem> inputItems;

    @Override
    protected Ternary visitFunctionCall(FunctionCall fc, TranslationContext context) {
        if (!fc.getWindow().isPresent())
            return Ternary.No;
        Window win = fc.getWindow().get();
        if (win.getOrderBy().isPresent())
            throw new TranslationException("group-by not supported", fc);
        if (win.getFrame().isPresent())
            throw new TranslationException("frames not yet supported", fc);
        List<Expression> partitions = win.getPartitionBy();
        for (Expression e: partitions) {
            DDlogExpression g = context.translateExpression(e);
            String gb = context.freshLocalName("gb");
            TranslationVisitor.GroupByInfo gr = new TranslationVisitor.GroupByInfo(e, g, gb);
            this.groupOn.add(gr);
        }
        String name = TranslationVisitor.convertQualifiedName(fc.getName());
        Ternary result = Ternary.Maybe;
        boolean isAggregate = SqlSemantics.semantics.isAggregateFunction(name);
        if (isAggregate) {
            // All arguments become inputItems
            List<Expression> args = new ArrayList<Expression>();
            for (Expression e: fc.getArguments()) {
                String var = context.freshLocalName("tmp");
                Identifier arg = new Identifier(var);
                args.add(arg);
                SelectItem si = new SingleColumn(e, arg);
                inputItems.add(si);
            }
            FunctionCall replacement = new FunctionCall(fc.getName(), args);
            aggregatedItems.add(new SingleColumn(replacement));
            return Ternary.Yes;
        }
        return result;
    }

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
