package com.vmware.ddlog.translator;

import com.facebook.presto.sql.tree.*;
import com.vmware.ddlog.util.Linq;
import com.vmware.ddlog.util.Ternary;

import java.util.*;

/**
 * This visitor computes the AggregateDecomposition of an expression that
 * may contain aggregation functions over windows.
 * See for example https://www.postgresql.org/docs/9.1/tutorial-window.html
 * for a documentation of SQL Window functions.
 * It returns 'true' if the expression does contain window function aggregations.
 */
public class WindowVisitor
        extends AggregationVisitorBase {
    /*
        Consider this query:

        SELECT c1, 3 + count(c3 + 1) OVER (PARTITION BY c2) FROM T

        This can be executed as multiple queries:
        - Prepare the input for the windowed computation.
          (This query could even involve WHERE/GROUP BY/AGGREGAT/HAVING if the original
           query contained them).

          CREATE VIEW OverInput AS
          SELECT c3 + 1 as tmp, c2 AS gb FROM T

        - Compute the aggregation (one for each window):

          CREATE VIEW Over AS
          SELECT count(tmp) as tmp2, gb FROM OverInput
          GROUP BY gb

        - join the original table with the aggregations and
          apply the computations to combine the results:

          SELECT T.c1, 3 + Over.tmp2 FROM T JOIN Over ON T.c2 = Over.c2
     */

    /**
     * Information required to compute a Window query
     */
    static class WindowAggregation {
        /**
         * Expressions defining the window, e.g., c3tmp in the
         * previous example.
         */
        public final List<SingleColumn> groupOn;
        /**
         * Values computed by each window.
         * E.g., count(c3tmp) in the previous example.
         * These will depend on temporary variables that
         * show up in the inputSelectItems below.
         */
        public final List<SingleColumn> windowResult;

        public GroupBy getGroupBy() {
            //noinspection OptionalGetWithoutIsPresent
            return new GroupBy(false,
                    Collections.singletonList(
                        new SimpleGroupBy(Linq.map(this.groupOn, s -> s.getAlias().get()))));
        }

        WindowAggregation() {
            this.groupOn = new ArrayList<SingleColumn>();
            this.windowResult = new ArrayList<SingleColumn>();
        }

        @Override
        public String toString() {
            return "GroupOn: " + this.groupOn.toString() + "\n" +
                    "Aggregated: " + this.windowResult.toString() + "\n";
        }
    }

    /**
     * Expressions computed by the first select.  Each has an alias
     * writing to a temporary variable, e.g., "c3tmp" in the previous example.
     */
    final List<SingleColumn> firstSelect;
    /**
     * Aggregation windows found in the current expression visited.
     * An expression can involve multiple windows: e.g.
     *
     * SELECT COUNT(col2) OVER (PARTITION BY col1) + COUNT(col1) OVER (PARTITION BY col2).
     * This would produce two WindowAggregation structures.
     */
    final List<WindowAggregation> windows;
    /**
     * In the final select these expressions should be substituted.
     */
    final SubstitutionRewriter.SubstitutionMap substitutions;

    public WindowVisitor() {
        this.windows = new ArrayList<WindowAggregation>();
        this.firstSelect = new ArrayList<SingleColumn>();
        this.substitutions = new SubstitutionRewriter.SubstitutionMap();
    }

    @Override
    protected Ternary visitFunctionCall(FunctionCall fc, TranslationContext context) {
        if (!fc.getWindow().isPresent())
            return Ternary.No;

        WindowAggregation wag = new WindowAggregation();
        this.windows.add(wag);
        Window win = fc.getWindow().get();
        if (win.getOrderBy().isPresent())
            throw new TranslationException("group-by not supported", fc);
        if (win.getFrame().isPresent())
            throw new TranslationException("frames not yet supported", fc);
        List<Expression> partitions = win.getPartitionBy();
        for (Expression e: partitions) {
            String gb = context.freshLocalName("gb");
            wag.groupOn.add(new SingleColumn(e, new Identifier(gb)));
        }
        String name = TranslationVisitor.convertQualifiedName(fc.getName());
        Ternary result = Ternary.Maybe;
        boolean isAggregate = SqlSemantics.semantics.isAggregateFunction(name);
        if (isAggregate) {
            List<Expression> newArguments = new ArrayList<Expression>();
            for (Expression e: fc.getArguments()) {
                // temporary name for input arguments to aggregation function;
                // e.g., tmp in our example
                String var = context.freshLocalName("tmp");
                Identifier arg = new Identifier(var);
                SingleColumn si = new SingleColumn(e, arg);
                this.firstSelect.add(si);
                newArguments.add(arg);
            }
            // temporary name for the result of the window function;
            // e.g., tmp2 in our example.
            String var = context.freshLocalName(fc.getName().toString());
            Identifier agg = new Identifier(var);
            FunctionCall noWindow = new FunctionCall(fc.getName(), Optional.empty(), fc.getFilter(),
                    fc.getOrderBy(), fc.isDistinct(), newArguments);
            wag.windowResult.add(new SingleColumn(noWindow, agg));
            this.substitutions.add(fc, agg);
            return Ternary.Yes;
        }
        return result;
    }
}
