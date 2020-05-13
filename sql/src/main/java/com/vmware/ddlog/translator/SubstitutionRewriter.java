package com.vmware.ddlog.translator;

import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.ExpressionRewriter;
import com.facebook.presto.sql.tree.ExpressionTreeRewriter;

import javax.annotation.Nullable;
import java.util.HashMap;
import java.util.Map;

public class SubstitutionRewriter extends
        ExpressionRewriter<Void> {
    private final SubstitutionRewriter.SubstitutionMap map;

    public SubstitutionRewriter(SubstitutionRewriter.SubstitutionMap map) {
        super();
        this.map = map;
    }

    static class SubstitutionMap {
        private final Map<Expression, Expression> substitution;

        public SubstitutionMap() {
            substitution = new HashMap<Expression, Expression>();
        }

        public void add(Expression from, Expression to) {
            this.substitution.put(from, to);
        }

        @Nullable
        Expression get(Expression from) {
            if (this.substitution.containsKey(from))
                return this.substitution.get(from);
            return null;
        }
    }

    @Override
    public Expression rewriteExpression(Expression node, Void context, ExpressionTreeRewriter<Void> treeRewriter)
    {
        Expression result = this.map.get(node);
        if (result == null)
            return treeRewriter.defaultRewrite(node, context);
        return result;
    }
}
