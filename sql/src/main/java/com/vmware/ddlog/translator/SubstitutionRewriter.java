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
