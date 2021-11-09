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

package com.vmware.ddlog.ir;

import com.facebook.presto.sql.tree.Node;

import javax.annotation.Nullable;
import java.util.Objects;

/**
 * This is not a standard DDlog expression.  It stands for an expression
 * of the form R[Type{e}].  This expression is returned by SubqueryExpression.
 */
public class DDlogEInRelation extends DDlogExpression {
    @Nullable
    public final DDlogExpression var;
    public final String relationName;

    /**
     * Create an expression of the form R[v]
     * @param node            Original source node.
     * @param var             Expression used to index relation.  May be filled in later (then it's null).
     * @param relationName    Relation name.
     * @param type            Type of variable.
     */
    public DDlogEInRelation(@Nullable Node node, @Nullable DDlogExpression var, String relationName, DDlogType type) {
        super(node, type);
        this.var = var;
        this.relationName = relationName;
    }

    @Override
    public String toString() {
        return this.relationName + "[" +
                Objects.requireNonNull(this.type).toString() +
                "{" + Objects.requireNonNull(this.var).toString() + "}]";
    }
}
