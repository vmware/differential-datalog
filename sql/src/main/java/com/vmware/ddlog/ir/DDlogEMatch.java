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
import com.vmware.ddlog.util.Linq;

import javax.annotation.Nullable;
import java.util.List;

public class DDlogEMatch extends DDlogExpression {
    public DDlogEMatch(@Nullable Node node, DDlogExpression matchExpr, List<Case> cases) {
        super(node, DDlogType.sameType(Linq.map(cases, c -> c.second.getType())));
        this.matchExpr = matchExpr;
        this.cases = cases;
    }

    public static final class Case extends DDlogNode {
        final DDlogExpression first;
        final DDlogExpression second;

        public Case(@Nullable Node node, DDlogExpression first, DDlogExpression second) {
            super(node);
            this.first = first;
            this.second = second;
        }

        @Override
        public void accept(DDlogVisitor visitor) {
            if (!visitor.preorder(this)) return;
            this.first.accept(visitor);
            this.second.accept(visitor);
            visitor.postorder(this);
        }

        @Override
        public String toString() {
            return this.first + " -> " + this.second;
        }
    }

    @Override
    public void accept(DDlogVisitor visitor) {
        visitor.preorder(this);
        this.matchExpr.accept(visitor);
        for (Case c: this.cases)
            c.accept(visitor);
        visitor.postorder(this);
    }

    private final DDlogExpression matchExpr;
    private final List<Case> cases;

    @Override
    public String toString() {
        return "match(" + this.matchExpr + ") {" +
            String.join(",\n", Linq.map(this.cases, Case::toString)) +
                "\n}";
    }
}
