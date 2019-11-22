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

package com.vmware.ddlog.ir;

import com.vmware.ddlog.util.Linq;

import java.util.List;

public class DDlogEMatch extends DDlogExpression {
    public DDlogEMatch(DDlogExpression matchExpr, List<Case> cases) {
        super(DDlogType.reduceType(Linq.map(cases, c -> c.second.getType())));
        this.matchExpr = matchExpr;
        this.cases = cases;
    }

    public static final class Case implements DDlogIRNode {
        final DDlogExpression first;
        final DDlogExpression second;

        public Case(DDlogExpression first, DDlogExpression second) {
            this.first = first;
            this.second = second;
        }

        @Override
        public String toString() {
            return this.first + " -> " + this.second;
        }
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
