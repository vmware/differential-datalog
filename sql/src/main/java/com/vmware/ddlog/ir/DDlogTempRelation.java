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

import javax.annotation.Nullable;

/**
 * This class does not correspond to any DDlog construct; this is
 * used as to represent a partial result from a subquery.  Since in DDlog
 * clauses operate on rows, each TempRelation object has a "row" variable.
 */
public class DDlogTempRelation implements DDlogIRNode {
    private final String rowVariable;
    private final DDlogType type;
    private final DDlogRelation source;
    @Nullable
    private final DDlogExpression condition;

    public DDlogTempRelation(String rowVariable, DDlogType type,
                             DDlogRelation source, @Nullable DDlogExpression condition) {
        this.rowVariable = this.checkNull(rowVariable);
        this.type = this.checkNull(type);
        this.source = source;
        this.condition = condition;
    }

    public DDlogRelation getSource() { return this.source; }

    public String getRowVariable() { return this.rowVariable; }

    public DDlogType getType() { return this.type; }

    @Nullable
    public DDlogExpression getCondition() { return this.condition; }

    @Override
    public String toString() {
        // TODO
        return "";
    }
}
