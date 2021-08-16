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

public class DDlogESeq extends DDlogExpression {
    private final DDlogExpression left;
    private final DDlogExpression right;

    public DDlogESeq(@Nullable Node node, DDlogExpression left, DDlogExpression right) {
        super(node, right.getType());
        this.left = left;
        this.right = right;
    }

    @Nullable
    public static DDlogExpression seq(@Nullable Node node, DDlogExpression... seq) {
        if (seq.length == 0)
            return null;
        DDlogExpression result = null;
        for (DDlogExpression e: seq) {
            if (result == null)
                result = e;
            else if (e != null)
                result = new DDlogESeq(node, result, e);
        }
        return result;
    }

    @Override
    public boolean compare(DDlogExpression val, IComparePolicy policy) {
        if (!super.compare(val, policy))
            return false;
        if (!val.is(DDlogESeq.class))
            return false;
        DDlogESeq other = val.to(DDlogESeq.class);
        if (!this.left.compare(other.left, policy))
            return false;
        return this.right.compare(other.right, policy);
    }

    @Override
    public String toString() {
        return this.left.toString() + ";\n" + "(" + this.right.toString() + ")";
    }
}
