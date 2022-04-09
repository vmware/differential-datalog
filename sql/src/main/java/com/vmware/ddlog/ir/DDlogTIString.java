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

/**
 * Interned string type.
 */
public class DDlogTIString extends DDlogType implements IDDlogBaseType {
    public static DDlogTIString instance = new DDlogTIString(null,false);

    private DDlogTIString(@Nullable Node node, boolean mayBeNull) { super(node, mayBeNull); }

    @Override
    public String toString() { return this.wrapOption("istring"); }

    @Override
    public DDlogType setMayBeNull(boolean mayBeNull) {
        if (this.mayBeNull == mayBeNull)
            return this;
        return new DDlogTIString(this.getNode(), mayBeNull);
    }

    @Override
    public boolean same(DDlogType type) {
        if (!super.same(type))
            return false;
        return type.is(DDlogTIString.class);
    }

    /**
     * Given an expression with type istring unwrap the istring
     */
    public static DDlogExpression ival(DDlogExpression expression) {
        assert(expression.getType().is(DDlogTIString.class));
        return new DDlogEApply(expression.node, "ival", DDlogTString.instance, true, expression);
    }

    @Override
    public void accept(DDlogVisitor visitor) {
        if (!visitor.preorder(this)) return;
        visitor.postorder(this);
    }
}
