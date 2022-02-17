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

public class DDlogTFloat extends DDlogType implements IsNumericType, IDDlogBaseType {
    private DDlogTFloat(@Nullable Node node, boolean mayBeNull) { super(node, mayBeNull); }

    @Override
    public String toString() { return this.wrapOption("float"); }

    @Override
    public DDlogType setMayBeNull(boolean mayBeNull) {
        if (this.mayBeNull == mayBeNull)
            return this;
        return new DDlogTFloat(this.getNode(), mayBeNull);
    }

    public static DDlogTFloat instance = new DDlogTFloat(null,false);

    @Override
    public boolean same(DDlogType type) {
        if (!super.same(type))
            return false;
        return type.is(DDlogTFloat.class);
    }

    @Override
    public DDlogExpression zero() {
        return new DDlogEFloat(this.getNode(), 0);
    }

    @Override
    public DDlogExpression one() {
        return new DDlogEFloat(this.getNode(),0);
    }

    @Override
    public String simpleName() {
        return "float";
    }

    @Override
    public DDlogType aggregateType() {
        return this;
    }
}
