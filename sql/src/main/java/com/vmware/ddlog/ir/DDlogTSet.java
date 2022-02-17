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
 * Wrapper class for a DDlog set type.
 */
public class DDlogTSet extends DDlogType implements DDlogTContainer {
    private final DDlogType elemType;

    public DDlogTSet(@Nullable Node node, DDlogType elemType, boolean mayBeNull) {
        super(node, mayBeNull);
        this.elemType = elemType;
    }

    @Override
    public String toString() {
        return this.wrapOption("Set<" + this.elemType + ">");
    }

    @Override
    public DDlogType setMayBeNull(boolean mayBeNull) {
        if (this.mayBeNull == mayBeNull)
            return this;
        return new DDlogTSet(this.getNode(), this.elemType, mayBeNull);
    }

    @Override
    public boolean same(DDlogType other) {
        if (!super.same(other))
            return false;
        DDlogTSet oa = other.as(DDlogTSet.class);
        if (oa == null)
            return false;
        return this.elemType.same(oa.elemType);
    }

    @Override
    public DDlogType getElementType() {
        return this.elemType;
    }
}
