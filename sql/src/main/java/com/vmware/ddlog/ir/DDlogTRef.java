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
 * Reference type.
 */
public class DDlogTRef extends DDlogType implements IDDlogTContainer {
    private final DDlogType elemType;

    public DDlogTRef(@Nullable Node node, DDlogType elemType, boolean mayBeNull) {
        super(node, mayBeNull);
        this.elemType = elemType;
    }

    @Override
    public String toString() { return this.wrapOption("Ref<" + this.elemType + ">"); }

    @Override
    public DDlogType setMayBeNull(boolean mayBeNull) {
        if (this.mayBeNull == mayBeNull)
            return this;
        return new DDlogTRef(this.getNode(), this.elemType, mayBeNull);
    }

    @Override
    public boolean same(DDlogType type) {
        if (!super.same(type))
            return false;
        DDlogTRef oa = type.as(DDlogTRef.class);
        if (oa == null)
            return false;
        return this.elemType.same(oa.elemType);
    }

    /**
     * Given an expression whose type is a reference type, dereference it
     * by calling deref()
     */
    public static DDlogExpression deref(DDlogExpression expression) {
        DDlogType type = expression.getType();
        DDlogType resultType = type.to(DDlogTRef.class).elemType;
        return new DDlogEApply(expression.node, "deref",
                resultType, true, expression);
    }

    /**
     * Given an epression create a new expression that wraps this one in a reference
     * by calling ref_new()
     */
    public static DDlogExpression ref_new(DDlogExpression expression) {
        return new DDlogEApply(expression.node, "ref_new",
                new DDlogTRef(expression.getNode(), expression.getType(), false), true, expression);
    }

    @Override
    public DDlogType getElementType() {
        return this.elemType;
    }

    @Override
    public void accept(DDlogVisitor visitor) {
        if (!visitor.preorder(this)) return;
        this.elemType.accept(visitor);
        visitor.postorder(this);
    }
}
