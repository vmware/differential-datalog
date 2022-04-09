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

/**
 * The type of the _ expression: any type.
 */
public class DDlogTAny extends DDlogType {
    public static DDlogType instance = new DDlogTAny();

    private DDlogTAny() {
        super(true);
    }
    private DDlogTAny(boolean mayBeNull) { super(mayBeNull); }

    @Override
    public String toString() {
        return "*";
    }

    @Override
    public DDlogType setMayBeNull(boolean mayBeNull) {
        if (this.mayBeNull == mayBeNull)
            return this;
        return new DDlogTAny(mayBeNull);
    }

    @Override
    public boolean same(DDlogType type) {
        if (!super.same(type))
            return false;
        return type.is(DDlogTAny.class);
    }

    @Override
    public void accept(DDlogVisitor visitor) {
        if (!visitor.preorder(this)) return;
        visitor.postorder(this);
    }
}
