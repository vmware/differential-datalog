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

import com.facebook.presto.sql.tree.Node;

import javax.annotation.Nullable;

public class DDlogFuncArg extends DDlogNode {
    public final String name;
    private final boolean mut;
    private final DDlogType type;

    public DDlogFuncArg(@Nullable Node node, String name, boolean mut, DDlogType type) {
        super(node);
        this.name = name;
        this.mut = mut;
        this.type = type;
    }

    @Override
    public String toString() {
        return this.name + ": " + (this.mut ? "mut " : "") + this.type.toString();
    }

    public boolean compare(DDlogFuncArg other, IComparePolicy policy) {
        if (!policy.compareLocal(this.name, other.name))
            return false;
        if (this.mut != other.mut)
            return false;
        return this.type.compare(other.type, policy);
    }
}
