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
import com.vmware.ddlog.util.Utilities;

import javax.annotation.Nullable;

public abstract class DDlogExpression extends DDlogNode {
    /**
     * Inferred type of the expression.  We can't make it final
     * since sometimes it's not set in the constructor.  But it should be really final.
     */
    @Nullable
    protected /*final*/ DDlogType type;
    public abstract String toString();

    protected DDlogExpression(@Nullable Node node, DDlogType type) {
        super(node);
        this.type = this.checkNull(type);
    }

    protected DDlogExpression(@Nullable Node node) {
        super(node);
        this.type = null;
    }

    public DDlogType getType() {
        return this.checkNull(this.type);
    }

    public boolean compare(DDlogExpression val, IComparePolicy policy) {
        switch (Utilities.canBeSame(this.type, val.type)) {
            case Yes:
                return true;
            case No:
                return false;
        }
        assert val.type != null;
        assert this.type != null;
        return this.type.compare(val.type, policy);
    }
}
