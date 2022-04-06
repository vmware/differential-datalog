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
import com.vmware.ddlog.translator.TranslationException;

import javax.annotation.Nullable;

/**
 * This expression stands for left & right, but it is implemented
 * as a conjunction in DDlog with multiple terms.  It does not
 * correspond to a real DDlog construct.
 */
public class DDlogEComma extends DDlogExpression {
    public final DDlogExpression left;
    public final DDlogExpression right;

    public DDlogEComma(@Nullable Node node, DDlogExpression left, DDlogExpression right) {
        super(node, left.getType());
        if (!left.getType().same(right.getType()))
            throw new TranslationException("Type mismatch between " + left + " and " + right, node);
        this.left = left;
        this.right = right;
    }

    @Override
    public void accept(DDlogVisitor visitor) {
        if (!visitor.preorder(this)) return;
        this.left.accept(visitor);
        this.right.accept(visitor);
        visitor.postorder(this);
    }

    @Override
    public String toString() {
        return this.left.toString() + "," + this.right.toString();
    }
}
