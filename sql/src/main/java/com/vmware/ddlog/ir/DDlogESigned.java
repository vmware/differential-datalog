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
import java.math.BigInteger;

public class DDlogESigned extends DDlogExpression {
    private final int width;
    private final BigInteger ival;

    public DDlogESigned(@Nullable Node node, int width, BigInteger ival) {
        super(node, new DDlogTSigned(node, width, false));
        this.width = width;
        this.ival = ival;
        if (ival.bitLength() > this.width)
            throw new TranslationException("Value " + ival + " too wide to fix " + width + " bits", node);
    }

    public DDlogESigned(@Nullable Node node, long value) {
        this(node,64, BigInteger.valueOf(value));
    }

    @Override
    public void accept(DDlogVisitor visitor) {
        if (!visitor.preorder(this)) return;
        visitor.postorder(this);
    }

    @Override
    public String toString() {
        String result = "";
        if (this.width > 0)
            result = this.width + "'sd";
        result += this.ival.toString();
        return result;
    }
}
