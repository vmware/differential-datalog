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
import java.math.BigInteger;
import java.util.Objects;

public class DDlogTSigned extends DDlogType
        implements IsNumericType, IBoundedNumericType, IDDlogBaseType {
    private final int width;
    public static final DDlogTSigned signed32 = new DDlogTSigned(null, 32, false);
    public static final DDlogTSigned signed64 = new DDlogTSigned(null, 64, false);

    public DDlogTSigned(@Nullable Node node, int width, boolean mayBeNull) {
        super(node, mayBeNull);
        this.width = width;
    }

    @Override
    public int hashCode() {
        return Objects.hash(width);
    }

    @Override
    public String toString() { return this.wrapOption("signed<" + this.width + ">"); }

    @Override
    public DDlogType setMayBeNull(boolean mayBeNull) {
        return new DDlogTSigned(this.getNode(), this.width, mayBeNull);
    }

    @Override
    public DDlogExpression zero() {
        return new DDlogESigned(this.getNode(), this.width, BigInteger.valueOf(0));
    }

    @Override
    public DDlogExpression one() {
        return new DDlogESigned(this.getNode(), this.width, BigInteger.valueOf(1));
    }

    @Override
    public String simpleName() {
        return "signed";
    }

    @Override
    public int getWidth() {
        return this.width;
    }

    @Override
    public boolean same(DDlogType type) {
        if (!super.same(type))
            return false;
        if (!type.is(DDlogTSigned.class))
            return false;
        DDlogTSigned other = type.to(DDlogTSigned.class);
        return this.width == other.width;
    }

    @Override
    public IBoundedNumericType getWithWidth(int width) {
        return new DDlogTSigned(this.getNode(), width, this.mayBeNull);
    }
}
