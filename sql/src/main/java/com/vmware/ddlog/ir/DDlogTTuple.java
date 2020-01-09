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

import com.vmware.ddlog.util.Linq;
import java.util.Arrays;

public class DDlogTTuple extends DDlogType {
    final DDlogType[] tupArgs;

    public static DDlogTTuple emptyTupleType = new DDlogTTuple();

    public DDlogTTuple(DDlogType... tupArgs) {
        super(false);
        this.tupArgs = tupArgs;
    }

    public int size() {
        return this.tupArgs.length;
    }

    @Override
    public String toString() {
        return "(" + String.join(",",
                Linq.map(this.tupArgs, DDlogType::toString, String.class)) + ")";
    }

    @Override
    public DDlogType setMayBeNull(boolean mayBeNull) {
        if (mayBeNull)
            throw new RuntimeException("Nullable tuples not supported");
        return this;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        DDlogTTuple that = (DDlogTTuple) o;
        return Arrays.equals(tupArgs, that.tupArgs);
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(tupArgs);
    }
}
