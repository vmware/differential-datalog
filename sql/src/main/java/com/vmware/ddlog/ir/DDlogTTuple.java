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
import com.vmware.ddlog.util.Linq;

import javax.annotation.Nullable;
import java.util.Arrays;
import java.util.List;

public class DDlogTTuple extends DDlogType {
    final DDlogType[] tupArgs;

    public static DDlogTTuple emptyTupleType = new DDlogTTuple(null);

    private DDlogTTuple(@Nullable Node node, boolean mayBeNull, DDlogType... tupArgs) {
        super(node, mayBeNull);
        this.tupArgs = tupArgs;
    }

    public DDlogTTuple(@Nullable Node node, DDlogType... tupArgs) {
        this(node, false, tupArgs);
    }

    public DDlogTTuple(@Nullable Node node, List<DDlogType> tupArgs) {
        this(node, tupArgs.toArray(new DDlogType[0]));
    }

    public int size() {
        return this.tupArgs.length;
    }

    @Override
    public String toString() {
        if (this.tupArgs.length == 1)
            return this.tupArgs[0].toString();
        return this.wrapOption("(" + String.join(", ",
                Linq.map(this.tupArgs, DDlogType::toString, String.class)) + ")");
    }

    @Override
    public DDlogType setMayBeNull(boolean mayBeNull) {
        if (mayBeNull == this.mayBeNull)
            return this;
        return new DDlogTTuple(this.getNode(), mayBeNull, this.tupArgs);
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

    @Override
    public boolean compare(DDlogType type, IComparePolicy policy) {
        if (!super.compare(type, policy))
            return false;
        if (!type.is(DDlogTTuple.class))
            return false;
        DDlogTTuple other = type.to(DDlogTTuple.class);
        if (this.tupArgs.length != other.tupArgs.length)
            return false;
        for (int i = 0; i < this.tupArgs.length; i++)
            if (!this.tupArgs[i].compare(other.tupArgs[i], policy))
                return false;
        return true;
    }

    public DDlogType component(int index) {
        return this.tupArgs[index];
    }
}
