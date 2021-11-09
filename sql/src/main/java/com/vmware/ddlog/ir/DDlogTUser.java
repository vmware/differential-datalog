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
import java.util.Objects;

public class DDlogTUser extends DDlogType {
    public final String name;
    private final DDlogType[] typeArgs;

    public DDlogTUser(@Nullable Node node, String name, boolean mayBeNull, DDlogType... typeArgs) {
        super(node, mayBeNull);
        this.name = name;
        this.typeArgs = typeArgs;
    }

    public DDlogType getTypeArg(int index) {
        return this.typeArgs[index];
    }

    public String getName() {
        return this.name;
    }

    @Override
    public String toString() {
        String result = this.name;
        if (this.typeArgs.length > 0)
            result += "<" + String.join(", ",
                    Linq.map(this.typeArgs, DDlogType::toString, String.class)) + ">";
        return this.wrapOption(result);
    }

    @Override
    public DDlogType setMayBeNull(boolean mayBeNull) {
        if (this.mayBeNull == mayBeNull)
            return this;
        return new DDlogTUser(this.getNode(), this.name, mayBeNull, this.typeArgs);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        DDlogTUser that = (DDlogTUser) o;
        return name.equals(that.name) &&
                Arrays.equals(typeArgs, that.typeArgs);
    }

    @Override
    public boolean same(DDlogType type) {
        if (!super.same(type))
            return false;
        if (!type.is(DDlogTUser.class))
            return false;
        DDlogTUser other = type.to(DDlogTUser.class);
        if (!this.name.equals(other.name))
            return false;
        if (this.typeArgs.length != other.typeArgs.length)
            return false;
        for (int i = 0; i < this.typeArgs.length; i++)
            if (!this.typeArgs[i].same(other.typeArgs[i]))
                return false;
        return true;
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(name);
        result = 31 * result + Arrays.hashCode(typeArgs);
        return result;
    }
}
