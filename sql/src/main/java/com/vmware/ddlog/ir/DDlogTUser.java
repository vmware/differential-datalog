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

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class DDlogTUser extends DDlogType {
    private final String name;
    private final List<DDlogType> typeArgs;

    public DDlogTUser(String name, List<DDlogType> typeArgs, boolean mayBeNull) {
        super(mayBeNull);
        this.name = name;
        this.typeArgs = typeArgs;
    }

    public DDlogTUser(String name, boolean mayBeNull) {
        this(name, new ArrayList<DDlogType>(), mayBeNull);
    }

    public String getName() {
        return this.name;
    }

    @Override
    public String toString() {
        String result = this.name;
        if (!this.typeArgs.isEmpty())
            result += "<" + String.join(", ",
                    Linq.map(this.typeArgs, DDlogType::toString)) + ">";
        return this.wrapOption(result);
    }

    @Override
    public DDlogType setMayBeNull(boolean mayBeNull) {
        return new DDlogTUser(this.name, this.typeArgs, mayBeNull);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        DDlogTUser that = (DDlogTUser) o;
        return name.equals(that.name) &&
                typeArgs.equals(that.typeArgs);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, typeArgs);
    }
}
