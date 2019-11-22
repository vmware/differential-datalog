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

import java.util.List;
import java.util.Objects;

/**
 * DDlogTStruct can have multiple constructors, but in SQL we never need more than 1.
 */
public class DDlogTStruct extends DDlogType {
    private final String name;
    private final List<DDlogField> args;

    public DDlogTStruct(String name, List<DDlogField> args) {
        super(false);
        this.name = name;
        this.args = args;
    }

    @Override
    public String toString() {
        return this.name + "{" + String.join(", ",
                Linq.map(this.args, DDlogField::toString)) + "}";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        DDlogTStruct that = (DDlogTStruct) o;
        return name.equals(that.name) &&
                args.equals(that.args);
    }

    public String getName() { return this.name; }

    public List<DDlogField> getFields() { return this.args; }

    @Override
    public int hashCode() {
        return Objects.hash(name, args);
    }
}
