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

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;

public class DDlogTypeDef implements DDlogIRNode {
    private final String name;
    private final List<String> args;
    @Nullable
    private final DDlogType type;

    private DDlogTypeDef(String name, List<String> args, @Nullable DDlogType type) {
        this.name = name;
        this.args = args;
        this.type = type;
    }

    public DDlogTypeDef(String name, @Nullable DDlogType type) {
        this(name, new ArrayList<String>(), type);
    }

    public String getName() { return this.name; }

    @Nullable
    public DDlogType getType() { return this.type; }

    @Override
    public String toString() {
        String result;
        if (this.type != null) {
            result = "typedef " + this.name;
            if (!this.args.isEmpty())
                result += "<" + String.join(", ",
                        Linq.map(this.args, a -> "'" + a)) + ">";
            result += " = " + this.type.toString();
        } else {
            result = "extern type " + this.name;
            if (!this.args.isEmpty())
                result += "<" + String.join(", ",
                        Linq.map(this.args, a -> "'" + a)) + ">";
        }
        return result;
    }
}
