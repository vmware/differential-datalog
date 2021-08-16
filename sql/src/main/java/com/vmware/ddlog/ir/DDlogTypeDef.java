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
import com.vmware.ddlog.util.Utilities;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;

public class DDlogTypeDef extends DDlogNode {
    private final String name;
    private final List<String> args;
    @Nullable
    private final DDlogType type;

    private DDlogTypeDef(@Nullable Node node, String name, List<String> args, @Nullable DDlogType type) {
        super(node);
        this.name = name;
        this.args = args;
        this.type = type;
    }

    public DDlogTypeDef(@Nullable Node node, String name, @Nullable DDlogType type) {
        this(node, name, new ArrayList<String>(), type);
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

    public boolean compare(DDlogTypeDef other, IComparePolicy policy) {
        if (!policy.compareLocal(this.name, other.name))
            return false;
        if (this.args.size() != other.args.size())
            return false;
        /*
        arg names do not matter
        for (int i = 0; i < this.args.size(); i++)
            if (!policy.compareLocal(this.args.get(i), other.args.get(i)))
                return false;
         */
        switch (Utilities.canBeSame(this.type, other.type)) {
            case Yes:
                return true;
            case No:
                return false;
        }
        assert other.type != null;
        assert this.type != null;
        return this.type.compare(other.type, policy);
    }
}
