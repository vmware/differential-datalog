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
import java.util.*;

/**
 * DDlogTStruct can have multiple constructors, but in SQL we never need more than 1.
 */
public class DDlogTStruct extends DDlogType {
    private final String name;
    private final List<DDlogField> args;

    public DDlogTStruct(@Nullable Node node, String name, List<DDlogField> args) {
        super(node,false);
        this.name = name;
        this.args = args;
        HashSet<String> fields = new HashSet<String>();
        for (DDlogField f: args) {
            if (fields.contains(f.getName()))
                this.error("Field name " + f + " is duplicated");
            fields.add(f.getName());
        }
    }

    @Override
    public String toString() {
        return this.name + "{" + String.join(", ",
                Linq.map(this.args, DDlogField::toString)) + "}";
    }

    @Override
    public DDlogType setMayBeNull(boolean mayBeNull) {
        if (this.mayBeNull == mayBeNull)
            return this;
        if (mayBeNull)
            this.error("Nullable structs not supported");
        return this;
    }


    public String getName() { return this.name; }

    public List<DDlogField> getFields() { return this.args; }

    @Override
    public boolean same(DDlogType type) {
        if (!super.same(type))
            return false;
        if (!type.is(DDlogTStruct.class))
            return false;
        DDlogTStruct other = type.to(DDlogTStruct.class);
        if (this.name.equals(other.name))
            return false;
        if (this.args.size() != other.args.size())
            return false;
        for (int i = 0; i < this.args.size(); i++)
            if (!this.args.get(i).equals(other.args.get(i)))
                return false;
        return true;
    }

    public DDlogType getFieldType(String col) {
        for (DDlogField f : this.getFields()) {
            if (f.getName().equals(col))
                return f.getType();
        }
        this.error("Field " + col + " not present in struct " + this.name);
        throw new RuntimeException("unreachable");
    }
}
