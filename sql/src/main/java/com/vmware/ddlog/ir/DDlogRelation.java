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

import javax.annotation.Nullable;

public class DDlogRelation implements DDlogIRNode {
    public enum RelationRole {
        RelInput,
        RelOutput,
        RelInternal;

        @Override
        public String toString() {
            switch (this) {
                case RelInput:
                    return "input";
                case RelOutput:
                    return "output";
                case RelInternal:
                    return "";
            }
            throw new RuntimeException("Unexpected value");
        }
    }

    private final RelationRole role;
    private final String name;
    private final DDlogType type;
    @Nullable
    private final DDlogExpression primaryKey;

    public DDlogRelation(RelationRole role, String name,
                         DDlogType type, @Nullable DDlogExpression primaryKey) {
        this.role = role;
        this.name = this.checkNull(name);
        this.type = this.checkNull(type);
        this.primaryKey = primaryKey;
    }

    public DDlogRelation(RelationRole role, String name,
                         DDlogType type) {
        this(role, name, type, null);
    }

    public String getName() {
        return this.name;
    }

    public DDlogType getType() {
        return this.type;
    }

    static String relationName(String name) {
        return "R" + name;
    }

    @Override
    public String toString() {
        // We prefix the relation name with R
        String result = this.role.toString();
        if (!result.isEmpty())
            result += " ";
        result += "relation " + relationName(this.name)
                + "[" + this.type.toString() + "]";
        if (this.primaryKey != null)
            result += "primary key " + this.primaryKey;
        return result;
    }
}
