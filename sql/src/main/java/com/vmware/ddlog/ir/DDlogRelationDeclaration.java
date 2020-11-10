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

import com.facebook.presto.sql.tree.Node;
import com.vmware.ddlog.util.Linq;

import javax.annotation.Nullable;
import java.util.List;

public class DDlogRelationDeclaration extends DDlogNode {
    public enum Role {
        Input,
        Output,
        Internal;

        @Override
        public String toString() {
            switch (this) {
                case Input:
                    return "input";
                case Output:
                    return "output";
                case Internal:
                    return "";
            }
            throw new RuntimeException("Unexpected value");
        }
    }

    private final Role role;
    private final String name;
    private final DDlogType type;
    private final String primaryKeyVariable = "row";
    @Nullable
    private DDlogExpression keyExpression = null;

    public DDlogRelationDeclaration(@Nullable Node node, Role role, String name,
                                    DDlogType type) {
        super(node);
        this.role = role;
        this.name = this.checkNull(name);
        this.type = this.checkNull(type);
    }

    public DDlogRelationDeclaration setPrimaryKey(List<DDlogField> columns, String typeName) {
        if (this.role != Role.Input)
            throw new RuntimeException("Only input relations can have primary keys");
        DDlogTStruct ts = new DDlogTStruct(null, typeName, columns);
        DDlogRelationDeclaration result = new DDlogRelationDeclaration(this.node, this.role, this.name, this.type);
        DDlogExpression row = new DDlogEVar(null, primaryKeyVariable, ts);
        result.keyExpression = new DDlogETuple(null, Linq.map(columns,
                c -> new DDlogEField(null, row, c.getName(), c.getType())));
        return result;
    }

    public String getName() {
        return this.name;
    }

    public DDlogType getType() {
        return this.type;
    }

    public static String relationName(String name) {
        return "R" + name;
    }

    public boolean compare(DDlogRelationDeclaration other, IComparePolicy policy) {
        if (this.role != other.role)
            return false;
        if (!policy.compareRelation(this.name, other.name))
            return false;
        return type.compare(other.type, policy);
    }

    @Override
    public String toString() {
        String result = this.role.toString();
        if (!result.isEmpty())
            result += " ";
        result += "relation " + this.name
                + "[" + this.type.toString() + "]";
        if (this.keyExpression != null)
            result += " primary key (" + primaryKeyVariable + ") " + this.keyExpression.toString();
        return result;
    }
}
