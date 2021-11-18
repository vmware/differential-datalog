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

package com.vmware.ddlog.translator.environment;

import com.facebook.presto.sql.tree.Node;
import com.vmware.ddlog.ir.*;
import com.vmware.ddlog.util.IndentStringBuilder;

import javax.annotation.Nullable;

/**
 * Represents a relation and its columns.
 */
public class RelationEnvironent extends IEnvironment {
    public final Node node;
    final String scopeName;
    public final String rowVariable;
    public final DDlogTStruct type;

    public RelationEnvironent(Node node, String scopeName, String rowVariable, DDlogTStruct type) {
        this.node = node;
        this.scopeName = scopeName;
        this.rowVariable = rowVariable;
        this.type = type;
    }

    public RelationEnvironent cloneEnv() {
        return new RelationEnvironent(this.node, this.scopeName, this.rowVariable, this.type);
    }

    @Nullable
    public DDlogExpression lookupIdentifier(String identifier) {
        for (DDlogField f: this.type.getFields()) {
            if (identifier.equals(f.getName())) {
                DDlogEVar var = new DDlogEVar(this.node, this.rowVariable, f.getType());
                return new DDlogEField(this.node, var, identifier, f.getType());
            }
        }
        return null;
    }

    @Nullable
    public IEnvironment lookupRelation(String identifier) {
        if (identifier.equals(this.scopeName) ||
            identifier.equals(this.rowVariable))
            return this;
        return null;
    }

    public IndentStringBuilder toString(IndentStringBuilder builder) {
        return builder.append(this.scopeName + " - " + this.rowVariable + ": " + this.type);
    }
}
