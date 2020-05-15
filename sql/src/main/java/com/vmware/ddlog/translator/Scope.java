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

package com.vmware.ddlog.translator;

import com.facebook.presto.sql.tree.Node;
import com.vmware.ddlog.ir.*;

import javax.annotation.Nullable;

public class Scope {
    static int idGen = 0;
    public final int id;
    public final Node node;
    final String scopeName;
    public final String rowVariable;
    public final DDlogType type;

    Scope(Node node, String scopeName, String rowVariable, DDlogType type) {
        this.id = idGen++;
        this.node = node;
        this.scopeName = scopeName;
        this.rowVariable = rowVariable;
        this.type = type;
    }

    public String getName() { return this.scopeName; }

    /**
     * Lookup the specified identifier as a column name.
     * @param identifier  Identifier to look up.
     * @return            null if the identifier is not a field name.
     *                    A DDlogField expression of the row variable if the field is present.
     */
    @Nullable
    DDlogExpression lookupColumn(Node node, String identifier, TranslationContext context) {
        DDlogType type = context.resolveType(this.type);
        if (!(type instanceof DDlogTStruct))
            return null;
        DDlogTStruct ts = (DDlogTStruct)type;
        for (DDlogField f: ts.getFields()) {
            if (identifier.equals(f.getName())) {
                DDlogEVar var = new DDlogEVar(node, this.rowVariable, type);
                return new DDlogEField(node, var, identifier, f.getType());
            }
        }
        return null;
    }
}
