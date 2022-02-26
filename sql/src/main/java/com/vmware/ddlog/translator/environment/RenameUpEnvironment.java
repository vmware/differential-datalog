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

import com.vmware.ddlog.ir.DDlogEField;
import com.vmware.ddlog.ir.DDlogEVar;
import com.vmware.ddlog.ir.DDlogExpression;
import com.vmware.ddlog.ir.DDlogEnvironment;
import com.vmware.ddlog.util.IndentStringBuilder;

import javax.annotation.Nullable;
import java.util.Map;

/**
 * This environment will look up things then, if found, rename them.
 */
public class RenameUpEnvironment extends IEnvironment {
    private final IEnvironment base;
    /**
     * Original table name that will be renamed.
     */
    final String originalName;
    /**
     * New name to give to table.
     */
    final String newName;
    /**
     * Map that shows how table columns have to be renamed.
     * Maps the name that appears in the SQL program to the name that should be used in the implementation.
     */
    final Map<String, String> renameColumn;

    public RenameUpEnvironment(IEnvironment base, String sourceName, String implementationName, Map<String, String> renameColumn) {
        this.base = base;
        this.originalName = sourceName;
        this.newName = implementationName;
        this.renameColumn = renameColumn;
    }

    @Override
    public IEnvironment cloneEnv() {
        return new RenameUpEnvironment(this.base, this.originalName, this.newName, this.renameColumn);
    }

    @Nullable
    @Override
    public DDlogExpression lookupIdentifier(String identifier) {
        DDlogExpression expr = this.base.lookupIdentifier(identifier);
        if (expr == null)
            return null;
        DDlogEnvironment escope = expr.as(DDlogEnvironment.class);
        if (escope != null) {
            return escope;
        } else {
            // This field has a very particular shape, which we deconstruct here.
            // This is the inverse of the TableScope.lookupColumn process.
            DDlogEField field = expr.to(DDlogEField.class);
            DDlogEVar var = field.getStruct().to(DDlogEVar.class);
            if (var.var.equals(this.originalName)) {
                // Recursively rename
                String newFieldName = field.field;
                if (this.renameColumn.containsKey(newFieldName))
                    newFieldName = this.renameColumn.get(newFieldName);
                // Build the expression with the renamed fields
                var = new DDlogEVar(var.getNode(), this.newName, var.getType());
                field = new DDlogEField(field.getNode(), var, newFieldName, field.getType());
            }
            return field;
        }
    }

    @Nullable
    @Override
    public IEnvironment lookupRelation(String identifier) {
        if (identifier.equals(this.originalName))
            return this;
        IEnvironment result = this.base.lookupRelation(identifier);
        if (result == null)
            return null;
        return new RenameUpEnvironment(result, this.originalName, this.newName, this.renameColumn);
    }

    @Override
    public IndentStringBuilder toString(IndentStringBuilder builder) {
        return builder.append("RenameUp " + originalName + "->" + newName + " " + renameColumn)
                .increase()
                .append(this.base)
                .decrease();
    }
}
