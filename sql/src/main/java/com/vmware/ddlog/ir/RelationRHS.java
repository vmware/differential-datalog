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
import java.util.ArrayList;
import java.util.List;

/**
 * A partially-constructed relation; contains part of the RHS of a relation,
 * and the type produced by the RHS.
 */
public class RelationRHS extends DDlogNode {
    private final String rowVariable;
    private final DDlogType type;
    private final List<DDlogRuleRHS> definitions;

    public RelationRHS(@Nullable Node node, String rowVariable, DDlogType type) {
        super(node);
        this.rowVariable = rowVariable;
        this.type = this.checkNull(type);
        this.definitions = new ArrayList<DDlogRuleRHS>();
    }

    public RelationRHS addDefinition(DDlogExpression expression) {
        this.definitions.add(new DDlogRHSCondition(expression.getNode(), expression));
        return this;
    }

    public void addAssignment(String from) {
        DDlogExpression expr = new DDlogESet(this.node,
                new DDlogEVarDecl(this.node, this.rowVariable, this.type),
                new DDlogEVar(this.node, from, this.type));
        this.addDefinition(expr);
    }

    @SuppressWarnings("UnusedReturnValue")
    public RelationRHS addDefinition(DDlogRuleRHS rhs) {
        this.definitions.add(rhs);
        return this;
    }

    public DDlogType getType() { return this.type; }

    public List<DDlogRuleRHS> getDefinitions() {
        return this.definitions; }

    @Override
    public String toString() {
        return String.join(",", Linq.map(this.definitions, DDlogRuleRHS::toString));
    }

    public DDlogExpression getRowVariable(boolean declare) {
        if (declare)
            return new DDlogEVarDecl(this.node, this.rowVariable, this.type);
        return new DDlogEVar(this.node, this.rowVariable, this.type);
    }

    public DDlogExpression getRowVariable() {
        return this.getRowVariable(false);
    }

    public String getVarName() {
        return this.rowVariable;
    }
}
