/*
 * Copyright (c) 2022 VMware, Inc.
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

package com.vmware.ddlog.optimizer;

import com.vmware.ddlog.ir.DDlogRule;

import java.util.Objects;

/**
 * A graph node representing a field of a variable in a rule.
 * The variable has a struct-typed type.
 */
class RuleVarFieldNode implements DFNode {
    final DDlogRule rule;
    final String var;
    final String field;

    public RuleVarFieldNode(DDlogRule rule, String var, String field) {
        this.rule = rule;
        this.var = var;
        this.field = field;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        RuleVarFieldNode that = (RuleVarFieldNode) o;
        return Objects.equals(rule.id, that.rule.id) && Objects.equals(var, that.var) &&
                Objects.equals(field, that.field);
    }

    @Override
    public int hashCode() {
        return Objects.hash(rule.id, var, field);
    }

    public String getName() {
        return this.rule.head.relation.name + "." +
                this.rule.id + "." + this.var + "." + this.field;
    }

    @Override
    public String toString() { return this.getName(); }
}
