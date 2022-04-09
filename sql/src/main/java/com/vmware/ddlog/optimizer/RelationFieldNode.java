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
import com.vmware.ddlog.translator.RelationName;

import java.util.Objects;

/**
 * A graph node representing a field of a relation.
 */
class RelationFieldNode implements DFNode {
    final String relation;
    final String field;

    public RelationFieldNode(RelationName relation, String field) {
        this.relation = relation.name;
        this.field = field;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        RelationFieldNode that = (RelationFieldNode) o;
        return Objects.equals(relation, that.relation) &&
                Objects.equals(field, that.field);
    }

    @Override
    public int hashCode() {
        return Objects.hash(relation, field);
    }

    public String getName() {
        return this.relation + "." + this.field;
    }

    @Override
    public String toString() { return this.getName(); }
}
