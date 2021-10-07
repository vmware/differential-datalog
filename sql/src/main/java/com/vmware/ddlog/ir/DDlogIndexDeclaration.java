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

import javax.annotation.Nullable;
import java.util.List;
import java.util.stream.Collectors;

/**
 * The intermediate representation for a DDlog index during SQL -> DDlog translation.
 */
public class DDlogIndexDeclaration extends DDlogNode {

    private final String name;
    private final List<DDlogField> fields;
    private final DDlogRelationDeclaration baseRelation;
    private final String baseRelationTypeString;

    public DDlogIndexDeclaration(@Nullable Node node, String name,
                                 List<DDlogField> fields, DDlogRelationDeclaration baseRelation,
                                 String baseRelationTypeString) {
        super(node);
        this.name = this.checkNull(name);
        this.fields = this.checkNull(fields);
        this.baseRelation = this.checkNull(baseRelation);

        this.baseRelationTypeString = baseRelationTypeString;
    }

    public String getName() {
        return this.name;
    }

    public static String indexName(String name) {
        return "I" + name;
    }

    public boolean compare(DDlogIndexDeclaration other, IComparePolicy policy) {
        if (!policy.compareRelation(this.name, other.name))
            return false;

        if (this.fields.size() != other.fields.size())
            return false;

        for (int i = 0; i < this.fields.size(); i++) {
            if (!this.fields.get(i).compare(other.fields.get(i), policy)) {
                return false;
            }
        }

        if (!this.baseRelation.compare(other.baseRelation, policy)) {
            return false;
        }
        return this.baseRelationTypeString.equals(other.baseRelationTypeString);
    }

    @Override
    public String toString() {
        return "index " + this.name
                + "(" + String.join(",",
                    this.fields.stream().map(DDlogField::toString).collect(Collectors.toList()))  + ") on " +
                this.baseRelation.getName() +
                "[" + this.baseRelation.getType() +
                "{" + this.baseRelationTypeString + "}" +
                "]";
    }
}
