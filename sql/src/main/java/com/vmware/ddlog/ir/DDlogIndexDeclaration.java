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
import com.vmware.ddlog.translator.RelationName;

import javax.annotation.Nullable;
import java.util.List;
import java.util.stream.Collectors;

/**
 * The intermediate representation for a DDlog index during SQL -> DDlog translation.
 */
public class DDlogIndexDeclaration extends DDlogNode {
    private final RelationName name;
    private final List<DDlogField> fields;
    private final DDlogRelationDeclaration baseRelation;
    private final String baseRelationTypeString;

    public DDlogIndexDeclaration(@Nullable Node node, RelationName name,
                                 List<DDlogField> fields, DDlogRelationDeclaration baseRelation,
                                 String baseRelationTypeString) {
        super(node);
        this.name = this.checkNull(name);
        this.fields = this.checkNull(fields);
        this.baseRelation = this.checkNull(baseRelation);

        this.baseRelationTypeString = baseRelationTypeString;
    }

    public RelationName getName() {
        return this.name;
    }

    public static RelationName indexName(String name) {
        return new RelationName("I" + name, name,null);
    }

    @Override
    public void accept(DDlogVisitor visitor) {
        if (!visitor.preorder(this)) return;
        for (DDlogField f: this.fields)
            f.accept(visitor);
        this.baseRelation.accept(visitor);
        visitor.postorder(this);
    }

    @Override
    public String toString() {
        return "index " + this.name
                + "(" + this.fields.stream().map(DDlogField::toString).collect(Collectors.joining(",")) + ") on " +
                this.baseRelation.getName() +
                "[" + this.baseRelation.getType() +
                "{" + this.baseRelationTypeString + "}" +
                "]";
    }
}
