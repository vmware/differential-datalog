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
import java.util.List;

public class DDlogEStruct extends DDlogExpression {
    public static final class FieldValue extends DDlogNode {
        private final String name;
        private final DDlogExpression value;

        public FieldValue(@Nullable Node node, String name, DDlogExpression value) {
            super(node);
            this.name = name;
            this.value = value;
        }

        public String getName() { return this.name; }
        public DDlogExpression getValue() { return this.value; }

        @Override
        public void accept(DDlogVisitor visitor) { if (!visitor.preorder(this)) return;
            this.value.accept(visitor);
            visitor.postorder(this);
        }
    }

    public final String constructor;
    public final FieldValue[] fields;

    public DDlogEStruct(@Nullable Node node, String constructor, DDlogType type, FieldValue... fields) {
        super(node, type);
        this.constructor = constructor;
        this.fields = fields;
        // We cannot check the type if it is just a typedef.
    }

    public DDlogExpression getFieldValue(String field) {
        for (FieldValue f: this.fields) {
            if (f.name.equals(field))
                return f.value;
        }
        throw new RuntimeException("No field named " + field + " in expression " + this);
    }

    public DDlogEStruct(@Nullable Node node, String constructor, DDlogType type, List<FieldValue> fields) {
        this(node, constructor, type, fields.toArray(new FieldValue[0]));
    }

    @Override
    public void accept(DDlogVisitor visitor) {
        if (!visitor.preorder(this)) return;
        for (FieldValue fv: this.fields)
            fv.accept(visitor);
        visitor.postorder(this);
    }

    @Override
    public String toString() {
        return this.constructor + "{" + String.join(",",
                Linq.map(this.fields, f ->
                        (f.name.isEmpty() ? "" : "." + f.name) + " = "
                                + f.value.toString(), String.class)) + "}";
    }
}
