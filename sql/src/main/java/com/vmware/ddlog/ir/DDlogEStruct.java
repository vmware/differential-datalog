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

import com.vmware.ddlog.util.Linq;

import java.util.List;

public class DDlogEStruct extends DDlogExpression {
    public static final class FieldValue {
        private final String name;
        private final DDlogExpression value;

        public FieldValue(String name, DDlogExpression value) {
            this.name = name;
            this.value = value;
        }

        public String getName() { return this.name; }
        public DDlogExpression getValue() { return this.value; }
    }

    public final String constructor;
    public final FieldValue[] fields;

    public DDlogEStruct(String constructor, DDlogType type, FieldValue... fields) {
        super(type);
        this.constructor = constructor;
        this.fields = fields;
        // We cannot check the type if it is just a typedef.
    }

    public DDlogEStruct(String constructor, DDlogType type, List<FieldValue> fields) {
        this(constructor, type, fields.toArray(new FieldValue[0]));
    }

    @Override
    public String toString() {
        return this.constructor + "{" + String.join(",",
                Linq.map(this.fields, f ->
                        (f.name.isEmpty() ? "" : "." + f.name) + " = "
                                + f.value.toString(), String.class)) + "}";
    }
}
