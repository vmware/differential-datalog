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
 */

package org.dbsp.algebraic.dynamicTyping.types;

import org.dbsp.algebraic.dynamicTyping.DynamicGroup;
import org.dbsp.algebraic.staticTyping.Monoid;
import org.dbsp.lib.LinqIterator;

import java.util.List;

/**
 * A product group is a product of several groups, operating pointwise.
 */
public class ProductGroup implements DynamicGroup {
    final List<DynamicGroup> components;

    public ProductGroup(List<DynamicGroup> components) {
        this.components = components;
    }

    @SuppressWarnings("unchecked")
    @Override
    public Object negate(Object data) {
        List<Object> list = (List<Object>)data;
        return LinqIterator.create(this.components).zip(list).map(p -> p.first.negate(p.second));
    }

    @SuppressWarnings("unchecked")
    @Override
    public Object add(Object left, Object right) {
        List<Object> lList = (List<Object>)left;
        List<Object> rList = (List<Object>)right;
        return LinqIterator.create(this.components)
                .zip3(lList, rList)
                .map(p -> p.first.add(p.second, p.third))
                .toList();
    }

    @Override
    public Object zero() {
        return LinqIterator.create(this.components).map(Monoid::zero).toList();
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append("Group<");
        boolean first = true;
        for (DynamicGroup t: this.components) {
            if (!first)
                builder.append(", ");
            first = false;
            builder.append(t.toString());
        }
        builder.append(">");
        return builder.toString();
    }
}
