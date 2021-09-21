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
import org.dbsp.lib.LinqIterator;

import javax.annotation.Nullable;
import java.util.List;
import java.util.Objects;

/**
 * ListType is the type of objects that are tuples.
 * Values of this type are java lists (List) that contain values of the corresponding types.
 */
public class TupleType implements Type {
    /**
     * Types of the tuple components.
     */
    final List<Type> components;
    /**
     * Group that operates on tuples.
     */
    @Nullable
    final DynamicGroup group;

    public TupleType(List<Type> components) {
        this.components = components;
        boolean nogroup = LinqIterator.fromList(components)
                .map(Type::getGroup)
                .any(Objects::isNull);
        this.group = nogroup ? null :
                new ProductGroup(LinqIterator.fromList(components)
                .map(Type::getGroup)
                .toList());
    }

    @Nullable
    @Override
    public DynamicGroup getGroup() {
        return this.group;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        TupleType tupleType = (TupleType) o;
        return components.equals(tupleType.components);
    }

    @Override
    public int hashCode() {
        return Objects.hash(components);
    }
}
