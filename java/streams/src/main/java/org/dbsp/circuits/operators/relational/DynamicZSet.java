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

package org.dbsp.circuits.operators.relational;

import org.dbsp.algebraic.staticTyping.ZRing;
import org.dbsp.compute.policies.IntegerRing;
import org.dbsp.compute.relational.ZSet;
import org.dbsp.lib.ComparableObjectList;

import java.util.Objects;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Predicate;

/**
 * A ZSet having values of type ComparableObjectList.
 * @param <W>  Weight used for ZSet.
 */
public class DynamicZSet<W>  {
    protected final ZRing<W> weightRing;
    protected final ZSet<ComparableObjectList, W> data;

    protected DynamicZSet(ZRing<W> weightRing, ZSet<ComparableObjectList, W> data) {
        this.weightRing = weightRing;
        this.data = data;
    }

    public DynamicZSet(ZRing<W> weightRing, ComparableObjectList value, W weight) {
        this.weightRing = weightRing;
        this.data = new ZSet<ComparableObjectList, W>(weightRing, value, weight);
    }

    public DynamicZSet(ZRing<W> weightRing, ComparableObjectList value) {
        this.weightRing = weightRing;
        this.data = new ZSet<ComparableObjectList, W>(weightRing, value);
    }

    public DynamicZSet(ZRing<W> weightRing) {
        this.weightRing = weightRing;
        this.data = new ZSet<ComparableObjectList, W>(weightRing);
    }

    public DynamicZSet<W> filter(Predicate<ComparableObjectList> p) {
        return new DynamicZSet<W>(this.weightRing, this.data.filter(p));
    }

    public DynamicZSet<W> map(Function<ComparableObjectList, ComparableObjectList> map) {
        return new DynamicZSet<W>(this.weightRing, this.data.map(map));
    }

    public DynamicZSet<W> join(DynamicZSet<W> with,
                               Function<ComparableObjectList, Object> leftKey,
                               Function<ComparableObjectList, Object> rightKey,
                               BiFunction<ComparableObjectList, ComparableObjectList, ComparableObjectList> computation) {
        ZSet<ComparableObjectList, W> join = this.data.join(with.data, leftKey, rightKey, computation);
        return new DynamicZSet<W>(this.weightRing, join);
    }

    public void add(ComparableObjectList data, W weight) {
        this.data.add(data, weight);
    }

    @Override
    public String toString() {
        return this.data.toString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        DynamicZSet<?> that = (DynamicZSet<?>) o;
        return Objects.equals(data, that.data);
    }

    @Override
    public int hashCode() {
        return Objects.hash(data);
    }
}
