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

package org.dbsp.compute;

import org.dbsp.compute.relational.ZSet;
import org.dbsp.compute.time.IntegerRing;
import org.dbsp.lib.ComparableList;

import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Predicate;

// This class is just a wrapper for ZSet<TestTuple, Integer> so we write fewer templates.
// In C++ this would be a typedef.
public class ZS {
    public final ZSet<TestTuple, Integer> zs;

    public ZS() {
        this.zs = new ZSet<TestTuple, Integer>(IntegerRing.instance);
    }

    public ZS(ZSet<TestTuple, Integer> zs) {
        this.zs = zs;
    }

    public ZS(TestTuple t) {
        this.zs = new ZSet<TestTuple, Integer>(IntegerRing.instance, t);
    }

    public ZS(TestTuple t, int w) {
        this.zs = new ZSet<TestTuple, Integer>(IntegerRing.instance, t, w);
    }

    public ZS(ComparableList<TestTuple> l) {
        this.zs = new ZSet<TestTuple, Integer>(IntegerRing.instance, l);
    }

    public void add(TestTuple t, Integer w) {
        this.zs.add(t, w);
    }

    public ZS filter(Predicate<TestTuple> p) {
        return new ZS(this.zs.filter(p));
    }

    public int size() {
        return this.zs.size();
    }

    @Override
    public String toString() {
        return this.zs.toString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ZS zs1 = (ZS) o;
        return zs.equals(zs1.zs);
    }

    @Override
    public int hashCode() {
        return this.zs.hashCode();
    }

    public ZS map(Function<TestTuple, TestTuple> map) {
        return new ZS(this.zs.map(map));
    }

    public <K> ZS join(ZS zs2, Function<TestTuple, K> leftKey, Function<TestTuple, K> rightKey,
                       BiFunction<TestTuple, TestTuple, TestTuple> combiner) {
        return new ZS(this.zs.join(zs2.zs, leftKey, rightKey, combiner));
    }

    public ZS distinct() {
        return new ZS(this.zs.distinct());
    }

    public ZS add(ZS zset) {
        return new ZS(this.zs.add(zset.zs));
    }

    public ZS negate() {
        return new ZS(this.zs.negate());
    }

    public int weight(TestTuple t) {
        return this.zs.weight(t);
    }
}
