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

package org.dbsp.compute.relational;

import org.dbsp.algebraic.staticTyping.ZRing;

import java.util.function.BiFunction;

/**
 * A representation of a group of values produced by the group-by operator.
 * @param <K>   Type of key.
 * @param <V>   Type of values grouped.
 * @param <W>   Type of weights.
 */
public class Grouping<K extends Comparable<K>, V extends Comparable<V>, W> implements Comparable<Grouping<K, V, W>> {
    /**
     * The key common to all elements of the group.
     */
    public final K key;
    /**
     * All values that map to the same key.  This is a ZSet,
     * since an element could show up multiple times.
     */
    final ZSet<V, W> values;

    /**
     * Create an empty group.
     * @param key   Key of the group.
     * @param ring  Ring used to weight addition.
     */
    public Grouping(K key, ZRing<W> ring) {
        this.key = key;
        this.values = new ZSet<V, W>(ring);
    }

    /**
     * Add an element with the specified weight to this group.
     * @param value    Element to add.
     * @param weight   Element weight.
     */
    public void add(V value, W weight) {
        this.values.add(value, weight);
    }

    @Override
    public String toString() {
        return "Group<K=" +
                this.key +
                ",values=" +
                this.values.toString() +
                ">";
    }

    /**
     * Group comparator used when sorting groups in a ZSet.
     * Sorting is done on key.
     */
    @Override
    public int compareTo(Grouping<K, V, W> o) {
        return this.key.compareTo(o.key);
    }

    /**
     * Apply a function to a group.  These are usually called "aggregate" functions in SQL.
     * @param aggregate  Function to apply.
     * @param <R>        Type of result produced by function.
     */
    public <R> R aggregate(BiFunction<K, ZSet<V, W>, R> aggregate) {
        return aggregate.apply(this.key, this.values);
    }

    /**
     * The 'count' aggregate function: count elements in the set by summing their weights.
     */
    public W count() {
        BiFunction<K, ZSet<V, W>, W> count = (k, s) -> {
            W result = this.values.weightRing.zero();
            for (V v: s.support()) {
                W w = s.weight(v);
                result = this.values.weightRing.add(result, w);
            }
            return result;
        };
        return this.aggregate(count);
    }

    /**
     * The 'distinct count' aggregate function: count distinct elements in the set.
     */
    public W distinctCount() {
        BiFunction<K, ZSet<V, W>, W> count = (k, s) -> {
            W result = this.values.weightRing.zero();
            for (V ignored : s.support()) {
                result = this.values.weightRing.increment(result);
            }
            return result;
        };
        return this.aggregate(count);
    }
}
