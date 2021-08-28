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

import org.dbsp.algebraic.FiniteFunction;
import org.dbsp.algebraic.FiniteFunctionGroup;
import org.dbsp.algebraic.ZRing;

import javax.annotation.Nullable;
import java.util.*;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Predicate;

/**
 * A Z-set contains values of type T each a weight W.
 * @param <T>  Type of data in relation.
 * @param <W>  Type of weight.
 */
public class ZSet<T extends Comparable<T>, W> extends FiniteFunction<T, W> {
    final HashMap<T, W> data;
    final ZRing<W> ring;
    // Used for sanity checking.
    final FiniteFunctionGroup<T, W> functionGroup;
    static boolean safetyChecks = true;

    /**
     * Create an empty Z-relation.
     * @param ring  Ring that computes on weights.
     */
    protected ZSet(ZRing<W> ring) {
        this(ring, 0);
    }

    /**
     * Create a Z-relation with the data in the specified map.
     * @param map  Map containing the tuples each with its own weight.
     */
    public ZSet(ZRing<W> ring, Map<T, W> map) {
        this(ring, map.size());
        for (T t: map.keySet()) {
            W w = map.get(t);
            if (!this.ring.isZero(w))
                this.add(t, w);
        }
    }

    /**
     * Create a Z-relation with all the elements in the list each with weight 1.
     * @param data  Data to add to relation.
     */
    public ZSet(ZRing<W> ring, List<T> data) {
        this(ring, data.size());
        for (T t: data)
            this.add(t, this.ring.one());
    }

    /**
     * Create an empty Z-relation and reserve space for the specified number of elements.
     * @param size  Number of elements in Z-relation.
     */
    public ZSet(ZRing<W> ring, int size) {
        this.ring = ring;
        this.functionGroup = new FiniteFunctionGroup<>(ring);
        this.data = new HashMap<T, W>(size);
    }

    /**
     * Create a Z-relation containing a single element with the specified weight.
     * @param value  Element to insert.
     * @param weight Weight of the element.
     */
    public ZSet(ZRing<W> ring, T value, W weight) {
        this(ring, 1);
        if (ring.isZero(weight))
            return;
        this.data.put(value, weight);
    }

    /**
     * Create a Z-relation with a single element with weight 1.
     * @param value  Element to add.
     */
    public ZSet(ZRing<W> ring, T value) {
        this(ring, value, ring.one());
    }

    /**
     * @return Number of distinct elements in the relation with non-zero weights.
     */
    public int size() {
        return this.data.size();
    }

    public boolean isEmpty() {
        return this.size() == 0;
    }

    /**
     * Add to this relation other * coefficient.
     * @param other   Elements from other relation to add.
     * @param coefficient  Coefficient to multiply each weight with.
     */
    public void add(ZSet<T, W> other, W coefficient) {
        if (coefficient.equals(ring.zero()))
            return;
        for (T key: other.data.keySet()) {
            W value = this.ring.times(coefficient, other.data.get(key));
            this.add(key, value);
        }
    }

    /**
     * Add a tuple to this relation with the specified weight.
     * @param value   Element to add.
     * @param weight  Weight of the element.
     */
    public void add(T value, W weight) {
        if (this.data.containsKey(value)) {
            weight = this.ring.add(weight, this.data.get(value));
            if (this.ring.isZero(weight))
                this.data.remove(value);
            else
                this.data.put(value, weight);
        } else {
            this.data.put(value, weight);
        }
    }

    public void assertSameOnSupport(FiniteFunction<T, W> function) {
        for (T key: this.support()) {
            W thisW = this.weight(key);
            W otherW = function.apply(key);
            assert thisW.equals(otherW);
        }
    }

    /**
     * Create a Z-relation that contains all elements in this and other.
     * @param other  Relation to add to this.
     * @return       A fresh relation.
     */
    public ZSet<T, W> plus(ZSet<T, W> other) {
        ZSet<T, W> result = new ZSet<T, W>(this.ring);
        result.add(this, this.ring.one());
        result.add(other, this.ring.one());
        if (safetyChecks) {
            FiniteFunction<T, W> tmp = this.functionGroup.add(this, other);
            result.assertSameOnSupport(tmp);
        }
        return result;
    }

    /**
     * Create a Z-relation that has all elements of this with weights negated.
     * @return  A fresh relation.
     */
    public ZSet<T, W> minus() {
        ZSet<T, W> result = new ZSet<T, W>(this.ring, this.size());
        for (T key: this.data.keySet()) {
            result.data.put(key, this.ring.minus(this.data.get(key)));
        }
        if (safetyChecks) {
            FiniteFunction<T, W> tmp = this.functionGroup.minus(this);
            result.assertSameOnSupport(tmp);
        }
        return result;
    }

    /**
     * Apply a function to each element (producing a Z-relation) in this relation.
     * @param map  Map to apply to each element.
     * @param <S>  Type of elements produced.
     * @return     The union of all Z-relations produced.
     */
    public <S extends Comparable<S>> ZSet<S, W> flatMap(Function<T, ZSet<S, W>> map) {
        ZSet<S, W> result = new ZSet<S, W>(this.ring, this.size());
        for (T key: this.support()) {
            W weight = this.data.get(key);
            ZSet<S, W> mr = map.apply(key);
            result.add(mr, weight);
        }
        return result;
    }

    /**
     * @param value  Value to look for in relation.
     * @return  True if this Z-relation contains the specified value.
     */
    public boolean contains(T value) {
        return this.data.containsKey(value);
    }

    /**
     * The weight associated with the specified value.
     * @param value  Value to look for.
     * @return  The weight if present, 0 otherwise.
     */
    public W weight(T value) {
        return this.apply(value);
    }

    /**
     * Produce a new relation that contains all elements that
     * satisfy the specified predicate.
     * @param predicate  A predicate on elements.
     * @return  A fresh relation.
     */
    public ZSet<T, W> filter(Predicate<T> predicate) {
        ZSet<T, W> result = new ZSet<T, W>(this.ring, this.size());
        for (T key: this.support()) {
            if (predicate.test(key)) {
                W weight = this.data.get(key);
                result.data.put(key, weight);
            }
        }
        return result;
    }

    /**
     * An iterator that enumerates all elements in the support of this function.
     */
    public Iterable<T> support() {
        return this.data.keySet();
    }

    /**
     * Apply a function to each element of this relation.
     * @param map  Function to apply.
     * @param <S>  Type of result produced by function.
     * @return     A fresh relation containing all results.
     */
    public <S extends Comparable<S>> ZSet<S, W> map(Function<T, S> map) {
        Function<T, ZSet<S, W>> fm = t -> new ZSet<S, W>(this.ring, map.apply(t), this.ring.one());
        return this.flatMap(fm);
    }

    /**
     * Join this relation with the other relation.
     * @param other     Relation to join with.
     * @param thisKey   Function that computes the join key for this relation.
     * @param otherKey  Function that computes the join key for the other relation.
     * @param combiner  Function that produces a result element from a pair of elements.
     * @param <S>       Type of data in the other relation.
     * @param <K>       Type of key we join on.
     * @param <R>       Type of result produced.
     * @return          A fresh relation.
     */
    public <S extends Comparable<S>, K, R extends Comparable<R>> ZSet<R, W> join(
            ZSet<S, W> other, Function<T, K> thisKey,
            Function<S, K> otherKey, BiFunction<T, S, R> combiner) {
        ZSet<R, W> result = new ZSet<R, W>(this.ring);
        HashMap<K, List<T>> leftIndex = new HashMap<K, List<T>>();
        for (T key: this.support()) {
            K k = thisKey.apply(key);
            List<T> list = leftIndex.get(k);
            if (list == null) {
                list = new ArrayList<>(1);
                list.add(key);
                leftIndex.put(k, list);
            }
        }
        for (S okey: other.support()) {
            K k = otherKey.apply(okey);
            if (!leftIndex.containsKey(k))
                continue;
            W oweight = other.data.get(okey);
            List<T> list = leftIndex.get(k);
            for (T t: list) {
                W weight = this.data.get(t);
                R join = combiner.apply(t, okey);
                result.add(join, this.ring.times(weight, oweight));
            }
        }
        return result;
    }

    /**
     * @return 'true' if all weights of elements in the support are exactly 1.
     */
    public boolean isSet() {
        W one = this.ring.one();
        for (T t: this.support()) {
            if (!this.apply(t).equals(one))
                return false;
        }
        return true;
    }

    /**
     * Compute the cartesian product between this relation and other.
     * @param other     Relation to multiply with.
     * @param combiner  Function that produces a result element from a pair of values.
     * @param <S>       Type of data in the other relation.
     * @param <R>       Type of data in result.
     * @return          A fresh relation.
     */
    public <S extends Comparable<S>, R extends Comparable<R>> ZSet<R, W> product(
            ZSet<S, W> other, BiFunction<T, S, R> combiner) {
        return this.join(other, e -> 1, e -> 1, combiner);
    }

    /**
     * Apply the distinct operator to this Z-relation.
     * @return  A fresh Z-relation.
     */
    public ZSet<T, W> distinct() {
        ZSet<T, W> result = new ZSet<T, W>(this.ring);
        for (T t: this.support()) {
            if (this.ring.isPositive(this.data.get(t)))
                result.add(t, this.ring.one());
        }
        return result;
    }

    public <K extends Comparable<K>> ZSet<Grouping<K, T, W>, W> groupBy(Function<T, K> key) {
        Map<K, Grouping<K, T, W>> perKey = new HashMap<>();
        for (T t: this.support()) {
            K k = key.apply(t);
            W w = this.apply(t);
            Grouping<K, T, W> set = perKey.get(k);
            if (set == null) {
                set = new Grouping<K, T, W>(k, this.ring);
                perKey.put(k, set);
            }
            set.add(t, w);
        }
        ZSet<Grouping<K, T, W>, W> result = new ZSet<Grouping<K, T, W>, W>(this.ring);
        for (K k: perKey.keySet()) {
            Grouping<K, T, W> v = perKey.get(k);
            result.add(v, this.ring.one());
        }
        return result;
    }

    @Override
    public W apply(T key) {
        if (!this.contains(key))
            return this.ring.zero();
        return this.data.get(key);
    }

    private List<T> sort() {
        List<T> sorted = new ArrayList<T>();
        this.support().forEach(sorted::add);
        sorted.sort(Comparator.naturalOrder());
        return sorted;
    }

    /**
     * String representation of a ZSet.
     */
    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append("{");
        boolean first = true;
        List<T> sorted = this.sort();
        for (T value: sorted) {
            if (!first)
                builder.append(",");
            else
                first = false;
            builder.append(value);
            builder.append("->");
            builder.append(this.data.get(value).toString());
        }
        builder.append("}");
        return builder.toString();
    }

    /**
     * Given a ZSet which is actually a set, return it as a string.
     * This omits all the weights, which are 1.
     * @param comparator  Comparator that establishes the element order.
     */
    public String asSetString() {
        assert this.isSet();
        StringBuilder builder = new StringBuilder();
        builder.append("{");
        boolean first = true;
        List<T> sorted = this.sort();
        for (T value: sorted) {
            if (!first)
                builder.append(",");
            else
                first = false;
            builder.append(value);
        }
        builder.append("}");
        return builder.toString();
    }
}
