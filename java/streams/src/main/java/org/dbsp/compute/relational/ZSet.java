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

import org.dbsp.algebraic.staticTyping.FiniteFunction;
import org.dbsp.algebraic.staticTyping.FiniteFunctionGroup;
import org.dbsp.algebraic.staticTyping.ZRing;

import java.util.*;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Predicate;

/**
 * A Z-set contains values of type T each a weight W.
 * @param <T>  Type of data in relation.
 * @param <W>  Type of weight.
 */
public class ZSet<T extends Comparable<T>, W>
        extends FiniteFunction<T, W>
        implements Comparable<ZSet<T, W>> {
    /**
     * Data in the set: each value of type T is mapped to a weight.
     */
    final HashMap<T, W> data;
    /**
     * Ring that knows how to compute on weights.
     */
    final ZRing<W> weightRing;
    // Used for sanity checking.
    final FiniteFunctionGroup<T, W> functionGroup;
    static boolean safetyChecks = true;

    /**
     * Create an empty Z-set.
     * @param weightRing  Ring that computes on weights.
     */
    public ZSet(ZRing<W> weightRing) {
        this(weightRing, 0);
    }

    /**
     * Create a Z-set with the data in the specified map.
     * @param map  Map containing the tuples each with its own weight.
     */
    public ZSet(ZRing<W> weightRing, Map<T, W> map) {
        this(weightRing, map.size());
        for (T t: map.keySet()) {
            W w = map.get(t);
            if (!this.weightRing.isZero(w))
                this.add(t, w);
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ZSet<?, ?> zSet = (ZSet<?, ?>) o;
        return this.data.equals(zSet.data);
    }

    @Override
    public int hashCode() {
        return this.data.hashCode();
    }

    /**
     * Create a Z-set with all the elements in the supplied iterable, each with weight 1.
     * @param data  Data to add to set.
     */
    public ZSet(ZRing<W> weightRing, Iterable<T> data) {
        this(weightRing);
        for (T t: data)
            this.add(t, this.weightRing.one());
    }

    /**
     * Create an empty Z-set and reserve space for the specified number of elements.
     * @param size  Number of elements in Z-set.
     */
    public ZSet(ZRing<W> weightRing, int size) {
        this.weightRing = weightRing;
        this.functionGroup = new FiniteFunctionGroup<>(weightRing);
        this.data = new HashMap<T, W>(size);
    }

    /**
     * Create a Z-set containing a single element with the specified weight.
     * @param value  Element to insert.
     * @param weight Weight of the element.
     */
    public ZSet(ZRing<W> weightRing, T value, W weight) {
        this(weightRing, 1);
        if (weightRing.isZero(weight))
            return;
        this.data.put(value, weight);
    }

    /**
     * Create a Z-set with a single element with weight 1.
     * @param value  Element to add.
     */
    public ZSet(ZRing<W> weightRing, T value) {
        this(weightRing, value, weightRing.one());
    }

    /**
     * @return Number of distinct elements in the set with non-zero weights.
     */
    public int size() {
        return this.data.size();
    }

    public boolean isEmpty() {
        return this.size() == 0;
    }

    /**
     * Add to this set other * coefficient.
     * Mutates set.
     * @param other   Elements from other set to add.
     * @param coefficient  Coefficient to multiply each weight with.
     */
    public void add(ZSet<T, W> other, W coefficient) {
        if (weightRing.isZero(coefficient))
            return;
        for (T key: other.data.keySet()) {
            W value = this.weightRing.times(coefficient, other.data.get(key));
            this.add(key, value);
        }
    }

    /**
     * Add a tuple to this set with the specified weight.
     * Mutates set.
     * @param value   Element to add.
     * @param weight  Weight of the element.
     */
    public void add(T value, W weight) {
        if (this.data.containsKey(value)) {
            weight = this.weightRing.add(weight, this.data.get(value));
            if (this.weightRing.isZero(weight))
                this.data.remove(value);
            else
                this.data.put(value, weight);
        } else {
            this.data.put(value, weight);
        }
    }

    /**
     * Add a tuple to this set with weight 1.
     * Mutates set.
     * @param value  Tuple to add to the set.
     */
    public void add(T value) {
        this.add(value, this.weightRing.one());
    }

    public void remove(T value) {
        this.add(value, this.weightRing.negate(this.weightRing.one()));
    }

    /**
     * Removes all elements from this set.
     * Mutates set.
     */
    public void clear() {
        this.data.clear();
    }

    public void assertSameOnSupport(FiniteFunction<T, W> function) {
        for (T key: this.support()) {
            W thisW = this.weight(key);
            W otherW = function.apply(key);
            assert this.weightRing.equal(thisW, otherW);
        }
    }

    /**
     * Create a Z-set that contains all elements in this and other.
     * @param other  set to add to this.
     * @return       A fresh set.
     */
    public ZSet<T, W> plus(ZSet<T, W> other) {
        ZSet<T, W> result = new ZSet<T, W>(this.weightRing);
        result.add(this, this.weightRing.one());
        result.add(other, this.weightRing.one());
        if (safetyChecks) {
            FiniteFunction<T, W> tmp = this.functionGroup.add(this, other);
            result.assertSameOnSupport(tmp);
        }
        return result;
    }

    /**
     * Create a Z-set that has all elements of this with weights negated.
     * @return  A fresh set.
     */
    public ZSet<T, W> minus() {
        ZSet<T, W> result = new ZSet<T, W>(this.weightRing, this.size());
        for (T key: this.data.keySet()) {
            result.data.put(key, this.weightRing.negate(this.data.get(key)));
        }
        if (safetyChecks) {
            FiniteFunction<T, W> tmp = this.functionGroup.negate(this);
            result.assertSameOnSupport(tmp);
        }
        return result;
    }

    /**
     * Apply a function to each element (producing a Z-set) in this set.
     * @param map  Map to apply to each element.
     * @param <S>  Type of elements produced.
     * @return     The union of all Z-sets produced.
     */
    public <S extends Comparable<S>> ZSet<S, W> flatMap(Function<T, ZSet<S, W>> map) {
        ZSet<S, W> result = new ZSet<S, W>(this.weightRing, this.size());
        for (T key: this.support()) {
            W weight = this.data.get(key);
            ZSet<S, W> mr = map.apply(key);
            result.add(mr, weight);
        }
        return result;
    }

    /**
     * @param value  Value to look for in set.
     * @return  True if this Z-set contains the specified value.
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
     * Produce a new set that contains all elements that
     * satisfy the specified predicate.
     * @param predicate  A predicate on elements.
     * @return  A fresh set.
     */
    public ZSet<T, W> filter(Predicate<T> predicate) {
        ZSet<T, W> result = new ZSet<T, W>(this.weightRing, this.size());
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
     * Apply a function to each element of this set.
     * @param map  Function to apply.
     * @param <S>  Type of result produced by function.
     * @return     A fresh set containing all results.
     */
    public <S extends Comparable<S>> ZSet<S, W> map(Function<T, S> map) {
        Function<T, ZSet<S, W>> fm = t -> new ZSet<S, W>(this.weightRing, map.apply(t), this.weightRing.one());
        return this.flatMap(fm);
    }

    /**
     * Join this set with the other set.
     * @param other     set to join with.
     * @param thisKey   Function that computes the join key for this set.
     * @param otherKey  Function that computes the join key for the other set.
     * @param combiner  Function that produces a result element from a pair of elements.
     * @param <S>       Type of data in the other set.
     * @param <K>       Type of key we join on.
     * @param <R>       Type of result produced.
     * @return          A fresh set.
     */
    public <S extends Comparable<S>, K, R extends Comparable<R>> ZSet<R, W> join(
            ZSet<S, W> other, Function<T, K> thisKey,
            Function<S, K> otherKey, BiFunction<T, S, R> combiner) {
        ZSet<R, W> result = new ZSet<R, W>(this.weightRing);
        HashMap<K, List<T>> leftIndex = new HashMap<K, List<T>>();
        for (T key: this.support()) {
            K k = thisKey.apply(key);
            List<T> list = leftIndex.computeIfAbsent(k, k1 -> new ArrayList<>(1));
            list.add(key);
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
                result.add(join, this.weightRing.times(weight, oweight));
            }
        }
        return result;
    }

    /**
     * @return 'true' if all weights of elements in the support are exactly 1.
     */
    public boolean isSet() {
        for (T t: this.support()) {
            if (!this.weightRing.isOne(this.apply(t)))
                return false;
        }
        return true;
    }

    /**
     * Compute the cartesian product between this set and other.
     * @param other     set to multiply with.
     * @param combiner  Function that produces a result element from a pair of values.
     * @param <S>       Type of data in the other set.
     * @param <R>       Type of data in result.
     * @return          A fresh set.
     */
    public <S extends Comparable<S>, R extends Comparable<R>> ZSet<R, W> product(
            ZSet<S, W> other, BiFunction<T, S, R> combiner) {
        return this.join(other, e -> 1, e -> 1, combiner);
    }

    /**
     * Apply the distinct operator to this Z-set.
     * @return  A fresh Z-set.
     */
    public ZSet<T, W> distinct() {
        ZSet<T, W> result = new ZSet<T, W>(this.weightRing);
        for (T t: this.support()) {
            if (this.weightRing.isPositive(this.data.get(t)))
                result.add(t, this.weightRing.one());
        }
        return result;
    }

    /**
     * The data in the support of this set as a set.
     * If this is not applied to something that is a set it throws.
     */
    public RSet<T> asSet() {
        assert this.isSet();
        return new RSet<T>(this.data.keySet());
    }

    public <K extends Comparable<K>> ZSet<Grouping<K, T, W>, W> groupBy(Function<T, K> key) {
        Map<K, Grouping<K, T, W>> perKey = new HashMap<>();
        for (T t: this.support()) {
            K k = key.apply(t);
            W w = this.apply(t);
            Grouping<K, T, W> set = perKey.get(k);
            if (set == null) {
                set = new Grouping<K, T, W>(k, this.weightRing);
                perKey.put(k, set);
            }
            set.add(t, w);
        }
        ZSet<Grouping<K, T, W>, W> result = new ZSet<Grouping<K, T, W>, W>(this.weightRing);
        for (K k: perKey.keySet()) {
            Grouping<K, T, W> v = perKey.get(k);
            result.add(v, this.weightRing.one());
        }
        return result;
    }

    @Override
    public W apply(T key) {
        if (!this.contains(key))
            return this.weightRing.zero();
        return this.data.get(key);
    }

    private List<T> sort() {
        List<T> sorted = new ArrayList<T>();
        this.support().forEach(sorted::add);
        sorted.sort(Comparator.naturalOrder());
        return sorted;
    }

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

    @Override
    public int compareTo(ZSet<T, W> o) {
        return 0;
    }
}
