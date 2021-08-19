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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Predicate;

/**
 * A Z-relation contains values of type T each with an integer weight.
 * @param <T>  Type of data in relation.
 */
public class ZRelation<T> {
    final HashMap<T, Integer> data;

    /**
     * Create an empty Z-relation.
     */
    public ZRelation() {
        this(0);
    }

    /**
     * Create a Z-relation with the data in the specified map.
     * @param map  Map containing the tuples each with its own weight.
     */
    public ZRelation(HashMap<T, Integer> map) {
        this.data = new HashMap<T, Integer>(map);
    }

    /**
     * Create a Z-relation with all the elements in the list each with weight 1.
     * @param data  Data to add to relation.
     */
    public ZRelation(List<T> data) {
        this(data.size());
        for (T t: data)
            this.add(t, 1);
    }

    /**
     * Create an empty Z-relation and reserve space for the specified number of elements.
     * @param size  Number of elements in Z-relation.
     */
    public ZRelation(int size) {
        this.data = new HashMap<T, Integer>(size);
    }

    /**
     * Create a Z-relation containing a single element with the specified weight.
     * @param value  Element to insert.
     * @param weight Weight of the element.
     */
    public ZRelation(T value, int weight) {
        this(1);
        if (weight == 0)
            return;
        this.data.put(value, weight);
    }

    /**
     * Create a Z-relation with a single element with weight 1.
     * @param value  Element to add.
     */
    public ZRelation(T value) {
        this(value, 1);
    }

    /**
     * @return Number of distinct elements in the relation with non-zero weights.
     */
    public int size() {
        return this.data.size();
    }

    /**
     * Add to this relation other * coefficient.
     * @param other   Elements from other relation to add.
     * @param coefficient  Coefficient to multiply each weight with.
     */
    public void add(ZRelation<T> other, int coefficient) {
        if (coefficient == 0)
            return;
        for (T key: other.data.keySet()) {
            int value = coefficient * other.data.get(key);
            this.add(key, value);
        }
    }

    /**
     * Add a tuple to this relation with the specified weight.
     * @param value   Element to add.
     * @param weight  Weight of the element.
     */
    public void add(T value, int weight) {
        if (this.data.containsKey(value)) {
            weight += this.data.get(value);
            if (weight == 0)
                this.data.remove(value);
            else
                this.data.put(value, weight);
        } else {
            this.data.put(value, weight);
        }
    }

    /**
     * Create a Z-relation that contains all elements in this and other.
     * @param other  Relation to add to this.
     * @return       A fresh relation.
     */
    public ZRelation<T> plus(ZRelation<T> other) {
        ZRelation<T> result = new ZRelation<T>(this.data);
        result.add(other, 1);
        return result;
    }

    /**
     * Create a Z-relation that has all elements of this with weights negated.
     * @return  A fresh relation.
     */
    public ZRelation<T> minus() {
        ZRelation<T> result = new ZRelation<T>(this.size());
        for (T key: this.data.keySet()) {
            result.data.put(key, - this.data.get(key));
        }
        return result;
    }

    /**
     * Apply a function to each element (producing a Z-relation) in this relation.
     * @param map  Map to apply to each element.
     * @param <S>  Type of elements produced.
     * @return     The union of all Z-relations produced.
     */
    public <S> ZRelation<S> flatMap(Function<T, ZRelation<S>> map) {
        ZRelation<S> result = new ZRelation<S>(this.size());
        for (T key: this.data.keySet()) {
            Integer weight = this.data.get(key);
            ZRelation<S> mr = map.apply(key);
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
    public int weight(T value) {
        if (!this.contains(value))
            return 0;
        return this.data.get(value);
    }

    /**
     * Produce a new relation that contains all elements that
     * satisfy the specified predicate.
     * @param predicate  A predicate on elements.
     * @return  A fresh relation.
     */
    public ZRelation<T> filter(Predicate<T> predicate) {
        ZRelation<T> result = new ZRelation<T>(this.size());
        for (T key: this.data.keySet()) {
            if (predicate.test(key)) {
                int weight = this.data.get(key);
                result.data.put(key, weight);
            }
        }
        return result;
    }

    /**
     * Apply a function to each element of this relation.
     * @param map  Function to apply.
     * @param <S>  Type of result produced by function.
     * @return     A fresh relation containing all results.
     */
    public <S> ZRelation<S> map(Function<T, S> map) {
        Function<T, ZRelation<S>> fm = t -> new ZRelation<S>(map.apply(t), this.data.get(t));
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
    public <S, K, R> ZRelation<R> join(ZRelation<S> other, Function<T, K> thisKey,
                                       Function<S, K> otherKey, BiFunction<T, S, R> combiner) {
        ZRelation<R> result = new ZRelation<R>();
        HashMap<K, List<T>> leftIndex = new HashMap<K, List<T>>();
        for (T key: this.data.keySet()) {
            K k = thisKey.apply(key);
            List<T> list = leftIndex.get(k);
            if (list == null) {
                list = new ArrayList<>(1);
                list.add(key);
                leftIndex.put(k, list);
            }
        }
        for (S okey: other.data.keySet()) {
            K k = otherKey.apply(okey);
            if (!leftIndex.containsKey(k))
                continue;
            int oweight = other.data.get(okey);
            List<T> list = leftIndex.get(k);
            for (T t: list) {
                int weight = this.data.get(t);
                R join = combiner.apply(t, okey);
                result.add(join, weight * oweight);
            }
        }
        return result;
    }

    /**
     * Compute the cartesian product between this relation and other.
     * @param other     Relation to multiply with.
     * @param combiner  Function that produces a result element from a pair of values.
     * @param <S>       Type of data in the other relation.
     * @param <R>       Type of data in result.
     * @return          A fresh relation.
     */
    public <S, R> ZRelation<R> product(ZRelation<S> other, BiFunction<T, S, R> combiner) {
        return this.join(other, e -> 1, e -> 1, combiner);
    }

    /**
     * Apply the distinct operator to this Z-relation.
     * @return  A fresh Z-relation.
     */
    public ZRelation<T> distinct() {
        ZRelation<T> result = new ZRelation<>();
        for (T t: this.data.keySet()) {
            if (this.data.get(t) >= 0)
                result.add(t, 1);
        }
        return result;
    }
}
