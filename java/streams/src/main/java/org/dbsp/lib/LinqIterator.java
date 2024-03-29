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

package org.dbsp.lib;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Predicate;

/**
 * An iterator that returns values of type T.
 * These iterators cannot be reset.
 * @param <T>  Value returned.
 */
public abstract class LinqIterator<T> implements Iterator<T> {
    /**
     * An iterator over values of type S.
     * @param map  Function that converts each value of type T to a value of type S.
     * @param <S>  Value produced by function.
     */
    public <S> LinqIterator<S> map(Function<T, S> map) {
        return new LinqIterator<S>() {
            @Override
            public boolean hasNext() {
                return LinqIterator.this.hasNext();
            }

            @Nullable
            @Override
            public S next() {
                return map.apply(LinqIterator.this.next());
            }
        };
    }

    public LinqIterator<T> filter(Predicate<T> predicate) {
        return new LinqIterator<T>() {
            @Nullable
            T next = null;
            // True if we are done iterating over source.
            boolean done = false;
            // True if we don't know whether there are more elements.
            boolean needScan = true;

            /**
             * Sets 'next' to the next value that passes the predicate.
             * Sets 'done' to 'true' if the source iterator is not done.
             */
            void findNext() {
                if (!needScan)
                    return;
                this.needScan = false;
                do {
                    if (!LinqIterator.this.hasNext()) {
                        this.done = true;
                        return;
                    }
                    this.next = LinqIterator.this.next();
                } while (!predicate.test(this.next));
            }

            @Override
            public boolean hasNext() {
                this.findNext();
                return !this.done;
            }

            @Nullable
            @Override
            public T next() {
                this.findNext();
                if (this.done)
                    throw new RuntimeException("No more values");
                this.needScan = true;
                return this.next;
            }
        };
    }

    public <S> LinqIterator<Pair<T, S>> zip(LinqIterator<S> other) {
        return new LinqIterator<Pair<T, S>>() {
            @Override
            public boolean hasNext() {
                return LinqIterator.this.hasNext() && other.hasNext();
            }

            @Override
            public Pair<T, S> next() {
                return new Pair<T, S>(LinqIterator.this.next(), other.next());
            }
        };
    }

    /**
     * Zip three iterators; returns an iterator of triples with elements from all 3 iterators.
     * Stops when the first of the three iterators terminates.
     * @param left   Second iterator to zip with.
     * @param right  Third iterator to zip with.
     * @param <S>    Type of elements in the second iterator.
     * @param <U>    Type of elements in the third iterator.
     * @return       An iterator with the length the shorter of this, left, and right.
     */
    public <S, U> LinqIterator<Triple<T, S, U>> zip3(LinqIterator<S> left, LinqIterator<U> right) {
        return new LinqIterator<Triple<T, S, U>>() {
            @Override
            public boolean hasNext() {
                return LinqIterator.this.hasNext() && left.hasNext() && right.hasNext();
            }

            @Override
            public Triple<T, S, U> next() {
                return new Triple<T, S, U>(LinqIterator.this.next(), left.next(), right.next());
            }
        };
    }

    /**
     * Zip two iterators; returns an iterator of pairs with elements from both iterators.
     * Stops when the first of the two iterators terminates.
     * @param other  Iterator to zip with.
     * @param <S>    Type of elements in the other iterator.
     * @return       An iterator with the length the shorter of this and other.
     */
    public <S> LinqIterator<Pair<T, S>> zip(List<S> other) {
        return this.zip(LinqIterator.create(other));
    }

    public <S, U> LinqIterator<Triple<T, S, U>> zip3(List<S> left, List<U> right) {
        return this.zip3(LinqIterator.create(left), LinqIterator.create(right));
    }

    public List<T> toList() {
        ArrayList<T> result = new ArrayList<T>();
        while (this.hasNext()) {
            result.add(this.next());
        }
        return result;
    }

    public static <T> LinqIterator<T> create(List<T> list) {
        return create(list.iterator());
    }

    @SafeVarargs
    public static <T> LinqIterator<T> create(T... data) {
        return new LinqIterator<T>() {
            int index = 0;

            @Override
            public boolean hasNext() {
                return this.index < data.length;
            }

            @Override
            public T next() {
                return data[this.index++];
            }
        };
    }

    public static <T> LinqIterator<T> create(Iterator<T> data) {
        return new LinqIterator<T>() {
            @Override
            public boolean hasNext() {
                return data.hasNext();
            }

            @Override
            public T next() {
                return data.next();
            }
        };
    }

    public boolean any(Predicate<T> p) {
        return this.filter(p).hasNext();
    }

    /**
     * Return true if both iterators return the same
     * sequence of elements.
     * @param other  Iterator to compare against.
     */
    public boolean same(LinqIterator<T> other) {
        while (this.hasNext()) {
            if (!other.hasNext())
                return false;
            T item = this.next();
            T oItem = other.next();
            if (!item.equals(oItem))
                return false;
        }
        return !other.hasNext();
    }

    @SafeVarargs
    public final boolean same(T... data) {
        return this.same(LinqIterator.create(data));
    }

    /**
     * Returns first all elements from this iterator, then all
     * elements in the next iterator.
     * @param next  Iterator whose elements are concatenated.
     */
    public LinqIterator<T> concat(LinqIterator<T> next) {
        return new LinqIterator<T>() {
            boolean inFirst = true;

            @Override
            public boolean hasNext() {
                if (inFirst) {
                    if (LinqIterator.this.hasNext())
                        return true;
                    inFirst = false;
                }
                return next.hasNext();
            }

            @Override
            public T next() {
                if (inFirst)
                    return LinqIterator.this.next();
                return next.next();
            }
        };
    }

    /**
     * Apply a function from left to right to all elements in this iterator.
     * @param function  Function to apply.
     * @param seed      Result returned if the list is empty.
     * @param <S>       Type of result.
     */
    public <S> S foldLeft(BiFunction<S, T, S> function, S seed) {
        S result = seed;
        while (this.hasNext())
            result = function.apply(result, this.next());
        return result;
    }
}
