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
import java.util.function.Function;
import java.util.function.Predicate;

/**
 * An iterator that returns values of type T.
 * These iterators cannot be reset.
 * @param <T>  Value returned.
 */
public interface LinqIterator<T> extends Iterator<T> {
    /**
     * An iterator over values of type S.
     * @param map  Function that converts each value of type T to a value of type S.
     * @param <S>  Value produced by function.
     */
    default <S> LinqIterator<S> map(Function<T, S> map) {
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

    default LinqIterator<T> filter(Predicate<T> predicate) {
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

    default <S> LinqIterator<Pair<T, S>> zip(LinqIterator<S> other) {
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

    default <S, U> LinqIterator<Triple<T, S, U>> zip3(LinqIterator<S> left, LinqIterator<U> right) {
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

    default <S> LinqIterator<Pair<T, S>> zip(List<S> other) {
        return this.zip(LinqIterator.fromList(other));
    }

    default <S, U> LinqIterator<Triple<T, S, U>> zip3(List<S> left, List<U> right) {
        return this.zip3(LinqIterator.fromList(left), LinqIterator.fromList(right));
    }

    default List<T> toList() {
        ArrayList<T> result = new ArrayList<T>();
        while (this.hasNext()) {
            result.add(this.next());
        }
        return result;
    }

    static <T> LinqIterator<T> fromList(List<T> list) {
        return fromIterator(list.iterator());
    }

    static <T> LinqIterator<T> fromIterator(Iterator<T> data) {
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

    static <T> LinqIterator<T> fromArray(T[] data) {
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

    default boolean any(Predicate<T> p) {
        return this.filter(p).hasNext();
    }
}
