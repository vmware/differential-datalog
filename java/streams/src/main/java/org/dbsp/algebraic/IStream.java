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

package org.dbsp.algebraic;

import javafx.util.Pair;
import org.dbsp.compute.StreamBiFunction;
import org.dbsp.compute.StreamFunction;

import java.util.function.BiFunction;
import java.util.function.Function;

/**
 * Generic stream API containing values of type V.
 * @param <V>  Type of values in stream.
 */
public abstract class IStream<V> {
    public abstract V get(Time index);
    final TimeFactory timeFactory;

    public IStream(TimeFactory timeFactory) {
        this.timeFactory = timeFactory;
    }

    public V get(int index) {
        return this.get(this.timeFactory.fromInteger(index));
    }

    /**
     * Method that adds two streams pointwise.
     * @param other  Stream to add to this one.
     * @param group  Group that knows how to perform element-wise addition.
     * @return       A stream that is the sum of this and other.
     */
    public IStream<V> add(IStream<V> other, Group<V> group) {
        return new IStream<V>(this.timeFactory) {
            @Override
            public V get(Time index) {
                V left = IStream.this.get(index);
                V right = other.get(index);
                return group.add(left, right);
            }
        };
    }

    /**
     * Method that negates a stream pointwise.
     * @param group  Group that knows how to negate an element.
     * @return   A stream that is the negation of this.
     */
    public IStream<V> negate(Group<V> group) {
        return new IStream<V>(this.timeFactory) {
            @Override
            public V get(Time index) {
                V left = IStream.this.get(index);
                return group.minus(left);
            }
        };
    }

    /**
     * Method that delays this stream.
     * @param group  Group that knows how to produce a zero.
     * @return   A stream that is the delayed version of this.
     */
    public IStream<V> delay(Group<V> group) {
        return new IStream<V>(this.timeFactory) {
            @Override
            public V get(Time index) {
                if (index.isZero()) {
                    return group.zero();
                }
                return IStream.this.get(index.previous());
            }
        };
    }

    /**
     * Method that computes the derivative of elements of a stream.
     * @param group  Group that knows how to perform stream arithmetic.
     * @return   A new stream that is the differential of the values in this.
     */
    public IStream<V> differentiate(Group<V> group) {
        return this.add(this.delay(group).negate(group), group);
    }

    /**
     * Method that cuts the current stream at a specified time moment.
     * @param at     Time moment to cut at.
     * @param group  Group that knows how to generate a zero value.
     * @return       A stream that is the cut version of this.
     */
    public IStream<V> cut(Time at, Group<V> group) {
        return new IStream<V>(this.timeFactory) {
            @Override
            public V get(Time index) {
                if (index.compareTo(at) >= 0)
                    return group.zero();
                return IStream.this.get(index);
            }
        };
    }

    /**
     * Compare the prefixes of two streams.
     * @param other      Stream to compare against.
     * @param time       Time to compare to.
     * @return           True if the two stream prefixes are equal..
     */
    public boolean comparePrefix(IStream<V> other, Time time) {
        for (Time t = this.timeFactory.zero(); !t.equals(time); t = t.next()) {
            V v0 = this.get(t);
            V v1 = other.get(t);
            boolean compare = v0.equals(v1);
            if (!compare)
                return false;
        }
        return true;
    }

    public boolean comparePrefix(IStream<V> other, int value) {
        return this.comparePrefix(other, this.timeFactory.fromInteger(value));
    }

    public IStream<V> integrate(Group<V> group) {
        return new IStream<V>(this.timeFactory) {
            // Invariant is that accumulated is the integral of the input stream
            // up to lastIndex exclusively.
            Time lastIndex = this.timeFactory.zero();
            V accumulated = group.zero();

            @Override
            public V get(Time index) {
                // hopefully we are called with the lastIndex+1 value
                if (index.compareTo(lastIndex) < 0) {
                    this.lastIndex = this.timeFactory.zero();
                    this.accumulated = group.zero();
                }
                for (Time i = this.lastIndex; i.compareTo(index) <= 0; i = i.next())
                    this.accumulated = group.add(this.accumulated, IStream.this.get(i));
                this.lastIndex = index.next();
                return this.accumulated;
            }
        };
    }

    public String toString(Time limit) {
        StringBuilder builder = new StringBuilder();
        builder.append("[");
        for (Time i = this.timeFactory.zero(); i.compareTo(limit) < 0; i = i.next()) {
            if (!i.isZero())
                builder.append(",");
            V value = this.get(i);
            builder.append(value.toString());
        }
        builder.append(",...");
        builder.append("]");
        return builder.toString();
    }

    public String toString(int limit) {
        return this.toString(this.timeFactory.fromInteger(limit));
    }

    public <S> IStream<S> apply(StreamFunction<V, S> function) {
        return function.apply(this);
    }

    public <S> IStream<Pair<V, S>> pair(IStream<S> other) {
        return new IStream<Pair<V, S>>(this.timeFactory) {
            @Override
            public Pair<V, S> get(Time index) {
                return new Pair<V, S>(IStream.this.get(index), other.get(index));
            }
        };
    }

    public V sumToZero(Group<V> group) {
        V zero = group.zero();
        V result = zero;
        for (Time index = this.timeFactory.zero(); ; index = index.next()) {
            V value = this.get(index);
            if (value == zero)
                break;
            result = group.add(result, value);
        }
        return result;
    }

    public TimeFactory getTimeFactory() {
        return this.timeFactory;
    }

    @Override
    public String toString() {
        return this.toString(4);
    }
}
