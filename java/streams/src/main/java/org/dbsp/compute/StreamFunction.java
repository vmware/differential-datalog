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

import org.dbsp.algebraic.staticTyping.Group;
import org.dbsp.algebraic.staticTyping.IStream;
import org.dbsp.algebraic.Time;

import java.util.function.Function;

/**
 * A stream function consumes a stream and produces a stream.
 * @param <T>  Type of elements in the input stream.
 * @param <S>  Type of elements in the output stream.
 */
public interface StreamFunction<T, S> extends Function<IStream<T>, IStream<S>> {
    /**
     * Composition of two stream functions: this function is applied after 'before'.
     * @param before  Function to apply first.
     * @param <V>     Type of results consumed by the before function.
     * @return        A function that is the composition of before and this.
     */
    default <V> StreamFunction<V, S> composeStream(StreamFunction<V, T> before) {
        return (IStream<V> v) -> this.apply(before.apply(v));
    }

    /**
     * The incremental version of a stream function: a function that integrates
     * the inputs, applies this, and then differentiates the output.
     * @param groupT   Group used to integrate inputs.
     * @param groupS   Group used to differentiate outputs.
     * @return         The incremental version of this function.
     */
    default StreamFunction<T, S> inc(Group<T> groupT, Group<S> groupS) {
        return (IStream<T> t) -> this.apply(t.integrate(groupT)).differentiate(groupS);
    }

    static <U> StreamFunction<U, U> delay(Group<U> group) {
        return s -> s.delay(group);
    }

    static <U> StreamFunction<U, U> integrate(Group<U> group) {
        return s -> s.integrate(group);
    }

    static <U> StreamFunction<U, U> differentiate(Group<U> group) {
        return s -> s.differentiate(group);
    }

    static <U, V> StreamFunction<U, V> fromFunction(Function<IStream<U>, IStream<V>> func) {
        return func::apply;
    }

    static <T, S> StreamFunction<T, S> lift(Function<T, S> function) {
        return t -> new IStream<S>(t.getTimeFactory()) {
            @Override
            public S get(Time index) {
                T tVal = t.get(index);
                return function.apply(tVal);
            }
        };
    }

    /**
     * Given an operator op: t -> r, this builds the stream operator
     * \lambda t \fix r . op(t + z(r))
     * @param op      Unary function to use for feedback.
     * @param group   Group that knows how to add values of type R
     * @param <T>     Input type.
     * @return        A StreamFunction that computes the feedback of op.
     */
    static <T> StreamFunction<T, T> feedback(Function<T, T> op, Group<T> group) {
        return new StreamFunction<T, T>() {
            @Override
            public IStream<T> apply(IStream<T> s) {
                return new IStream<T>(s.getTimeFactory()) {
                    @Override
                    public T get(Time index) {
                        if (index.isZero())
                            return op.apply(s.get(index));
                        T prevOut = this.get(index.previous());
                        return op.apply(group.add(prevOut, s.get(index)));
                    }
                };
            }
        };
    }
}
