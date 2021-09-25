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
import org.dbsp.algebraic.TimeFactory;

import java.util.function.BiFunction;

/**
 * A function that consumes two streams and produces another stream.
 * @param <T>  Type of elements in left stream.
 * @param <S>  Type of elements in right stream.
 * @param <R>  Type of elements in the result stream.
 */
public interface StreamBiFunction<T, S, R> extends BiFunction<IStream<T>, IStream<S>, IStream<R>> {
    /**
     * The incremental version of a stream function: a function that integrates
     * the inputs, applies this, and then differentiates the output.
     * @param groupT   Group used to integrate the left input.
     * @param groupS   Group used to integrate the right input.
     * @param groupR   Group used to differentiate outputs.
     * @return         The incremental version of this function.
     */
    default StreamBiFunction<T, S, R> inc(Group<T> groupT, Group<S> groupS, Group<R> groupR) {
        return (IStream<T> t, IStream<S> s) -> this.apply(t.integrate(groupT), s.integrate(groupS))
                .differentiate(groupR);
    }

    /**
     * Given an operator op: (t,s) -> r, this builds the stream operator
     * \lambda (t,s) \fix r . op(t, s + z(r))
     * @param op      Binary function to use for feedback.
     * @param group   Group that knows how to add values of type R
     * @param <T>     Left input type.
     * @param <R>     Right input type.
     * @return        A StreamBiFunction that computes the feedback of op.
     */
    static <T, R> StreamBiFunction<T, R, R> feedback(BiFunction<T, R, R> op, Group<R> group) {
        return new StreamBiFunction<T, R, R>() {
            @Override
            public IStream<R> apply(IStream<T> s, IStream<R> t) {
                return new IStream<R>(s.getTimeFactory()) {
                    @Override
                    public R get(Time index) {
                        T si = s.get(index);
                        if (index.isZero())
                            return op.apply(si, t.get(index));
                        R prevOut = this.get(index.previous());
                        return op.apply(si, group.add(prevOut, t.get(index)));
                    }
                };
            }
        };
    }

    static <T, S, R> StreamBiFunction<T, S, R> lift(BiFunction<T, S, R> function, TimeFactory fac) {
        return (t, s) -> new IStream<R>(fac) {
            @Override
            public R get(Time index) {
                T tVal = t.get(index);
                S sVal = s.get(index);
                return function.apply(tVal, sVal);
            }
        };
    }
}
