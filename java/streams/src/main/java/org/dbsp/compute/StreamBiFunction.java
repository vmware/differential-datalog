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

import org.dbsp.algebraic.Group;
import org.dbsp.types.IStream;

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
}
