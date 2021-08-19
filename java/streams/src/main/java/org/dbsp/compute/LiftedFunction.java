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

import org.dbsp.algebraic.Time;
import org.dbsp.types.IStream;

import java.util.function.Function;

/**
 * A lifted function lifts a function from T to S to compute on streams of values.
 * @param <T>  Type of left input.
 * @param <S>  Type of right input.
 */
public class LiftedFunction<T, S> implements StreamFunction<T, S> {
    final Function<T, S> function;

    public LiftedFunction(Function<T, S> function) {
        this.function = function;
    }

    @Override
    public IStream<S> apply(IStream<T> s) {
        return new IStream<S>(s.getTimeFactory()) {
            @Override
            public S get(Time index) {
                return LiftedFunction.this.function.apply(s.get(index));
            }
        };
    }
}
