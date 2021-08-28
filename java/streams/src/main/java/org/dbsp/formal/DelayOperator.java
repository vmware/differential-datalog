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

package org.dbsp.formal;

import org.dbsp.algebraic.TimeFactory;
import org.dbsp.types.IStream;
import org.dbsp.types.StreamType;
import org.dbsp.types.Type;

import java.util.function.Function;

/**
 * An operator that works on streams.  It delays the input stream by 1 clock.
 * @param <T> Concrete implementation of elements of the input and output stream.
 */
public class DelayOperator<T, F extends TimeFactory> extends
        UniformUnaryOperator<IStream<T>> {
    final Type<T> elementType;

    public DelayOperator(Type<T> elementType, F factory) {
        super(new StreamType<T>(elementType, factory));
        this.elementType = elementType;
        if (!this.getInputType().isStream())
            throw new RuntimeException("Delay must be applied to a stream type, not to " + elementType);
    }

    @Override
    public Function<IStream<T>, IStream<T>> getComputation() {
        return (IStream<T> s) -> s.delay(this.elementType.getGroup());
    }

    @Override
    public String toString() {
        return "z";
    }
}
