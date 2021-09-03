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

package org.dbsp.operators;

import org.dbsp.algebraic.TimeFactory;
import org.dbsp.algebraic.IStream;
import org.dbsp.circuits.types.StreamType;
import org.dbsp.circuits.types.Type;

import java.util.function.Function;

/**
 * An operator that produces a stream from a value.
 * @param <T> Type of input value.
 */
public class Delta0<T> extends UnaryOperator<T, IStream<T>> {
    protected final TimeFactory factory;

    public Delta0(Type<T> inputType, TimeFactory factory) {
        super(inputType, new StreamType<T>(inputType, factory));
        this.factory = factory;
    }

    @Override
    public Function<T, IStream<T>> getComputation() {
        return value -> new org.dbsp.compute.Delta0<T>(value, this.inputType.getGroup(), this.factory);
    }

    @Override
    public String toString() {
        return "Delta0";
    }
}
