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
import org.dbsp.compute.LiftedBifunction;
import org.dbsp.types.StreamType;
import org.dbsp.types.Type;

import java.util.function.BiFunction;

/**
 * An operator that has a two inputs of the same type.
 * @param <T> Concrete Java implementation of input type.
 * @param <S> Concrete Java implementation of output type.
 */
public abstract class BinaryOperator<T, S> {
    public final Type<T> inputType;
    public final Type<S> outputType;

    protected BinaryOperator(Type<T> inputType, Type<S> outputType) {
        this.inputType = inputType;
        this.outputType = outputType;
    }

    public Type<S> getOutputType() {
        return this.outputType;
    }

    public Type<T> getInputType(int input) {
        return this.inputType;
    }

    /**
     * The code that performs the computation of this operator.
     */
    public abstract BiFunction<T, T, S> getComputation();

    /**
     * @return A lifted version of this operator.
     */
    public <F extends TimeFactory> BinaryOperator<IStream<T>, IStream<S>> lift(F factory) {
        return new BinaryOperator<IStream<T>, IStream<S>>(
                new StreamType<T>(this.inputType, factory),
                new StreamType<S>(this.outputType, factory)) {
            @Override
            public BiFunction<IStream<T>, IStream<T>, IStream<S>> getComputation() {
                return new LiftedBifunction<T, T, S>(BinaryOperator.this.getComputation());
            }

            @Override
            public String toString() {
                return "^" + BinaryOperator.this.toString();
            }
        };
    }
}
