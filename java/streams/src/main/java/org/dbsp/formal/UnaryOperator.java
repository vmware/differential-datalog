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
import org.dbsp.compute.LiftedFunction;
import org.dbsp.types.StreamType;
import org.dbsp.types.Type;

import java.util.function.Function;

/**
 * An operator that has a single input.
 * @param <T> Concrete Java implementation of input type.
 * @param <S> Concrete Java implementation of output type.
 */
public abstract class UnaryOperator<T, S> {
    public final Type<T> inputType;
    public final Type<S> outputType;

    protected UnaryOperator(Type<T> inputType, Type<S> outputType) {
        this.inputType = inputType;
        this.outputType = outputType;
    }

    /**
     * The type of the output.
     */
    public Type<S> getOutputType() {
        return this.outputType;
    }

    /**
     * The type of the input.
     */
    public Type<T> getInputType(int input) {
        return this.inputType;
    }

    /**
     * @return Actual function that performs the computation of this operator.
     */
    public abstract Function<T, S> getComputation();

    /**
     * @return A lifted version of this operator.
     */
    public UnaryOperator<IStream<T>, IStream<S>> lift(TimeFactory factory) {
        return new UnaryOperator<IStream<T>, IStream<S>>(
                new StreamType<T>(this.inputType, factory),
                new StreamType<S>(this.outputType, factory)) {
            @Override
            public Function<IStream<T>, IStream<S>> getComputation() {
                return new LiftedFunction<T, S>(UnaryOperator.this.getComputation());
            }

            @Override
            public String toString() {
                return "^" + UnaryOperator.this.toString();
            }
        };
    }

    /**
     * Compose this operator by applying the other operator afterwards.
     * @param other   Operator to apply after this one.
     * @param <U>     Type of output produced by the other operator.
     * @return        A unary operator that is the composition of this followed by other.
     */
    public <U> UnaryOperator<T, U> compose(UnaryOperator<S, U> other) {
        return new UnaryOperator<T, U>(this.inputType, other.outputType) {
            @Override
            public Function<T, U> getComputation() {
                return other.getComputation().compose(UnaryOperator.this.getComputation());
            }
        };
    }
}
