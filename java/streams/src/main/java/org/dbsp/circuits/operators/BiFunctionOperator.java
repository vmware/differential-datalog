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

package org.dbsp.circuits.operators;

import org.dbsp.algebraic.dynamicTyping.types.Type;

import java.util.function.BiFunction;

/**
 * A unary operator that applies a specified function to its input.
 */
public class BiFunctionOperator extends BinaryOperator {
    private final BiFunction<Object, Object, Object> computation;
    final String name;

    /**
     * Create a unary operator that computes by applying a function to its input.
     * @param name           Operator name.
     * @param input0Type     Left input type.
     * @param input1Type     Right input type.
     * @param outputType     Output type.
     * @param computation    Function that is applied to input to produce the output.
     */
    public BiFunctionOperator(String name, Type input0Type, Type input1Type,
                              Type outputType, BiFunction<Object, Object, Object> computation) {
        super(input0Type, input1Type, outputType);
        this.computation = computation;
        this.name = name;
    }

    @Override
    public Object evaluate(Object left, Object right) {
        return this.computation.apply(left, right);
    }

    public String toString() {
        return this.name;
    }
}
