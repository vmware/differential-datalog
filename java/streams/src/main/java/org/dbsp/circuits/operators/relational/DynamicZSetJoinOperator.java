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

package org.dbsp.circuits.operators.relational;

import org.dbsp.algebraic.dynamicTyping.types.Type;
import org.dbsp.circuits.operators.BiFunctionOperator;
import org.dbsp.lib.ComparableObjectList;

import java.util.function.BiFunction;
import java.util.function.Function;

/**
 * An operator which joins two DynamicZSets
 * @param <W> Weight of elements in the DynamicZSet
 */
public class DynamicZSetJoinOperator<W> extends BiFunctionOperator {
    /**
     * Create a unary operator that computes by applying a function to its input.
     *
     * @param name        Operator name.
     * @param input0Type  Left input type.
     * @param input1Type  Right input type.
     * @param outputType  Output type.
     * @param leftKey     Function computing the left key.
     * @param rightKey    Function computing the right key.
     * @param computation Function that is applied to input to produce the output.
     */
    public DynamicZSetJoinOperator(String name, Type input0Type, Type input1Type, Type outputType,
                                   Function<ComparableObjectList, Object> leftKey,
                                   Function<ComparableObjectList, Object> rightKey,
                                   BiFunction<ComparableObjectList, ComparableObjectList, ComparableObjectList> computation) {
        //noinspection unchecked
        super(name, input0Type, input1Type, outputType,
                (z1, z2) -> ((DynamicZSet<W>)z1).join(
                        ((DynamicZSet<W>)z2),
                        leftKey, rightKey,
                        computation
                ));
    }
}
