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
import org.dbsp.circuits.operators.FunctionOperator;
import org.dbsp.lib.ComparableObjectList;

import java.util.function.Function;

/**
 * An operator that transforms DynamicZSet values by applying a function to each element.
 * @param <W>  Type of weights used by ZSet.
 */
public class DynamicZSetMapOperator<W> extends FunctionOperator {
    /**
     * Create an operator that applies a map function to each element of a ZSet.
     * @param name    Operator name.
     * @param inputType    Input type.
     * @param outputType   Output type.
     * @param map     Apply this function to each element of the ZSet.
     */
    public DynamicZSetMapOperator(String name, Type inputType, Type outputType,
                                  Function<ComparableObjectList, ComparableObjectList> map) {
        //noinspection unchecked
        super(name, inputType, outputType, e -> ((DynamicZSet<W>)e).map(map));
    }
}
