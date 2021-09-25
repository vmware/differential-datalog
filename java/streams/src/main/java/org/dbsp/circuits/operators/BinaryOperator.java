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
import org.dbsp.circuits.Scheduler;
import org.dbsp.lib.Utilities;

/**
 * Base class for all binary operators.
 */
public abstract class BinaryOperator extends Operator {
    public BinaryOperator(Type input0type, Type input1Type, Type outputType) {
        super(Utilities.list(input0type, input1Type), outputType);
    }

    public abstract Object evaluate(Object left, Object right);

    @Override
    public Object evaluate(Scheduler scheduler) {
        Object v0 = this.inputs.get(0).getValue(scheduler);
        Object v1 = this.inputs.get(1).getValue(scheduler);
        return this.evaluate(v0, v1);
    }
}
