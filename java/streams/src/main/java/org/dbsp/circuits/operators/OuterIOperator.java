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


/**
 * The OuterI operator is in fact an integration operator that computes on
 * the outer stream in a nested stream.  (We don't need an InnerI, that's
 * just a lifted integral).
 */
public class OuterIOperator extends OuterOperator {
    public OuterIOperator(Type type) {
        super(type);
    }

    @Override
    public String toString() {
        return "II";
    }

    @Override
    public Object evaluate(Object input, Scheduler scheduler) {
        Object result;
        if (this.history.size() > this.currentIndex) {
            Object previous = this.history.get(this.currentIndex);
            result = this.group.add(previous, input);
            this.history.set(this.currentIndex, result);
        } else {
            assert this.history.size() == this.currentIndex;
            this.history.add(input);
            result = input;
        }
        this.currentIndex++;
        return result;
    }
}
