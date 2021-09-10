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

import org.dbsp.algebraic.Group;
import org.dbsp.circuits.types.Type;

import java.util.Objects;

/**
 * An operator that works on streams.  It delays the input stream by 1 clock.
 */
public class DelayOperator extends UnaryOperator {
    Object previous;
    final Group<Object> group;

    public DelayOperator(Type elementType) {
        super(elementType, elementType);
        this.group = Objects.requireNonNull(elementType.getGroup());
        this.previous = this.group.zero();
    }

    @Override
    public String toString() {
        return "z";
    }

    @Override
    public void reset() {
        this.previous = this.group.zero();
    }

    @Override
    public void latch() {
        this.output.setValue(this.previous);
        this.output.push();
    }

    @Override
    public void emitOutput(Object result) {
        // delays do not emit their output at the normal time,
        // they emit it when asked to latch it.
    }

    @Override
    public Object evaluate(Object input) {
        Object result = this.previous;
        this.previous = input;
        return result;
    }
}
