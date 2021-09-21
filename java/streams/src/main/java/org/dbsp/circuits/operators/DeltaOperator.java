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

import java.util.Objects;

/**
 * The DeltaOperator has a scalar input but produces a stream output.
 */
public class DeltaOperator extends UnaryOperator {
    private final Object zero;

    protected DeltaOperator(Type type) {
        super(type, type);
        this.zero = Objects.requireNonNull(type.getGroup()).zero();
    }

    @Override
    public Object evaluate(Object input) {
        return input;
    }

    @Override
    public String toString() {
        return "Delta";
    }

    /**
     * Method called by the IntOperator to indicate that a new cycle should be executed.
     */
    public void repeat() {
        this.emitOutput(this.zero);
    }
}
