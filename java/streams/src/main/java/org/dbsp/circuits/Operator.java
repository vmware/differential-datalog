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

package org.dbsp.circuits;

import org.dbsp.circuits.types.Type;

import java.util.function.Function;

public abstract class Operator {
    static int crtid = 0;

    final Wire output;
    final Wire[] inputs;
    final int inputsPresent;
    final int id;
    final Type[] inputTypes;
    final Type outputType;

    public Operator(Type[] inputTypes, Type outputType) {
        this.output = new Wire(outputType);
        // Input wires are not connected.
        this.inputs = new Wire[inputTypes.length];
        this.inputsPresent = 0;
        this.id = crtid++;
        this.inputTypes = inputTypes;
        this.outputType = outputType;
    }

    /**
     * A new computation cycle has started, reset the internal state.
     * Most operators do nothing.
     */
    public void reset() {}

    public int arity() {
        return this.inputs.length;
    }

    public void connectInput(int index, Wire wire) {
        if (this.inputs[index] != null)
            throw new RuntimeException("Input " + index + " already connected");
        this.inputs[index] = wire;
        if (!this.inputTypes[index].equals(wire.getType()))
            throw new RuntimeException("Type mismatch: operator input " + index + " expects " +
                    this.inputTypes[index] + " but was provided with " + wire.getType());
    }

    boolean checked = false;
    private void checkConnected() {
        if (this.checked)
            return;
        for (Wire w: this.inputs) {
            if (w == null)
                throw new RuntimeException("Input not connected");
        }
        checked = true;
    }

    public abstract Value evaluate(Function<Integer, Value> inputProvider);

    // Extract values from the input wires, compute the
    // result.
    public void compute() {
        this.checkConnected();
        Value result = this.evaluate(index -> this.inputs[index].getValue());
        this.output.setValue(result);
    }

    @SafeVarargs
    static <T> T[] makeArray(T... data) {
        return data;
    }
}
