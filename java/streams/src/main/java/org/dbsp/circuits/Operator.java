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

import javax.annotation.Nullable;
import java.util.function.Function;

/**
 * Abstract class representing an operator.
 * An operator has one or more input wires, and one output wire.
 * The output wire "belongs" to this operator; the inputs are
 * references to the outputs of other operators.
 * An operator also has types for all inputs and for its output.
 */
public abstract class Operator implements Consumer {
    static int crtid = 0;

    final Wire output;
    final Wire[] inputs;
    final int inputsPresent;
    final int id;
    final Type[] inputTypes;
    final Type outputType;
    @Nullable
    Circuit parent;

    public Operator(Type[] inputTypes, Type outputType) {
        this.output = new Wire(outputType);
        // Input wires are not connected.
        this.inputs = new Wire[inputTypes.length];
        this.inputsPresent = 0;
        this.id = crtid++;
        this.inputTypes = inputTypes;
        this.outputType = outputType;
        this.parent = null;
    }

    /**
     * A new computation cycle has started, reset the internal state.
     * Most operators do nothing.
     */
    public void reset() {}

    public int arity() {
        return this.inputs.length;
    }

    void connectInput(int index, Wire wire) {
        if (this.inputs[index] != null)
            throw new RuntimeException("Input " + index + " already connected");
        wire.addConsumer(this);
        this.inputs[index] = wire;
        if (!this.inputTypes[index].equals(wire.getType()))
            throw new RuntimeException("Type mismatch: operator input " + index + " expects " +
                    this.inputTypes[index] + " but was provided with " + wire.getType());
    }

    public void connectTo(Operator to, int input) {
        to.connectInput(input, this.output);
    }

    public void setParent(Circuit circuit) {
        if (this.parent != null)
            throw new RuntimeException("Operator already has a parent: " + this.parent);
        this.parent = circuit;
    }

    @Nullable
    public Circuit getParent() {
        return this.parent;
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

    public abstract Object evaluate(Function<Integer, Object> inputProvider);

    // Extract values from the input wires, compute the
    // result.
    public void compute() {
        this.checkConnected();
        Object result = this.evaluate(index -> this.inputs[index].getValue());
        this.log(this + " computed " + result);
        this.output.setValue(result);
        this.output.push();
    }

    @SafeVarargs
    static <T> T[] makeArray(T... data) {
        return data;
    }
}
