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

import org.dbsp.circuits.ComputationalElement;
import org.dbsp.circuits.types.Type;
import org.dbsp.lib.Linq;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

/**
 * Abstract class representing an operator.
 * An operator has one or more input wires, and one output wire.
 * The output wire "belongs" to this operator; the inputs are
 * references to the outputs of other operators.
 * An operator also has types for all inputs and for its output.
 */
public abstract class Operator extends ComputationalElement {
    /* final */ Wire output;
    final List<Wire> inputs;
    // Number of inputs received in the current computation cycle.
    int inputsPresent;

    public Operator(List<Type> inputTypes, Type outputType) {
        super(inputTypes, Linq.list(outputType));
        this.output = new Wire(outputType, this);
        // Input wires are not connected.
        this.inputs = new ArrayList<Wire>(inputTypes.size());
        for (int i = 0; i < inputTypes.size(); i++)
            this.inputs.add(null);
        this.inputsPresent = 0;
    }

    public void setInput(int index, Wire source) {
        this.inputs.set(index, source);
    }

    /**
     * A new computation cycle has started, reset the internal state.
     * Most operators do nothing.
     */
    public void reset() {}

    public void connectTo(Operator to, int input) {
        if (to.getInputWires().get(input) != null)
            throw new RuntimeException(to.getName() + ": input " + input + " already connected");
        Operator actual = to.getActualConsumer(to, input);
        this.output.addConsumer(actual);
        actual.setInput(input, this.output);
        if (!to.inputTypes.get(input).equals(this.output.getType()))
            throw new RuntimeException("Type mismatch: operator input " + input + " expects " +
                    to.inputTypes.get(input) + " but was provided with " + this.output.getType());
    }

    /**
     * The actual node that will receive the input for the specified input of the operator.
     * @param to     Operator we connect to.
     * @param input  Input we connect to.
     * @return       The actual operator that this connection is made to.
     */
    protected Operator getActualConsumer(Operator to, int input) {
        return to;
    }

    public void checkConnected() {
        for (int i = 0; i < this.inputs.size(); i++) {
            Wire w = this.inputs.get(i);
            if (w == null)
                throw new RuntimeException(this.getName() + ": input " + i + " not connected");
        }
    }

    public abstract Object evaluate(Function<Integer, Object> inputProvider);

    @Override
    public String getName() {
        return this.toString() + " (" + this.id + ")";
    }

    // Default behavior of an operator on an input notification:
    // Extract values from the input wires, compute the
    // result, notify consumers.
    public void notifyInput() {
        this.inputsPresent++;
        if (this.inputsPresent != this.inputCount())
            return;
        this.inputsPresent = 0;

        // All inputs are present, we can compute.
        Object result = this.evaluate(index -> this.inputs.get(index).getValue(true));
        this.log("computed " + result);
        this.emitOutput(result);
    }

    public void emitOutput(Object result) {
        this.output.setValue(result);
        this.output.notifyConsumers();
    }

    public Wire outputWire() {
        return this.output;
    }

    public List<Wire> getInputWires() { return this.inputs; }

    @Override
    public String graphvizId() {
        return "node" + this.id;
    }

    /**
     * Generate a graphviz representation of the node in the specified builder.
     */
    @Override
    public void toGraphvizNodes(int indent, StringBuilder builder) {
        Linq.indent(indent, builder);
        builder.append(this.graphvizId())
               .append(" [label=\"").append(this.toString())
               .append(" (").append(this.id).append(")\"]\n");
    }

    @Override
    public void toGraphvizWires(int indent, StringBuilder builder) {
        this.outputWire().toGraphviz(indent, builder);
    }
}
