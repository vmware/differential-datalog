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

import java.util.*;

/**
 * A circuit contains multiple operators interconnected.
 * It also exposes wires that are not yet connected.
 */
public class Circuit implements Consumer {
    /*
     * All operators in this circuit.
     */
    final List<Operator> operators;
    final List<Wire> inputWires;
    final Set<Wire> outputWires;
    final String name;
    boolean sealed;
    int time;

    public Circuit(String name) {
        this.name = name;
        this.operators = new ArrayList<>();
        this.inputWires = new ArrayList<>();
        this.outputWires = new HashSet<Wire>();
        this.sealed = false;
        this.time = 0;
    }

    /**
     * The list of input wires: operator wires that are not connected to a source.
     */
    public List<Wire> getInputWires() {
        if (!this.sealed)
            throw new RuntimeException("Circuit " + this + " is sealed");
        return this.inputWires;
    }

    /**
     * No more nodes will be added.
     */
    public void seal() {
        if (this.sealed)
            throw new RuntimeException("Circuit " + this + " is sealed");
        this.sealed = true;
    }

    public void addOperator(Operator op) {
        if (this.sealed)
            throw new RuntimeException("Circuit " + this + " is sealed");
        this.operators.add(op);
        this.outputWires.add(op.output);
        op.setParent(this);
    }

    /**
     * Add an input wire to the circuit, connecting to the specified operator as its specified input.
     * @param op    operator that this wire connects to.
     * @param index operator input that the wire connects to.
     * @return      the new wire.
     */
    public Wire addInputWire(Operator op, int index) {
        Wire w = new Wire(op.inputTypes[index]);
        this.inputWires.add(w);
        op.connectInput(index, w);
        return w;
    }

    /**
     * The wire of the specified operator is an output wire of the circuit.
     */
    public Wire addOutputWire(Operator op) {
        if (op.output.hasConsumers())
            throw new RuntimeException("Output wire of " + op + " has consumers");
        this.outputWires.add(op.output);
        op.output.addConsumer(this);
        return op.output;
    }

    /**
     * Execute the circuit from one clock cycle.  Sets the output wires.
     * The protocol to use a circuit is:
     * - set all input wires using setValue
     * - step
     * - get the values from all output wires using getValue
     */
    public void step() {
        this.log("Time step " + this.time);
        for (Wire w: this.inputWires) {
            w.log();
            w.push();
        }
        this.time++;
    }

    /**
     * Nothing to do for circuits.
     */
    @Override
    public void compute() {}

    public void reset() {
        this.time = 0;
        for (Operator op: this.operators)
            op.reset();
    }
}
