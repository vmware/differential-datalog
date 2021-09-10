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

import org.dbsp.circuits.operators.Consumer;
import org.dbsp.circuits.operators.Operator;
import org.dbsp.circuits.operators.Wire;
import org.dbsp.lib.Linq;

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
    final List<Wire> outputWires;
    final String name;
    boolean sealed;
    int time;

    public Circuit(String name) {
        this.name = name;
        this.operators = new ArrayList<>();
        this.inputWires = new ArrayList<Wire>();
        this.outputWires = new ArrayList<Wire>();
        this.sealed = false;
        this.time = 0;
    }

    /**
     * The list of input wires: operator wires that are not connected to a source.
     */
    public List<Wire> getInputWires() {
        if (!this.sealed)
            throw new RuntimeException("Circuit " + this + " is not sealed");
        return this.inputWires;
    }

    public List<Wire> getOutputWires() {
        if (!this.sealed)
            throw new RuntimeException("Circuit " + this + " is not sealed");
        return this.outputWires;
    }

    /**
     * No more nodes will be added.
     */
    public Circuit seal() {
        if (this.sealed)
            throw new RuntimeException("Circuit " + this + " is sealed");
        this.sealed = true;
        for (Operator op: this.operators)
            op.checkConnected();
        return this;
    }

    public void addOperator(Operator op) {
        if (this.sealed)
            throw new RuntimeException("Circuit " + this + " is sealed");
        this.operators.add(op);
        op.setParent(this);
    }

    /**
     * Add an input wire to the circuit.
     */
    public void addInputWire(Wire wire) {
        if (this.sealed)
            throw new RuntimeException("Circuit " + this + " is sealed");
        this.inputWires.add(wire);
    }

    /**
     * The wire of the specified operator is an output wire of the circuit.
     */
    public Wire addOutputWire(Operator op) {
        if (this.sealed)
            throw new RuntimeException("Circuit " + this + " is sealed");
        this.outputWires.add(op.outputWire());
        op.outputWire().addConsumer(this);
        return op.outputWire();
    }

    /**
     * Execute the circuit from one clock cycle.  Sets the output wires.
     * The protocol to use a circuit is:
     * - set all input wires using setValue
     * - step
     * - get the values from all output wires using getValue
     */
    public void step() {
        this.latch();
        this.log("Time step " + this.time);
        for (Wire w: this.inputWires) {
            w.push();
        }
        this.time++;
    }

    /**
     * Tell all latches to emit their stored output.
     */
    public void latch() {
        this.log("Latching circuit " + this);
        for (Operator op: this.operators) {
            op.latch();
        }
    }

    /**
     * Nothing to do for circuits.
     */
    @Override
    public void notifyInput() {}

    public void reset() {
        this.time = 0;
        for (Operator op: this.operators)
            op.reset();
    }

    @Override
    public String toString() {
        return this.name;
    }

    public void toGraphviz(int indent, StringBuilder builder) {
        indent += 2;
        for (Wire in: this.inputWires) {
            Linq.indent(indent, builder);
            // create a 'fake' node for the input
            String fakeNodeName = "wire" + in.id + "source";
            builder.append(fakeNodeName).append(" [label=\"o\"]\n");
            in.toGraphviz(indent, builder, fakeNodeName);
        }
        for (Operator op: this.operators) {
            op.toGraphviz(indent, builder);
        }
        for (Operator op: this.operators) {
            op.outputWire().toGraphviz(indent, builder, "");
        }
    }

    public String toGraphvizWrapped() {
        StringBuilder builder = new StringBuilder();
        builder.append("digraph ").append(this.name).append(" {\n");
        this.toGraphviz(0, builder);
        builder.append("}\n");
        return builder.toString();
    }

    @Override
    public String graphvizId() {
        return this.name;
    }
}
