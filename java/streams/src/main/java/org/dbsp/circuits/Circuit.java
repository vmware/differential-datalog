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

import org.dbsp.circuits.operators.*;
import org.dbsp.circuits.types.Type;
import org.dbsp.lib.Linq;

import java.util.*;

/**
 * A circuit contains multiple operators interconnected.
 * It also exposes output wires.
 * The input wires have to be connected manually.
 * For each expected input wire the circuit has an input "port".
 */
public class Circuit extends ComputationalElement implements Latch {
    /*
     * All operators in this circuit.
     */
    final List<Operator> operators;
    final List<Port> inputPorts;
    final List<Wire> outputWires;
    final List<Latch> latches;
    final String name;
    boolean sealed;
    int time;

    public Circuit(String name, List<Type> inputTypes, List<Type> outputTypes) {
        super(inputTypes, outputTypes);
        this.name = name;
        this.operators = new ArrayList<>();
        this.inputPorts = new ArrayList<Port>(inputTypes.size());
        for (Type t: inputTypes) {
            Port port = new Port(t);
            this.inputPorts.add(port);
            this.operators.add(port);
        }
        this.outputWires = new ArrayList<Wire>(outputTypes.size());
        this.latches = new ArrayList<>();
        this.sealed = false;
        this.time = 0;
    }

    public Port getInputPort(int index) {
        return this.inputPorts.get(index);
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
        this.checkConnected();
        return this;
    }

    public void checkConnected() {
        for (Operator op: this.operators)
            op.checkConnected();
    }

    public void addOperator(Operator op) {
        if (this.sealed)
            throw new RuntimeException("Circuit " + this + " is sealed");
        this.operators.add(op);
        if (op instanceof Latch)
            this.latches.add((Latch)op);
        op.setParent(this);
    }

    /**
     * The wire of the specified operator is an output wire of the circuit.
     */
    public Wire addOutputWireFromOperator(Operator op) {
        if (this.sealed)
            throw new RuntimeException("Circuit " + this + " is sealed");
        this.outputWires.add(op.outputWire());
        return op.outputWire();
    }

    public void setInput(int index, Wire source) {
        this.inputPorts.get(index).setInput(0, source);
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
        this.latch();
        this.push();
        this.time++;
    }

    /**
     * Tell all latches to emit their stored output.
     */
    public void latch() {
        this.log("Latching circuit " + this);
        for (Latch op: this.latches) {
            op.latch();
        }
    }

    @Override
    public void push() {
        for (Latch op: this.latches) {
            op.push();
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
    public String getName() {
        return "Circuit " + this.toString();
    }

    @Override
    public String toString() {
        return this.name;
    }

    @Override
    public void toGraphvizNodes(int indent, StringBuilder builder) {
        Linq.indent(indent, builder);
        builder.append("subgraph cluster_").append(this.graphvizId()).append(" {\n");
        indent += 2;
        Linq.indent(indent, builder);
        builder.append("label=\"").append(this.graphvizId()).append("\"\n");
        for (Operator op: this.operators) {
            op.toGraphvizNodes(indent, builder);
        }
        indent -= 2;
        Linq.indent(indent, builder);
        builder.append("}\n");
    }

    @Override
    public void toGraphvizWires(int indent, StringBuilder builder) {
        for (Operator op: this.operators) {
            op.toGraphvizWires(indent, builder);
        }
    }

    public String toGraphvizTop() {
        StringBuilder builder = new StringBuilder();
        builder.append("digraph ").append(this.name).append(" {\n");
        this.toGraphvizNodes(2, builder);
        this.toGraphvizWires(2, builder);

        for (Wire w: this.outputWires) {
            String fakeOutput = "wire" + this.id + "dest";
            Linq.indent(2, builder);
            builder.append(fakeOutput).append(" [label=\".\"]\n");
            Linq.indent(2, builder);
            builder.append(w.source.graphvizId()).append(" -> ").append(fakeOutput)
                    .append(" [label=\"(").append(w.id).append(")\"]\n");
        }

        builder.append("}\n");
        return builder.toString();
    }

    @Override
    public String graphvizId() {
        return this.name;
    }
}
