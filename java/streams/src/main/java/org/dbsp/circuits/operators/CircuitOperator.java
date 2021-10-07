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

import org.dbsp.circuits.Circuit;
import org.dbsp.circuits.Scheduler;
import org.dbsp.circuits.Wire;
import org.dbsp.algebraic.dynamicTyping.types.Type;
import org.dbsp.lib.Pair;
import org.dbsp.lib.Utilities;

/**
 * An operator that contains inside a circuit with a single output.
 */
public class CircuitOperator extends Operator implements Latch {
    public final Circuit circuit;
    /**
     * If true we usually don't want to see inside the circuit.
     */
    public final boolean basic;

    public CircuitOperator(Circuit circuit, boolean basic) {
        super(circuit.getInputTypes(), circuit.getOutputTypes().get(0));
        this.basic = basic;
        this.circuit = circuit;
        if (this.circuit.getOutputTypes().size() != 1)
            throw new RuntimeException("Operators must have only 1 output wire, not " +
                    this.circuit.getOutputWires().size() + ": " + circuit);
        if (circuit.getOutputWires().size() < 1)
            throw new RuntimeException("Circuit " + this + " does not have an output wire");
        this.output = circuit.getOutputWires().get(0);
    }

    public CircuitOperator(Circuit circuit) {
        this(circuit, false);
    }

    @Override
    public Object evaluate(Scheduler scheduler) {
        for (int i = 0; i < this.inputCount(); i++) {
            Wire w = this.inputs.get(i);
            Object ii = w.getValue(scheduler);
            this.circuit.getInputPort(i).setValue(ii);
        }
        this.circuit.step(scheduler);
        return this.outputWire().getValue(scheduler);
    }

    @Override
    public void reset(Scheduler scheduler) {
        this.circuit.reset(scheduler);
    }

    @Override
    public void latch(Scheduler scheduler) { this.circuit.latch(scheduler); }

    @Override
    public void push(Scheduler scheduler) { this.circuit.push(scheduler); }

    @Override
    public void toGraphvizNodes(boolean deep, int indent, StringBuilder builder) {
        if (deep || !this.basic)
            this.circuit.toGraphvizNodes(deep, indent, builder);
        else {
            Utilities.indent(indent, builder);
            builder.append(this.graphvizId())
                    .append(" [label=\"").append(this.circuit.toString())
                    .append(" (").append(this.id).append(")\"]\n");
        }
    }

    @SuppressWarnings("ConstantConditions")
    @Override
    public void toGraphvizWires(boolean deep, int indent, StringBuilder builder) {
        if (deep || !this.basic)
            this.circuit.toGraphvizWires(deep, indent, builder);
        else {
            this.outputWire().toGraphviz(this, deep, indent, builder);
        }
    }

    public void setInput(int index, Wire source) {
        this.circuit.setInput(index, source);
    }

    public void checkConnected() {
        this.circuit.checkConnected();
    }

    protected Pair<Operator, Integer> getActualConsumer(int input) {
        return new Pair<Operator, Integer>(this.circuit.getInputPort(input), 0);
    }

    /**
     * Return an operator that performs integration over a stream of values of type @{type}.
     * @param type  Type of values in the stream.
     */
    public static CircuitOperator integrationOperator(Type type) {
        Circuit circuit = new Circuit("I",
                Utilities.list(type), Utilities.list(type));
        PlusOperator plus = new PlusOperator(type);
        circuit.addOperator(plus);
        DelayOperator delay = new DelayOperator(type);
        circuit.addOperator(delay);
        plus.connectTo(delay, 0);
        delay.connectTo(plus, 1);
        circuit.addOutputWireFromOperator(plus);
        Operator input = circuit.getInputPort(0);
        input.connectTo(plus, 0);
        return new CircuitOperator(circuit.seal(), true);
    }

    /**
     * Return an operator that performs differentiation over a stream of values of type @{type}.
     * @param type  Type of values in the stream.
     */
    public static CircuitOperator derivativeOperator(Type type) {
        Circuit circuit = new Circuit("D",
                Utilities.list(type), Utilities.list(type));
        PlusOperator plus = new PlusOperator(type);
        circuit.addOperator(plus);
        DelayOperator delay = new DelayOperator(type);
        circuit.addOperator(delay);
        NegateOperator neg = new NegateOperator(type);
        circuit.addOperator(neg);
        Operator port = circuit.getInputPort(0);
        port.connectTo(plus, 0);
        port.connectTo(delay, 0);
        delay.connectTo(neg, 0);
        neg.connectTo(plus, 1);
        circuit.addOutputWireFromOperator(plus);
        return new CircuitOperator(circuit.seal(), true);
    }

    @Override
    public String graphvizId() {
        return "circuit" + this.id;
    }

    @Override
    public String getName() { return "Op:" + this.circuit.getName(); }

    @Override
    public String toString() { return super.toString() + "{" + this.circuit.toString() + "}"; }

    /**
     * Creates a bracketed operator by putting a delta in front and an int at the back of this operator.
     * The operator must be unary.
     */
    public CircuitOperator bracket() {
        if (this.inputCount() != 1)
            throw new RuntimeException("Only unary operators can be bracketed");
        Circuit circuit = new Circuit("[" + this + "]",
                Utilities.list(this.getInputType(0)), Utilities.list(this.getOutputType()));
        DeltaOperator delta = new DeltaOperator(this.getInputType(0));
        circuit.addOperator(delta);
        circuit.getInputPort(0).connectTo(delta, 0);
        circuit.addOperator(this);
        delta.connectTo(this, 0);
        Operator intOp = circuit.addOperator(new IntOperator(this.getOutputType(), delta, this));
        this.connectTo(intOp, 0);
        circuit.addOutputWireFromOperator(intOp);
        return new CircuitOperator(circuit.seal());
    }
}
