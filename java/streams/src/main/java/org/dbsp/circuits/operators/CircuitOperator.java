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
import org.dbsp.circuits.types.Type;
import org.dbsp.lib.Linq;

import java.util.List;
import java.util.function.Function;

/**
 * An operator that contains inside a circuit with a single output.
 */
public class CircuitOperator extends Operator {
    public final Circuit circuit;
    final List<Wire> inputWires;
    final Wire outputWire;

    static List<Type> typesFromWires(List<Wire> wires) {
        return Linq.map(wires, Wire::getType);
    }

    public CircuitOperator(Circuit circuit) {
        super(typesFromWires(circuit.getInputWires()), typesFromWires(circuit.getOutputWires()).get(0));
        this.circuit = circuit;
        if (this.circuit.getOutputWires().size() != 1)
            throw new RuntimeException("Operators must have only 1 output wire, not " +
                    this.circuit.getOutputWires().size() + ": " + circuit);
        this.inputWires = circuit.getInputWires();
        this.outputWire = circuit.getOutputWires().get(0);
    }

    @Override
    public Object evaluate(Function<Integer, Object> inputProvider) {
        for (int i = 0; i < this.arity(); i++) {
            Object ii = inputProvider.apply(i);
            this.inputWires.get(i).setValue(ii);
        }
        this.circuit.step();
        return this.outputWire.getValue();
    }

    @Override
    public void reset() {
        this.circuit.reset();
    }

    @Override
    public void latch() { this.circuit.latch(); }

    /**
     * Return an operator that performs integration over a stream of values of type @{type}.
     * @param type  Type of values in the stream.
     */
    public static CircuitOperator integrationOperator(Type type) {
        Circuit circuit = new Circuit("I");
        PlusOperator plus = new PlusOperator(type);
        circuit.addOperator(plus);
        DelayOperator delay = new DelayOperator(type);
        circuit.addOperator(delay);
        plus.connectTo(delay, 0);
        delay.connectTo(plus, 1);
        circuit.addOutputWire(delay);
        Wire input = new Wire(type);
        plus.connectInput(0, input);
        circuit.addInputWire(input);
        return new CircuitOperator(circuit.seal());
    }

    /**
     * Return an operator that performs differentiation over a stream of values of type @{type}.
     * @param type  Type of values in the stream.
     */
    public static CircuitOperator derivativeOperator(Type type) {
        Circuit circuit = new Circuit("D");
        PlusOperator plus = new PlusOperator(type);
        circuit.addOperator(plus);
        DelayOperator delay = new DelayOperator(type);
        circuit.addOperator(delay);
        MinusOperator minus = new MinusOperator(type);
        circuit.addOperator(minus);
        Wire input = new Wire(type);
        circuit.addInputWire(input);
        plus.connectInput(0, input);
        delay.connectInput(0, input);
        delay.connectTo(minus, 0);
        delay.connectTo(plus, 0);
        circuit.addOutputWire(plus);
        return new CircuitOperator(circuit.seal());
    }

    @Override
    public String graphvizId() {
        return "circuit" + this.id;
    }
}
