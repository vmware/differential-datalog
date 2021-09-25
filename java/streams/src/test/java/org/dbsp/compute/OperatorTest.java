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

package org.dbsp.compute;

import org.dbsp.circuits.*;
import org.dbsp.circuits.operators.*;
import org.dbsp.algebraic.dynamicTyping.types.IntegerType;
import org.dbsp.algebraic.dynamicTyping.types.Type;
import org.dbsp.lib.Utilities;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;

public class OperatorTest {
    static final Type IT = IntegerType.instance;
    static final List<Type> ITL = Utilities.list(IT);

    void show(Circuit c) {
        TestUtil.show(c.toGraphvizTop(false));
    }

    @Test
    public void simpleOperatorTest() {
        Circuit circuit = new Circuit("Simple", ITL, ITL);
        Operator delay = new DelayOperator(IT);
        circuit.addOperator(delay);
        Port port = circuit.getInputPort(0);
        port.connectTo(delay, 0);
        Wire o = circuit.addOutputWireFromOperator(delay);
        circuit.seal();
        Sink sink = o.addSink();
        // Let's run
        this.show(circuit);
        
        Scheduler scheduler = new Scheduler();
        circuit.reset(scheduler);
        for (int i = 0; i < 10; i++) {
            port.setValue(i);
            circuit.step(scheduler);
            Object out = sink.getValue(scheduler);
            int expected = i > 0 ? i - 1 : 0;
            Assert.assertEquals(expected, out);
        }
    }

    @Test
    public void chainedDelayTest() {
        Circuit circuit = new Circuit("Simple", ITL, ITL);
        Operator delay0 = new DelayOperator(IT);
        Operator delay1 = new DelayOperator(IT);
        circuit.addOperator(delay0);
        circuit.addOperator(delay1);
        Port port = circuit.getInputPort(0);
        port.connectTo(delay0, 0);
        delay0.connectTo(delay1, 0);
        Wire o = circuit.addOutputWireFromOperator(delay1);
        Sink sink = o.addSink();
        circuit.seal();
        this.show(circuit);
        // Let's run
        Scheduler scheduler = new Scheduler();
        circuit.reset(scheduler);
        for (int i = 0; i < 10; i++) {
            port.setValue(i);
            circuit.step(scheduler);
            Object out = sink.getValue(scheduler);
            int expected = i > 1 ? i - 2 : 0;
            Assert.assertEquals(expected, out);
        }
    }

    @Test
    public void integrateTest() {
        CircuitOperator op = CircuitOperator.integrationOperator(IT);
        Circuit circuit = op.circuit;
        Port port = circuit.getInputPort(0);
        Sink sink = circuit.getOutputWires().get(0).addSink();
        this.show(circuit);
        // Let's run
        Scheduler scheduler = new Scheduler();
        circuit.reset(scheduler);
        for (int i = 0; i < 10; i++) {
            port.setValue(i);
            circuit.step(scheduler);
            Object out = sink.getValue(scheduler);
            int expected = 0;
            for (int j = 0; j <= i; j++)
                expected += j;
            Assert.assertEquals(expected, out);
        }
    }

    @Test
    public void derivativeTest() {
        CircuitOperator op = CircuitOperator.derivativeOperator(IT);
        Circuit circuit = op.circuit;
        Sink sink = circuit.getOutputWires().get(0).addSink();
        // Let's run
        this.show(circuit);
        Scheduler scheduler = new Scheduler();
        circuit.reset(scheduler);
        Port port = circuit.getInputPort(0);
        for (int i = 0; i < 10; i++) {
            port.setValue(i);
            circuit.step(scheduler);
            Object out = sink.getValue(scheduler);
            int expected = i > 0 ? 1 : 0;
            Assert.assertEquals(expected, out);
        }
    }

    @Test
    public void chainIDTest() {
        Circuit c = new Circuit("top", ITL, ITL);
        CircuitOperator i = CircuitOperator.integrationOperator(IT);
        c.addOperator(i);
        CircuitOperator d = CircuitOperator.derivativeOperator(IT);
        c.addOperator(d);
        i.connectTo(d, 0);
        Wire output = c.addOutputWireFromOperator(d);
        Port input = c.getInputPort(0);
        input.connectTo(i, 0);
        c.seal();
        Sink sink = output.addSink();

        this.show(c);
        Scheduler scheduler = new Scheduler();
        c.reset(scheduler);
        for (int iv = 0; iv < 10; iv++) {
            input.setValue(iv);
            c.step(scheduler);
            Object out = sink.getValue(scheduler);
            Assert.assertEquals(iv, out);
        }
    }

    @Test
    public void feedbackTest() {
        Circuit c = new Circuit("top", ITL, ITL);
        Operator plus = c.addOperator(new PlusOperator(IT));
        Port input = c.getInputPort(0);
        input.connectTo(plus, 0);
        Operator delay = c.addOperator(new DelayOperator(IT));
        delay.connectTo(plus, 1);
        Operator map = c.addOperator(new FunctionOperator("inc", IT, IT, i -> (Integer)i + 1));
        plus.connectTo(map, 0);
        map.connectTo(delay, 0);
        Wire output = c.addOutputWireFromOperator(map);
        c.seal();

        Sink sink = output.addSink();
        this.show(c);
        Scheduler scheduler = new Scheduler();
        c.reset(scheduler);
        for (int iv = 0; iv < 10; iv++) {
            input.setValue(iv);
            c.step(scheduler);
            Object out = sink.getValue(scheduler);
            int expected = 0;
            for (int i = 0; i <= iv; i++)
                expected += i+1;
            Assert.assertEquals(expected, out);
        }
    }

    @Test
    public void bracketTest() {
        UnaryOperator id = new IdOperator(IT);
        Circuit c = new Circuit("wrapper", ITL, ITL);
        c.addOperator(id);
        c.getInputPort().connectTo(id);
        c.addOutputWireFromOperator(id);
        CircuitOperator cop = new CircuitOperator(c.seal());
        CircuitOperator body = cop.bracket();

        Circuit top = new Circuit("top", ITL, ITL);
        top.addOperator(body);
        Port input = top.getInputPort();
        input.connectTo(body);
        Wire output = top.addOutputWireFromOperator(body);
        top.seal();
        Sink sink = output.addSink();
        this.show(top);

        Scheduler scheduler = new Scheduler();
        top.reset(scheduler);
        for (int iv = 0; iv < 10; iv++) {
            input.setValue(iv);
            top.step(scheduler);
            Object out = sink.getValue(scheduler);
            Assert.assertEquals(iv, out);
        }
    }
}
