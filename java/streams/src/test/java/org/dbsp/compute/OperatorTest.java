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
import org.dbsp.circuits.types.IntegerType;
import org.dbsp.circuits.types.Type;
import org.dbsp.lib.Linq;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;

public class OperatorTest {
    static final Type IT = IntegerType.instance;
    static final List<Type> ITL = Linq.list(IT);

    @Test
    public void simpleOperatorTest() {
        Circuit circuit = new Circuit("Simple", ITL, ITL);
        Operator delay = new DelayOperator(IT);
        circuit.addOperator(delay);
        Port port = circuit.getInputPort(0);
        port.connectTo(delay, 0);
        Wire o = circuit.addOutputWireFromOperator(delay);
        circuit.seal();
        // Let's run
        System.out.println(circuit.toGraphvizTop());
        circuit.reset();
        for (int i = 0; i < 10; i++) {
            port.setValue(i);
            circuit.step();
            Object out = o.getValue(false);
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
        circuit.seal();
        System.out.println(circuit.toGraphvizTop());
        // Let's run
        circuit.reset();
        for (int i = 0; i < 10; i++) {
            System.out.println("===========");
            port.setValue(i);
            circuit.step();
            Object out = o.getValue(false);
            int expected = i > 1 ? i - 2 : 0;
            Assert.assertEquals(expected, out);
        }
    }

    @Test
    public void integrateTest() {
        CircuitOperator op = CircuitOperator.integrationOperator(IT);
        Circuit circuit = op.circuit;
        Port port = circuit.getInputPort(0);
        System.out.println(circuit.toGraphvizTop());
        // Let's run
        circuit.reset();
        for (int i = 0; i < 10; i++) {
            port.setValue(i);
            circuit.step();
            Object out = circuit.getOutputWires().get(0).getValue(false);
            int expected = 0;
            for (int j = 0; j < i; j++)
                expected += j;
            Assert.assertEquals(expected, out);
        }
    }

    @Test
    public void derivativeTest() {
        CircuitOperator op = CircuitOperator.derivativeOperator(IT);
        Circuit circuit = op.circuit;
        // Let's run
        System.out.println(circuit.toGraphvizTop());
        circuit.reset();
        Port port = circuit.getInputPort(0);
        for (int i = 0; i < 10; i++) {
            port.setValue(i);
            circuit.step();
            Object out = circuit.getOutputWires().get(0).getValue(false);
            int expected = i > 0 ? 1 : 0;
            Assert.assertEquals(expected, out);
        }
    }

    @Test
    public void chainIDTest() {
        Circuit c = new Circuit("top", ITL, ITL);
        Port input = c.getInputPort(0);
        CircuitOperator i = CircuitOperator.integrationOperator(IT);
        c.addOperator(i);
        CircuitOperator d = CircuitOperator.derivativeOperator(IT);
        c.addOperator(d);
        i.connectTo(d, 0);
        Wire output = c.addOutputWireFromOperator(d);
        input.connectTo(i, 0);
        c.seal();

        System.out.println(c.toGraphvizTop());
        c.reset();
        for (int iv = 0; iv < 10; iv++) {
            input.setValue(iv);
            c.step();
            Object out = output.getValue(false);
            Assert.assertEquals(iv, out);
        }
    }
}
