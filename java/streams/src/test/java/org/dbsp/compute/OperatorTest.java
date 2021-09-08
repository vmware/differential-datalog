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

import org.dbsp.algebraic.Group;
import org.dbsp.circuits.*;
import org.dbsp.circuits.types.IntegerType;
import org.dbsp.compute.policies.IntegerRing;
import org.junit.Assert;
import org.junit.Test;

public class OperatorTest {
    @Test
    public void simpleOperatorTest() {
        Circuit circuit = new Circuit("Simple");
        Operator delay = new DelayOperator(IntegerType.instance, IntegerRing.instance.asUntyped());
        circuit.addOperator(delay);
        Wire w = circuit.addInputWire(delay, 0);
        circuit.seal();
        Wire o = circuit.addOutputWire(delay);
        // Let's run
        circuit.reset();
        w.setValue(1);
        circuit.step();
        Object out = o.getValue();
        Assert.assertEquals(0, out);
        w.setValue(2);
        circuit.step();
        out = o.getValue();
        Assert.assertEquals(1, out);
    }

    @Test
    public void chainedDelayTest() {
        Circuit circuit = new Circuit("Simple");
        Operator delay0 = new DelayOperator(IntegerType.instance, IntegerRing.instance.asUntyped());
        Operator delay1 = new DelayOperator(IntegerType.instance, IntegerRing.instance.asUntyped());
        circuit.addOperator(delay0);
        circuit.addOperator(delay1);
        Wire w = circuit.addInputWire(delay0, 0);
        delay0.connectTo(delay1, 0);
        circuit.seal();
        Wire o = circuit.addOutputWire(delay1);
        // Let's run
        circuit.reset();
        for (int i = 0; i < 10; i++) {
            w.setValue(i);
            circuit.step();
            Object out = o.getValue();
            int expected = i > 1 ? i - 2 : 0;
            Assert.assertEquals(expected, out);
        }
    }
}
