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

import org.dbsp.algebraic.dynamicTyping.types.*;
import org.dbsp.circuits.Circuit;
import org.dbsp.circuits.Wire;
import org.dbsp.circuits.operators.*;
import org.dbsp.circuits.operators.relational.DynamicZSet;
import org.dbsp.circuits.operators.relational.DynamicZSetFilterOperator;
import org.dbsp.circuits.operators.relational.DynamicZSetJoinOperator;
import org.dbsp.circuits.operators.relational.DynamicZSetMapOperator;
import org.dbsp.compute.policies.IntegerRing;
import org.dbsp.compute.relational.ZSet;
import org.dbsp.lib.ComparableObjectList;
import org.dbsp.lib.Linq;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;
import java.util.function.Function;
import java.util.function.Predicate;

/**
 * Circuits computing on relational data.
 */
public class RelationalOperatorTest {
    static final Type IT = IntegerType.instance;
    static final Type ZT = new ZSetType(IT);
    static final List<Type> ZTL = Linq.list(ZT);

    static final Type TT = new TupleType(Linq.list(StringType.instance, IntegerType.instance));
    static final Type ZTT = new DynamicZSetType(TT);
    static final List<Type> ZTTL = Linq.list(ZTT);

    void show(Circuit c) {
        System.out.println(c.toGraphvizTop());
    }

    @Test
    public void relationIdTest() {
        Circuit c = new Circuit("top", ZTL ,ZTL);
        Operator id = c.addOperator(new IdOperator(ZT));
        Port input = c.getInputPort(0);
        input.connectTo(id, 0);
        Wire output = c.addOutputWireFromOperator(id);
        c.seal();
        Sink sink = output.addSink();
        this.show(c);

        c.reset();
        ZSet<Integer, Integer> zs = new ZSet<Integer, Integer>(IntegerRing.instance);
        for (int iv = 0; iv < 10; iv++) {
            zs.add(10, 1);
            input.setValue(zs);
            c.step();
            Object out = sink.getValue();
            Assert.assertEquals(zs, out);
        }
    }

    @Test
    public void integrationDifferentiationTest() {
        Circuit c = new Circuit("top", ZTL, ZTL);
        Operator id = c.addOperator(new IdOperator(ZT));
        Operator i = c.addOperator(CircuitOperator.integrationOperator(ZT));
        Operator d = c.addOperator(CircuitOperator.derivativeOperator(ZT));
        i.connectTo(id, 0);
        id.connectTo(d, 0);
        Port input = c.getInputPort(0);
        input.connectTo(i, 0);
        Wire output = c.addOutputWireFromOperator(d);
        Sink sink = output.addSink();
        c.seal();
        this.show(c);

        c.reset();
        ZSet<Integer, Integer> zs = new ZSet<Integer, Integer>(IntegerRing.instance);
        for (int iv = 0; iv < 10; iv++) {
            zs.add(10, 1);
            input.setValue(zs);
            c.step();
            Object out = sink.getValue();
            Assert.assertEquals(zs, out);
        }
    }

    @Test
    public void tupleRelationsTest() {
        Circuit c = new Circuit("top", ZTTL ,ZTTL);
        Operator id = c.addOperator(new IdOperator(ZTT));
        Operator i = c.addOperator(CircuitOperator.integrationOperator(ZTT));
        Operator d = c.addOperator(CircuitOperator.derivativeOperator(ZTT));
        i.connectTo(id, 0);
        id.connectTo(d, 0);
        Port input = c.getInputPort(0);
        input.connectTo(i, 0);
        Wire output = c.addOutputWireFromOperator(d);
        Sink sink = output.addSink();
        c.seal();
        this.show(c);

        c.reset();
        DynamicZSet<Integer> zs = new DynamicZSet<Integer>(IntegerRing.instance);
        for (int iv = 0; iv < 10; iv++) {
            zs.add(new ComparableObjectList(iv, "String"), iv);
            input.setValue(zs);
            c.step();
            Object out = sink.getValue();
            Assert.assertEquals(zs, out);
        }
    }

    @Test
    public void tupleFilterTest() {
        Circuit c = new Circuit("top", ZTTL ,ZTTL);
        Predicate<ComparableObjectList> filter = x -> ((Integer)x.get(0)) % 2 == 0;
        Operator id = c.addOperator(new DynamicZSetFilterOperator<Integer>(
                "filter", ZTT, filter));
        Operator i = c.addOperator(CircuitOperator.integrationOperator(ZTT));
        Operator d = c.addOperator(CircuitOperator.derivativeOperator(ZTT));
        i.connectTo(id, 0);
        id.connectTo(d, 0);
        Port input = c.getInputPort(0);
        input.connectTo(i, 0);
        Wire output = c.addOutputWireFromOperator(d);
        Sink sink = output.addSink();
        c.seal();
        this.show(c);

        c.reset();
        DynamicZSet<Integer> zs = new DynamicZSet<Integer>(IntegerRing.instance);
        for (int iv = 0; iv < 10; iv++) {
            zs.add(new ComparableObjectList(iv, "String"), iv);
            input.setValue(zs);
            c.step();
            Object out = sink.getValue();
            Assert.assertEquals(zs.filter(e -> (Integer)e.get(0) % 2 == 0), out);
        }
    }

    @Test
    public void tupleMapTest() {
        Circuit c = new Circuit("top", ZTTL ,ZTTL);
        Function<ComparableObjectList, ComparableObjectList> m = x ->
                new ComparableObjectList(0, x.get(1));
        Operator map = c.addOperator(new DynamicZSetMapOperator<Integer>(
                "map", ZTT, ZTT, m));
        Operator i = c.addOperator(CircuitOperator.integrationOperator(ZTT));
        Operator d = c.addOperator(CircuitOperator.derivativeOperator(ZTT));
        i.connectTo(map, 0);
        map.connectTo(d, 0);
        Port input = c.getInputPort(0);
        input.connectTo(i, 0);
        Wire output = c.addOutputWireFromOperator(d);
        Sink sink = output.addSink();
        c.seal();
        this.show(c);

        c.reset();
        DynamicZSet<Integer> zs =
                new DynamicZSet<Integer>(IntegerRing.instance);
        for (int iv = 0; iv < 10; iv++) {
            zs.add(new ComparableObjectList(iv, "String"), iv);
            input.setValue(zs);
            c.step();
            Object out = sink.getValue();
            Assert.assertEquals(zs.map(e -> new ComparableObjectList(0, e.get(1))), out);
        }
    }

    @Test
    public void tupleJoinTest() {
        Circuit c = new Circuit("top", ZTTL ,ZTTL);
        Operator join = c.addOperator(new DynamicZSetJoinOperator<Integer>(
                "join", ZTT, ZTT, ZTT, e -> e.get(0), f -> (Integer)f.get(0) + 1,
                (e, f) -> new ComparableObjectList(e.get(0), e.get(1), f.get(0), f.get(1))));
        Operator i = c.addOperator(CircuitOperator.integrationOperator(ZTT));
        Operator d = c.addOperator(CircuitOperator.derivativeOperator(ZTT));
        i.connectTo(join, 0);
        i.connectTo(join, 1);
        join.connectTo(d, 0);
        Port input = c.getInputPort(0);
        input.connectTo(i, 0);
        Wire output = c.addOutputWireFromOperator(d);
        Sink sink = output.addSink();
        c.seal();
        this.show(c);

        c.reset();
        DynamicZSet<Integer> zs =
                new DynamicZSet<Integer>(IntegerRing.instance);
        for (int iv = 0; iv < 10; iv++) {
            zs.add(new ComparableObjectList(iv, "String"), iv);
            input.setValue(zs);
            c.step();
            Object out = sink.getValue();
            DynamicZSet<Integer> j = zs.join(zs, e -> e.get(0), f -> (Integer) f.get(0) + 1,
                    (e, f) -> new ComparableObjectList(e.get(0), e.get(1), f.get(0), f.get(1)));
            Assert.assertEquals(j, out);
        }
    }

    @Test
    public void transitiveClosureTest() {
        Circuit inner = new Circuit("inner", ZTL, ZTL);
        Operator i = inner.addOperator(CircuitOperator.integrationOperator(ZT));
        Operator plus = inner.addOperator(new PlusOperator(ZT));
        i.connectTo(plus, 0);
    }
}
