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
import org.dbsp.circuits.Scheduler;
import org.dbsp.circuits.Wire;
import org.dbsp.circuits.operators.*;
import org.dbsp.circuits.operators.relational.*;
import org.dbsp.compute.time.IntegerRing;
import org.dbsp.compute.relational.ZSet;
import org.dbsp.lib.ComparableObjectList;
import org.dbsp.lib.Utilities;
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
    static final List<Type> ZTL = Utilities.list(ZT);

    static final Type TT = new TupleType(Utilities.list(StringType.instance, IntegerType.instance));
    static final Type ZTT = new DynamicZSetType(TT);
    static final List<Type> ZTTL = Utilities.list(ZTT);

    static void show(Circuit c, boolean deep) {
        TestUtil.show(c.toGraphvizTop(deep));
    }

    static void show(Circuit c) {
        show(c, true);
    }

    @Test
    public void relationIdTest() {
        Circuit c = new Circuit("top", ZTL ,ZTL);
        Operator id = c.addOperator(new IdOperator(ZT));
        Port input = c.getInputPort(0);
        input.connectTo(id);
        Sink sink = c.addSinkFromOperator(id);
        c.seal();
        show(c);

        Scheduler scheduler = new Scheduler();
        c.reset(scheduler);
        ZSet<Integer, Integer> zs = new ZSet<Integer, Integer>(IntegerRing.instance);
        for (int iv = 0; iv < 10; iv++) {
            zs.add(10);
            input.setValue(zs);
            c.step(scheduler);
            Object out = sink.getValue(scheduler);
            Assert.assertEquals(zs, out);
        }
    }

    @Test
    public void integrationDifferentiationTest() {
        Circuit c = new Circuit("top", ZTL, ZTL);
        Operator id = c.addOperator(new IdOperator(ZT));
        Operator i = c.addOperator(CircuitOperator.integrationOperator(ZT));
        Operator d = c.addOperator(CircuitOperator.derivativeOperator(ZT));
        i.connectTo(id);
        id.connectTo(d);
        Port input = c.getInputPort(0);
        input.connectTo(i);
        Sink sink = c.addSinkFromOperator(d);
        c.seal();
        show(c);

        Scheduler scheduler = new Scheduler();
        c.reset(scheduler);
        ZSet<Integer, Integer> zs = new ZSet<Integer, Integer>(IntegerRing.instance);
        for (int iv = 0; iv < 10; iv++) {
            zs.add(10);
            input.setValue(zs);
            c.step(scheduler);
            Object out = sink.getValue(scheduler);
            Assert.assertEquals(zs, out);
        }
    }

    @Test
    public void tupleRelationsTest() {
        Circuit c = new Circuit("top", ZTTL ,ZTTL);
        Operator id = c.addOperator(new IdOperator(ZTT));
        Operator i = c.addOperator(CircuitOperator.integrationOperator(ZTT));
        Operator d = c.addOperator(CircuitOperator.derivativeOperator(ZTT));
        i.connectTo(id);
        id.connectTo(d);
        Port input = c.getInputPort(0);
        input.connectTo(i);
        Sink sink = c.addSinkFromOperator(d);
        c.seal();
        show(c);

        Scheduler scheduler = new Scheduler();
        c.reset(scheduler);
        DynamicZSet<Integer> zs = new DynamicZSet<Integer>(IntegerRing.instance);
        for (int iv = 0; iv < 10; iv++) {
            zs.add(new ComparableObjectList(iv, "String"), iv);
            input.setValue(zs);
            c.step(scheduler);
            Object out = sink.getValue(scheduler);
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
        i.connectTo(id);
        id.connectTo(d);
        Port input = c.getInputPort(0);
        input.connectTo(i);
        Sink sink = c.addSinkFromOperator(d);
        c.seal();
        show(c);

        Scheduler scheduler = new Scheduler();
        c.reset(scheduler);
        DynamicZSet<Integer> zs = new DynamicZSet<Integer>(IntegerRing.instance);
        for (int iv = 0; iv < 10; iv++) {
            zs.add(new ComparableObjectList(iv, "String"), iv);
            input.setValue(zs);
            c.step(scheduler);
            Object out = sink.getValue(scheduler);
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
        i.connectTo(map);
        map.connectTo(d);
        Port input = c.getInputPort(0);
        input.connectTo(i);
        Sink sink = c.addSinkFromOperator(d);
        c.seal();
        show(c);

        Scheduler scheduler = new Scheduler();
        c.reset(scheduler);
        DynamicZSet<Integer> zs =
                new DynamicZSet<Integer>(IntegerRing.instance);
        for (int iv = 0; iv < 10; iv++) {
            zs.add(new ComparableObjectList(iv, "String"), iv);
            input.setValue(zs);
            c.step(scheduler);
            Object out = sink.getValue(scheduler);
            Assert.assertEquals(zs.map(e -> new ComparableObjectList(0, e.get(1))), out);
        }
    }

    @Test
    public void tupleJoinTest() {
        Circuit c = new Circuit("top", ZTTL ,ZTTL);
        Operator join = c.addOperator(new DynamicZSetJoinOperator<Integer>(
                "join", ZTT, ZTT, ZTT, e -> e.get(0), f -> (Integer)f.get(0) + 1,
                (e, f) -> new ComparableObjectList(e.get(0), f.get(0))));
        Port input = c.getInputPort(0);
        input.connectTo(join, 0);
        input.connectTo(join, 1);
        Sink sink = c.addSinkFromOperator(join);
        c.seal();
        show(c);

        Scheduler scheduler = new Scheduler();
        c.reset(scheduler);
        DynamicZSet<Integer> zs =
                new DynamicZSet<Integer>(IntegerRing.instance);
        for (int iv = 0; iv < 10; iv++) {
            zs.add(new ComparableObjectList(iv, "String"), iv);
            input.setValue(zs);
            c.step(scheduler);
            Object out = sink.getValue(scheduler);
            DynamicZSet<Integer> j = zs.join(zs, e -> e.get(0), f -> (Integer) f.get(0) + 1,
                    (e, f) -> new ComparableObjectList(e.get(0), f.get(0)));
            Assert.assertEquals(j, out);
        }
    }

    @Test
    public void transitiveClosureTest() {
        final Type EdgeType = new TupleType(Utilities.list(IntegerType.instance, IntegerType.instance));
        final Type ZEdgeSetType = new DynamicZSetType(EdgeType);
        final List<Type> ZEdgeSetTypeSingletonList = Utilities.list(ZEdgeSetType);

        Circuit inner = new Circuit("inner", ZEdgeSetTypeSingletonList, ZEdgeSetTypeSingletonList);
        Operator i = inner.addOperator(CircuitOperator.integrationOperator(ZEdgeSetType));
        inner.getInputPort().connectTo(i);
        Operator plus = inner.addOperator(new PlusOperator(ZEdgeSetType));
        i.connectTo(plus, 0);
        Operator join = inner.addOperator(new DynamicZSetJoinOperator<Integer>(
                "join", ZEdgeSetType, ZEdgeSetType, ZEdgeSetType, e -> e.get(1), f -> f.get(0), (e, f) ->
                new ComparableObjectList(e.get(0), f.get(1))));
        i.connectTo(join, 0);
        plus.connectTo(join, 1);
        Operator plus1 = inner.addOperator(new PlusOperator(ZEdgeSetType));
        i.connectTo(plus1, 0);
        join.connectTo(plus1, 1);

        Operator delay = inner.addOperator(new DelayOperator(ZEdgeSetType));
        delay.connectTo(plus, 1);
        Operator distinct = inner.addOperator(new DynamicDistinctZSetOperator<Integer>("distinct", ZEdgeSetType));
        plus1.connectTo(distinct);
        distinct.connectTo(delay);
        Operator d = inner.addOperator(CircuitOperator.derivativeOperator(ZEdgeSetType));
        distinct.connectTo(d);
        inner.addOutputWireFromOperator(d);
        CircuitOperator loopBody = new CircuitOperator(inner.seal());
        Circuit top = new Circuit("top", ZEdgeSetTypeSingletonList, ZEdgeSetTypeSingletonList);
        Operator loop = top.addOperator(loopBody.bracket());
        Port input = top.getInputPort();
        input.connectTo(loop);
        Sink sink = top.addSinkFromOperator(loop);
        top.seal();

        show(top);
        Scheduler scheduler = new Scheduler();
        top.reset(scheduler);
        DynamicZSet<Integer> zs = new DynamicZSet<Integer>(IntegerRing.instance);
        zs.add(1, 2).add(2, 3);
        input.setValue(zs);
        top.step(scheduler);
        Object out = sink.getValue(scheduler);
        DynamicZSet<Integer> expected =
                new DynamicZSet<Integer>(IntegerRing.instance);
        expected.add(1, 3);
        expected = expected.plus(zs);
        Assert.assertEquals(expected, out);
        // Second computation step, non-incremental
        zs.add(3, 4);
        input.setValue(zs);
        top.step(scheduler);
        out = sink.getValue(scheduler);
        expected.add(1, 4).add(2, 4).add(3, 4);
        Assert.assertEquals(expected, out);
    }

    @Test
    public void incrementalTransitiveClosureTest() {
        final Type EdgeType = new TupleType(Utilities.list(IntegerType.instance, IntegerType.instance));
        final Type ZEdgeSetType = new DynamicZSetType(EdgeType);
        final List<Type> ZEdgeSetTypeSingletonList = Utilities.list(ZEdgeSetType);

        Circuit inner = new Circuit("inner", ZEdgeSetTypeSingletonList, ZEdgeSetTypeSingletonList);

        Operator i = inner.addOperator(CircuitOperator.integrationOperator(ZEdgeSetType));
        inner.getInputPort().connectTo(i);
        Operator plus = inner.addOperator(new PlusOperator(ZEdgeSetType));
        i.connectTo(plus, 0);
        Operator join = inner.addOperator(new DynamicZSetJoinOperator<Integer>(
                "join", ZEdgeSetType, ZEdgeSetType, ZEdgeSetType, e -> e.get(1), f -> f.get(0), (e, f) ->
                new ComparableObjectList(e.get(0), f.get(1))));
        i.connectTo(join, 0);
        plus.connectTo(join, 1);
        Operator plus1 = inner.addOperator(new PlusOperator(ZEdgeSetType));
        i.connectTo(plus1, 0);
        join.connectTo(plus1, 1);

        Operator delay = inner.addOperator(new DelayOperator(ZEdgeSetType));
        delay.connectTo(plus, 1);
        Operator distinct = inner.addOperator(new DynamicDistinctZSetOperator<Integer>("distinct", ZEdgeSetType));
        plus1.connectTo(distinct);
        distinct.connectTo(delay);
        Operator d = inner.addOperator(CircuitOperator.derivativeOperator(ZEdgeSetType));
        distinct.connectTo(d);
        inner.addOutputWireFromOperator(d);
        CircuitOperator loopBody = new CircuitOperator(inner.seal());
        Circuit top = new Circuit("top", ZEdgeSetTypeSingletonList, ZEdgeSetTypeSingletonList);
        Operator loop = top.addOperator(loopBody.bracket());
        Operator iTop = top.addOperator(CircuitOperator.integrationOperator(ZEdgeSetType));
        iTop.connectTo(loop);
        Port input = top.getInputPort();
        input.connectTo(iTop);
        Operator dTop = top.addOperator(CircuitOperator.derivativeOperator(ZEdgeSetType));
        loop.connectTo(dTop);
        Sink sink = top.addSinkFromOperator(dTop);
        top.seal();

        show(top, false);
        Scheduler scheduler = new Scheduler();
        top.reset(scheduler);
        DynamicZSet<Integer> zs = new DynamicZSet<Integer>(IntegerRing.instance);
        zs.add(1, 2).add(2, 3);
        input.setValue(zs);
        top.step(scheduler);
        Object out = sink.getValue(scheduler);
        DynamicZSet<Integer> expected =
                new DynamicZSet<Integer>(IntegerRing.instance);
        expected.add(1, 3);
        expected = expected.plus(zs);
        Assert.assertEquals(expected, out);
        // Add one edge
        zs.clear().add(3, 4);
        input.setValue(zs);
        top.step(scheduler);
        out = sink.getValue(scheduler);
        expected.clear().add(1, 4).add(2, 4).add(3, 4);
        Assert.assertEquals(expected, out);
        // Remove another edge
        zs.clear().remove(1, 2);
        input.setValue(zs);
        top.step(scheduler);
        out = sink.getValue(scheduler);
        expected.clear().remove(1, 4).remove(1, 2).remove(1, 3);
        Assert.assertEquals(expected, out);
        // Create a cycle
        zs.clear();
        zs.add(4, 2);
        input.setValue(zs);
        top.step(scheduler);
        out = sink.getValue(scheduler);
        expected.clear().add(2, 2).add(3, 2).add(3, 3).add(4, 2).add(4, 3).add(4, 4);
        Assert.assertEquals(expected, out);
    }
}
