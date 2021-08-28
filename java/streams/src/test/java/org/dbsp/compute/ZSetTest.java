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

import org.dbsp.algebraic.IntegerRing;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

public class ZSetTest {

    @Test
    public void emptyRelation() {
        ZSet<TestTuple, Integer> zrel = new ZSet<TestTuple, Integer>(IntegerRing.instance);
        Assert.assertEquals(0, zrel.size());
        Assert.assertEquals("{}", zrel.toString());
        ZSet<TestTuple, Integer> distinct = zrel.distinct();
        Assert.assertEquals(0, distinct.size());
        Assert.assertEquals("{}", distinct.toString());
    }

    @Test
    public void addRelations() {
        TestTuple t = new TestTuple("Me", 10);
        ZSet<TestTuple, Integer> zrel = new ZSet<TestTuple, Integer>(IntegerRing.instance, t);
        Assert.assertEquals(1, zrel.size());
        Assert.assertEquals("{<Me,10>->1}", zrel.toString());

        ZSet<TestTuple, Integer> doub = zrel.plus(zrel);
        Assert.assertEquals(1, doub.size());
        Assert.assertEquals("{<Me,10>->2}", doub.toString());

        TestTuple t1 = new TestTuple("You", 5);
        ZSet<TestTuple, Integer> y = new ZSet<TestTuple, Integer>(IntegerRing.instance, t1);
        doub = doub.plus(y);
        Assert.assertEquals(2, doub.size());
        Assert.assertEquals("{<Me,10>->2,<You,5>->1}", doub.toString());

        ZSet<TestTuple, Integer> neg = doub.minus();
        Assert.assertEquals(2, neg.size());
        Assert.assertEquals("{<Me,10>->-2,<You,5>->-1}", neg.toString());
        ZSet<TestTuple, Integer> distinctNeg = neg.distinct();
        Assert.assertEquals(0, distinctNeg.size());
        Assert.assertEquals("{}", distinctNeg.toString());

        doub = doub.plus(neg);
        Assert.assertEquals(0, doub.size());
        Assert.assertEquals("{}", doub.toString());
    }

    @Test
    public void filterTest() {
        TestTuple t = new TestTuple("Me", 10);
        ZSet<TestTuple, Integer> rel = new ZSet<>(IntegerRing.instance, t, 2);
        TestTuple t1 = new TestTuple("You", 8);
        rel.add(t1, 1);
        ZSet<TestTuple, Integer> result = rel.filter(e -> e.v >= 10);
        Assert.assertEquals(1, result.size());
        Assert.assertEquals(2, (int)result.weight(t));
        Assert.assertEquals(0, (int)result.weight(t1));
        ZSet<TestTuple, Integer> distinct = result.distinct();
        Assert.assertEquals(1, distinct.size());
        Assert.assertEquals(0, (int)distinct.weight(t1));
        Assert.assertEquals(1, (int)distinct.weight(t));
    }

    static class ComparableList<T extends Comparable<T>> extends ArrayList<T> implements Comparable<ComparableList<T>> {
        public ComparableList(int i) {
            super(i);
        }

        @Override
        public int compareTo(ComparableList<T> o) {
            for (int i = 0; i < this.size() && i < o.size(); i++) {
                int compare = this.get(i).compareTo(o.get(i));
                if (compare != 0)
                    return compare;
            }
            return Integer.compare(this.size(), o.size());
        }
    }

    @Test
    public void flatmapTest() {
        TestTuple t0 = new TestTuple("Me", 10);
        TestTuple t1 = new TestTuple("You", 5);
        ComparableList<TestTuple> list = new ComparableList<TestTuple>(2);
        list.add(t0);
        list.add(t1);
        ZSet<ComparableList<TestTuple>, Integer> rel = new ZSet<ComparableList<TestTuple>, Integer>(IntegerRing.instance, list, 2);
        ZSet<TestTuple, Integer> flat = rel.flatMap(l -> new ZSet<TestTuple, Integer>(IntegerRing.instance, l));
        Assert.assertEquals(2, flat.size());
        Assert.assertEquals(2, (int)flat.weight(t0));
        Assert.assertEquals(2, (int)flat.weight(t1));
    }

    @Test
    public void mapTest() {
        TestTuple t0 = new TestTuple("Me", 10);
        ZSet<TestTuple, Integer> rel = new ZSet<>(IntegerRing.instance, t0, 2);
        TestTuple t1 = new TestTuple("You", 8);
        rel.add(t1, 1);
        ZSet<String, Integer> map = rel.map(t -> t.s + t.v.toString());
        Assert.assertEquals(2, (int)map.size());
        Assert.assertEquals("{Me10->2,You8->1}", map.toString());
    }

    @Test
    public void joinTest() {
        TestTuple t0 = new TestTuple("Me", 10);
        TestTuple t1 = new TestTuple("Me", 5);
        ZSet<TestTuple, Integer> r0 = new ZSet<TestTuple, Integer>(IntegerRing.instance, t0, 2);
        ZSet<TestTuple, Integer> r1 = new ZSet<TestTuple, Integer>(IntegerRing.instance, t1, 3);
        r1.add(new TestTuple("You", 5), 1);
        ZSet<Integer, Integer> result = r0.join(r1, t -> t.s, t -> t.s, (tt0, tt1) -> tt0.v + tt1.v);
        Assert.assertEquals(1, (int)result.size());
        Assert.assertEquals(6, (int)result.weight(15)); // 10 + 5 has weight 2 * 3
        Assert.assertEquals("{15->6}", result.toString());
    }

    @Test
    public void productTest() {
        TestTuple t0 = new TestTuple("Me", 10);
        TestTuple t1 = new TestTuple("Me", 5);
        ZSet<TestTuple, Integer> r0 = new ZSet<TestTuple, Integer>(IntegerRing.instance, t0, 2);
        ZSet<TestTuple, Integer> r1 = new ZSet<TestTuple, Integer>(IntegerRing.instance, t1, 3);
        r1.add(new TestTuple("You", 2), 1);
        ZSet<TestTuple, Integer> result = r0.product(r1, (v1, v2) -> new TestTuple(v1.s + v2.s, v1.v + v2.v));
        Assert.assertEquals("{<MeMe,15>->6,<MeYou,12>->2}", result.toString());
    }

    @Test
    public void groupByTest() {
        ZSet<TestTuple, Integer> r0 = new ZSet<TestTuple, Integer>(IntegerRing.instance);
        r0.add(new TestTuple("Me", 10), 2);
        r0.add(new TestTuple("Me", 5), 1);
        r0.add(new TestTuple("You", 2), 1);
        ZSet<Grouping<String, TestTuple, Integer>, Integer> gr = r0.groupBy(t -> t.s);
        String result = gr.toString();
        Assert.assertEquals("{Group<K=Me,values={<Me,5>->1,<Me,10>->2}>->1,Group<K=You,values={<You,2>->1}>->1}", result);
    }

    @Test
    public void groupByAggregate() {
        ZSet<TestTuple, Integer> r0 = new ZSet<TestTuple, Integer>(IntegerRing.instance);
        r0.add(new TestTuple("Me", 10), 2);
        r0.add(new TestTuple("Me", 5), 1);
        r0.add(new TestTuple("You", 2), 1);
        ZSet<Grouping<String, TestTuple, Integer>, Integer> gr = r0.groupBy(t -> t.s);
        ZSet<TestTuple, Integer> counts = gr.map(g -> new TestTuple(g.key, g.count()));
        Assert.assertEquals("{<Me,3>->1,<You,1>->1}", counts.toString());
        ZSet<TestTuple, Integer> dcounts = gr.map(g -> new TestTuple(g.key, g.distinctCount()));
        Assert.assertEquals("{<Me,2>->1,<You,1>->1}", dcounts.toString());
    }
}
