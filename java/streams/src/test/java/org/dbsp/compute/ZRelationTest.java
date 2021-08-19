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

import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

public class ZRelationTest {
    static class Tuple {
        final String s;
        final Integer v;

        Tuple(String s, Integer v) {
            this.s = s;
            this.v = v;
        }
    }

    @Test
    public void emptyRelation() {
        ZRelation<Tuple> zrel = new ZRelation<Tuple>();
        Assert.assertEquals(0, zrel.size());
        ZRelation<Tuple> distinct = zrel.distinct();
        Assert.assertEquals(0, distinct.size());
    }

    @Test
    public void addRelations() {
        Tuple t = new Tuple("Me", 10);
        ZRelation<Tuple> zrel = new ZRelation<Tuple>(t);
        Assert.assertEquals(1, zrel.size());

        ZRelation<Tuple> doub = zrel.plus(zrel);
        Assert.assertEquals(1, doub.size());

        Tuple t1 = new Tuple("You", 5);
        ZRelation<Tuple> y = new ZRelation<Tuple>(t1);
        doub = doub.plus(y);
        Assert.assertEquals(2, doub.size());

        ZRelation<Tuple> neg = doub.minus();
        Assert.assertEquals(2, neg.size());
        ZRelation<Tuple> distinctNeg = neg.distinct();
        Assert.assertEquals(0, distinctNeg.size());

        doub = doub.plus(neg);
        Assert.assertEquals(0, doub.size());
    }

    @Test
    public void filterTest() {
        Tuple t = new Tuple("Me", 10);
        ZRelation<Tuple> rel = new ZRelation<>(t, 2);
        Tuple t1 = new Tuple("You", 8);
        rel.add(t1, 1);
        ZRelation<Tuple> result = rel.filter(e -> e.v >= 10);
        Assert.assertEquals(1, result.size());
        Assert.assertEquals(2, result.weight(t));
        Assert.assertEquals(0, result.weight(t1));
        ZRelation<Tuple> distinct = result.distinct();
        Assert.assertEquals(1, distinct.size());
        Assert.assertEquals(0, distinct.weight(t1));
        Assert.assertEquals(1, distinct.weight(t));
    }

    @Test
    public void flatmapTest() {
        Tuple t0 = new Tuple("Me", 10);
        Tuple t1 = new Tuple("You", 5);
        List<Tuple> list = new ArrayList<Tuple>(2);
        list.add(t0);
        list.add(t1);
        ZRelation<List<Tuple>> rel = new ZRelation<>(list, 2);
        ZRelation<Tuple> flat = rel.flatMap((Function<List<Tuple>, ZRelation<Tuple>>) ZRelation::new);
        Assert.assertEquals(2, flat.size());
        Assert.assertEquals(2, flat.weight(t0));
        Assert.assertEquals(2, flat.weight(t1));
    }

    @Test
    public void joinTest() {
        Tuple t0 = new Tuple("Me", 10);
        Tuple t1 = new Tuple("Me", 5);
        ZRelation<Tuple> r0 = new ZRelation<>(t0, 2);
        ZRelation<Tuple> r1 = new ZRelation<>(t1, 3);
        ZRelation<Integer> result = r0.join(r1, t -> t.s, t -> t.s, (tt0, tt1) -> tt0.v + tt1.v);
        Assert.assertEquals(1, result.size());
        Assert.assertEquals(6, result.weight(15)); // 10 + 5 has weight 2 * 3
    }
}
