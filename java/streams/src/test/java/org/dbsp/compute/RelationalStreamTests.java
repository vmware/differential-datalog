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

import org.dbsp.algebraic.*;
import org.dbsp.algebraic.staticTyping.Group;
import org.dbsp.algebraic.staticTyping.IStream;
import org.dbsp.compute.time.IntegerRing;
import org.dbsp.compute.time.IntegerTime;
import org.dbsp.compute.relational.ZSet;
import org.dbsp.compute.relational.ZSetGroup;
import org.junit.Assert;
import org.junit.Test;

import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Predicate;

import static org.dbsp.compute.StreamTest.show;

/**
 * Tests that compute streaming relational queries.
 */
public class RelationalStreamTests {
    static final TimeFactory tf = IntegerTime.Factory.instance;

    static final ZSetGroup<TestTuple, Integer> gr = new ZSetGroup<>(IntegerRing.instance);
    static class ZSGroup implements Group<ZS> {
        private ZSGroup() {}
        public static final ZSGroup instance = new ZSGroup();

        @Override
        public ZS negate(ZS data) {
            return new ZS(data.zs.negate());
        }

        @Override
        public ZS add(ZS left, ZS right) {
            return new ZS(left.zs.add(right.zs));
        }

        @Override
        public ZS zero() {
            return new ZS(gr.zero());
        }
    }

    public IStream<ZS> getInput() {
        return new IStream<ZS>(tf) {
            @Override
            public ZS get(Time index) {
                ZS r = new ZS();
                if (index.isZero())
                    r.add(new TestTuple("Me", 10), 2);
                else if (index.previous().isZero())
                    r.add(new TestTuple("Me", 5), 1);
                else if (index.previous().previous().isZero())
                    r.add(new TestTuple("You", 2), 1);
                return r;
            }
        };
    }

    @Test
    public void testFilter() {
        Predicate<TestTuple> predicate = t -> t.v >= 5;
        Function<ZS, ZS> filter = s -> s.filter(predicate);
        StreamFunction<ZS, ZS> streamFilter = StreamFunction.lift(filter);
        IStream<ZS> input = this.getInput();

        IStream<ZS> result = streamFilter.apply(input);
        ZS o = result.get(0);
        Assert.assertEquals(1, o.size());
        o = result.get(1);
        Assert.assertEquals(1, o.size());
        o = result.get(2);
        Assert.assertEquals(0, o.size());
        o = result.get(3);
        Assert.assertEquals(0, o.size());

        show("input", input);
        Assert.assertEquals("[{<Me,10>->2},{<Me,5>->1},{<You,2>->1},{},...]", input.toString());

        IStream<ZS> integ = input.integrate(ZSGroup.instance);
        show("integrate(input)", integ);
        Assert.assertEquals("[{<Me,10>->2},{<Me,5>->1,<Me,10>->2},{<Me,5>->1,<Me,10>->2,<You,2>->1},{<Me,5>->1,<Me,10>->2,<You,2>->1},...]",
                integ.toString());

        IStream<ZS> diff = input.differentiate(ZSGroup.instance);
        show("differentiate(input)", diff);
        Assert.assertEquals("[{<Me,10>->2},{<Me,5>->1,<Me,10>->-2},{<Me,5>->-1,<You,2>->1},{<You,2>->-1},...]", diff.toString());

        IStream<ZS> difInt = integ.differentiate(ZSGroup.instance);
        Assert.assertEquals("[{<Me,10>->2},{<Me,5>->1},{<You,2>->1},{},...]", difInt.toString());
        show("differentiate(integrate(input))", difInt);
        Assert.assertTrue(difInt.comparePrefix(input, 4));

        IStream<ZS> filtered = streamFilter.apply(integ);
        show("filter(t -> t.v >= 5)(integrate(input))", filtered);
        Assert.assertEquals("[{<Me,10>->2},{<Me,5>->1,<Me,10>->2},{<Me,5>->1,<Me,10>->2},{<Me,5>->1,<Me,10>->2},...]", filtered.toString());

        StreamFunction<ZS, ZS> incFunc = streamFilter.inc(ZSGroup.instance, ZSGroup.instance);
        IStream<ZS> result1 = incFunc.apply(input);
        show("differentiate(filter(integrate(input)))", result1);
        boolean compare = result.comparePrefix(result1, 4);
        Assert.assertTrue(compare);
    }

    @Test
    public void testMap() {
        Function<TestTuple, TestTuple> func = testTuple -> new TestTuple(testTuple.s, testTuple.v + 1);
        Function<ZS, ZS> map = zs -> zs.map(func);
        StreamFunction<ZS, ZS> sf = StreamFunction.lift(map);
        IStream<ZS> input = this.getInput();
        show("input", input);
        IStream<ZS> mapped = sf.apply(input);
        show("map(t -> {t.s, t.v+1})(input))", mapped);
        Assert.assertEquals("[{<Me,11>->2},{<Me,6>->1},{<You,3>->1},{},...]", mapped.toString());

        StreamFunction<ZS, ZS> inc = sf.inc(ZSGroup.instance, ZSGroup.instance);
        IStream<ZS> incMapped = inc.apply(input);
        show("differentiate(map(integrate(input))))", incMapped);
        boolean compare = mapped.comparePrefix(incMapped, 4);
        Assert.assertTrue(compare);
    }

    @Test
    public void testJoin() {
        BiFunction<ZS, ZS, ZS> join = (zs, zs2) -> zs.join(zs2, t -> t.s, t1 -> t1.s, (t, t1) -> new TestTuple(t.s, t.v + t1.v));
        StreamBiFunction<ZS, ZS, ZS> sjFunc = StreamBiFunction.lift(join, IntegerTime.Factory.instance);
        IStream<ZS> left = this.getInput();
        IStream<ZS> right = new IStream<ZS>(tf) {
            @Override
            public ZS get(Time index) {
                ZS r = new ZS();
                if (index.isZero())
                    r.add(new TestTuple("Me", 2), 2);
                else if (index.previous().isZero())
                    r.add(new TestTuple("You", 3), 3);
                return r;
            }
        };
        // Stream join
        Assert.assertEquals("[{<Me,2>->2},{<You,3>->3},{},{},...]", right.toString());
        IStream<ZS> joined = sjFunc.apply(left, right);
        Assert.assertEquals("[{<Me,12>->4},{},{},{},...]", joined.toString());

        show("left", left);
        show("right", right);
        // Incremental streaming join
        IStream<ZS> intLeft = left.integrate(ZSGroup.instance);
        show("integrate(left)", intLeft);
        IStream<ZS> intRight = right.integrate(ZSGroup.instance);
        show("integrate(right)", intRight);
        IStream<ZS> intJoin = sjFunc.apply(intLeft, intRight);
        show("join(integrate(left), integrate(right))", intJoin);

        IStream<ZS> incJoin = intJoin.differentiate(ZSGroup.instance);
        show("differentiate(join(integrate(left), integrate(right)))", incJoin);
        Assert.assertEquals("[{<Me,12>->4},{<Me,7>->2},{<You,5>->3},{},...]", incJoin.toString());
        // Join identity for bilinear operators
        IStream<ZS> intLeftDelayed = intLeft.delay(ZSGroup.instance);
        show("ileftdel", intLeftDelayed);
        IStream<ZS> intRightDelayed = intRight.delay(ZSGroup.instance);
        show("irightdel", intRightDelayed);
        IStream<ZS> lj = sjFunc.apply(intLeftDelayed, right);
        show("lj", lj);
        IStream<ZS> rj = sjFunc.apply(left, intRightDelayed);
        show("rj", rj);
        IStream<ZS> inc = sjFunc.apply(left, right);
        show("inc", inc);
        IStream<ZS> fin = lj.add(rj, ZSGroup.instance).add(inc, ZSGroup.instance);
        show("fin", fin);
        boolean compare = incJoin.comparePrefix(fin, 4);
        Assert.assertTrue(compare);
    }
}
