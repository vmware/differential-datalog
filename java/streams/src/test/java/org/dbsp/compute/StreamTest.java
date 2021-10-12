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

import org.dbsp.compute.time.IntegerRing;
import org.dbsp.compute.time.IntegerTime;
import org.dbsp.algebraic.staticTyping.StreamGroup;
import org.dbsp.algebraic.Time;
import org.dbsp.algebraic.staticTyping.IStream;
import org.dbsp.lib.Pair;
import org.junit.Assert;
import org.junit.Test;

public class StreamTest {
    static final IntegerTime.Factory factory = IntegerTime.Factory.instance;
    static public final StreamGroup<Integer> sg = new StreamGroup<Integer>(
            IntegerRing.instance, factory);
    static final int showCount = 4;

    public static <T> String show(String prefix, IStream<T> value) {
        String s = value.toString(showCount);
        TestUtil.show(prefix + " = " + s);
        return s;
    }

    static <T> String toString(IStream<IStream<T>> ss, int limit0, int limit1) {
        StringBuilder builder = new StringBuilder();
        for (int i = 0; i < limit0; i++) {
            IStream<T> s = ss.get(i);
            String str = s.toString(limit1);
            builder.append(str);
            builder.append("\n");
        }
        return builder.toString();
    }

    private static <T> String show2d(String prefix, IStream<IStream<T>> value) {
        String s = toString(value, showCount, showCount);
        TestUtil.show(prefix + " = " + s);
        return s;
    }

    @Test
    public void testId() {
        IdStream id = new IdStream(factory);
        String s = show("id", id);
        Assert.assertEquals("[0,1,2,3,...]", s);
        Assert.assertEquals((Integer)3, id.get(3));
        IStream<Integer> iid = id.integrate(IntegerRing.instance);
        s = show("I(id)", iid);
        Assert.assertEquals("[0,1,3,6,...]", s);
        Assert.assertEquals((Integer)6, iid.get(3));
        IStream<Integer> did = id.differentiate(IntegerRing.instance);
        s = show("D(id)", did);
        Assert.assertEquals("[0,1,1,1,...]", s);
        IStream<Pair<Integer, Integer>> pid = id.pair(id);
        s = show("<id,id>", pid);
        Assert.assertEquals("[<0,0>,<1,1>,<2,2>,<3,3>,...]", s);
        IStream<Integer> delay = id.delay(IntegerRing.instance);
        s = show("z(id)", delay);
        Assert.assertEquals("[0,0,1,2,...]", s);
        IStream<Integer> cut = id.cut(id.getTimeFactory().fromInteger(3), IntegerRing.instance);
        s = show("cut3(id)", cut);
        Assert.assertEquals("[0,1,2,0,...]", s);
        IStream<Integer> delta = new Delta0<>(5, IntegerRing.instance, factory);
        s = show("delta(5)", delta);
        Assert.assertEquals("[5,0,0,0,...]", s);
    }

    public static IStream<IStream<Integer>> get2dStream() {
        return new IStream<IStream<Integer>>(factory) {
            @Override
            public IStream<Integer> get(Time index0) {
                return new IStream<Integer>(factory) {
                    @Override
                    public Integer get(Time index1) {
                        return 2 * index0.asInteger() + index1.asInteger();
                    }
                };
            }
        };
    }

    @Test
    public void testNested() {
        IStream<IStream<Integer>> i = get2dStream();
        String h = show2d("i", i);
        Assert.assertEquals("[0,1,2,3,...]\n[2,3,4,5,...]\n[4,5,6,7,...]\n[6,7,8,9,...]\n", h);
        Assert.assertEquals((Integer)9, i.get(3).get(3));

        StreamFunction<Integer, Integer> mod = StreamFunction.lift(x -> x % 2);
        StreamFunction<IStream<Integer>, IStream<Integer>> liftMod = StreamFunction.lift(mod);
        IStream<IStream<Integer>> lm = liftMod.apply(i);
        h = show2d("^^mod(i)", lm);
        Assert.assertEquals("[0,1,0,1,...]\n" +
                "[0,1,0,1,...]\n" +
                "[0,1,0,1,...]\n" +
                "[0,1,0,1,...]\n", h);
        Assert.assertEquals((Integer)1, lm.get(3).get(3));

        IStream<IStream<Integer>> ii = i.integrate(sg);
        h = show2d("I(i)", ii);
        Assert.assertEquals("[0,1,2,3,...]\n" +
                "[2,4,6,8,...]\n" +
                "[6,9,12,15,...]\n" +
                "[12,16,20,24,...]\n", h);
        Assert.assertEquals((Integer)24, ii.get(3).get(3));

        IStream<IStream<Integer>> di = i.differentiate(sg);
        h = show2d("D(i)", di);
        Assert.assertEquals("[0,1,2,3,...]\n" +
                "[2,2,2,2,...]\n" +
                "[2,2,2,2,...]\n" +
                "[2,2,2,2,...]\n", h);
        Assert.assertEquals((Integer)2, di.get(3).get(3));

        StreamFunction<IStream<Integer>, IStream<Integer>> liftI =
           StreamFunction.lift(
                   s -> s.integrate(IntegerRing.instance));
        IStream<IStream<Integer>> ui = liftI.apply(i);
        h = show2d("^I(i)", ui);
        Assert.assertEquals("[0,1,3,6,...]\n" +
                "[2,5,9,14,...]\n" +
                "[4,9,15,22,...]\n" +
                "[6,13,21,30,...]\n", h);
        Assert.assertEquals((Integer)30, ui.get(3).get(3));

        StreamFunction<IStream<Integer>, IStream<Integer>> liftD =
                StreamFunction.lift(s -> s.differentiate(IntegerRing.instance));
        IStream<IStream<Integer>> ud = liftD.apply(i);
        h = show2d("^D(i)", ud);
        Assert.assertEquals("[0,1,1,1,...]\n" +
                "[2,1,1,1,...]\n" +
                "[4,1,1,1,...]\n" +
                "[6,1,1,1,...]\n", h);

        IStream<IStream<Integer>> idd = liftD.apply(i.differentiate(sg));
        h = show2d("^D(D(i))", idd);
        Assert.assertEquals("[0,1,1,1,...]\n" +
                "[2,0,0,0,...]\n" +
                "[2,0,0,0,...]\n" +
                "[2,0,0,0,...]\n", h);
        idd = liftD.apply(i).differentiate(sg);
        h = show2d("D(^D(i))", idd);
        Assert.assertEquals("[0,1,1,1,...]\n" +
                "[2,0,0,0,...]\n" +
                "[2,0,0,0,...]\n" +
                "[2,0,0,0,...]\n", h);

        IStream<IStream<Integer>> iii = liftI.apply(i.integrate(sg));
        h = show2d("^I(I(i))", iii);
        Assert.assertEquals("[0,1,3,6,...]\n" +
                "[2,6,12,20,...]\n" +
                "[6,15,27,42,...]\n" +
                "[12,28,48,72,...]\n", h);
        iii = liftI.apply(i).integrate(sg);
        h = show2d("I(^I(i))", iii);
        Assert.assertEquals("[0,1,3,6,...]\n" +
                "[2,6,12,20,...]\n" +
                "[6,15,27,42,...]\n" +
                "[12,28,48,72,...]\n", h);

        IStream<IStream<Integer>> zi = i.delay(sg);
        h = show2d("z(i)", zi);
        Assert.assertEquals("[0,0,0,0,...]\n" +
                "[0,1,2,3,...]\n" +
                "[2,3,4,5,...]\n" +
                "[4,5,6,7,...]\n", h);

        StreamFunction<IStream<Integer>, IStream<Integer>> liftZ =
                StreamFunction.lift(s -> s.delay(IntegerRing.instance));
        IStream<IStream<Integer>> lzi = i.apply(liftZ);
        h = show2d("^z(i)", lzi);
        Assert.assertEquals("[0,0,1,2,...]\n" +
                "[0,2,3,4,...]\n" +
                "[0,4,5,6,...]\n" +
                "[0,6,7,8,...]\n", h);
    }
}
