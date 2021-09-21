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

import javafx.util.Pair;
import org.dbsp.compute.policies.IntegerRing;
import org.dbsp.compute.policies.IntegerTime;
import org.dbsp.algebraic.staticTyping.StreamGroup;
import org.dbsp.algebraic.Time;
import org.dbsp.algebraic.staticTyping.IStream;
import org.junit.Assert;
import org.junit.Test;

public class StreamTest {
    final IntegerTime.Factory factory = IntegerTime.Factory.instance;
    public final StreamGroup<Integer> sg = new StreamGroup<Integer>(
            IntegerRing.instance, factory);
    private static final boolean verbose = true;

    public static <T> void show(String prefix, IStream<T> value) {
        if (!verbose)
            return;
        String s = value.toString(4);
        System.out.println(prefix + " = " + s);
    }

    <T> String toString(IStream<IStream<T>> ss, int limit0, int limit1) {
        StringBuilder builder = new StringBuilder();
        for (int i = 0; i < limit0; i++) {
            IStream<T> s = ss.get(i);
            String str = s.toString(limit1);
            builder.append(str);
            builder.append("\n");
        }
        return builder.toString();
    }

    private <T> void show2d(String prefix, IStream<IStream<T>> value) {
        if (!verbose)
            return;
        System.out.println(prefix + " = " + toString(value, 4, 4));
    }

    @Test
    public void testId() {
        IdStream id = new IdStream(factory);
        show("id", id);
        Assert.assertEquals((Integer)3, id.get(3));
        IStream<Integer> iid = id.integrate(IntegerRing.instance);
        show("I(id)", iid);
        Assert.assertEquals((Integer)6, iid.get(3));
        IStream<Integer> did = id.differentiate(IntegerRing.instance);
        show("D(id)", did);
        IStream<Pair<Integer, Integer>> pid = id.pair(id);
        show("<id,id>", pid);
        IStream<Integer> delay = id.delay(IntegerRing.instance);
        show("z(id)", delay);
        IStream<Integer> cut = id.cut(id.getTimeFactory().fromInteger(3), IntegerRing.instance);
        show("cut3(id)", cut);
        IStream<Integer> delta = new Delta0<>(5, IntegerRing.instance, factory);
        show("delta(5)", delta);
    }

    public IStream<IStream<Integer>> get2dStream() {
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
        IStream<IStream<Integer>> i = this.get2dStream();
        this.show2d("i", i);
        Assert.assertEquals((Integer)9, i.get(3).get(3));

        LiftedFunction<Integer, Integer> mod = new LiftedFunction<>(x -> x % 2);
        LiftedFunction<IStream<Integer>, IStream<Integer>> liftmod =
                new LiftedFunction<>(mod);
        IStream<IStream<Integer>> lm = liftmod.apply(i);
        this.show2d("^^mod(i)", lm);
        Assert.assertEquals((Integer)1, lm.get(3).get(3));

        IStream<IStream<Integer>> ii = i.integrate(sg);
        this.show2d("I(i)", ii);
        Assert.assertEquals((Integer)24, ii.get(3).get(3));

        IStream<IStream<Integer>> di = i.differentiate(sg);
        this.show2d("D(i)", di);
        Assert.assertEquals((Integer)2, di.get(3).get(3));

        LiftedFunction<IStream<Integer>, IStream<Integer>> lifti =
           new LiftedFunction<IStream<Integer>, IStream<Integer>>(
                   s -> s.integrate(IntegerRing.instance));
        IStream<IStream<Integer>> ui = lifti.apply(i);
        this.show2d("^I(i)", ui);
        Assert.assertEquals((Integer)30, ui.get(3).get(3));

        LiftedFunction<IStream<Integer>, IStream<Integer>> liftd =
                new LiftedFunction<IStream<Integer>, IStream<Integer>>(
                        s -> s.differentiate(IntegerRing.instance));
        IStream<IStream<Integer>> ud = liftd.apply(i);
        this.show2d("^D(i)", ud);

        IStream<IStream<Integer>> idd = liftd.apply(i.differentiate(sg));
        this.show2d("^D(D(i))", idd);
        idd = liftd.apply(i).differentiate(sg);
        this.show2d("D(^D(i))", idd);

        IStream<IStream<Integer>> iii = lifti.apply(i.integrate(sg));
        this.show2d("^I(I(i))", iii);
        iii = lifti.apply(i).integrate(sg);
        this.show2d("I(^I(i))", iii);

        IStream<IStream<Integer>> zi = i.delay(sg);
        this.show2d("z(i)", zi);

        LiftedFunction<IStream<Integer>, IStream<Integer>> liftz =
                new LiftedFunction<>(s -> s.delay(IntegerRing.instance));
        IStream<IStream<Integer>> lzi = i.apply(liftz);
        this.show2d("^z(i)", lzi);
    }
}
