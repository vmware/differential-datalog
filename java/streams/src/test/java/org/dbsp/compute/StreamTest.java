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
import org.dbsp.algebraic.IntegerGroup;
import org.dbsp.algebraic.IntegerTime;
import org.dbsp.algebraic.StreamGroup;
import org.dbsp.algebraic.Time;
import org.dbsp.types.IStream;
import org.junit.Assert;
import org.junit.Test;

public class StreamTest {
    final IntegerTime.Factory factory = IntegerTime.Factory.instance;
    public final StreamGroup<Integer> sg = new StreamGroup<Integer>(
            IntegerGroup.instance, factory);
    private final boolean verbose = true;

    private <T> void show(String prefix, IStream<T> value) {
        if (!verbose)
            return;
        String s = value.toString(4);
        System.out.println(prefix + " = " + s);
    }

    private <T> void show2d(String prefix, Stream2D<T> value) {
        if (!verbose)
            return;
        System.out.println(prefix + " = " + value.toString(4, 4));
    }

    @Test
    public void testId() {
        IdStream id = new IdStream(factory);
        this.show("id", id);
        Assert.assertEquals((Integer)3, id.get(3));
        IStream<Integer> iid = id.integrate(IntegerGroup.instance);
        this.show("I(id)", iid);
        Assert.assertEquals((Integer)6, iid.get(3));
        IStream<Integer> did = id.differentiate(IntegerGroup.instance);
        this.show("D(id)", did);
        IStream<Pair<Integer, Integer>> pid = id.pair(id);
        this.show("<id,id>", pid);
        IStream<Integer> delay = id.delay(IntegerGroup.instance);
        this.show("z(id)", delay);
        IStream<Integer> cut = id.cut(id.getTimeFactory().fromInteger(3), IntegerGroup.instance);
        this.show("cut3(id)", cut);
        IStream<Integer> delta = new Delta0<>(5, IntegerGroup.instance, factory);
        this.show("delta(5)", delta);
    }

    public Stream2D<Integer> get2dStream() {
        return new Stream2D<Integer>(
                new IStream<IStream<Integer>>(factory) {
                    @Override
                    public IStream<Integer> get(Time index0) {
                        return new IStream<Integer>(factory) {
                            @Override
                            public Integer get(Time index1) {
                                return 2 * index0.asInteger() + index1.asInteger();
                            }
                        };
                    }
                }
        ) {};
    }

    @Test
    public void testNested() {
        Stream2D<Integer> i = this.get2dStream();
        this.show2d("i", i);
        Assert.assertEquals((Integer)9, i.get(3).get(3));

        LiftedFunction<Integer, Integer> mod = new LiftedFunction<>(x -> x % 2);
        LiftedFunction<IStream<Integer>, IStream<Integer>> liftmod =
                new LiftedFunction<>(mod);
        Stream2D<Integer> lm = new Stream2D<>(liftmod.apply(i));
        this.show2d("^^mod(i)", lm);
        Assert.assertEquals((Integer)1, lm.get(3).get(3));

        Stream2D<Integer> ii = new Stream2D<Integer>(i.integrate(sg));
        this.show2d("I(i)", ii);
        Assert.assertEquals((Integer)24, ii.get(3).get(3));

        Stream2D<Integer> di = new Stream2D<>(i.differentiate(sg));
        this.show2d("D(i)", di);
        Assert.assertEquals((Integer)2, di.get(3).get(3));

        LiftedFunction<IStream<Integer>, IStream<Integer>> lifti =
           new LiftedFunction<IStream<Integer>, IStream<Integer>>(
                   s -> s.integrate(IntegerGroup.instance));
        Stream2D<Integer> ui = new Stream2D<Integer>(lifti.apply(i));
        this.show2d("^I(i)", ui);
        Assert.assertEquals((Integer)30, ui.get(3).get(3));

        LiftedFunction<IStream<Integer>, IStream<Integer>> liftd =
                new LiftedFunction<IStream<Integer>, IStream<Integer>>(
                        s -> s.differentiate(IntegerGroup.instance));
        Stream2D<Integer> ud = new Stream2D<Integer>(liftd.apply(i));
        this.show2d("^D(i)", ud);

        Stream2D<Integer> idd = new Stream2D<Integer>(liftd.apply(i.differentiate(sg)));
        this.show2d("^D(D(i))", idd);
        idd = new Stream2D<Integer>(liftd.apply(i).differentiate(sg));
        this.show2d("D(^D(i))", idd);

        Stream2D<Integer> iii = new Stream2D<Integer>(lifti.apply(i.integrate(sg)));
        this.show2d("^I(I(i))", iii);
        iii = new Stream2D<Integer>(lifti.apply(i).integrate(sg));
        this.show2d("I(^I(i))", iii);
    }
}
