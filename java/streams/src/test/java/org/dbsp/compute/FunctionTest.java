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
import org.dbsp.algebraic.Time;
import org.dbsp.algebraic.staticTyping.IStream;
import org.junit.Assert;
import org.junit.Test;

import java.util.function.BiFunction;
import java.util.function.Function;

public class FunctionTest {
    @Test
    public void testIdOperator() {
        Function<Integer, Integer> func = s -> s;
        int result = func.apply(6);
        Assert.assertEquals(6, result);

        StreamFunction<Integer, Integer> idlift = StreamFunction.lift(func, IntegerTime.Factory.instance);
        IStream<Integer> input = new IStream<Integer>(IntegerTime.Factory.instance) {
            @Override
            public Integer get(Time index) {
                return 2 * index.asInteger();
            }
        };
        IStream<Integer> output = idlift.apply(input);
        int elem5 = output.get(5);
        Assert.assertEquals(10, elem5);
    }

    @Test
    public void testPlus() {
        BiFunction<Integer, Integer, Integer> plus = IntegerRing.instance::add;
        int result = plus.apply(3, 5);
        Assert.assertEquals(8, result);

        StreamBiFunction<Integer, Integer, Integer> plift =
                StreamBiFunction.lift(plus, IntegerTime.Factory.instance);
        IdStream id = new IdStream(IntegerTime.Factory.instance);
        IStream<Integer> output = plift.apply(id, id);
        int elem5 = output.get(5);
        Assert.assertEquals(10, elem5);
    }

    @Test
    public void testComposeUnary0() {
        Function<Integer, Integer> inc = x -> x + 1;
        Function<Integer, Integer> twice = inc.compose(inc);
        int result = twice.apply(5);
        Assert.assertEquals(7, result);

        StreamFunction<Integer, Integer> liftinc = StreamFunction.lift(inc, IntegerTime.Factory.instance);
        StreamFunction<Integer, Integer> compose = liftinc.composeStream(liftinc);
        IdStream input = new IdStream(IntegerTime.Factory.instance);
        IStream<Integer> output = compose.apply(input);
        int elem5 = output.get(5);
        Assert.assertEquals(7, elem5);
    }

    @Test
    public void testComposeUnary1() {
        IdStream input = new IdStream(IntegerTime.Factory.instance);
        IStream<Integer> output = input.delay(IntegerRing.instance).delay(IntegerRing.instance);
        int elem5 = output.get(5);
        Assert.assertEquals(3, elem5);
    }

    @Test
    public void testComposeBinary() {
        BiFunction<Integer,Integer,Integer> plus = IntegerRing.instance::add;
        StreamBiFunction<Integer, Integer, Integer> plusLifted = StreamBiFunction.lift(plus, IntegerTime.Factory.instance);

        IdStream input = new IdStream(IntegerTime.Factory.instance);
        IStream<Integer> delinput = input.delay(IntegerRing.instance);
        IStream<Integer> output = plusLifted.apply(input, delinput);
        int elem5 = output.get(5);
        Assert.assertEquals(9, elem5);
    }

    @Test
    public void testFeedback() {
        Function<Integer, Integer> id = x -> x;
        StreamFunction<Integer, Integer> feedback = StreamFunction.feedback(id, IntegerRing.instance);
        IdStream input = new IdStream(IntegerTime.Factory.instance);
        IStream<Integer> result = feedback.apply(input);
        Assert.assertEquals("[0,1,3,6,...]", result.toString(4));
        IStream<Integer> integrate = input.integrate(IntegerRing.instance);
        boolean compare = result.comparePrefix(integrate, 100);
        Assert.assertTrue(compare);
    }
}
