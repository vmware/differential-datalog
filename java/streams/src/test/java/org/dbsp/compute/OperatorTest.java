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

import org.dbsp.algebraic.IntegerTime;
import org.dbsp.algebraic.Time;
import org.dbsp.algebraic.TimeFactory;
import org.dbsp.formal.*;
import org.dbsp.types.IStream;
import org.dbsp.types.IntegerType;
import org.junit.Assert;
import org.junit.Test;

import java.util.function.Function;

public class OperatorTest {
    @Test
    public void testIdOperator() {
        IdOperator<Integer> op = new IdOperator<Integer>(IntegerType.instance);
        Function<Integer, Integer> func = op.getComputation();
        int result = func.apply(6);
        Assert.assertEquals(6, result);

        UnaryOperator<IStream<Integer>, IStream<Integer>> idlift = op.lift(IntegerTime.Factory.instance);
        IStream<Integer> input = new IStream<Integer>(IntegerTime.Factory.instance) {
            @Override
            public Integer get(Time index) {
                return 2 * index.asInteger();
            }
        };
        IStream<Integer> output = idlift.getComputation().apply(input);
        int elem5 = output.get(5);
        Assert.assertEquals(10, elem5);
    }

    @Test
    public void testPlusOperator() {
        PlusOperator<Integer> plus = new PlusOperator<Integer>(IntegerType.instance);
        int result = plus.getComputation().apply(3, 5);
        Assert.assertEquals(8, result);

        BinaryOperator<IStream<Integer>, IStream<Integer>, IStream<Integer>> plift =
                plus.lift(IntegerTime.Factory.instance);
        IdStream id = new IdStream(IntegerTime.Factory.instance);
        IStream<Integer> output = plift.getComputation().apply(id, id);
        int elem5 = output.get(5);
        Assert.assertEquals(10, elem5);
    }

    @Test
    public void testComposeUnary0() {
        UnaryOperator<Integer, Integer> inc = new UnaryOperator<Integer, Integer>(
                IntegerType.instance, IntegerType.instance) {
            @Override
            public Function<Integer, Integer> getComputation() {
                return x -> x + 1;
            }
        };
        UnaryOperator<Integer, Integer> twice = inc.compose(inc);
        int result = twice.getComputation().apply(5);
        Assert.assertEquals(7, result);

        UnaryOperator<IStream<Integer>, IStream<Integer>> liftinc = inc.lift(IntegerTime.Factory.instance);
        UnaryOperator<IStream<Integer>, IStream<Integer>> compose = liftinc.compose(liftinc);
        IdStream input = new IdStream(IntegerTime.Factory.instance);
        IStream<Integer> output = compose.getComputation().apply(input);
        int elem5 = output.get(5);
        Assert.assertEquals(7, elem5);
    }

    @Test
    public void testComposeUnary1() {
        DelayOperator<Integer, IntegerTime.Factory> delay = new DelayOperator<>(
                IntegerType.instance, IntegerTime.Factory.instance);
        UnaryOperator<IStream<Integer>, IStream<Integer>> compose = delay.compose(delay);
        IdStream input = new IdStream(IntegerTime.Factory.instance);
        IStream<Integer> output = compose.getComputation().apply(input);
        int elem5 = output.get(5);
        Assert.assertEquals(3, elem5);
    }

    @Test
    public void testComposeBinary() {
        DelayOperator<Integer, IntegerTime.Factory> delay = new DelayOperator<>(
                IntegerType.instance, IntegerTime.Factory.instance);
        PlusOperator<Integer> plus = new PlusOperator<Integer>(IntegerType.instance);
        BinaryOperator<IStream<Integer>, IStream<Integer>, IStream<Integer>> plift =
                plus.lift(IntegerTime.Factory.instance);
        BinaryOperator<IStream<Integer>, IStream<Integer>, IStream<Integer>> pd = delay.composeAsFirst(plift);
        IdStream id = new IdStream(IntegerTime.Factory.instance);
        IStream<Integer> output = pd.getComputation().apply(id, id);
        int elem5 = output.get(5);
        Assert.assertEquals(9, elem5);
    }

    @Test
    public void testFeedback() {
        IdOperator<Integer> op = new IdOperator<>(IntegerType.instance);
        UniformUnaryOperator<Integer> uid = op.asUniformOperator();
        UniformUnaryOperator<IStream<Integer>> feedback = uid.feedback(IntegerTime.Factory.instance);
        IdStream id = new IdStream(IntegerTime.Factory.instance);
        IStream<Integer> result = feedback.getComputation().apply(id);
        Assert.assertEquals("[0,1,3,6,...]", result.toString(4));
        IStream<Integer> integrate = id.integrate(IntegerType.instance.getGroup());
        for (int i = 0; i < 100; i++)
            Assert.assertEquals(result.get(i), integrate.get(i));
    }
}
