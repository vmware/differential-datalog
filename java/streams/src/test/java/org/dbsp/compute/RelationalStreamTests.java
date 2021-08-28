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
import org.dbsp.formal.UnaryOperator;
import org.dbsp.formal.UniformUnaryOperator;
import org.dbsp.types.IStream;
import org.dbsp.types.ScalarType;
import org.junit.Assert;
import org.junit.Test;

import java.util.function.Function;
import java.util.function.Predicate;

/**
 * Tests that compute streaming relational queries.
 */
public class RelationalStreamTests {
    static final ZSetGroup<TestTuple, Integer> gr = new ZSetGroup<>(IntegerRing.instance);
    final ScalarType<ZSet<TestTuple, Integer>> tgr = new ScalarType<ZSet<TestTuple, Integer>>() {
        @Override
        public Group<ZSet<TestTuple, Integer>> getGroup() {
            return gr;
        }
    };

    public IStream<ZSet<TestTuple, Integer>> getInput() {
        return new IStream<ZSet<TestTuple, Integer>>(IntegerTime.Factory.instance) {
            @Override
            public ZSet<TestTuple, Integer> get(Time index) {
                ZSet<TestTuple, Integer> r = new ZSet<TestTuple, Integer>(IntegerRing.instance);
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
        Predicate<TestTuple> f = t -> t.v >= 5;
        UniformUnaryOperator<ZSet<TestTuple, Integer>> filter =
                new UniformUnaryOperator<ZSet<TestTuple, Integer>>(tgr) {
            @Override
            public Function<ZSet<TestTuple, Integer>, ZSet<TestTuple, Integer>> getComputation() {
                return s -> s.filter(f);
            }
        };
        UnaryOperator<IStream<ZSet<TestTuple, Integer>>, IStream<ZSet<TestTuple, Integer>>> sft =
                filter.lift(IntegerTime.Factory.instance);
        IStream<ZSet<TestTuple, Integer>> input = this.getInput();
        IStream<ZSet<TestTuple, Integer>> result = sft.getComputation().apply(input);
        ZSet<TestTuple, Integer> o = result.get(0);
        Assert.assertEquals(1, o.size());
        o = result.get(1);
        Assert.assertEquals(1, o.size());
        o = result.get(2);
        Assert.assertEquals(0, o.size());
        o = result.get(3);
        Assert.assertEquals(0, o.size());
    }
}
