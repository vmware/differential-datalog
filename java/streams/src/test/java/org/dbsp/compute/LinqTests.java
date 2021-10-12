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

import org.dbsp.lib.LinqIterator;
import org.dbsp.lib.Pair;
import org.dbsp.lib.Triple;
import org.dbsp.lib.Utilities;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;
import java.util.function.BiFunction;
import java.util.function.Function;

public class LinqTests {
    LinqIterator<Integer> create123() {
        return LinqIterator.create(1, 2, 3);
    }

    @Test
    public void constructorTests() {
        LinqIterator<Integer> i1 = create123();
        LinqIterator<Integer> i2 = create123();
        Assert.assertTrue(i1.same(i2));

        i1 = LinqIterator.create();
        Assert.assertFalse(i1.hasNext());

        LinqIterator<Integer> i3 = LinqIterator.create(Utilities.list(1, 2, 3));
        LinqIterator<Integer> i4 = create123();
        Assert.assertTrue(i3.same(i4));

        i1 = LinqIterator.create();
        i3 = create123();
        Assert.assertFalse(i1.same(i3));

        List<Integer> list = create123().toList();
        Assert.assertEquals(3, list.size());
        Assert.assertEquals((Integer)1, list.get(0));
    }

    @Test
    public void filterTest() {
        LinqIterator<Integer> l = create123();
        LinqIterator<Integer> odd = l.filter(e -> e % 2 == 1);
        Assert.assertTrue(odd.hasNext());
        Assert.assertEquals((Integer)1, odd.next());
        Assert.assertEquals((Integer)3, odd.next());
        Assert.assertFalse(odd.hasNext());

        LinqIterator<Integer> empty = LinqIterator.create();
        odd = empty.filter(e -> e % 2 == 1);
        Assert.assertFalse(odd.hasNext());

        empty = LinqIterator.create();
        odd = empty.filter(e -> e % 2 == 1);
        LinqIterator<Integer> none = odd.filter(e -> e % 2 == 0);
        Assert.assertFalse(none.hasNext());

        l = create123();
        none = l.filter(e -> e % 2 == 1).filter(e -> e % 2 == 0);
        Assert.assertFalse(none.hasNext());
    }

    @Test
    public void mapTest() {
        LinqIterator<Integer> l = create123();
        LinqIterator<Integer> parity = l.map(e -> e % 2);
        Assert.assertTrue(parity.hasNext());
        Assert.assertEquals((Integer)1, parity.next());
        Assert.assertEquals((Integer)0, parity.next());
        Assert.assertEquals((Integer)1, parity.next());
        Assert.assertFalse(parity.hasNext());

        LinqIterator<Integer> empty = LinqIterator.create();
        parity = empty.map(e -> e % 2);
        Assert.assertFalse(parity.hasNext());

        empty = LinqIterator.create();
        parity = empty.map(e -> e % 2);
        LinqIterator<Integer> none = parity.map(e -> e % 2);
        Assert.assertFalse(none.hasNext());

        l = create123();
        none = l.map(e -> e % 2 == 1).map(e -> e ? 1 : 0);
        Assert.assertTrue(none.same(1, 0, 1));
    }

    @Test
    public void anyTest() {
        boolean anyEven = create123().any(e -> e % 2 == 0);
        Assert.assertTrue(anyEven);
        boolean anyOdd = create123().any(e -> e % 2 == 1);
        Assert.assertTrue(anyOdd);
        anyOdd = create123().map(e -> 2).any(e -> e % 2 == 1);
        Assert.assertFalse(anyOdd);
        anyEven = LinqIterator.<Integer>create().any(e -> e % 2 == 0);
        Assert.assertFalse(anyEven);
    }

    @Test
    public void concatTest() {
        LinqIterator<Integer> l = create123().concat(create123());
        Assert.assertTrue(l.same(1, 2, 3, 1, 2, 3));
        l = LinqIterator.<Integer>create().concat(LinqIterator.create());
        Assert.assertFalse(l.hasNext());
        l = create123().concat(LinqIterator.create());
        Assert.assertTrue(l.same(create123()));
        l = LinqIterator.<Integer>create().concat(create123());
        Assert.assertTrue(l.same(create123()));
    }

    @Test
    public void zipTest() {
        Function<Pair<Integer, Integer>, Integer> sum = p -> p.first + p.second;
        LinqIterator<Integer> l = create123().zip(create123()).map(sum);
        Assert.assertTrue(l.same(2, 4, 6));
        l = create123().zip(create123().filter(e -> e % 2 == 1)).map(sum);
        Assert.assertTrue(l.same(2, 5));
        l = create123().zip(create123().concat(create123())).map(sum);
        Assert.assertTrue(l.same(2, 4, 6));
        l = LinqIterator.<Integer>create().zip(create123()).map(sum);
        Assert.assertFalse(l.hasNext());
    }

    @Test
    public void zip3Test() {
        Function<Triple<Integer, Integer, Integer>, Integer> sum = p -> p.first + p.second + p.third;
        LinqIterator<Integer> l = create123().zip3(create123(), create123()).map(sum);
        Assert.assertTrue(l.same(3, 6, 9));
        l = create123().zip3(create123(), LinqIterator.<Integer>create()).map(sum);
        Assert.assertFalse(l.hasNext());
        l = create123().zip3(LinqIterator.<Integer>create(), create123()).map(sum);
        Assert.assertFalse(l.hasNext());
        l = LinqIterator.<Integer>create().zip3(create123(), create123()).map(sum);
        Assert.assertFalse(l.hasNext());
        l = create123().zip3(create123().concat(create123()), create123().concat(create123()).concat(create123())).map(sum);
        Assert.assertTrue(l.same(3, 6, 9));
    }

    @Test
    public void testFold() {
        BiFunction<Integer, Integer, Integer> sum = Integer::sum;
        int s = create123().foldLeft(sum, 0);
        Assert.assertEquals(6, s);
        BiFunction<Boolean, Integer, Boolean> parity = (a, b) -> a ^ (b % 2 == 0);
        boolean p = create123().foldLeft(parity, false);
        Assert.assertTrue(p);
        s = LinqIterator.<Integer>create().foldLeft(sum, 0);
        Assert.assertEquals(s, 0);
    }
}
