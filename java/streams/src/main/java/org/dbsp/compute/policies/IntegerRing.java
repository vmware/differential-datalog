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

package org.dbsp.compute.policies;

import org.dbsp.algebraic.ZRing;

/**
 * The group of finite integer values.
 * This will throw on overflow.
 * This implementation uses finite Java integers.
 */
public class IntegerRing implements ZRing<Integer> {
    private IntegerRing() {}

    public static final IntegerRing instance = new IntegerRing();

    @Override
    public Integer minus(Integer data) {
        return Math.negateExact(data);
    }

    @Override
    public Integer add(Integer left, Integer right) {
        return Math.addExact(left, right);
    }

    @Override
    public Integer zero() {
        return 0;
    }

    @Override
    public Integer times(Integer left, Integer right) {
        return Math.multiplyExact(left, right);
    }

    @Override
    public Integer one() {
        return 1;
    }

    @Override
    public boolean equal(Integer w0, Integer w1) {
        return w0.equals(w1);
    }

    public boolean isPositive(Integer value) {
        return value >= 0;
    }
}
