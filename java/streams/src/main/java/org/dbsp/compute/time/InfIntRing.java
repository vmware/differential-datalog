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

package org.dbsp.compute.time;

import org.dbsp.algebraic.staticTyping.ZRing;

import java.math.BigInteger;

/**
 * The group of infinite-precision integer values.
 * This implementation uses BigInt Java integers.
 */
public class InfIntRing implements ZRing<BigInteger> {
    static final BigInteger zero = BigInteger.valueOf(0);
    static final BigInteger one = BigInteger.valueOf(1);

    private InfIntRing() {}

    public static final InfIntRing instance = new InfIntRing();

    @Override
    public BigInteger negate(BigInteger data) {
        return data.negate();
    }

    @Override
    public BigInteger add(BigInteger left, BigInteger right) {
        return left.add(right);
    }

    @Override
    public BigInteger zero() {
        return zero;
    }

    @Override
    public BigInteger times(BigInteger left, BigInteger right) {
        return left.multiply(right);
    }

    @Override
    public BigInteger one() {
        return one;
    }

    @Override
    public boolean equal(BigInteger w0, BigInteger w1) {
        return w0.equals(w1);
    }

    public boolean isPositive(BigInteger value) {
        return value.compareTo(zero) >= 0;
    }
}
