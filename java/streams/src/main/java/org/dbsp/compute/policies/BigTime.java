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

import org.dbsp.algebraic.Time;
import org.dbsp.algebraic.TimeFactory;

import java.math.BigInteger;

/**
 * Representation for time using unbounded integers.
 */
public class BigTime implements Time {
    public final BigInteger value;
    static final BigInteger zero = BigInteger.valueOf(0);
    static final BigTime zeroTime = new BigTime(zero);
    static final BigInteger one = BigInteger.valueOf(1);

    public BigTime(BigInteger value) {
        this.value = value;
    }

    public BigTime(int value) {
        this.value = BigInteger.valueOf(value);
    }

    public BigTime() {
        this(0);
    }

    public Time zero() {
        return BigTime.zeroTime;
    }

    public boolean isZero() {
        return this.value.equals(zero);
    }

    public Time previous() {
        if (this.isZero())
            throw new RuntimeException("Previous of time 0");
        return new BigTime(this.value.subtract(one));
    }

    public Time next() {
        return new BigTime(this.value.add(one));
    }

    @Override
    public int compareTo(Time other) {
        return this.value.compareTo(((BigTime)other).value);
    }

    @Override
    public int asInteger() {
        return this.value.intValueExact();
    }

    @Override
    public String toString() {
        return this.value.toString();
    }

    public static class Factory implements TimeFactory {
        private Factory() {}

        public static Factory instance = new Factory();

        @Override
        public Time zero() {
            return new BigTime();
        }

        @Override
        public Time fromInteger(int value) {
            return new BigTime(BigInteger.valueOf(value));
        }
    }
}
