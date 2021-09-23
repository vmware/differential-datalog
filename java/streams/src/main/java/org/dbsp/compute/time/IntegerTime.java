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

import org.dbsp.algebraic.Time;
import org.dbsp.algebraic.TimeFactory;

/**
 * Time represented as a Java integer, which can overflow.
 */
public class IntegerTime implements Time {
    final int value;

    public IntegerTime(int value) {
        this.value = value;
    }

    public IntegerTime() {
        this(0);
    }

    @Override
    public boolean isZero() {
        return this.value == 0;
    }

    @Override
    public Time previous() {
        if (this.value == 0)
            throw new RuntimeException("Previous of zero time");
        return new IntegerTime(this.value - 1);
    }

    @Override
    public Time next() {
        if (this.value == Integer.MAX_VALUE)
            throw new RuntimeException("Next of max time value");
        return new IntegerTime(this.value + 1);
    }

    @Override
    public int compareTo(Time time) {
        return Integer.compare(this.value, ((IntegerTime)time).value);
    }

    @Override
    public int asInteger() {
        return this.value;
    }

    @Override
    public String toString() {
        return Integer.toString(this.value);
    }

    public Integer value() {
        return this.value;
    }

    public static class Factory implements TimeFactory {
        private Factory() {}

        public static final Factory instance = new Factory();

        @Override
        public Time zero() {
            return new IntegerTime();
        }

        @Override
        public Time fromInteger(int value) {
            return new IntegerTime(value);
        }
    }
}
