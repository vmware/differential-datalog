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

import java.util.Objects;

/**
 * A simple tuple used for testing.  Two fields.
 */
class TestTuple implements Comparable<TestTuple> {
    final String s;
    final Integer v;

    TestTuple(String s, Integer v) {
        this.s = s;
        this.v = v;
    }

    public String toString() {
        return "<" + this.s + "," + v.toString() + ">";
    }

    @Override
    public int compareTo(TestTuple o) {
        int c = this.s.compareTo(o.s);
        if (c != 0)
            return c;
        return this.v.compareTo(o.v);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        TestTuple testTuple = (TestTuple) o;
        return s.equals(testTuple.s) &&
                v.equals(testTuple.v);
    }

    @Override
    public int hashCode() {
        return Objects.hash(s, v);
    }
}
