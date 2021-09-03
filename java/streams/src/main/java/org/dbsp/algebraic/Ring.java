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

package org.dbsp.algebraic;

/**
 * An algebraic structure of a ring.
 * @param <T>  Type of elements in the ring.
 */
public interface Ring<T> extends Group<T> {
    /**
     * Multiplication in the ring.
     * @param left   Left value to multiply.
     * @param right  Right value to multiply.
     * @return       The result of the multiplication.  This operation better be associative.
     */
    T times(T left, T right);

    /**
     * The neutral element for multiplication.
     */
    T one();

    /**
     * Add one to a value.
     * @param value  Value to increment.
     */
    default T increment(T value) {
        return this.add(value, this.one());
    }

    /**
     * Check if a value is one.
     * @param value  Value to compare.
     * @return       True if the value is the ring one element.
     */
    default boolean isOne(T value) { return this.one().equals(value); }
}
