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

package org.dbsp.compute.relational;

import org.dbsp.algebraic.staticTyping.FiniteFunctionGroup;
import org.dbsp.algebraic.staticTyping.Group;
import org.dbsp.algebraic.staticTyping.ZRing;

/**
 * The group structure that operates on Z-sets with elements of type T
 * and weights W
 * @param <T>  Type of elements in the Z-sets.
 * @param <W>  Type of weights.
 */
public class ZSetGroup<T extends Comparable<T>, W>
        // extends FiniteFunctionGroup<T, W> //  -- unfortunately Java does not allow this.
        implements Group<ZSet<T, W>>
{
    final ZRing<W> ring;

    public ZSetGroup(ZRing<W> ring) {
        this.ring = ring;
    }

    @Override
    public ZSet<T, W> minus(ZSet<T, W> data) {
        return data.minus();
    }

    @Override
    public ZSet<T, W> add(ZSet<T, W> left, ZSet<T, W> right) {
        return left.plus(right);
    }

    @Override
    public ZSet<T, W> zero() {
        return new ZSet<T, W>(this.ring);
    }
}
