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

package org.dbsp.circuits.operators.relational;

import org.dbsp.algebraic.staticTyping.Group;
import org.dbsp.algebraic.staticTyping.ZRing;

public class DynamicZSetGroup<W> implements Group<DynamicZSet<W>> {
    final ZRing<W> ring;

    public DynamicZSetGroup(ZRing<W> ring) {
        this.ring = ring;
    }

    @Override
    public DynamicZSet<W> negate(DynamicZSet<W> data) {
        return new DynamicZSet<W>(this.ring, data.data.minus());
    }

    @Override
    public DynamicZSet<W> add(DynamicZSet<W> left, DynamicZSet<W> right) {
        return new DynamicZSet<W>(this.ring, left.data.plus(right.data));
    }

    @Override
    public DynamicZSet<W> zero() {
        return new DynamicZSet<W>(this.ring);
    }
}
