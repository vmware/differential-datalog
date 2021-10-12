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

import org.dbsp.algebraic.staticTyping.ZRing;

import java.util.*;

/**
 * Simple sets.  Inputs and outputs of computations are usually sets.
 * Unlike a ZSet, in a RSet all elements have weight 1.
 * @param <T>  Type of elements in the sets.
 */
public class RSet<T extends Comparable<T>> {
    public final Set<T> data;

    public RSet(Collection<T> data) {
        this.data = new HashSet<T>(data);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        RSet<?> set = (RSet<?>) o;
        return this.data.equals(set.data);
    }

    @Override
    public int hashCode() {
        return this.data.hashCode();
    }

    @Override
    public String toString() {
        List<T> sorted = new ArrayList<T>(this.data);
        sorted.sort(Comparator.naturalOrder());
        StringBuilder builder = new StringBuilder();
        builder.append("{");
        boolean first = true;
        for (T value: sorted) {
            if (!first)
                builder.append(",");
            else
                first = false;
            builder.append(value);
        }
        builder.append("}");
        return builder.toString();
    }

    public <W> ZSet<T, W> toZSet(ZRing<W> ring) {
        return new ZSet<T, W>(ring, this.data);
    }
}
