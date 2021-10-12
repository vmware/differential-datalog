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

package org.dbsp.lib;

import java.util.ArrayList;
import java.util.Arrays;

/**
 * Lists that are compared elementwise.
 */
public class ComparableList<T extends Comparable<T>>
        extends ArrayList<T>
        implements Comparable<ComparableList<T>> {
    public ComparableList(int capacity) {
        super(capacity);
    }

    @SafeVarargs
    public ComparableList(T... o) {
        super(o.length);
        this.addAll(Arrays.asList(o));
    }

    @Override
    public int compareTo(ComparableList<T> o) {
        for (int i = 0; i < this.size() && i < o.size(); i++) {
            int compare = this.get(i).compareTo(o.get(i));
            if (compare != 0)
                return compare;
        }
        return Integer.compare(this.size(), o.size());
    }
}
