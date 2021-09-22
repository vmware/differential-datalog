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
 * A dynamically typed version of ComparableList.
 */
public class ComparableObjectList
    extends ArrayList<Object>
        implements Comparable<ComparableObjectList>
        // implements ComparableObject
{
    public ComparableObjectList(Object... o) {
        super(o.length);
        this.addAll(Arrays.asList(o));
    }

    @SuppressWarnings("unchecked")
    @Override
    public int compareTo(ComparableObjectList o) {
        for (int i = 0; i < this.size() && i < o.size(); i++) {
            // The following may not implement ComparableObject, but they may actually have a compareTo method.
            // We cannot retrofit this interface to existing classes like Integer or String.
            int compare = ((Comparable<Object>)this.get(i)).compareTo((Comparable<Object>)o.get(i));
            if (compare != 0)
                return compare;
        }
        return Integer.compare(this.size(), o.size());
    }
}
