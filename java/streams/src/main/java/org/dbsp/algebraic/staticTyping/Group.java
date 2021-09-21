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

package org.dbsp.algebraic.staticTyping;

import org.dbsp.algebraic.dynamicTyping.DynamicGroup;

/**
 * Abstract interface implemented by a Group: a monoid where each element has an inverse.
 * @param <T>  Type that is used to represent the values in the group.
 */
public interface Group<T> extends Monoid<T> {
    /**
     * The inverse of value data in the group.
     * @param data  A value from the group.
     * @return  The inverse.
     */
    T minus(T data);

    default boolean equal(T left, T right) {
        return this.isZero(this.add(this.minus(left), right));
    }

    /**
     * Convert this group into a group that operates on Object values.
     * Needed for the dynamically-typed version of computations.
     */
    @SuppressWarnings("unchecked")
    default DynamicGroup asUntyped() {
        return new DynamicGroup() {
            @Override
            public Object add(Object left, Object right) {
                return Group.this.add((T)left, (T)right);
            }

            @Override
            public Object zero() {
                return Group.this.zero();
            }

            @Override
            public Object minus(Object data) {
                return Group.this.minus((T)data);
            }

            @Override
            public DynamicGroup asUntyped() {
                return this;
            }
        };
    }
}
