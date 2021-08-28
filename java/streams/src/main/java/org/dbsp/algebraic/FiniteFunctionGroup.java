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
 * Finite functions that map values into a group G also form a group themselves.
 * This is the implementation of that group.
 * @param <D>   Domain of the finite functions.
 * @param <T>   Codomain of finite functions.
 */
public class FiniteFunctionGroup<D, T> implements Group<FiniteFunction<D, T>> {
    final Group<T> group;

    public FiniteFunctionGroup(Group<T> group) {
        this.group = group;
    }

    @Override
    public FiniteFunction<D, T> minus(FiniteFunction<D, T> data) {
        return new FiniteFunction<D, T>() {
            @Override
            public T apply(D d) {
                return FiniteFunctionGroup.this.group.minus(data.apply(d));
            }
        };
    }

    @Override
    public FiniteFunction<D, T> add(FiniteFunction<D, T> left, FiniteFunction<D, T> right) {
        return new FiniteFunction<D, T>() {
            @Override
            public T apply(D d) {
                T first = left.apply(d);
                T second = right.apply(d);
                return FiniteFunctionGroup.this.group.add(first, second);
            }
        };
    }

    @Override
    public FiniteFunction<D, T> zero() {
        return new FiniteFunction<D, T>() {
            @Override
            public T apply(D d) {
                return FiniteFunctionGroup.this.group.zero();
            }
        };
    }
}