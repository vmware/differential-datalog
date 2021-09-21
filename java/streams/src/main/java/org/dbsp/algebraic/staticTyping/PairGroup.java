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

import javafx.util.Pair;

/**
 * A pair group combines two groups to create a group that operates pointwise on a pair's values.
 * @param <T>   Type of left value.
 * @param <S>   Type of right value.
 */
public class PairGroup<T, S> implements Group<Pair<T, S>> {
    final Group<T> gt;
    final Group<S> gs;

    /**
     * Creates a pair group from a pair of groups.
     * @param gt  Group operating on values in the left of the pair.
     * @param gs  Group operating on values in the right of the pair.
     */
    public PairGroup(Group<T> gt, Group<S> gs) {
        this.gt = gt;
        this.gs = gs;
    }

    @Override
    public Pair<T, S> minus(Pair<T, S> data) {
        return new Pair<T, S>(this.gt.minus(data.getKey()), this.gs.minus(data.getValue()));
    }

    @Override
    public Pair<T, S> add(Pair<T, S> left, Pair<T, S> right) {
        return new Pair<T, S>(this.gt.add(left.getKey(), right.getKey()),
                this.gs.add(left.getValue(), right.getValue()));
    }

    @Override
    public Pair<T, S> zero() {
        return new Pair<T, S>(this.gt.zero(), this.gs.zero());
    }
}
