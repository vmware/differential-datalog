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

import org.dbsp.algebraic.Time;
import org.dbsp.algebraic.TimeFactory;

/**
 * The group structure induced by a stream where values belong to a group.
 * @param <T>  Values in the stream.
 */
public class StreamGroup<T> implements Group<IStream<T>> {
    final Group<T> group;
    final TimeFactory timeFactory;

    /**
     * Create a stream group from a group and a time factory.
     * @param group        Group that stream elements belong to.
     * @param timeFactory  Factory that knows how to create time indexes for stream.
     */
    public StreamGroup(Group<T> group, TimeFactory timeFactory) {
        this.group = group;
        this.timeFactory = timeFactory;
    }

    @Override
    public IStream<T> negate(IStream<T> data) {
        return data.negate(group);
    }

    @Override
    public IStream<T> add(IStream<T> left, IStream<T> right) {
        return left.add(right, group);
    }

    @Override
    public IStream<T> zero() {
        return new IStream<T>(this.timeFactory) {
            @Override
            public T get(Time index) {
                return StreamGroup.this.group.zero();
            }
        };
    }
}
