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

package org.dbsp.types;

import org.dbsp.algebraic.Group;
import org.dbsp.algebraic.TimeFactory;
import org.dbsp.algebraic.StreamGroup;

/**
 * Type representing a stream of values of type T
 * @param <T> Concrete Java type implementing T.
 */
public class StreamType<T> implements Type<IStream<T>> {
    public final Type<T> baseType;
    protected final StreamGroup<T> group;

    /**
     * Create a StreamType having baseType as the element type.
     * @param baseType: type of elements in stream.
     */
    public StreamType(Type<T> baseType, TimeFactory factory) {
        this.baseType = baseType;
        this.group = new StreamGroup<T>(baseType.getGroup(), factory);
    }

    @Override
    public Group<IStream<T>> getGroup() {
        return this.group;
    }

    @Override
    public boolean isStream() {
        return true;
    }

    @Override
    public String toString() {
        return "S<" + baseType + ">";
    }
}
