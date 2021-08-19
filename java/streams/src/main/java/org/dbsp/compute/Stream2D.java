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

package org.dbsp.compute;

import org.dbsp.algebraic.Time;
import org.dbsp.types.IStream;

/**
 * A stream where each element is a stream itself.
 * @param <T>  Type of elements in the bottom stream structure.
 */
public class Stream2D<T> extends IStream<IStream<T>> {
    private final IStream<IStream<T>> data;

    public Stream2D(IStream<IStream<T>> data) {
        super(data.getTimeFactory());
        this.data = data;
    }

    @Override
    public IStream<T> get(Time index) {
        return this.data.get(index);
    }

    String toString(int limit0, int limit1) {
        StringBuilder builder = new StringBuilder();
        for (int i = 0; i < limit0; i++) {
            IStream<T> s = this.get(i);
            String str = s.toString(limit1);
            builder.append(str);
            builder.append("\n");
        }
        return builder.toString();
    }
}
