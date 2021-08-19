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

package org.dbsp.circuits;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;

/**
 * A wire connects an operator output to one or more operator inputs.
 */
public class Wire implements Consumer {
    private final List<Consumer> consumers = new ArrayList<>();

    /**
     * Current value on the wire.  If 'null' the wire has no value.
     */
    @Nullable
    public Object value;

    /**
     * Receive a value and sent it to all consumers.
     * @param val  Value to receive.
     */
    @Override
    public void receive(Object val) {
        if (this.value != null)
            throw new RuntimeException("Wire already has a value:" + this.value + " when receiving " + val);
        this.value = val;
        for (Consumer o: this.consumers)
            o.receive(value);
        this.value = null;
    }

    /**
     * Invoked at circuit construction time.  Add an operator
     * that receives the data from this wire.
     * @param consumer  Operator that consumes the produced values.
     */
    public void addConsumer(Consumer consumer) {
        this.consumers.add(consumer);
    }
}
