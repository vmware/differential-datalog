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

package org.dbsp.circuits.operators;

import org.dbsp.algebraic.dynamicTyping.types.Type;
import org.dbsp.circuits.Scheduler;

import javax.annotation.Nullable;
import java.util.Objects;

/**
 * A port may not have input wires if it is an external circuit port.
 * In all other respects it behaves like an IdOperator.
 */
public class Port extends IdOperator {
    @Nullable
    Object value;

    public Port(Type elementType) {
        super(elementType);
    }

    public void setValue(Object value) {
        this.value = value;
    }

    public void setOutput(Scheduler scheduler) {
        this.outputWire().setValue(Objects.requireNonNull(this.value));
        this.outputWire().notifyConsumers(scheduler);
    }

    public void checkConnected() {
        // ok not to have an input
    }

    @Override
    public String toString() {
        return ".";
    }

    public boolean hasNoInputWire() {
        return this.inputs.get(0) == null;
    }
}
