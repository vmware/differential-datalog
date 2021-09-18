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

import org.dbsp.algebraic.Group;
import org.dbsp.circuits.types.Type;

import javax.annotation.Nullable;
import java.util.Objects;

public class IntOperator extends UnaryOperator {
    @Nullable
    private Object value = null;
    final Group<Object> group;
    private final Operator body;
    // Int operators are always associated with a Delta
    final DeltaOperator delta;

    protected IntOperator(Type type, DeltaOperator delta, Operator body) {
        super(type, type);
        this.delta = delta;
        this.group = Objects.requireNonNull(type.getGroup());
        this.body = body;
    }

    @Override
    public Object evaluate(Object input) {
        if (this.value == null)
            this.value = input;
        else
            this.value = this.group.add(this.value, input);
        return input;
    }

    @Override
    public void reset() {
        this.log("reset");
        this.value = null;
        this.body.reset();
        this.delta.reset();
    }

    @Override
    public void emitOutput(Object result) {
        // int does not emit an output when expected, only when it is actually zero
        if (this.group.isZero(Objects.requireNonNull(result))) {
            this.output.setValue(Objects.requireNonNull(this.value));
            this.output.notifyConsumers();
            this.reset();
        } else {
            this.delta.repeat();
        }
    }

    @Override
    public String toString() {
        return "Int";
    }
}
