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

import org.dbsp.algebraic.dynamicTyping.DynamicGroup;
import org.dbsp.algebraic.dynamicTyping.types.Type;
import org.dbsp.circuits.Scheduler;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * Base class for OuterD and OuterI operators.
 * These are operators that compute on an outer stream that contains nested streams as values.
 */
public abstract class OuterOperator extends UnaryOperator {
    public final List<Object> history;
    protected int currentIndex;
    protected final DynamicGroup group;

    public OuterOperator(Type type) {
        super(type, type);
        this.group = Objects.requireNonNull(type.getGroup());
        this.history = new ArrayList<>();
        this.currentIndex = 0;
    }

    @Override
    public void reset(Scheduler scheduler) {
        scheduler.log("Resetting " + this);
        this.currentIndex = 0;
        super.reset(scheduler);
    }
}
