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

import org.dbsp.circuits.ComputationalElement;
import org.dbsp.circuits.Scheduler;
import org.dbsp.circuits.Wire;
import org.dbsp.lib.Utilities;

public class Sink extends ComputationalElement {
    final String name;
    final Wire wire;

    public Sink(Wire input) {
        super(Utilities.list(input.getType()), Utilities.list());
        this.name = "sink";
        this.wire = input;
    }

    @Override
    public void checkConnected() {    }

    public Object getValue(Scheduler scheduler) {
        return this.wire.getValue(scheduler);
    }

    @Override
    public void notifyInputIsAvailable(Scheduler scheduler) {}

    @Override
    public String getName() {
        return this.name;
    }

    public void setInput(int index, Wire source) {
        throw new RuntimeException(this + ": Input already set");
    }

    @Override
    public String graphvizId() {
        return "sink" + this.id;
    }

    @Override
    public String toString() {
        return "_";
    }

    @Override
    public void toGraphvizNodes(boolean deep, int indent, StringBuilder builder) {
        Utilities.indent(indent, builder);
        builder.append(this.graphvizId())
                .append(" [label=\"").append(this.toString())
                .append(" (").append(this.id).append(")\"]\n");
    }

    @Override
    public void toGraphvizWires(boolean deep, int indent, StringBuilder builder) {}
}
