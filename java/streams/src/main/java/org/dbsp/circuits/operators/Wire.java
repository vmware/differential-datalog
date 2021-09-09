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

import org.dbsp.circuits.types.Type;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;

/**
 * A wire connects an operator output to one or more operator inputs.
 * The wire maintains a state machine to ensure that values are not
 * overwritten before they have been consumed by everyone.
 */
public class Wire {
    private final List<Consumer> consumers = new ArrayList<>();
    /**
     * State-machine: number of consumers who have not consumed the
     * value yet.
     */
    private int toConsume;
    private final Type valueType;
    @Nullable
    private Operator source;
    static int crtid = 0;
    private final int id;
    /**
     * Current value on the wire.  If 'null' the wire has no value.
     * Would be nice to have an interface for this, but then we cannot
     * use standard java types like Integer.
     */
    @Nullable
    Object value;

    public Wire(Type valueType) {
        this.valueType = valueType;
        this.toConsume = 0;
        this.source = null;
        this.id = crtid++;
    }

    public void setSource(Operator op) {
        this.source = op;
    }

    /**
     * A consumer of this wire wants to know the value.
     * @return  The value on the wire.
     */
    public Object getValue() {
        if (this.value == null)
            throw new RuntimeException("Wire has no value: " + this);
        if (this.toConsume == 0)
            throw new RuntimeException("Too many consumers for wire " + this);
        this.toConsume--;
        Object result = this.value;
        if (this.toConsume == 0) {
            this.value = null;
            this.log();
        }
        return result;
    }

    /**
     * The source of this wire has produced a value.
     * @param value  Value to set to wire.
     */
    public void setValue(Object value) {
        if (this.value != null || this.toConsume != 0)
            throw new RuntimeException("Setting wire " + this + " to value " + value +
                    " but prior value not yet consumed");
        this.value = value;
        this.log();
        this.toConsume = this.consumers.size();
    }

    /**
     * Invoked at circuit construction time.  Add an operator
     * that receives the data from this wire.
     * @param consumer  Operator that consumes the wire value.
     */
    public void addConsumer(Consumer consumer) {
        this.consumers.add(consumer);
    }

    public Type getType() {
        return this.valueType;
    }

    public boolean hasConsumers() {
        return this.consumers.size() > 0;
    }

    public void push() {
        for (Consumer op: this.consumers)
            op.notifyInput();
    }

    public void log() {
        System.out.println("Wire " + this.id + " set to " + this.value);
    }

    @Override
    public String toString() {
        return "Wire " + this.id + " " + ((this.value == null) ? "no value" : "value " + this.value);
    }

    public void toGraphviz(int indent, StringBuilder builder) {
        String src = "(none)";
        if (this.source != null)
            src = this.source.graphvizId();
        for (int i = 0; i < indent; i++)
            builder.append(" ");
        for (Consumer c: this.consumers)
            builder.append(src).append(" -> ").append(c.graphvizId())
                    .append(" [label=\"id=").append(this.id).append("\"]").append("\n");
    }
}
