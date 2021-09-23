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

import org.dbsp.circuits.operators.*;
import org.dbsp.algebraic.dynamicTyping.types.Type;
import org.dbsp.lib.HasId;
import org.dbsp.lib.Pair;
import org.dbsp.lib.Utilities;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;

/**
 * A wire connects an operator output to one or more operator inputs.
 * The wire maintains a state machine to ensure that values are not
 * overwritten before they have been consumed by everyone.
 * Each wire has a source operator.  However, a wire may have zero
 * consumers if it is an output wire.
 */
public class Wire extends HasId {
    // For each consumer a node that it represents.  For example,
    // some ports represent the circuit they belong to.
    public final List<Pair<ComputationalElement, ComputationalElement>> consumers = new ArrayList<>();
    /**
     * State-machine: number of consumers who have not consumed the
     * value yet.
     */
    private int toConsume;
    private final Type valueType;
    public Operator source;
    /**
     * Current value on the wire.  If 'null' the wire has no value.
     * Would be nice to have an interface for this, but then we cannot
     * use standard java types like Integer.
     */
    @Nullable
    Object value;

    public Wire(Type valueType, Operator source) {
        this.valueType = valueType;
        this.toConsume = 0;
        this.source = source;
    }

    /**
     * A consumer of this wire wants to know the value.
     * @return  The value on the wire.
     */
    public Object getValue() {
        if (this.value == null)
            throw new RuntimeException("Wire has no value: " + this);
        if (this.toConsume == 0)
            throw new RuntimeException("Too many consumers for " + this);
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
    public void addConsumer(ComputationalElement consumer, ComputationalElement representative) {
        this.consumers.add(
                new Pair<ComputationalElement, ComputationalElement>(consumer, representative));
    }

    public Type getType() {
        return this.valueType;
    }

    public void notifyConsumers(Scheduler scheduler) {
        for (Pair<ComputationalElement, ComputationalElement> op: this.consumers)
            op.first.notifyInputIsAvailable(scheduler);
    }

    public void log() {
        if (this.value == null)
            System.out.println("Wire " + this.id + " consumed");
        else
            System.out.println("Wire " + this.id + " set to " + this.value);
    }

    @Override
    public String toString() {
        return "Wire " + this.id + " " + this.valueAsString();
    }

    public String valueAsString() {
        return (this.value == null) ? "-" : ": " + this.value;
    }

    /**
     * Write a wire as a graphviz string.
     * @param sourceToShow  Node to show as wire source.  May not be actual source
     *                      when we want to collapse a circuit in the drawing.
     * @param deep          If true recurse deeply into all circuits.
     * @param indent        Indentation level in graphviz file.
     * @param builder       Write result here.
     */
    public void toGraphviz(
            @Nullable
            CircuitOperator sourceToShow, boolean deep, int indent, StringBuilder builder) {
        String src = sourceToShow != null ? sourceToShow.graphvizId() : this.source.graphvizId();
        for (Pair<ComputationalElement, ComputationalElement> cp: this.consumers) {
            Utilities.indent(indent, builder);
            ComputationalElement toShow = cp.first;
            if (sourceToShow != null && toShow.parent == sourceToShow.circuit)
                // The destination is a member of the circuit element which is the source,
                // skip it.
                continue;
            if (cp.second instanceof CircuitOperator) {
                CircuitOperator co = (CircuitOperator)cp.second;
                if (!deep && co.basic)
                    toShow = co;
            }
            builder.append(src).append(" -> ").append(toShow.graphvizId())
                    .append(" [label=\"(").append(this.id)
                    .append(") ").append(this.valueAsString())
                    .append("\"]").append("\n");
        }
    }

    public Sink addSink() {
        Sink sink = new Sink(this);
        this.addConsumer(sink, sink);
        return sink;
    }
}
