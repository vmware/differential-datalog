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

import org.dbsp.algebraic.dynamicTyping.types.Type;
import org.dbsp.lib.HasId;

import javax.annotation.Nullable;
import java.util.List;

/**
 * A base class for various operators and circuits.
 */
public abstract class ComputationalElement extends HasId {
    protected final List<Type> inputTypes;
    protected final List<Type> outputTypes;
    @Nullable
    protected Circuit parent;

    protected ComputationalElement(List<Type> inputTypes, List<Type> outputTypes) {
        this.inputTypes = inputTypes;
        this.outputTypes = outputTypes;
        this.parent = null;
    }

    public void setParent(Circuit circuit) {
        if (this.parent != null)
            throw new RuntimeException("Operator " + this + " already has a parent: " + this.parent);
        this.parent = circuit;
    }

    @Nullable
    public Circuit getParent() {
        return this.parent;
    }

    /**
     * Number of inputs expected.
     */
    public int inputCount() {
        return this.inputTypes.size();
    }

    /**
     * Number of outputs produced.
     */
    public int outputCount() {
        return this.outputTypes.size();
    }

    /**
     * Type of an input.
     * @param index Input index.
     */
    public Type getInputType(int index) {
        return this.inputTypes.get(index);
    }

    /**
     * Type of an output.
     * @param index Output index.
     */
    public Type getOutputType(int index) {
        return this.outputTypes.get(index);
    }

    public List<Type> getInputTypes() {
        return this.inputTypes;
    }
    public List<Type> getOutputTypes() {
        return this.outputTypes;
    }

    public abstract void setInput(int index, Wire source);

    /**
     * Check if all inputs and outputs that should be connected to wires
     * are indeed connected.
     */
    public abstract void checkConnected();

    /**
     * Write the graphviz representation of the nodes in the circuit into the builder.
     * @param deep     If true recurse all the way to the lowermost nodes.
     * @param indent   Indentation level.
     * @param builder  Write result here.
     */
    public abstract void toGraphvizNodes(boolean deep, int indent, StringBuilder builder);

    /**
     * Write the graphviz representation of the wires in the circuit into the stringBuilder.
     * @param deep     If true recurse all the way to the lowermost nodes.
     * @param indent   Indentation level.
     * @param builder  Write result here.
     */
    public abstract void toGraphvizWires(boolean deep, int indent, StringBuilder builder);

    /**
     * Notify the consumer that an input has arrived.
     */
    public abstract void notifyInputIsAvailable(Scheduler scheduler);

    /**
     * A human-friendly name to identify this Consumer.
     */
    public abstract String getName();

    public void log(Scheduler scheduler, String message, Object... arguments) {
        scheduler.log(this.getName() + ": " + message, arguments);
    }

    /**
     * An id that is used in the generated graphviz representation.
     */
    public abstract String graphvizId();
}
