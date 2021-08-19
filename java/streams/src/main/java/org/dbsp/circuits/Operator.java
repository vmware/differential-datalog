package org.dbsp.circuits;

public abstract class Operator {
    final Wire output;
    final Wire[] inputs;
    final int inputsPresent;

    public Operator(int arity) {
        this.output = new Wire();
        this.inputs = new Wire[arity];
        this.inputsPresent = 0;
    }

    public int arity() {
        return this.inputs.length;
    }

    /**
     * Return a consumer that represents the index-th input
     * of this operator.
     * @param index  Index of the input that consumes data.
     * @return  A consumer.
     */
    public Consumer getInputWire(int index) {
        return this.inputs[index];
    }
}
