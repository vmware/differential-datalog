package org.dbsp.formal;

import org.dbsp.types.Type;

import java.util.function.BiFunction;

/**
 * An operator that adds its inputs.
 * @param <T>  Concrete type of inputs.
 */
public class PlusOperator<T> extends BinaryOperator<T, T> {
    public PlusOperator(Type<T> type) {
        super(type, type);
    }

    @Override
    public BiFunction<T, T, T> getComputation() {
        return (left, right) -> PlusOperator.this.inputType.getGroup().add(left, right);
    }

    public String toString() {
        return "+";
    }
}
