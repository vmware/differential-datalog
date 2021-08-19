package org.dbsp.formal;

import org.dbsp.types.Type;

import java.util.function.Function;

/**
 * An operator that negates its input.
 * @param <T>  Type of input.
 */
public class MinusOperator<T> extends UniformUnaryOperator<T> {
    protected MinusOperator(Type<T> type) {
        super(type);
    }

    @Override
    public Function<T, T> getComputation() {
        return value -> inputType.getGroup().minus(value);
    }

    @Override
    public String toString() {
        return "-";
    }
}
