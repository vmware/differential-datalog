package org.dbsp.formal;

import org.dbsp.types.Type;

/**
 * A Unary operator that has the same input and output types.
 * @param <T>  Type of values processed.
 */
public abstract class UniformUnaryOperator<T> extends UnaryOperator<T, T> {
    protected UniformUnaryOperator(Type<T> inputType) {
        super(inputType, inputType);
    }
}
