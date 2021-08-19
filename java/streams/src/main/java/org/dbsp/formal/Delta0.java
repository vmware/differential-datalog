package org.dbsp.formal;

import org.dbsp.algebraic.TimeFactory;
import org.dbsp.types.IStream;
import org.dbsp.types.StreamType;
import org.dbsp.types.Type;

import java.util.function.Function;

/**
 * An operator that produces a stream from a value.
 * @param <T> Type of input value.
 */
public class Delta0<T> extends UnaryOperator<T, IStream<T>> {
    protected final TimeFactory factory;

    public Delta0(Type<T> inputType, TimeFactory factory) {
        super(inputType, new StreamType<T>(inputType, factory));
        this.factory = factory;
    }

    @Override
    public Function<T, IStream<T>> getComputation() {
        return value -> new org.dbsp.compute.Delta0<T>(value, this.inputType.getGroup(), this.factory);
    }

    @Override
    public String toString() {
        return "Delta0";
    }
}
