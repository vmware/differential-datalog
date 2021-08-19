package org.dbsp.formal;

import org.dbsp.algebraic.TimeFactory;
import org.dbsp.types.IStream;
import org.dbsp.types.StreamType;
import org.dbsp.types.Type;

import java.util.function.Function;

/**
 * An operator that consumes a stream and produces a scalar.
 * The operator sums up all values in the stream up to the first 0.
 * @param <T>  Type of scalar produced.
 */
public class IntOperator<T, F extends TimeFactory> extends UnaryOperator<IStream<T>, T> {
    protected IntOperator(Type<T> outputType, F factory) {
        super(new StreamType<T>(outputType, factory), outputType);
    }

    @Override
    public Function<IStream<T>, T> getComputation() {
        return s -> s.sumToZero(this.outputType.getGroup());
    }
}
