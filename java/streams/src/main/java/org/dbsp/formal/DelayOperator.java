package org.dbsp.formal;

import org.dbsp.algebraic.TimeFactory;
import org.dbsp.types.IStream;
import org.dbsp.types.StreamType;
import org.dbsp.types.Type;

import java.util.function.Function;

/**
 * An operator that works on streams.  It delays the input stream by 1 clock.
 * @param <T> Concrete implementation of elements of the input and output stream.
 */
public class DelayOperator<T, F extends TimeFactory> extends
        UniformUnaryOperator<IStream<T>> {
    final Type<T> elementType;

    public DelayOperator(Type<T> type, F factory) {
        super(new StreamType<T>(type, factory));
        this.elementType = type;
        if (!type.isStream())
            throw new RuntimeException("Delay must be applied to a stream type, not to " + type);
    }

    @Override
    public Function<IStream<T>, IStream<T>> getComputation() {
        return (IStream<T> s) -> s.delay(this.elementType.getGroup());
    }

    @Override
    public String toString() {
        return "z";
    }
}
