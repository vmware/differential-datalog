package org.dbsp.compute;

import org.dbsp.algebraic.Time;
import org.dbsp.types.IStream;

import java.util.function.Function;

/**
 * A lifted function lifts a function from T to S to compute on streams of values.
 * @param <T>  Type of left input.
 * @param <S>  Type of right input.
 */
public class LiftedFunction<T, S> implements StreamFunction<T, S> {
    final Function<T, S> function;

    public LiftedFunction(Function<T, S> function) {
        this.function = function;
    }

    @Override
    public IStream<S> apply(IStream<T> s) {
        return new IStream<S>(s.getTimeFactory()) {
            @Override
            public S get(Time index) {
                return LiftedFunction.this.function.apply(s.get(index));
            }
        };
    }
}
