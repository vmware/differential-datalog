package org.dbsp.compute;

import org.dbsp.algebraic.Time;
import org.dbsp.types.IStream;

import java.util.function.BiFunction;

/**
 * A lifted binary function lifts a binary function from T,S to U to
 * operate on streams of corresponding types.
 * @param <T>  Type of left input.
 * @param <S>  Type of right input.
 * @param <U>  Type of result.
 */
public class LiftedBifunction<T, S, U> implements StreamBiFunction<T, S, U> {
    final BiFunction<T, S, U> function;

    public LiftedBifunction(BiFunction<T, S, U> function) {
        this.function = function;
    }

    @Override
    public IStream<U> apply(IStream<T> left, IStream<S> right) {
        return new IStream<U>(left.getTimeFactory()) {
            @Override
            public U get(Time index) {
                return LiftedBifunction.this.function.apply(left.get(index), right.get(index));
            }
        };
    }
}
