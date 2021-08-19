package org.dbsp.compute;

import org.dbsp.algebraic.Group;
import org.dbsp.types.IStream;

import java.util.function.BiFunction;

/**
 * A function that consumes two streams and produces another stream.
 * @param <T>  Type of elements in left stream.
 * @param <S>  Type of elements in right stream.
 * @param <R>  Type of elements in the result stream.
 */
public interface StreamBiFunction<T, S, R> extends BiFunction<IStream<T>, IStream<S>, IStream<R>> {
    /**
     * The incremental version of a stream function: a function that integrates
     * the inputs, applies this, and then differentiates the output.
     * @param groupT   Group used to integrate the left input.
     * @param groupS   Group used to integrate the right input.
     * @param groupR   Group used to differentiate outputs.
     * @return         The incremental version of this function.
     */
    default StreamBiFunction<T, S, R> inc(Group<T> groupT, Group<S> groupS, Group<R> groupR) {
        return (IStream<T> t, IStream<S> s) -> this.apply(t.integrate(groupT), s.integrate(groupS))
                .differentiate(groupR);
    }
}
