package org.dbsp.compute;

import org.dbsp.algebraic.Group;
import org.dbsp.types.IStream;

import java.util.function.Function;

/**
 * A stream function consumes a stream and produces a stream.
 * @param <T>  Type of elements in the input stream.
 * @param <S>  Type of elements in the output stream.
 */
public interface StreamFunction<T, S> extends Function<IStream<T>, IStream<S>> {
    /**
     * Composition of two stream functions: this function is applied after 'before'.
     * @param before  Function to apply first.
     * @param <V>     Type of results consumed by the before function.
     * @return        A function that is the composition of before and this.
     */
    default <V> StreamFunction<V, S> composeStream(StreamFunction<V, T> before) {
        return (IStream<V> v) -> this.apply(before.apply(v));
    }

    /**
     * The incremental version of a stream function: a function that integrates
     * the inputs, applies this, and then differentiates the output.
     * @param groupT   Group used to integrate inputs.
     * @param groupS   Group used to differentiate outputs.
     * @return         The incremental version of this function.
     */
    default StreamFunction<T, S> inc(Group<T> groupT, Group<S> groupS) {
        return (IStream<T> t) -> this.apply(t.integrate(groupT)).differentiate(groupS);
    }
}
