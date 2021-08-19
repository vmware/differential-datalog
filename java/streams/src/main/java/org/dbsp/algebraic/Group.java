package org.dbsp.algebraic;

import org.dbsp.types.IStream;

/**
 * Abstract interface implemented by a Group: a monoid where each element has an inverse.
 * @param <T>  Type that is used to represent the values in the group.
 */
public interface Group<T> extends Monoid<T> {
    /**
     * The inverse of value data in the group.
     * @param data  A value from the group.
     * @return  The inverse.
     */
    T minus(T data);

    /**
     * Constructs a stream of zero values.
     * @param f  A factory that knows how to create time indexes for the stream.
     * @return  A stream consisting entirely of zero values.
     */
    default IStream<T> zeroStream(TimeFactory f) {
        return new IStream<T>(f) {
            @Override
            public T get(Time index) {
                return Group.this.zero();
            }
        };
    }
}
