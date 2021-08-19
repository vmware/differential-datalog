package org.dbsp.types;

import org.dbsp.algebraic.Group;
import org.dbsp.algebraic.TimeFactory;
import org.dbsp.algebraic.StreamGroup;

/**
 * Type representing a stream of values of type T
 * @param <T> Concrete Java type implementing T.
 */
public class StreamType<T> implements Type<IStream<T>> {
    public final Type<T> baseType;
    protected final StreamGroup<T> group;

    /**
     * Create a StreamType having baseType as the element type.
     * @param baseType: type of elements in stream.
     */
    public StreamType(Type<T> baseType, TimeFactory factory) {
        this.baseType = baseType;
        this.group = new StreamGroup<T>(baseType.getGroup(), factory);
    }

    @Override
    public Group<IStream<T>> getGroup() {
        return this.group;
    }

    @Override
    public boolean isStream() {
        return true;
    }

    @Override
    public String toString() {
        return "S<" + baseType + ">";
    }
}
