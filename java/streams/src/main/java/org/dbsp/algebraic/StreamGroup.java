package org.dbsp.algebraic;

import org.dbsp.algebraic.Group;
import org.dbsp.algebraic.TimeFactory;
import org.dbsp.types.IStream;

/**
 * The group structure induced by a stream where values belong to a group.
 * @param <T>  Values in the stream.
 */
public class StreamGroup<T> implements Group<IStream<T>> {
    final Group<T> group;
    final TimeFactory timeFactory;

    /**
     * Create a stream group from a group and a time factory.
     * @param group        Group that stream elements belong to.
     * @param timeFactory  Factory that knows how to create time indexes for stream.
     */
    public StreamGroup(Group<T> group, TimeFactory timeFactory) {
        this.group = group;
        this.timeFactory = timeFactory;
    }

    @Override
    public IStream<T> minus(IStream<T> data) {
        return data.negate(group);
    }

    @Override
    public IStream<T> add(IStream<T> left, IStream<T> right) {
        return left.add(right, group);
    }

    @Override
    public IStream<T> zero() {
        return this.group.zeroStream(this.timeFactory);
    }
}
