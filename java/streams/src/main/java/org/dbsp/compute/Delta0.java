package org.dbsp.compute;

import org.dbsp.algebraic.Group;
import org.dbsp.algebraic.Time;
import org.dbsp.algebraic.TimeFactory;
import org.dbsp.types.IStream;

public class Delta0<T> extends IStream<T> {
    final T value;
    final Group<T> group;

    public Delta0(T value, Group<T> group, TimeFactory factory) {
        super(factory);
        this.value = value;
        this.group = group;
    }

    public T get(Time index) {
        if (index.isZero())
            return this.value;
        return this.group.zero();
    }
}
