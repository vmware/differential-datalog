package org.dbsp.compute;

import org.dbsp.algebraic.Time;
import org.dbsp.types.IStream;

/**
 * A stream where each element is a stream itself.
 * @param <T>  Type of elements in the bottom stream structure.
 */
public class Stream2D<T> extends IStream<IStream<T>> {
    private final IStream<IStream<T>> data;

    public Stream2D(IStream<IStream<T>> data) {
        super(data.getTimeFactory());
        this.data = data;
    }

    @Override
    public IStream<T> get(Time index) {
        return this.data.get(index);
    }

    String toString(int limit0, int limit1) {
        StringBuilder builder = new StringBuilder();
        for (int i = 0; i < limit0; i++) {
            IStream<T> s = this.get(i);
            String str = s.toString(limit1);
            builder.append(str);
            builder.append("\n");
        }
        return builder.toString();
    }
}
