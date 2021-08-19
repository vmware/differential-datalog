package org.dbsp.compute;

import org.dbsp.algebraic.IntegerTime;
import org.dbsp.algebraic.Time;
import org.dbsp.algebraic.TimeFactory;
import org.dbsp.types.IStream;

/**
 * The "identity stream" of integers, returning the values 0,1,2,...
 */
public class IdStream extends IStream<Integer> {
    public IdStream(TimeFactory timeFactory) {
        super(timeFactory);
    }

    @Override
    public Integer get(Time index) {
        return ((IntegerTime)index).value();
    }
}
