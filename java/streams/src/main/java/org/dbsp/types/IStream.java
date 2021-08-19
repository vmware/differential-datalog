package org.dbsp.types;

import javafx.util.Pair;
import org.dbsp.algebraic.Group;
import org.dbsp.algebraic.Time;
import org.dbsp.algebraic.TimeFactory;
import org.dbsp.compute.StreamFunction;

/**
 * Generic stream API containing values of type V.
 * @param <V>  Type of values in stream.
 */
public abstract class IStream<V> {
    public abstract V get(Time index);
    final TimeFactory timeFactory;

    public IStream(TimeFactory timeFactory) {
        this.timeFactory = timeFactory;
    }

    public V get(int index) {
        return this.get(this.timeFactory.fromInteger(index));
    }

    /**
     * Method that adds two streams pointwise.
     * @param other  Stream to add to this one.
     * @param group  Group that knows how to perform element-wise addition.
     * @return       A stream that is the sum of this and other.
     */
    public IStream<V> add(IStream<V> other, Group<V> group) {
        return new IStream<V>(this.timeFactory) {
            @Override
            public V get(Time index) {
                V left = IStream.this.get(index);
                V right = other.get(index);
                return group.add(left, right);
            }
        };
    }

    /**
     * Method that negates a stream pointwise.
     * @param group  Group that knows how to negate an element.
     * @return   A stream that is the negation of this.
     */
    public IStream<V> negate(Group<V> group) {
        return new IStream<V>(this.timeFactory) {
            @Override
            public V get(Time index) {
                V left = IStream.this.get(index);
                return group.minus(left);
            }
        };
    }

    /**
     * Method that delays this stream.
     * @param group  Group that knows how to produce a zero.
     * @return   A stream that is the delayed version of this.
     */
    public IStream<V> delay(Group<V> group) {
        return new IStream<V>(this.timeFactory) {
            @Override
            public V get(Time index) {
                if (index.isZero()) {
                    return group.zero();
                }
                return IStream.this.get(index.previous());
            }
        };
    }

    /**
     * Method that computes the derivative of elements of a stream.
     * @param group  Group that knows how to perform stream arithmetic.
     * @return   A new stream that is the differential of the values in this.
     */
    public IStream<V> differentiate(Group<V> group) {
        return this.add(this.delay(group).negate(group), group);
    }

    /**
     * Method that cuts the current stream at a specified time moment.
     * @param at     Time moment to cut at.
     * @param group  Group that knows how to generate a zero value.
     * @return       A stream that is the cut version of this.
     */
    public IStream<V> cut(Time at, Group<V> group) {
        return new IStream<V>(this.timeFactory) {
            @Override
            public V get(Time index) {
                if (index.compareTo(at) >= 0)
                    return group.zero();
                return IStream.this.get(index);
            }
        };
    }

    public IStream<V> integrate(Group<V> group) {
        return new IStream<V>(this.timeFactory) {
            // Invariant is that accumulated is the integral of the input stream
            // up to lastIndex exclusively.
            Time lastIndex = this.timeFactory.zero();
            V accumulated = group.zero();

            @Override
            public V get(Time index) {
                // hopefully we are called with the lastIndex+1 value
                if (index.compareTo(lastIndex) > 0) {
                    this.lastIndex = this.timeFactory.zero();
                    this.accumulated = group.zero();
                }
                for (Time i = this.lastIndex; i.compareTo(index) <= 0; i = i.next())
                    this.accumulated = group.add(this.accumulated, IStream.this.get(i));
                this.lastIndex = index.next();
                return this.accumulated;
            }
        };
    }

    public String toString(Time limit) {
        StringBuilder builder = new StringBuilder();
        builder.append("[");
        for (Time i = this.timeFactory.zero(); i.compareTo(limit) < 0; i = i.next()) {
            if (!i.isZero())
                builder.append(",");
            builder.append(this.get(i).toString());
        }
        builder.append(",...");
        builder.append("]");
        return builder.toString();
    }

    public String toString(int limit) {
        return this.toString(this.timeFactory.fromInteger(limit));
    }

    public <S> IStream<S> apply(StreamFunction<V, S> function) {
        return function.apply(this);
    }

    public <S> IStream<Pair<V, S>> pair(IStream<S> other) {
        return new IStream<Pair<V, S>>(this.timeFactory) {
            @Override
            public Pair<V, S> get(Time index) {
                return new Pair<V, S>(IStream.this.get(index), other.get(index));
            }
        };
    }

    public V sumToZero(Group<V> group) {
        V zero = group.zero();
        V result = zero;
        for (Time index = this.timeFactory.zero(); ; index = index.next()) {
            V value = this.get(index);
            if (value == zero)
                break;
            result = group.add(result, value);
        }
        return result;
    }

    public TimeFactory getTimeFactory() {
        return this.timeFactory;
    }
}
