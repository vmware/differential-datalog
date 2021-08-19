package org.dbsp.formal;

import org.dbsp.algebraic.TimeFactory;
import org.dbsp.types.IStream;
import org.dbsp.compute.LiftedBifunction;
import org.dbsp.types.StreamType;
import org.dbsp.types.Type;

import java.util.function.BiFunction;

/**
 * An operator that has a two inputs of the same type.
 * @param <T> Concrete Java implementation of input type.
 * @param <S> Concrete Java implementation of output type.
 */
public abstract class BinaryOperator<T, S> {
    public final Type<T> inputType;
    public final Type<S> outputType;

    protected BinaryOperator(Type<T> inputType, Type<S> outputType) {
        this.inputType = inputType;
        this.outputType = outputType;
    }

    public Type<S> getOutputType() {
        return this.outputType;
    }

    public Type<T> getInputType(int input) {
        return this.inputType;
    }

    /**
     * The code that performs the computation of this operator.
     */
    public abstract BiFunction<T, T, S> getComputation();

    /**
     * @return A lifted version of this operator.
     */
    public <F extends TimeFactory> BinaryOperator<IStream<T>, IStream<S>> lift(F factory) {
        return new BinaryOperator<IStream<T>, IStream<S>>(
                new StreamType<T>(this.inputType, factory),
                new StreamType<S>(this.outputType, factory)) {
            @Override
            public BiFunction<IStream<T>, IStream<T>, IStream<S>> getComputation() {
                return new LiftedBifunction<T, T, S>(BinaryOperator.this.getComputation());
            }

            @Override
            public String toString() {
                return "^" + BinaryOperator.this.toString();
            }
        };
    }
}
