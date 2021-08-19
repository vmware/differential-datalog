package org.dbsp.algebraic;

import java.math.BigInteger;

/**
 * Representation for time using unbounded integers.
 * We could switch this to 'int' for some cases.
 */
public class BigTime implements Time {
    public final BigInteger value;
    static final BigInteger zero = BigInteger.valueOf(0);
    static final BigTime zeroTime = new BigTime(zero);
    static final BigInteger one = BigInteger.valueOf(1);

    public BigTime(BigInteger value) {
        this.value = value;
    }

    public BigTime(int value) {
        this.value = BigInteger.valueOf(value);
    }

    public BigTime() {
        this(0);
    }

    public Time zero() {
        return BigTime.zeroTime;
    }

    public boolean isZero() {
        return this.value.equals(zero);
    }

    public Time previous() {
        if (this.isZero())
            throw new RuntimeException("Previous of time 0");
        return new BigTime(this.value.subtract(one));
    }

    public Time next() {
        return new BigTime(this.value.add(one));
    }

    @Override
    public int compareTo(Time other) {
        return this.value.compareTo(((BigTime)other).value);
    }

    @Override
    public int asInteger() {
        return this.value.intValueExact();
    }

    @Override
    public String toString() {
        return this.value.toString();
    }

    public static class Factory implements TimeFactory {
        private Factory() {}

        public static Factory instance = new Factory();

        @Override
        public Time zero() {
            return new BigTime();
        }

        @Override
        public Time fromInteger(int value) {
            return new BigTime(BigInteger.valueOf(value));
        }
    }
}
