package org.dbsp.algebraic;

/**
 * Time represented as a Java integer, which can overflow.
 */
public class IntegerTime implements Time {
    final int value;

    public IntegerTime(int value) {
        this.value = value;
    }

    public IntegerTime() {
        this(0);
    }

    @Override
    public boolean isZero() {
        return this.value == 0;
    }

    @Override
    public Time previous() {
        if (this.value == 0)
            throw new RuntimeException("Previous of zero time");
        return new IntegerTime(this.value - 1);
    }

    @Override
    public Time next() {
        if (this.value == Integer.MAX_VALUE)
            throw new RuntimeException("Next of max time value");
        return new IntegerTime(this.value + 1);
    }

    @Override
    public int compareTo(Time time) {
        return Integer.compare(this.value, ((IntegerTime)time).value);
    }

    @Override
    public int asInteger() {
        return this.value;
    }

    @Override
    public String toString() {
        return Integer.toString(this.value);
    }

    public Integer value() {
        return this.value;
    }

    public static class Factory implements TimeFactory {
        private Factory() {}

        public static final Factory instance = new Factory();

        @Override
        public Time zero() {
            return new IntegerTime();
        }

        @Override
        public Time fromInteger(int value) {
            return new IntegerTime(value);
        }
    }
}
