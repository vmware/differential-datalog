package org.dbsp.algebraic;

public interface Time {
    /**
     * @return  True if this is the zero time.
     */
    boolean isZero();

    /**
     * @return Previous time if defined (throws if time is zero).
     */
    Time previous();

    /**
     * @return Next time.
     */
    Time next();

    /**
     * Compare two times.
     * @param time  Time to compare to on the right.
     * @return -1, 0, or 1 depending on how this compares with time.
     */
    int compareTo(Time time);

    /**
     * Compare two times for equality.
     * @param other  Time to compare with.
     * @return  True if the times are equal.
     */
    default boolean equals(Time other) {
        return this.compareTo(other) == 0;
    }

    /**
     * Current time as integer value; throws if this overflows.
     */
    int asInteger();
}
