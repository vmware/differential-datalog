package org.dbsp.types;

/**
 * Types that are scalars, e.g., integers, booleans, multisets.
 */
public abstract class ScalarType<T> implements Type<T> {
    public boolean isStream() { return false; }
}
