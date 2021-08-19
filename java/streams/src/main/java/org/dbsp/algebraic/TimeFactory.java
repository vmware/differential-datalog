package org.dbsp.algebraic;

/**
 * A class that knows how to create a fresh instance of Time.
 */
public interface TimeFactory {
    Time zero();
    Time fromInteger(int value);
}
