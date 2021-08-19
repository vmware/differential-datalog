package org.dbsp.circuits;

/**
 * A consumer expects values.
 * Unfortunately we can't make this type-safe.
 */
public interface Consumer {
    void receive(Object value);
}
