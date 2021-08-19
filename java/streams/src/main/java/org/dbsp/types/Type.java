package org.dbsp.types;

import org.dbsp.algebraic.Group;

/**
 * Base class for all types.
 * @param <T> concrete Java T implementing this type.
 */
public interface Type<T> {
    /**
     * @return The group that knows how to perform operations on values of this type.
     */
    Group<T> getGroup();

    /**
     * @return True if this is a stream type.
     */
    boolean isStream();
}
