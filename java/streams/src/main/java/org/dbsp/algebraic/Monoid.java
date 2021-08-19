package org.dbsp.algebraic;

public interface Monoid<T> {
    T add(T left, T right);
    T zero();
}
