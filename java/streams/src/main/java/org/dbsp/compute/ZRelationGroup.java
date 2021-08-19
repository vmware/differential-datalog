package org.dbsp.compute;

import org.dbsp.algebraic.Group;

/**
 * The group structure that operates on Z-relations with elements of type T.
 * @param <T>  Type of elements in the Z-relations.
 */
public class ZRelationGroup<T> implements Group<ZRelation<T>> {
    @Override
    public ZRelation<T> minus(ZRelation<T> data) {
        return data.minus();
    }

    @Override
    public ZRelation<T> add(ZRelation<T> left, ZRelation<T> right) {
        return left.plus(right);
    }

    @Override
    public ZRelation<T> zero() {
        return new ZRelation<T>();
    }
}
