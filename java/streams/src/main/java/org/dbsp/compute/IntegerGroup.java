package org.dbsp.compute;

import org.dbsp.algebraic.Group;

public class IntegerGroup implements Group<Integer> {
    private IntegerGroup() {}

    public static final IntegerGroup instance = new IntegerGroup();

    @Override
    public Integer minus(Integer data) {
        return -data;
    }

    @Override
    public Integer add(Integer left, Integer right) {
        return left + right;
    }

    @Override
    public Integer zero() {
        return 0;
    }
}
