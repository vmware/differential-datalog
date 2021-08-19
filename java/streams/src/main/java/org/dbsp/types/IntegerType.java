package org.dbsp.types;

import org.dbsp.algebraic.Group;
import org.dbsp.compute.IntegerGroup;

public class IntegerType extends ScalarType<Integer> {
    @Override
    public Group<Integer> getGroup() {
        return IntegerGroup.instance;
    }

    public String toString() {
        return "Integer";
    }
}
