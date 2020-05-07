package com.vmware.ddlog.ir;

import com.facebook.presto.sql.tree.Node;

import javax.annotation.Nullable;

/**
 * An expression which stands for a NULL in SQL.
 */
public class DDlogENull extends DDlogExpression {
    public DDlogENull(@Nullable Node node) {
        super(node, DDlogTUnknown.instance);
    }

    public DDlogENull(@Nullable Node node, DDlogType type) {
        super(node, type);
    }

    @Override
    public String toString() {
        if (this.getType().is(DDlogTUnknown.class))
            return "None{}";
        return "None{}: " + this.type;
    }
}
