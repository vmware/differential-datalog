package com.vmware.ddlog.ir;

/**
 * An expression which stands for a NULL in SQL.
 */
public class DDlogENull extends DDlogExpression {
    public DDlogENull() {
        super(DDlogTUnknown.instance);
    }

    public DDlogENull(DDlogType type) {
        super(type);
    }

    @Override
    public String toString() {
        if (this.getType().is(DDlogTUnknown.class))
            return "None{}";
        return "None{}: " + this.type;
    }
}
