package com.vmware.ddlog.ir;

/**
 * This represents the type of an expression that cannot be inferred from the context
 * bottom-up.  For example, it is used for the type of SQL NULL.
 */
public class DDlogTUnknown extends DDlogType implements IDDlogBaseType {
    protected DDlogTUnknown(boolean mayBeNull) {
        super(mayBeNull);
    }

    @Override
    public String toString() {
        return "?";
    }

    @Override
    public DDlogType setMayBeNull(boolean mayBeNull) {
        if (this.mayBeNull == mayBeNull)
            return this;
        return new DDlogTUnknown(mayBeNull);
    }

    @Override
    public boolean compare(DDlogType type, IComparePolicy policy) {
        return true;
    }

    public static DDlogTUnknown instance = new DDlogTUnknown(true);
}
