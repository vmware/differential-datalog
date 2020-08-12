package com.vmware.ddlog.ir;

import com.facebook.presto.sql.tree.Node;

import javax.annotation.Nullable;

/**
 * Wrapper class for a DDlog vector type.
 */
public class DDlogTArray extends DDlogType {
    private final DDlogType elemType;

    public DDlogTArray(@Nullable Node node, DDlogType elemType, boolean mayBeNull) {
        super(node, mayBeNull);
        this.elemType = elemType;
    }

    @Override
    public String toString() {
        return this.wrapOption("Vec<" + this.elemType + ">");
    }

    @Override
    public DDlogType setMayBeNull(boolean mayBeNull) {
        return new DDlogTArray(this.getNode(), this.elemType, mayBeNull);
    }
}
