package com.vmware.ddlog.ir;

import com.facebook.presto.sql.tree.Node;

import javax.annotation.Nullable;

public abstract class DDlogNode implements DDlogIRNode {
    @Nullable
    protected final Node node;

    protected DDlogNode(@Nullable Node node) {
        this.node = node;
    }

    @Nullable
    public Node getNode() { return this.node; }
}
