package com.vmware.ddlog.ir;

import com.facebook.presto.sql.tree.Node;
import com.vmware.ddlog.util.Linq;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;

public abstract class DDlogNode implements DDlogIRNode {
    protected final List<DDlogComment> comments;
    @Nullable
    protected final Node node;

    protected DDlogNode(@Nullable Node node) {
        this.node = node;
        this.comments = new ArrayList<DDlogComment>();
    }

    @Nullable
    public Node getNode() { return this.node; }

    public void addComment(DDlogComment comment) {
        this.comments.add(comment);
    }

    public String comments() {
        return String.join("\n", Linq.map(this.comments, DDlogComment::toString));
    }
}
