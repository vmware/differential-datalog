package com.vmware.ddlog.ir;

import com.facebook.presto.sql.tree.Node;

import javax.annotation.Nullable;

public class DDlogEFor extends DDlogExpression {
    private final String loopIter;
    private final DDlogExpression iter;
    private final DDlogExpression body;

    public DDlogEFor(@Nullable Node node, String loopIter, DDlogExpression iter, DDlogExpression body) {
        super(node, body.getType());
        this.loopIter = loopIter;
        this.iter = iter;
        this.body = body;
    }

    @Override
    public boolean compare(DDlogExpression val, IComparePolicy policy) {
        if (!super.compare(val, policy))
            return false;
        if (!val.is(DDlogEFor.class))
            return false;
        DDlogEFor other = val.to(DDlogEFor.class);
        if (!policy.compareLocal(this.loopIter, other.loopIter))
            return false;
        if (!this.iter.compare(other.iter, policy))
            return false;
        if (!this.body.compare(other.body, policy))
            return false;
        policy.exitScope(this.loopIter, other.loopIter);
        return true;
    }

    @Override
    public String toString() {
        return "for (" + this.loopIter + " in " + this.iter.toString() + ") {\n" +
                this.body.toString() + "}\n";
    }
}
