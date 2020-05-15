package com.vmware.ddlog.ir;

public class Identical implements IComparePolicy {
    @Override
    public boolean compareRelation(String relation, String other) {
        return relation.equals(other);
    }

    @Override
    public boolean compareLocal(String varName, String other) {
        return varName.equals(other);
    }

    @Override
    public void exitScope(String varName, String other) {
    }

    @Override
    public boolean compareIdentifier(String var, String other) {
        return var.equals(other);
    }
}
