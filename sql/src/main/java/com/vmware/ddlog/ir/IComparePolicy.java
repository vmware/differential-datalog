package com.vmware.ddlog.ir;

/**
 * Interface that knows how to compare some program elements for
 * equivalence.
 */
public interface IComparePolicy {
    /**
     * Are these two relations the same relation?
     */
    boolean compareRelation(String relation, String other);
    /**
     * Introduces a new local variable and compare it.
     */
    boolean compareLocal(String varName, String other);
    /**
     * These variables move out of scope.
     */
    void exitScope(String varName, String other);
    /**
     * Compare these two local identifiers
     */
    boolean compareIdentifier(String var, String other);
}
