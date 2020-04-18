package com.vmware.ddlog.ir;

/**
 * Interface implemented by numeric types.
 */
public interface IsNumericType {
    /**
     * The zero of the numeric type.
     */
    DDlogExpression zero();
    /**
     * The one of the numeric type.
     */
    DDlogExpression one();
    String simpleName();
}
