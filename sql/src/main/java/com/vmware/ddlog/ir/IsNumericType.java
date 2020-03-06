package com.vmware.ddlog.ir;

/**
 * Interface implemented by numeric types.
 */
public interface IsNumericType {
    /**
     * The zero of the numeric type.
     */
    DDlogExpression zero();
    String simpleName();
}
