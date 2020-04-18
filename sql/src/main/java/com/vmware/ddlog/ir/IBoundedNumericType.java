package com.vmware.ddlog.ir;

/**
 * A numeric type with bounded width.
 */
public interface IBoundedNumericType extends DDlogIRNode {
    int getWidth();

    /**
     * Get another type of the same kind with the specified width.
     * @param width width in bits.
     */
    IBoundedNumericType getWithWidth(int width);
}
