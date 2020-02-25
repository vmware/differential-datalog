/*
 * Copyright (c) 2019 VMware Inc. All Rights Reserved.
 * SPDX-License-Identifier: MIT
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the "Software"), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice (including the next paragraph) shall be included in all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 */

package com.vmware.ddlog.ir;

import com.vmware.ddlog.util.Linq;

import java.util.List;

public abstract class DDlogType implements DDlogIRNode {
    /**
     * True if this type may include null values.
     */
    public final boolean mayBeNull;

    protected DDlogType(boolean mayBeNull) {
        this.mayBeNull = mayBeNull;
    }

    /**
     * True if the given type is a numeric type.
     * @param type  Type to analyze.
     */
    @SuppressWarnings("BooleanMethodIsAlwaysInverted")
    static boolean isNumeric(DDlogType type) {
        return type instanceof DDlogTBit ||
                type instanceof DDlogTSigned ||
                type instanceof DDlogTInt;
    }

    String wrapOption(String type) {
        if (this.mayBeNull)
            return "Option<" + type + ">";
        return type;
    }

    /**
     * Given a set of similar types (which differ only in mayBeNull)
     * return a type with mayBeNull if any of them has mayBeNull.
     * @param types  List of types to reduce.
     */
    public static DDlogType reduceType(List<DDlogType> types) {
        if (types.isEmpty())
            return DDlogTTuple.emptyTupleType;
        DDlogType result = types.get(0);
        for (int i = 1; i < types.size(); i++) {
            DDlogType.checkCompatible(result, types.get(i));
            if (types.get(i).mayBeNull)
                result = types.get(i);
        }
        return result;
    }

    public static DDlogType reduceType(DDlogType left, DDlogType right) {
        return reduceType(Linq.list(left, right));
    }

    static void checkCompatible(DDlogType type0, DDlogType type1) {
        // TODO: refine this.
        if (type0.getClass() != type1.getClass())
            throw new RuntimeException("Incompatible types " + type0 + " and " + type1);
    }

    public static String typeName(String name) {
        return "T" + name;
    }

    public abstract String toString();

    /**
     * Return a copy of this type with the mayBeNull bit set to the specified value.
     * @param mayBeNull  Value for the mayBeNull bit.
     */
    public abstract DDlogType setMayBeNull(boolean mayBeNull);

    /**
     * Get the None{} value of the option type corresponding to this type.
     */
    public DDlogExpression getNone() {
        return new DDlogEStruct("None", this.setMayBeNull(true));
    }
}
