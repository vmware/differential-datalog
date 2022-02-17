/*
 * Copyright (c) 2021 VMware, Inc.
 * SPDX-License-Identifier: MIT
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 *
 */

package com.vmware.ddlog.ir;

import com.facebook.presto.sql.tree.Node;
import com.vmware.ddlog.translator.TranslationException;
import com.vmware.ddlog.util.Linq;

import javax.annotation.Nullable;
import java.util.List;

public abstract class DDlogType extends DDlogNode {
    /**
     * True if this type may include null values.
     */
    public final boolean mayBeNull;

    protected DDlogType(@Nullable Node node, boolean mayBeNull) {
        super(node);
        this.mayBeNull = mayBeNull;
    }

    protected DDlogType(boolean mayBeNull) {
        super(null);
        this.mayBeNull = mayBeNull;
    }

    /**
     * True if the given type is a numeric type.
     * @param type  Type to analyze.
     */
    @SuppressWarnings("BooleanMethodIsAlwaysInverted")
    public static boolean isNumeric(DDlogType type) {
        return type instanceof IsNumericType;
    }

    String wrapOption(String type) {
        if (this.mayBeNull)
            return "Option<" + type + ">";
        return type;
    }

    /**
     * Given a set of similar types (which differ only in mayBeNull)
     * return a type with mayBeNull if any of them has mayBeNull.
     * Works only for base types.
     * @param types  List of types to reduce.
     */
    public static DDlogType reduceType(List<DDlogType> types) {
        if (types.isEmpty())
            return DDlogTTuple.emptyTupleType;
        DDlogType result = types.get(0);
        if (!result.isBaseType())
            throw new RuntimeException("Not a base type: " + result);
        for (int i = 1; i < types.size(); i++) {
            DDlogType ti = types.get(i);
            if (!ti.isBaseType())
                throw new RuntimeException("Not a base type: " + ti);
            DDlogType.checkCompatible(result, ti, false);
            if (result.is(DDlogTUnknown.class))
                result = ti.setMayBeNull(true);
            else if (ti.is(DDlogTUnknown.class))
                result = result.setMayBeNull(true);
            else if (ti.mayBeNull)
                result = ti;
        }
        return result;
    }

    /**
     * Given a set of types check that they are all the same.
     * Return one of them.
     */
    public static DDlogType sameType(List<DDlogType> types) {
        if (types.isEmpty())
            throw new RuntimeException("Empty set of types");
        DDlogType result = types.get(0);
        for (int i = 1; i < types.size(); i++) {
            DDlogType ti = types.get(i);
            if (!result.same(ti))
                throw new TranslationException("Incompatible types: " + result + " and " + ti, result.getNode());
        }
        return result;
    }

    public static DDlogType reduceType(DDlogType... types) {
        return reduceType(Linq.list(types));
    }

    public static void checkCompatible(DDlogType type0, DDlogType type1, boolean checkNullability) {
        if (checkNullability && type0.mayBeNull != type1.mayBeNull)
            type0.error("Types have different nullabilities: " + type0 + " and " + type1);
        if (!type0.is(DDlogTUnknown.class) && !type1.is(DDlogTUnknown.class)) {
            if (type0.getClass() != type1.getClass())
                type0.error("Incompatible types " + type0 + " and " + type1);
            if (type0.is(DDlogTBit.class)) {
                DDlogTBit tb0 = type0.to(DDlogTBit.class);
                DDlogTBit tb1 = type1.to(DDlogTBit.class);
                if (tb0.getWidth() != tb1.getWidth())
                    type0.error("Types have different widths " + type0 + " and " + type1);
            }
            if (type0.is(DDlogTSigned.class)) {
                DDlogTSigned tb0 = type0.to(DDlogTSigned.class);
                DDlogTSigned tb1 = type1.to(DDlogTSigned.class);
                if (tb0.getWidth() != tb1.getWidth())
                    type0.error("Types have different widths " + type0 + " and " + type1);
            }
        }
    }

    public boolean same(DDlogType other) {
        return this.mayBeNull == other.mayBeNull;
    }

    public static String typeName(String name) {
        return "T" + name;
    }

    public abstract String toString();

    public IsNumericType toNumeric() {
        return this.as(IsNumericType.class, "Expected as numeric type");
    }

    /**
     * Return a copy of this type with the mayBeNull bit set to the specified value.
     * @param mayBeNull  Value for the mayBeNull bit.
     */
    public abstract DDlogType setMayBeNull(boolean mayBeNull);

    /**
     * Get the None{} value of the option type corresponding to this type.
     */
    public DDlogExpression getNone(@Nullable Node node) {
        return new DDlogENull(node, this.setMayBeNull(true));
    }

    public boolean isBaseType() {
        return this.is(IDDlogBaseType.class);
    }
}
