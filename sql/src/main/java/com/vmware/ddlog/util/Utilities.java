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

package com.vmware.ddlog.util;

import com.facebook.presto.sql.tree.QualifiedName;

import javax.annotation.Nullable;
import java.util.*;

public class Utilities {
    public static Set<String> makeSet(String... data) {
        return new HashSet<String>(Arrays.asList(data));
     }

     @SafeVarargs
     public static <T> List<T> concatenate(List<T>... lists) {
        ArrayList<T> result = new ArrayList<T>();
        for (List<T> l: lists)
            result.addAll(l);
        return result;
     }

    public static String convertQualifiedName(QualifiedName name) {
        return String.join(".", name.getParts());
    }

    /**
     * Given two references checks if they can be to equal objects.
     * @param left   Left reference
     * @param right  Right reference
     * @return       Yes if both objects are null, No if one of them is null,
     *               Maybe otherwise.
     */
     public static <T> Ternary canBeSame(@Nullable T left, @Nullable T right) {
         if (left == null && right == null)
             return Ternary.Yes;
         if (left == null || right == null)
             return Ternary.No;
         return Ternary.Maybe;
     }

     public static <K, V> void copyMap(Map<K, V> destination, Map<K, V> source) {
         destination.putAll(source);
     }

     public static String singleQuote(String other) {
         return "'" + other + "'";
     }

    /**
     * put something in a hashmap that is supposed to be new.
     * @param map    Map to insert in.
     * @param key    Key to insert in map.
     * @param value  Value to insert in map.
     */
    public static <K, V> void putNew(@Nullable Map<K, V> map, K key, V value) {
        V previous = Objects.requireNonNull(map).put(Objects.requireNonNull(key), Objects.requireNonNull(value));
        assert previous == null : "Key " + key + " already mapped to " + previous + " when adding " + value;
    }
}
