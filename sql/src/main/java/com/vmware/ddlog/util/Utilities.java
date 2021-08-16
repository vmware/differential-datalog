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
         for (Map.Entry<K, V> e: source.entrySet())
             destination.put(e.getKey(), e.getValue());
     }
}
