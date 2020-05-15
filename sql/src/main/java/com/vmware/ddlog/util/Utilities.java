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
