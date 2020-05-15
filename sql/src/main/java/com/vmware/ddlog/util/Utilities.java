package com.vmware.ddlog.util;

import javax.annotation.Nullable;
import java.util.*;

public class Utilities {
    public static Set<String> makeSet(String... data) {
        return new HashSet<String>(Arrays.asList(data));
     }

     public static <T> List<T> concatenate(List<T>... lists) {
        ArrayList<T> result = new ArrayList<T>();
        for (List<T> l: lists)
            result.addAll(l);
        return result;
     }

     public static <T> Ternary canBeSame(@Nullable T left, @Nullable T right) {
        if (left == null && right != null)
            return Ternary.No;
         if (left != null && right == null)
             return Ternary.No;
         if (left == null && right == null)
             return Ternary.Yes;
         return Ternary.Maybe;
     }
}
