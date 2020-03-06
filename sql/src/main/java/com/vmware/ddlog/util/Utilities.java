package com.vmware.ddlog.util;

import java.util.HashSet;
import java.util.Set;

public class Utilities {
    public static Set<String> makeSet(String... data) {
        HashSet<String> result = new HashSet<String>();
        for (String s: data)
            result.add(s);
        return result;
     }
}
