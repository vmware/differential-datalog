/*
 * Copyright (c) 2018 VMware Inc. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.vmware.ddlog.util;

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.function.Function;
import java.util.function.Predicate;

/**
 * Some utility classes inspired by C# Linq.
 */
public class Linq {
    static class MapIterator<T, S> implements Iterator<S> {
        final Iterator<T> data;
        final Function<T, S> map;

        MapIterator(Iterator<T> data, Function<T, S> function) {
            this.data = data;
            this.map = function;
        }

        @Override
        public boolean hasNext() {
            return this.data.hasNext();
        }

        public S next() {
            T next = this.data.next();
            return this.map.apply(next);
        }
    }

    static class MapIterable<T, S> implements Iterable<S> {
        final MapIterator<T, S> mapIterator;

        MapIterable(Iterable<T> data, Function<T, S> function) {
            this.mapIterator = new MapIterator<T, S>(data.iterator(), function);
        }

        @Override
        public Iterator<S> iterator() {
            return this.mapIterator;
        }
    }

    public static <T, S> Iterable<S> map(Iterable<T> data, Function<T, S> function) {
        return new MapIterable<T, S>(data, function);
    }

    public static <T, S> Iterator<S> map(Iterator<T> data, Function<T, S> function) {
        return new MapIterator<T, S>(data, function);
    }

    public static <T, S> List<S> map(List<T> data, Function<T, S> function) {
        List<S> result = new ArrayList<S>(data.size());
        for (T aData : data)
            result.add(function.apply(aData));
        return result;
    }

    public static <T, S> List<S> flatMap(List<T> data, Function<T, List<S>> function) {
        List<S> result = new ArrayList<S>(data.size());
        for (T aData : data)
            result.addAll(function.apply(aData));
        return result;
    }

    public static <T, S> List<S> as(List<T> data, Class<S> sc) {
        return Linq.map(data, sc::cast);
    }

    public static <T, S> S[] map(T[] data, Function<T, S> function, Class<S> sc) {
        @SuppressWarnings("unchecked")
        S[] result = (S[])Array.newInstance(sc, data.length);
        for (int i=0; i < data.length; i++)
            result[i] = function.apply(data[i]);
        return result;
    }

    @SafeVarargs
    public static <T> List<T> list(T... data) {
        return Arrays.asList(data);
    }

    public static <T> List<T> where(List<T> data, Predicate<T> function) {
        List<T> result = new ArrayList<T>();
        for (T aData : data)
            if (function.test(aData))
                result.add(aData);
        return result;
    }

    @SuppressWarnings("unchecked")
    public static <T> T[] where(T[] data, Predicate<T> function) {
        List<T> result = new ArrayList<T>();
        for (T datum : data)
            if (function.test(datum))
                result.add(datum);
        return (T[]) result.toArray();
    }

    public static <T> boolean any(Iterable<T> data, Predicate<T> test) {
        for (T d: data)
            if (test.test(d)) {
                return true;
            }
        return false;
    }
}
