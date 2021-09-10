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

package org.dbsp.lib;

import org.dbsp.circuits.types.Type;

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
    public static void indent(int amount, StringBuilder builder) {
        for (int i = 0; i < amount; i++)
            builder.append(" ");
    }

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
