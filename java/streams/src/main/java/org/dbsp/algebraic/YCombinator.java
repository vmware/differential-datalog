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
 */

package org.dbsp.algebraic;

import java.util.function.Function;

/**
 * Standard Y combinator from lambda calculus to create recursive lambdas.
 * Example usage:
 * <pre>
 * {@code
 * Function<Integer,Integer> fac = Y(f -> n ->
 *       (n <= 1)
 *         ? 1
 *         : (n * f.apply(n - 1))
 *     );
 * }
 * </pre>
 */
public interface YCombinator {
    interface RecursiveFunction<F> extends Function<RecursiveFunction<F>, F> { }
    static <A,B> Function<A,B> Y(Function<Function<A,B>, Function<A,B>> f) {
        RecursiveFunction<Function<A,B>> r = w -> f.apply(x -> w.apply(w).apply(x));
        return r.apply(r);
    }
}
