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
import com.vmware.ddlog.util.Linq;

import javax.annotation.Nullable;
import java.util.Arrays;
import java.util.List;

public class DDlogEApply extends DDlogExpression {
    private final String func;
    private final List<DDlogExpression> args;
    // If true the suffix notation with dot is preferred.
    private final boolean suffix;

    /**
     * Create a node that describes a function application.
     * @param node   Source object.
     * @param func   Function me to apply.
     * @param type   Type of result produced by the function.
     * @param suffix If true use the .func() notation for the function.
     * @param args   Function arguments.
     */
    public DDlogEApply(@Nullable Node node, String func, DDlogType type, boolean suffix, DDlogExpression... args) {
        super(node, type);
        this.func = func;
        this.suffix = suffix;
        this.args = Arrays.asList(args);
    }

    /**
     * Create a node that describes a function application.
     * @param node   Source object.
     * @param func   Function me to apply.
     * @param type   Type of result produced by the function.
     * @param args   Function arguments.
     */
    public DDlogEApply(@Nullable Node node, String func, DDlogType type, DDlogExpression... args) {
        this(node, func, type, false, args);
    }

    @Override
    public String toString() {
        if (this.suffix && this.args.size() > 0) {
            List<DDlogExpression> tail = this.args.subList(1, this.args.size());
            return this.args.get(0).toString() + "." + this.func + "(" +
                    String.join(", ", Linq.map(tail, DDlogExpression::toString)) + ")";
        } else {
            return this.func + "(" +
                    String.join(", ",
                            Linq.map(this.args, DDlogExpression::toString)) + ")";
        }
    }
}
