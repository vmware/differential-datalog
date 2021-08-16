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

    public DDlogEApply(@Nullable Node node, String func, DDlogType type, DDlogExpression... args) {
        super(node, type);
        this.func = func;
        this.args = Arrays.asList(args);
    }

    @Override
    public String toString() {
        return this.func + "(" +
                String.join(", ",
                        Linq.map(this.args, DDlogExpression::toString)) + ")";
    }

    @Override
    public boolean compare(DDlogExpression val, IComparePolicy policy) {
        if (!super.compare(val, policy))
            return false;
        if (!val.is(DDlogEApply.class))
            return false;
        DDlogEApply other = val.to(DDlogEApply.class);
        if (!this.func.equals(other.func))
            return false;
        if (this.args.size() != other.args.size())
            return false;
        for (int i = 0; i < this.args.size(); i++) {
            if (this.args.get(i).compare(other.args.get(i), policy))
                return false;
        }
        return true;
    }
}
