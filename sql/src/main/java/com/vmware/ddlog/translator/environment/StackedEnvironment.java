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

package com.vmware.ddlog.translator.environment;

import com.vmware.ddlog.ir.DDlogExpression;
import com.vmware.ddlog.util.IndentStringBuilder;

import javax.annotation.Nullable;

public class StackedEnvironment extends IEnvironment {
    final IEnvironment top;
    final IEnvironment bottom;

    public StackedEnvironment(IEnvironment top, IEnvironment bottom) {
        this.top = top;
        this.bottom = bottom;
    }

    @Override
    public IEnvironment cloneEnv() {
        return new StackedEnvironment(this.top.cloneEnv(), this.bottom.cloneEnv());
    }

    @Nullable
    @Override
    public DDlogExpression lookupIdentifier(String identifier) {
        DDlogExpression expr = this.top.lookupIdentifier(identifier);
        if (expr != null)
            return expr;
        return this.bottom.lookupIdentifier(identifier);
    }

    @Nullable
    @Override
    public IEnvironment lookupRelation(String identifier) {
        IEnvironment env = this.top.lookupRelation(identifier);
        if (env != null)
            return env;
        return this.bottom.lookupRelation(identifier);
    }

    @Override
    public IndentStringBuilder toString(IndentStringBuilder builder) {
        return builder.append("StackedEnvironment")
                .increase()
                .append(top)
                .newline()
                .append(bottom)
                .decrease();
    }
}
