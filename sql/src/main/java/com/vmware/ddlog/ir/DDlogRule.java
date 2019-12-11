/*
 * Copyright (c) 2019 VMware Inc. All Rights Reserved.
 * SPDX-License-Identifier: MIT
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the "Software"), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice (including the next paragraph) shall be included in all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 */

package com.vmware.ddlog.ir;

import com.vmware.ddlog.util.Linq;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;

/**
 * The Haskell rule allows multiple values on the LHS,
 * but when translating SQL we never need more than 1.
 */
public class DDlogRule implements DDlogIRNode {
    private final DDlogAtom lhs;
    private final List<DDlogRuleRHS> rhs;
    @Nullable
    public DDlogType type;

    public DDlogRule(DDlogAtom lhs, List<DDlogRuleRHS> rhs) {
        this.lhs = this.checkNull(lhs);
        this.rhs = this.checkNull(rhs);
    }

    public DDlogRule(DDlogAtom lhs, DDlogRuleRHS rhs) {
        this(lhs, new ArrayList<DDlogRuleRHS>());
        this.rhs.add(this.checkNull(rhs));
    }

    @Override
    public String toString() {
        String result = this.lhs.toString();
        if (!this.rhs.isEmpty()) {
            result += " :- " + String.join(",",
                Linq.map(this.rhs, DDlogRuleRHS::toString));
        }
        result += ".";
        return result;
    }
}
