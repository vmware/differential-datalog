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

public class DDlogEUnOp extends DDlogExpression {
    public enum UOp {
        Not, BNeg, UMinus;

        @Override
        public String toString() {
            switch (this) {
                case Not:
                    return "not";
                case BNeg:
                    return "~";
                case UMinus:
                    return "-";
            }
            throw new RuntimeException("Unexpected operator");
        }
    }

    public DDlogEUnOp(UOp uop, DDlogExpression expr) {
        this.uop = uop;
        this.expr = this.checkNull(expr);
        switch (this.uop) {
            case Not:
                if (!(expr.type instanceof DDlogTBool))
                    throw new RuntimeException("Not is not applied to Boolean type");
                this.type = DDlogTBool.instance;
            case BNeg:
            case UMinus:
                if (!DDlogType.isNumeric(expr.getType()))
                    throw new RuntimeException(this.uop + " is not applied to numeric type");
                this.type = expr.type;
        }
    }

    private final UOp uop;
    private final DDlogExpression expr;

    @Override
    public String toString() {
        return "(" + this.uop.toString() +
                " " + this.expr.toString() + ")";
    }
}
