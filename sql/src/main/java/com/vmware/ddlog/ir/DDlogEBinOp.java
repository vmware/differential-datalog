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

public class DDlogEBinOp extends DDlogExpression {
    public enum BOp {
        Eq, Neq, Lt, Gt, Lte, Gte, And, Or, Impl, Plus,
        Minus, Mod, Times, Div, ShiftR, ShiftL, BAnd, Bor, BXor, Concat;

        @Override
        public String toString() {
            switch (this) {
                case Eq:
                    return "==";
                case Neq:
                    return "!=";
                case Lt:
                    return "<";
                case Gt:
                    return ">";
                case Lte:
                    return "<=";
                case Gte:
                    return ">=";
                case And:
                    return "and";
                case Or:
                    return "or";
                case Impl:
                    return "=>";
                case Plus:
                    return "+";
                case Minus:
                    return "-";
                case Mod:
                    return "%";
                case Times:
                    return "*";
                case Div:
                    return "/";
                case ShiftR:
                    return ">>";
                case ShiftL:
                    return "<<";
                case BAnd:
                    return "&";
                case Bor:
                    return "|";
                case BXor:
                    return "^";
                case Concat:
                    return "++";
            }
            throw new RuntimeException("Unexpected operator");
        }
    }

    public DDlogEBinOp(BOp bop, DDlogExpression left, DDlogExpression right) {
        this.bop = bop;
        this.left = left;
        this.right = right;
    }

    final BOp bop;
    final DDlogExpression left;
    final DDlogExpression right;

    @Override
    public String toString() {
        return "(" + this.left.toString() + " " + this.bop.toString() +
                " " + this.right.toString() + ")";
    }
}
