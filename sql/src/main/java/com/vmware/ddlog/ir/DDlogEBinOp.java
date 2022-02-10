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

import javax.annotation.Nullable;

public class DDlogEBinOp extends DDlogExpression {
    public enum BOp {
        Eq, Neq, Lt, Gt, Lte, Gte, And, Or, Impl, Plus,
        Minus, Mod, Times, Div, ShiftR, ShiftL, BAnd, BOr, BXor, Concat;

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
                case BOr:
                    return "|";
                case BXor:
                    return "^";
                case Concat:
                    return "++";
            }
            throw new RuntimeException("Unexpected operator");
        }

        public boolean isComparison() {
            switch (this) {
                case Eq:
                case Neq:
                case Lt:
                case Gt:
                case Lte:
                case Gte:
                    return true;
                default:
                    return false;
            }
        }

        public boolean isBoolean() {
            switch (this) {
                case Or:
                case And:
                case Impl:
                    return true;
                default:
                    return false;
            }
        }
    }

    private final BOp bop;
    private final DDlogExpression left;
    private final DDlogExpression right;

    public DDlogEBinOp(@Nullable Node node, BOp bop, DDlogExpression left, DDlogExpression right) {
        super(node);
        this.bop = bop;
        boolean mayBeNull = left.getType().mayBeNull || right.getType().mayBeNull;
        switch (this.bop) {
            case Eq:
            case Neq:
                this.type = DDlogTBool.instance.setMayBeNull(mayBeNull);
                DDlogType.checkCompatible(left.getType(), right.getType(), false);
                break;
            case Lt:
            case Gt:
            case Lte:
            case Gte:
                if (!DDlogType.isNumeric(left.getType()))
                    this.error(this.bop + " is not applied to numeric type: " + left.getType());
                if (!DDlogType.isNumeric(right.getType()))
                    this.error(this.bop + " is not applied to numeric type: " + right.getType());
                this.type = DDlogTBool.instance.setMayBeNull(mayBeNull);
                DDlogType.checkCompatible(left.getType(), right.getType(), false);
                break;
            case And:
            case Or:
            case Impl:
                if (!(left.getType() instanceof DDlogTBool))
                    this.error(this.bop + " is not applied to Boolean type: " + left.getType());
                if (!(right.getType() instanceof DDlogTBool))
                    this.error(this.bop + " is not applied to Boolean type: " + right.getType());
                this.type = DDlogType.reduceType(left.getType(), right.getType());
                break;
            case Plus:
            case Minus:
            case Mod:
            case Times:
            case Div:
            case ShiftR:
            case ShiftL:
            case BAnd:
            case BOr:
            case BXor:
                if (!DDlogType.isNumeric(left.getType()))
                    this.error(this.bop + " is not applied to numeric type: " + left.getType());
                if (!DDlogType.isNumeric(right.getType()))
                    this.error(this.bop + " is not applied to numeric type: " + right.getType());
                this.type = DDlogType.reduceType(left.getType(), right.getType());
                break;
            case Concat:
                if ((left.getType() instanceof DDlogTIString))
                    left = DDlogTIString.ival(left);
                else if ((left.getType() instanceof DDlogTString))
                    this.error(this.bop + " is not applied to (i)string type: " + left.getType());
                if ((right.getType() instanceof DDlogTIString))
                    right = DDlogTIString.ival(right);
                else if (!(right.getType() instanceof DDlogTString))
                    this.error(this.bop + " is not applied to (i)string type: " + right.getType());
                this.type = DDlogType.reduceType(left.getType(), right.getType());
                break;
        }
        this.left = this.checkNull(left);
        this.right = this.checkNull(right);
    }

    @Override
    public String toString() {
        return "(" + this.left + " " + this.bop +
                " " + this.right + ")";
    }

}
