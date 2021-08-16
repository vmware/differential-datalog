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

import java.util.List;

/**
 * This class helps generating nicely indented code.
 */
public class CodeFormatter {
    private final StringBuilder builder;
    private int indent = 0;
    private final int indentAmount = 2;

    public CodeFormatter() {
        builder = new StringBuilder();
    }

    public CodeFormatter append(CodeFormatter other) {
        this.builder.append(other.builder);
        return other;
    }

    public CodeFormatter indent() {
        this.indent += this.indentAmount;
        return this;
    }

    public CodeFormatter unindent() {
        if (this.indent < this.indentAmount)
            throw new RuntimeException("Negative indentation requested");
        this.indent -= this.indentAmount;
        return this;
    }

    public CodeFormatter append(String str) {
        this.builder.append(str);
        return this;
    }

    public String toString() {
        return this.builder.toString();
    }

    public CodeFormatter space() {
        return this.append(" ");
    }

    public CodeFormatter parens(DDlogIRNode node) {
        CodeFormatter result = this.append("(");
        return node.format(result).append(")");
    }

    public CodeFormatter block(DDlogIRNode node) {
        CodeFormatter result = this.append("{").newline().indent();
        result = node.format(result);
        return result.unindent().newline().append("}");
    }

    private CodeFormatter newline() {
        return this.append("\n").appendIndent();
    }

    private CodeFormatter appendIndent() {
        for (int i = 0; i < this.indent; i++)
            this.append(" ");
        return this;
    }

    public CodeFormatter commaSep(List<DDlogIRNode> nodes) {
        CodeFormatter result = this;
        boolean first = true;
        for (DDlogIRNode node: nodes) {
            if (!first)
                this.append(", ");
            first = false;
            result = node.format(result);
        }
        return result;
    }
}

