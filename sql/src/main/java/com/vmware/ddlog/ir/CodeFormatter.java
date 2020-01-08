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

