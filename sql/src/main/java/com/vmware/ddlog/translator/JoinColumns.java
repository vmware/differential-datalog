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

package com.vmware.ddlog.translator;

import com.facebook.presto.sql.tree.*;
import com.vmware.ddlog.ir.DDlogExpression;
import com.vmware.ddlog.ir.DDlogType;
import com.vmware.ddlog.translator.environment.EnvHandle;
import com.vmware.ddlog.translator.environment.IEnvironment;
import com.vmware.ddlog.util.Utilities;

import javax.annotation.Nullable;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Objects;

/**
 * This visitor inspects the predicate of a join to detect whether it is an equijoin.
 * If it is, it maintains a list of pairs of columns checked for equivalence.
 */
public class JoinColumns extends AstVisitor<Void, Void> {
    // Maps a column in the right table to the equal column in the left table.
    @Nullable
    protected HashMap<String, String> rightToLeftColumn;
    protected final HashSet<String> leftColumns;
    // Type of each column
    protected HashMap<String, DDlogType> leftColumnType;
    // For each column in the left table keep here the variable name used in the code to match it
    protected HashMap<String, String> leftColumnVariable;
    protected EnvHandle leftEnvironment;
    protected EnvHandle rightEnvironment;
    @Nullable
    protected Expression condition;

    public JoinColumns(TranslationContext leftContext, TranslationContext rightContext) {
        this.rightToLeftColumn = new HashMap<>();
        this.leftColumns = new HashSet<>();
        this.leftColumnVariable = new HashMap<>();
        this.leftEnvironment = leftContext.environment;
        this.rightEnvironment = rightContext.environment;
        this.leftColumnType = new HashMap<>();
        this.condition = null;
    }

    public boolean isEquiJoin() {
        return this.rightToLeftColumn != null;
    }

    public void analyze(Expression condition) {
        this.condition = condition;
        this.process(condition);
    }

    public Expression getCondition() {
        return Objects.requireNonNull(this.condition);
    }

    // Called when the join expression is not equivalent to an equijoin.
    protected Void setNotEquijoin() {
        this.rightToLeftColumn = null;
        return null;
    }

    @Override
    protected Void visitExpression(Expression node, Void context) {
        return this.setNotEquijoin();
    }

    public void addColumnPair(String left, String right) {
        Objects.requireNonNull(this.rightToLeftColumn).put(right, left);
        this.leftColumns.add(left);
    }

    @Override
    protected Void visitLogicalBinaryExpression(LogicalBinaryExpression node, Void context) {
        if (node.getOperator() != LogicalBinaryExpression.Operator.AND)
            return this.setNotEquijoin();
        this.process(node.getLeft(), context);
        if (this.isEquiJoin())
            this.process(node.getRight(), context);
        return null;
    }

    @Nullable
    public String findLeftVariable(String rightColumn) {
        if (!this.isEquiJoin())
            throw new RuntimeException("Not an equijoin");
        Objects.requireNonNull(this.rightToLeftColumn);
        String leftColumn = this.rightToLeftColumn.get(rightColumn);
        if (leftColumn == null)
            return null;
        return Objects.requireNonNull(this.leftColumnVariable.get(leftColumn));
    }

    @Nullable
    public String findLeftColumn(String rightColumn) {
        return Objects.requireNonNull(this.rightToLeftColumn).get(rightColumn);
    }

    public void setColumnVariable(String name, String varName, DDlogType type) {
        this.leftColumnVariable.put(name, varName);
        this.leftColumnType.put(name, type);
    }

    boolean isLeftColumn(String column) {
        return this.leftColumns.contains(column);
    }

    public DDlogType findLeftType(String rightColumn) {
        String leftColumn = Objects.requireNonNull(this.rightToLeftColumn).get(rightColumn);
        return Objects.requireNonNull(this.leftColumnType.get(leftColumn));
    }

    static class ColumnReference {
        @Nullable
        final String originalTableName;
        final String column;
        final Expression expression;

        ColumnReference(Expression expression) {
            this.expression = expression;
            if (expression instanceof DereferenceExpression) {
                DereferenceExpression dleft = (DereferenceExpression)expression;
                Identifier tbl = (Identifier)dleft.getBase();
                if (tbl == null)
                    throw new TranslationException("Unexpected column reference " + expression, expression);
                this.originalTableName = tbl.getValue();
                this.column = dleft.getField().getValue();
            } else if (expression instanceof Identifier) {
                this.originalTableName = null;
                this.column = ((Identifier)expression).getValue();
            } else {
                throw new TranslationException("Unexpected column reference " + expression, expression);
            }
        }

        @Override
        public String toString() {
            return this.expression.toString();
        }
    }

    /**
     * Find out of a column reference is to the left table.
     * @param column  Column reference.
     * @return        True if this refers to the left table, false if it refers to the right table.
     * Throw if it is ambiguous or refers to neither.
     */
    boolean isLeftTable(ColumnReference column) {
        if (column.originalTableName != null) {
            String tableName = column.originalTableName;
            IEnvironment scope = this.leftEnvironment.lookupRelation(tableName);
            if (scope != null) {
                DDlogExpression col = scope.lookupIdentifier(column.column);
                if (col == null)
                    throw new TranslationException("Column " + Utilities.singleQuote(column.column)
                            + " not present in " + Utilities.singleQuote(tableName),
                            column.expression);
                return true;
            }
            scope = this.rightEnvironment.lookupRelation(tableName);
            if (scope != null) {
                DDlogExpression col = scope.lookupIdentifier(column.column);
                if (col == null)
                    throw new TranslationException("Column " + Utilities.singleQuote(column.column) +
                            " not present in " + Utilities.singleQuote(tableName), column.expression);
                return false;
            }
            throw new TranslationException("Column " + Utilities.singleQuote(column.toString()) +
                    " not in one of the joined relation", column.expression);
        } else {
            // no table specified, try to guess
            boolean mayBeLeft = false;
            boolean mayBeRight = false;
            if (this.leftEnvironment.lookupIdentifier(column.column) != null)
                mayBeLeft = true;
            if (this.rightEnvironment.lookupIdentifier(column.column) != null)
                mayBeRight = true;
            if (mayBeLeft && mayBeRight)
                throw new TranslationException("Column name " + Utilities.singleQuote(column.toString()) +
                        " is ambiguous; could be in either joined table ", column.expression);
            if (mayBeLeft)
                return true;
            if (mayBeRight)
                return false;
            throw new TranslationException("Column name " + Utilities.singleQuote(column.toString()) +
                    " could not be found in either joined relation", column.expression);
        }
    }

    @Override
    protected Void visitComparisonExpression(ComparisonExpression node, Void context) {
        if (node.getOperator() != ComparisonExpression.Operator.EQUAL) {
            this.rightToLeftColumn = null;
            return null;
        }
        ColumnReference leftColumn = new ColumnReference(node.getLeft());
        ColumnReference rightColumn = new ColumnReference(node.getRight());
        boolean left = this.isLeftTable(leftColumn);
        boolean right = this.isLeftTable(rightColumn);
        if (left == right)
            throw new TranslationException(" Columns joined " + Utilities.singleQuote(leftColumn.toString()) +
                    " and " + Utilities.singleQuote(rightColumn.toString()) + " must be from different tables", node);
        if (right) {
            // need to swap
            this.addColumnPair(rightColumn.column, leftColumn.column);
        } else {
            this.addColumnPair(leftColumn.column, rightColumn.column);
        }
        return null;
    }
}
