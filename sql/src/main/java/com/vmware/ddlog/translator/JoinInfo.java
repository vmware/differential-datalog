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
import com.vmware.ddlog.ir.DDlogEVar;
import com.vmware.ddlog.ir.DDlogEVarDecl;
import com.vmware.ddlog.ir.DDlogExpression;
import com.vmware.ddlog.translator.environment.EnvHandle;
import com.vmware.ddlog.translator.environment.IEnvironment;
import com.vmware.ddlog.util.Utilities;

import javax.annotation.Nullable;
import java.util.*;

/**
 * This visitor inspects the predicate of a join to detect whether it is an equijoin.
 * If it is, it maintains information about the columns that participate in the join.
 * The values below are for the example in TranslationVisitor.visitJoin.
 */
public class JoinInfo extends AstVisitor<Void, Void> {
    /**
     * Maps a column in the right table to the equal column in the left table.
     * Contents: c1 -> c1, c2 -> c3.
     * If this is null the join is not an equijoin.
     */
    @Nullable
    protected Map<String, String> rightToLeftColumn;
    /**
     * Maps a column name in the left table to the equal column in the right table.
     * c1 -> c1, c3 -> c2.
     */
    protected final Map<String, String> leftToRightColumn;
    /**
     * The left table columns that participate in the join.
     * Contents: c1, c3.
     */
    protected final Set<String> joinedLeftColumns;
    /**
     * For each column in the left table keep here the variable name used in the code to match it.
     * c1 -> c1, c2 -> c2, c3 -> c3.
     */
    protected final Map<String, DDlogEVar> leftColumnVariable;
    /**
     * For each column in the right table keep here the variable used in the code to match it.
     * c1 -> c1, c2 -> c3, c4 -> c4.
     */
    protected final Map<String, DDlogEVar> rightColumnVariable;
    /**
     * For each right column the name used in the result.
     * c1 -> c10, c2 -> c3, c4 -> c4.
     */
    protected final Map<String, String> renamedRightColumn;
    protected final EnvHandle leftEnvironment;
    protected final EnvHandle rightEnvironment;
    @Nullable
    protected Expression condition;
    /**
     * Maps the renamed column name to the original name for the right table.
     * c3 -> c2, c4 -> c4, c10 -> c1.
     */
    protected final HashMap<String, String> originalRightColumnName;

    public JoinInfo(TranslationContext leftContext, TranslationContext rightContext) {
        this.rightToLeftColumn = new HashMap<>();
        this.leftToRightColumn = new HashMap<>();
        this.joinedLeftColumns = new HashSet<>();
        this.leftColumnVariable = new HashMap<>();
        this.rightColumnVariable = new HashMap<>();
        this.renamedRightColumn = new HashMap<>();
        this.originalRightColumnName = new HashMap<>();
        this.leftEnvironment = leftContext.environment;
        this.rightEnvironment = rightContext.environment;
        this.condition = null;
    }

    /**
     * True if the analyzed join is an equijoin.
     */
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

    /**
     * Called when the join expression is not equivalent to an equijoin.
     */
    protected Void setNotEquijoin() {
        this.rightToLeftColumn = null;
        return null;
    }

    @Override
    protected Void visitExpression(Expression node, Void context) {
        return this.setNotEquijoin();
    }

    public void addColumnPair(String left, String right) {
        Utilities.putNew(this.rightToLeftColumn, right, left);
        Utilities.putNew(this.leftToRightColumn, left, right);
        this.joinedLeftColumns.add(left);
    }

    /**
     * Set the new rightColumnName for a column from the right table.
     * @param rightColumnName     Original right column name.
     * @param resultName          Name used in the result structure.
     */
    public void setRightColumnNameInResult(String rightColumnName, String resultName) {
        Objects.requireNonNull(rightColumnName);
        Objects.requireNonNull(resultName);
        Utilities.putNew(this.renamedRightColumn, rightColumnName, resultName);
        Utilities.putNew(this.originalRightColumnName, resultName, rightColumnName);
    }

    public String getOriginalRightColumnName(String resultColumnName) {
        return Objects.requireNonNull(this.originalRightColumnName.get(resultColumnName));
    }

    public Map<String, String> getRenameMap() {
        return this.renamedRightColumn;
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

    /**
     * Given the name of a column in the right table, find the corresponding
     * variable in the left column if the column is being joined on.
     * @param rightColumn  Right column name.
     * @return  Null if the column is not being joined on.
     */
    @Nullable
    public DDlogEVar findLeftVariableFromRightColumn(String rightColumn) {
        if (!this.isEquiJoin())
            throw new RuntimeException("Not an equijoin");
        Objects.requireNonNull(this.rightToLeftColumn);
        String leftColumn = this.rightToLeftColumn.get(rightColumn);
        if (leftColumn == null)
            return null;
        return this.findLeftVariable(leftColumn);
    }

    /**
     * Given a column in the left table find the variable associated in the code.
     * @param leftColumn  Name of left column.
     */
    public DDlogEVar findLeftVariable(String leftColumn) {
        return Objects.requireNonNull(this.leftColumnVariable.get(leftColumn));
    }

    /**
     * Given a column in the right table find the variable used in the code
     * corresponding to this column.
     * @param rightColumn  Column name.
     */
    public DDlogEVar findRightVariable(String rightColumn) {
        return Objects.requireNonNull(this.rightColumnVariable.get(rightColumn));
    }

    /**
     * Remember the variable used in the code for matching a left column.
     * @param name    Let table column name.
     * @param variable    Associated variable in the generated code.
     */
    public void setLeftColumnVariable(String name, DDlogEVar variable) {
        Utilities.putNew(this.leftColumnVariable, name, variable);
    }

    /**
     * Remember the variable used in the code for matching a left column.
     * @param name    Let table column name.
     * @param variable Variable used the generatedd code.
     */
    public void setRightColumnVariable(String name, DDlogEVar variable) {
        Utilities.putNew(this.rightColumnVariable, name, variable);
    }

    /**
     * True if this column is one of the equijoin columns in the left table.
     * @param column  Column name.
     */
    boolean isJoinedLeftColumn(String column) {
        if (this.rightToLeftColumn == null)
            return false; // not an equijoin
        return this.joinedLeftColumns.contains(column);
    }

    /**
     * True if this column is one of the equijoin columns from the right table.
     * @param column  Column name.
     */
    boolean isJoinedRightColumn(String column) {
        if (this.rightToLeftColumn == null)
            return false; // not an equijoin
        return this.rightToLeftColumn.containsKey(column);
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
