/*
 * Copyright (c) 2018-2021 VMware, Inc.
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
 */

package com.vmware.ddlog.util.sql;

import org.apache.calcite.schema.ColumnStrategy;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.ddl.SqlColumnDeclaration;
import org.apache.calcite.sql.ddl.SqlCreateTable;
import org.apache.calcite.sql.ddl.SqlCreateView;
import org.apache.calcite.sql.ddl.SqlKeyConstraint;
import org.apache.calcite.sql.parser.SqlAbstractParserImpl;
import org.apache.calcite.sql.util.SqlBasicVisitor;

import java.util.ArrayList;
import java.util.List;

import static com.vmware.ddlog.util.sql.CalciteUtils.createCalciteParser;

/**
 * Translate some subset of SQL DDL statements from Calcite to Presto.
 */
public class CalciteToPrestoTranslator extends ToPrestoTranslator {

    @Override
    public String toPresto(String sql) {
        SqlAbstractParserImpl calciteParser = createCalciteParser(sql);
        try {
            org.apache.calcite.sql.SqlNodeList parseTree = calciteParser.parseSqlStmtList();
            if (parseTree.get(0) instanceof SqlCreateView) {
                return sql;
            }
            CalciteToPresto h2Translator = new CalciteToPresto();
            return parseTree.accept(h2Translator);
        } catch (Exception e) {
            System.out.println("Calcite to Presto translator encountered exception: " + e.getMessage());
        }
        return new String();
    }
}

/**
 * Translates Calcite to Presto by implementing a Calcite parse node visitor.
 */
class CalciteToPresto extends SqlBasicVisitor<String> {
    @Override
    public String visit(SqlNodeList nodeList) {
        for (SqlNode statement : nodeList) {
            if (statement.getKind() == SqlKind.CREATE_TABLE && statement instanceof SqlCreateTable) {
                SqlCreateTable createStatement = (SqlCreateTable) statement;

                List<SqlNode> operands = createStatement.getOperandList();
                // For the SqlCreateTable class, getOperandList returns [name, columnList, query]

                // We need these two lists so we can add primary key to the right column
                // These two lists cannot be a Map because we need to preserve the same order of columns as in the
                // original SQL statement.
                ArrayList<String> prestoColumnsId = new ArrayList<>();
                ArrayList<String> prestoColumns = new ArrayList<>();
                if (operands.get(1) instanceof SqlNodeList) {
                    SqlNodeList columns = (SqlNodeList) operands.get(1);
                    for (SqlNode column : columns) {
                        if (column instanceof SqlColumnDeclaration) {
                            SqlColumnDeclaration castColumn = (SqlColumnDeclaration) column;
                            // Presto doesn't like null columns
                            String nullOperand = (castColumn.strategy == ColumnStrategy.NULLABLE) ? "" : "not null";
                            if (castColumn.expression != null) {
                                throw new UnsupportedOperationException(
                                        "Don't know how to translate Calcite column expressions yet");
                            }

                            String transcribed = String.format("%s %s %s",
                                    castColumn.name.toString(),
                                    castColumn.dataType,
                                    nullOperand);
                            prestoColumnsId.add(castColumn.name.toString());
                            prestoColumns.add(transcribed);
                        }
                        if (column instanceof SqlKeyConstraint) {
                            // Only translate PRIMARY_KEY to Presto. Other kinds of constraints (unique, check) cannot
                            // be expressed in Presto
                            SqlKeyConstraint constraint = (SqlKeyConstraint) column;
                            if (constraint.getKind() == SqlKind.PRIMARY_KEY) {
                                List<SqlNode> constraintOperands = constraint.getOperandList();
                                if (constraintOperands.get(1) instanceof SqlNodeList) {
                                    SqlNodeList constraintColumns = (SqlNodeList) constraintOperands.get(1);
                                    if (constraintColumns.size() > 1) {
                                        throw new UnsupportedOperationException(
                                                "Cannot translate primary key with more than one column");
                                    }
                                    String pk = constraintColumns.get(0).toString();
                                    // Now look up this pk in the column Map
                                    int indexOfPk = prestoColumnsId.indexOf(pk);
                                    prestoColumns.set(indexOfPk,
                                            String.format("%s with (primary_key = true)", prestoColumns.get(indexOfPk)));
                                }
                            }
                        }
                    }
                }
                String columnsString = String.join(", ", prestoColumns);
                return String.format("%s %s (%s)",
                        createStatement.getOperator().toString(),
                        operands.get(0),
                        columnsString).toLowerCase();
            }
        }

        return new String();
    }
}