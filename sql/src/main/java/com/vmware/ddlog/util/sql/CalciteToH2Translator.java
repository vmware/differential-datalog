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
import org.apache.calcite.sql.parser.SqlAbstractParserImpl;
import org.apache.calcite.sql.util.SqlBasicVisitor;

import java.util.ArrayList;
import java.util.List;

import static com.vmware.ddlog.util.sql.CalciteUtils.createCalciteParser;

/**
 * Translate some subset of SQL DDL statements from Calcite to H2.
 */
public class CalciteToH2Translator extends ToH2Translator {

    /**
     * Translate Calcite SQL statement to H2. Unfortunately, Calcite and H2 aren't completely equivalent,
     * so this method isn't just a noop. For example, H2 arrays do not accept subtypes, so we need to strip them.
     * @param sql
     * @return
     */
    @Override
    public String toH2(String sql) {
        SqlAbstractParserImpl calciteParser = createCalciteParser(sql);
        String ret = sql;
        try {
            org.apache.calcite.sql.SqlNodeList parseTree = calciteParser.parseSqlStmtList();
            // Pass `create view` straight through
            if (!(parseTree.get(0) instanceof SqlCreateView)) {
                CalciteToH2 h2Translator = new CalciteToH2();
                ret = parseTree.accept(h2Translator);
            }
        } catch (Exception e) {
            System.out.println("Calcite to H2 translator exception: " + e.getMessage());
        }
        return ret;
    }
}

/**
 * Translates Calcite to H2 by implementing a Calcite parse node visitor.
 */
class CalciteToH2 extends SqlBasicVisitor<String> {
    @Override
    public String visit(SqlNodeList nodeList) {
        // We just need to strip out the subtype for the array, which H2 doesn't like.
        // Unfortunately this means we need to walk all columns in the parse tree.
        for (SqlNode statement : nodeList) {
            if (statement.getKind() == SqlKind.CREATE_TABLE && statement instanceof SqlCreateTable) {
                SqlCreateTable createStatement = (SqlCreateTable) statement;

                List<SqlNode> operands = createStatement.getOperandList();
                // For the SqlCreateTable class, getOperandList returns [name, columnList, query]

                List<String> prestoColumns = new ArrayList();
                if (operands.get(1) instanceof SqlNodeList) {
                    SqlNodeList columns = (SqlNodeList) operands.get(1);
                    for (SqlNode column : columns) {
                        if (column instanceof SqlColumnDeclaration) {
                            SqlColumnDeclaration castColumn = (SqlColumnDeclaration) column;
                            String type = castColumn.dataType.toString();

                            if (castColumn.dataType.getCollectionsTypeName().toString().equals("ARRAY")) {
                                type = "array";
                            }

                            String nullOperand = (castColumn.strategy == ColumnStrategy.NULLABLE) ? "null" : "not null";
                            if (castColumn.expression != null) {
                                throw new UnsupportedOperationException(
                                        "Don't know how to translate Calcite column expressions yet");
                            }

                            String transcribed = String.format("%s %s %s",
                                    castColumn.name.toString(),
                                    type,
                                    nullOperand);
                            prestoColumns.add(transcribed);
                        }
                    }
                }
                String columnsString = String.join(", ", prestoColumns);
                return String.format("%s %s (%s)",
                        createStatement.getOperator(),
                        operands.get(0),
                        columnsString).toLowerCase();
            }
        }

        return new String();
    }
}

