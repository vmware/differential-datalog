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
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.ddl.SqlColumnDeclaration;
import org.apache.calcite.sql.ddl.SqlCreateView;
import org.apache.calcite.sql.ddl.SqlKeyConstraint;
import org.apache.calcite.sql.parser.SqlAbstractParserImpl;

import java.util.List;
import java.util.stream.Collectors;

import static com.vmware.ddlog.util.sql.CalciteUtils.createCalciteParser;

/**
 * Translate some subset of SQL DDL statements from Calcite to H2.
 */
public class CalciteToH2Translator implements ToH2Translator<CalciteSqlStatement> {

    /**
     * Translate Calcite SQL statement to H2.
     */
    @Override
    public H2SqlStatement toH2(CalciteSqlStatement sql) {
        SqlAbstractParserImpl calciteParser = createCalciteParser(sql);
        try {
            org.apache.calcite.sql.SqlNodeList parseTree = calciteParser.parseSqlStmtList();
            // Don't need to modify `create view`
            if (parseTree.get(0) instanceof SqlCreateView) {
                return new H2SqlStatement(sql.getStatement());
            }
            CalciteToH2 h2Translator = new CalciteToH2();
            return new H2SqlStatement(parseTree.accept(h2Translator));
        } catch (Exception e) {
            throw new RuntimeException(e.getMessage());
        }
    }
}

/**
 * Translates Calcite to H2 by implementing a Calcite parse node visitor.
 */
class CalciteToH2 extends CalciteDDLVisitorBase {

    // Unfortunately, Calcite and H2 aren't completely equivalent, so this method isn't just a noop.
    // For example, H2 arrays do not accept subtypes, so we need to strip them, but this also means the Visitor needs to
    // walk the entire parse tree and translate it into a string.
    @Override
    public String visit(SqlCall call) {
        if (call instanceof SqlColumnDeclaration) {

            SqlColumnDeclaration castColumn = (SqlColumnDeclaration) call;
            String type = castColumn.dataType.toString();

            String nullOperand = (castColumn.strategy == ColumnStrategy.NULLABLE) ? "null" : "not null";
            if (castColumn.expression != null) {
                throw new UnsupportedOperationException(
                        "Don't know how to translate Calcite column expressions yet");
            }

            String transcribed = String.format("%s %s %s",
                    castColumn.name.toString(),
                    type,
                    nullOperand);
            translatedColumns.add(transcribed);
            return transcribed;
        }
        if (call instanceof SqlKeyConstraint) {
            SqlKeyConstraint castColumn = (SqlKeyConstraint) call;
            List<SqlNode> constraintOperands = castColumn.getOperandList();
            if (constraintOperands.get(1) instanceof SqlNodeList) {
                SqlNodeList constraintColumns = (SqlNodeList) constraintOperands.get(1);

                String constraintColumnsString = constraintColumns.getList().stream().map(SqlNode::toString).collect(Collectors.joining(","));

                String transcribed = String.format("%s (%s)", castColumn.getOperator(), constraintColumnsString);
                translatedColumns.add(transcribed);
                return transcribed;
            }
        }
        throw new UnsupportedOperationException("CalciteToH2 cannot translate SqlCall's of kind " + call.getKind());
    }
}

