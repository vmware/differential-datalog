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

import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.ddl.SqlCreateTable;
import org.apache.calcite.sql.util.SqlBasicVisitor;

import java.util.ArrayList;
import java.util.List;

/**
 * Base class for translating DDL statements to Calcite SQL.
 */
public abstract class CalciteDDLVisitorBase extends SqlBasicVisitor<String> {
    /**
     * Holds the return values from visit(SqlCall call);
     * Any subclass of CalciteDDLVisitorBase should override visit(SqlCall call) and add the result to translatedColumns.
     */
    protected List<String> translatedColumns = new ArrayList();

    /**
     *  Many DDL statements are of type SqlNodeList, so this part can be shared
     *  across any Calcite-to-\<dialect\> translator.
     */
    @Override
    public String visit(SqlNodeList nodeList) {
        for (SqlNode statement : nodeList) {
            if (statement instanceof SqlCreateTable) {
                SqlCreateTable createStatement = (SqlCreateTable) statement;

                // For the SqlCreateTable class, getOperandList returns [name, columnList, query]
                List<SqlNode> operands = createStatement.getOperandList();

                if (operands.get(1) instanceof SqlNodeList) {
                    SqlNodeList columns = (SqlNodeList) operands.get(1);
                    for (SqlNode column : columns) {
                        if (column instanceof SqlCall) {
                            // The return value is ignored; we assume the overridden method will add
                            // the result to translatedColumns
                            visit((SqlCall) column);
                        } else {
                            throw new UnsupportedOperationException(
                                    "CalciteDDLVisitorBase found unsupported nodes while parsing `create table`");
                        }
                    }
                }

                String columnsString = String.join(", ", translatedColumns);
                return String.format("%s %s (%s)",
                        createStatement.getOperator(),
                        operands.get(0),
                        columnsString).toLowerCase();
            }
        }
        throw new UnsupportedOperationException(
                "CalciteDDLVisitorBase currently cannot translate DDL statements that are not `create table ..`");
    }
}
