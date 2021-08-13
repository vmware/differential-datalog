/*
 * Copyright 2018-2020 VMware, Inc. All Rights Reserved.
 * SPDX-License-Identifier: BSD-2
 */

package com.vmware.ddlog.util.sql;

import org.apache.calcite.schema.ColumnStrategy;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.ddl.SqlColumnDeclaration;
import org.apache.calcite.sql.ddl.SqlCreateTable;
import org.apache.calcite.sql.util.SqlBasicVisitor;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

// We just need to strip out the subtype for the array, which H2 doesn't like.
// Unfortunately this means we need to walk all columns in the parse tree.
public class CalciteToH2 extends SqlBasicVisitor<String> {
    @Override
    public String visit(SqlNodeList nodeList) {
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
