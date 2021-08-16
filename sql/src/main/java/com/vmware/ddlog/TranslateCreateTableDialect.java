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

package com.vmware.ddlog;

import com.facebook.presto.sql.tree.AstVisitor;
import com.facebook.presto.sql.tree.ColumnDefinition;
import com.facebook.presto.sql.tree.CreateTable;
import com.facebook.presto.sql.tree.CreateView;
import com.facebook.presto.sql.tree.TableElement;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

class TranslateCreateTableDialect extends AstVisitor<String, String> {
    @Override
    protected String visitCreateTable(final CreateTable node, final String sql) {
        final List<String> primaryKeyColumns = new ArrayList<>();
        final List<String> columnsToCreate = new ArrayList<>();
        for (final TableElement element : node.getElements()) {
            if (element instanceof ColumnDefinition) {
                final ColumnDefinition cd = (ColumnDefinition) element;
                String h2Type = cd.getType();

                // We need to strip the array subtype for H2
                final Pattern arrayType = Pattern.compile("ARRAY\\((.+)\\)");
                Matcher m = arrayType.matcher(h2Type);
                if (m.find()) {
                    h2Type = "array";
                }

                if (cd.getProperties().size() == 1) {
                    final String propertyName = cd.getProperties().get(0).getName().getValue();
                    final String propertyValue = cd.getProperties().get(0).getValue().toString();
                    if (propertyName.equals("primary_key") && propertyValue.equals("true")) {
                        primaryKeyColumns.add(cd.getName().getValue());
                    } else {
                        throw new RuntimeException(String.format("Unsupported property %s in sql: %s",
                                propertyName, sql));
                    }
                }
                if (cd.getProperties().size() > 1) {
                    throw new RuntimeException(String.format("Unsupported properties %s in sql: %s",
                            cd.getProperties(), sql));
                }
                columnsToCreate.add(String.format("%s %s %s", cd.getName().getValue(), h2Type, cd.isNullable() ? "" : "not null"));
            }
        }

        final String columns = String.join(", ", columnsToCreate);
        if (primaryKeyColumns.size() > 0) {
            final String primaryKeyColumnsStr = String.join(", ", primaryKeyColumns);
            return String.format("create table %s (%s, primary key (%s))", node.getName().toString(), columns,
                    primaryKeyColumnsStr);
        } else {
            return String.format("create table %s (%s)", node.getName().toString(), columns);
        }
    }

    @Override
    protected String visitCreateView(final CreateView node, final String sql) {
        return sql;
    }
}
