/*
 * Copyright 2018-2020 VMware, Inc. All Rights Reserved.
 * SPDX-License-Identifier: BSD-2
 */
package com.vmware.ddlog;

import ddlogapi.DDlogAPI;
import ddlogapi.DDlogException;
import ddlogapi.DDlogRecord;
import org.jooq.DSLContext;
import org.jooq.Field;
import org.jooq.Record;
import org.jooq.Result;
import org.jooq.Table;
import org.jooq.impl.DSL;
import org.jooq.tools.jdbc.MockDataProvider;
import org.jooq.tools.jdbc.MockExecuteContext;
import org.jooq.tools.jdbc.MockResult;

import javax.annotation.Nullable;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DDlogJooqProvider implements MockDataProvider {
    private static final String INTEGER_TYPE = "java.lang.Integer";
    private static final String STRING_TYPE = "java.lang.String";
    private static final String BOOLEAN_TYPE = "java.lang.Boolean";
    private static final String LONG_TYPE = "java.lang.Long";

    private final DDlogAPI dDlogAPI;
    private final DSLContext dslContext;
    private final Map<String, List<Field<?>>> tables = new HashMap<>();

    public DDlogJooqProvider(final DDlogAPI dDlogAPI, final List<String> sqlStatements) {
        this.dDlogAPI = dDlogAPI;
        dslContext = DSL.using("jdbc:h2:mem:");
        for (final String sql : sqlStatements) {
            dslContext.execute(sql);
        }
        for (final Table<?> table: dslContext.meta().getTables()) {
            if (table.getSchema().getName().equals("PUBLIC")) {
                tables.put(table.getName(), Arrays.asList(table.fields()));
            }
        }
    }

    @Override
    public MockResult[] execute(final MockExecuteContext ctx) throws SQLException {
        final MockResult[] mock = new MockResult[1];
        // The execute context contains SQL string(s), bind values, and other meta-data
        final String sql = ctx.sql();

        if (sql.toUpperCase().startsWith("SELECT")) {
            final String[] s = ctx.sql().toUpperCase().split(" ");
            if (!s[s.length - 2].equals("FROM")) {
                throw new SQLException("Statement not supported: " + sql);
            }
            final String tableName = s[s.length - 1];
            final List<Field<?>> fields = tables.get(tableName);
            final Result<Record> result = dslContext.newResult(fields);
            try {
                dDlogAPI.dumpTable("R" + tableName.toLowerCase(), (record, l) -> {
                    final Record jooqRecord = dslContext.newRecord(fields);
                    final Object[] returnValue = new Object[fields.size()];
                    for (int i = 0; i < fields.size(); i++) {
                        returnValue[i] = structToValue(fields.get(i), record.getStructField(i));
                    }
                    jooqRecord.fromArray(returnValue);
                    result.add(jooqRecord);
                });
            } catch (final DDlogException e) {
                e.printStackTrace();
            }
            mock[0] = new MockResult(1, result);
        } else {
            // Exceptions are propagated through the JDBC and jOOQ APIs
            throw new SQLException("Statement not supported: " + sql);
        }
        return mock;
    }

    @Nullable
    private Object structToValue(final Field<?> field, final DDlogRecord record) {
        final Class<?> cls = field.getType();
        if (record.isStruct() && record.getStructName().equals("std.None")) {
            return null;
        }
        if (record.isStruct() && record.getStructName().equals("ddlog_std::Some")) {
            return structToValue(field, record.getStructField(0));
        }
        switch (cls.getName()) {
            case BOOLEAN_TYPE:
                return record.getBoolean();
            case INTEGER_TYPE:
                return record.getInt().intValue();
            case LONG_TYPE:
                return record.getInt().longValue();
            case STRING_TYPE:
                return record.getString();
            default:
                throw new RuntimeException("Unknown datatype %s of field %s in table %s in update received");
        }
    }
}