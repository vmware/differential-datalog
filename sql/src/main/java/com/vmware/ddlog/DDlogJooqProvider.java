/*
 * Copyright 2018-2020 VMware, Inc. All Rights Reserved.
 * SPDX-License-Identifier: BSD-2
 */
package com.vmware.ddlog;

import ddlogapi.DDlogAPI;
import ddlogapi.DDlogCommand;
import ddlogapi.DDlogException;
import ddlogapi.DDlogRecCommand;
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
import java.util.Locale;
import java.util.Map;

public class DDlogJooqProvider implements MockDataProvider {
    private static final String INTEGER_TYPE = "java.lang.Integer";
    private static final String STRING_TYPE = "java.lang.String";
    private static final String BOOLEAN_TYPE = "java.lang.Boolean";
    private static final String LONG_TYPE = "java.lang.Long";
    private static final String DDLOG_SOME_TYPE = "ddlog_std::Some";
    private static final String DDLOG_NONE_TYPE = "ddlog_std::None";
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
            mock[0] = executeSelectStar(sql);
        } else if (sql.toUpperCase().startsWith("INSERT INTO")) {
            mock[0] = executeInsert(sql);
        } else {
            // Exceptions are propagated through the JDBC and jOOQ APIs
            throw new SQLException("Statement not supported: " + sql);
        }
        return mock;
    }

    private MockResult executeSelectStar(final String sql) throws SQLException {
        final String[] s = sql.split("[ ]+");
        if (!(s.length == 4 && s[1].equalsIgnoreCase("*") && s[2].equalsIgnoreCase("FROM"))) {
            throw new SQLException("Statement not supported: " + sql);
        }
        final String tableName = s[3];
        final List<Field<?>> fields = tables.get(tableName.toUpperCase());
        if (fields == null) {
            throw new SQLException("Unknown table: " + tableName);
        }
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
            throw new SQLException(e);
        }
        return new MockResult(1, result);
    }

    private MockResult executeInsert(final String sql) throws SQLException {
        final String[] s = sql.replaceAll("[(),]", "")
                              .split("[ ]+");
        if (!(s.length >= 4 && s[1].equalsIgnoreCase("INTO")
                && s[3].equalsIgnoreCase("VALUES"))) {
            throw new SQLException("Statement not supported: " + sql);
        }
        final String tableName = s[2];
        final int valuesIndex = 4; // the index representing the 1st field of the value being inserted
        final List<Field<?>> fields = tables.get(tableName.toUpperCase());
        if (fields == null) {
            throw new SQLException("Table cannot be queried: " + tableName);
        }
        String[] valuesTuple = Arrays.copyOfRange(s, valuesIndex, s.length);
        final DDlogRecord dDlogRecord = toDDlogRecord(tableName, valuesTuple);
        final int tableId = dDlogAPI.getTableId("R" + tableName.toLowerCase());
        final DDlogRecCommand command = new DDlogRecCommand(DDlogCommand.Kind.Insert, tableId, dDlogRecord);
        try {
            dDlogAPI.transactionStart();
            dDlogAPI.applyUpdates(new DDlogRecCommand[]{command});
            dDlogAPI.transactionCommit();
        } catch (final DDlogException e) {
            e.printStackTrace();
        }
        return new MockResult(1, null);
    }

    @Nullable
    private Object structToValue(final Field<?> field, final DDlogRecord record) {
        final Class<?> cls = field.getType();
        if (record.isStruct() && record.getStructName().equals(DDLOG_NONE_TYPE)) {
            return null;
        }
        if (record.isStruct() && record.getStructName().equals(DDLOG_SOME_TYPE)) {
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

    private DDlogRecord toDDlogRecord(final String tableName, final String[] args) {
        final DDlogRecord[] recordsArray = new DDlogRecord[args.length];
        final List<Field<?>> fields = tables.get(tableName.toUpperCase());

        int fieldIndex = 0;
        for (final Field<?> field : fields) {
            final Class<?> cls = field.getType();
            try {
                // Handle nullable columns here
                if (args[fieldIndex] == null) {
                    recordsArray[fieldIndex] = DDlogRecord.makeStruct(DDLOG_NONE_TYPE, new DDlogRecord[0]);
                }
                else {
                    switch (cls.getName()) {
                        case BOOLEAN_TYPE:
                            recordsArray[fieldIndex] = maybeOption(field,
                                    new DDlogRecord(Boolean.parseBoolean(args[fieldIndex])));
                            break;
                        case INTEGER_TYPE:
                            recordsArray[fieldIndex] = maybeOption(field,
                                    new DDlogRecord(Integer.parseInt(args[fieldIndex])));
                            break;
                        case LONG_TYPE:
                            recordsArray[fieldIndex] = maybeOption(field,
                                    new DDlogRecord(Long.parseLong(args[fieldIndex])));
                            break;
                        case STRING_TYPE:
                            // Strings have to be escaped by single quotes
                            if (args[fieldIndex].startsWith("'") && args[fieldIndex].endsWith("'")) {
                                recordsArray[fieldIndex] = maybeOption(field,
                                        new DDlogRecord(args[fieldIndex].substring(1, args[fieldIndex].length() - 1)));
                            } else {
                                throw new RuntimeException("Unexpected string");
                            }
                            break;
                        default:
                            throw new RuntimeException(String.format("Unknown datatype %s of field %s in table %s" +
                                            " while sending DB data to DDLog", args[fieldIndex].getClass().getName(),
                                    field.getName(), tableName));
                    }
                }
            } catch (final DDlogException e) {
                throw new RuntimeException(e);
            }
            fieldIndex = fieldIndex + 1;
        }

        try {
            return DDlogRecord.makeStruct("T" + tableName.toLowerCase(Locale.US), recordsArray);
        } catch (final DDlogException | NullPointerException e) {
            throw new RuntimeException(e);
        }
    }

    public DDlogRecord maybeOption(final Field<?> field, final DDlogRecord record) {
        if (field.getDataType().nullable()) {
            try {
                final DDlogRecord[] arr = new DDlogRecord[1];
                arr[0] = record;
                return DDlogRecord.makeStruct(DDLOG_SOME_TYPE, arr);
            } catch (final DDlogException e) {
                throw new RuntimeException(e);
            }
        } else {
            return record;
        }
    }
}