/*
 * Copyright 2018-2020 VMware, Inc. All Rights Reserved.
 * SPDX-License-Identifier: BSD-2
 */
package com.vmware.ddlog;

import com.facebook.presto.sql.parser.ParsingOptions;
import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.tree.AllColumns;
import com.facebook.presto.sql.tree.AstVisitor;
import com.facebook.presto.sql.tree.BooleanLiteral;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.Insert;
import com.facebook.presto.sql.tree.LongLiteral;
import com.facebook.presto.sql.tree.Query;
import com.facebook.presto.sql.tree.QuerySpecification;
import com.facebook.presto.sql.tree.Row;
import com.facebook.presto.sql.tree.Select;
import com.facebook.presto.sql.tree.Statement;
import com.facebook.presto.sql.tree.StringLiteral;
import com.facebook.presto.sql.tree.Values;
import ddlogapi.DDlogAPI;
import ddlogapi.DDlogCommand;
import ddlogapi.DDlogException;
import ddlogapi.DDlogRecCommand;
import ddlogapi.DDlogRecord;
import org.jooq.DSLContext;
import org.jooq.Field;
import org.jooq.Record;
import org.jooq.Record1;
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

import static org.jooq.impl.DSL.field;

public class DDlogJooqProvider implements MockDataProvider {
    private static final String INTEGER_TYPE = "java.lang.Integer";
    private static final String STRING_TYPE = "java.lang.String";
    private static final String BOOLEAN_TYPE = "java.lang.Boolean";
    private static final String LONG_TYPE = "java.lang.Long";
    private static final String DDLOG_SOME_TYPE = "ddlog_std::Some";
    private static final String DDLOG_NONE_TYPE = "ddlog_std::None";
    private final DDlogAPI dDlogAPI;
    private final DSLContext dslContext;
    private final Field<Integer> updateCountField;
    private final Map<String, List<Field<?>>> tables = new HashMap<>();
    private final SqlParser parser = new SqlParser();
    private final ParsingOptions options = ParsingOptions.builder().build();
    private final QueryVisitor queryVisitor = new QueryVisitor();
    private final ParseLiterals parseLiterals = new ParseLiterals();

    public DDlogJooqProvider(final DDlogAPI dDlogAPI, final List<String> sqlStatements) {
        this.dDlogAPI = dDlogAPI;
        this.dslContext = DSL.using("jdbc:h2:mem:");
        this.updateCountField = field("UPDATE_COUNT", Integer.class);
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
        final String[] batchSql = ctx.batchSQL();
        final MockResult[] mock = new MockResult[batchSql.length];
        try {
            dDlogAPI.transactionStart();
            for (int i = 0; i < batchSql.length; i++) {
                mock[i] = execute(batchSql[i]);
            }
            dDlogAPI.transactionCommit();
        } catch (final DDlogException e) {
            throw new RuntimeException(e);
        }
        return mock;
    }

    private MockResult execute(final String sql) throws SQLException {
        final Statement statement = parser.createStatement(sql, options);
        final MockResult result = queryVisitor.process(statement, sql);
        if (result == null) {
            throw new SQLException("Could not execute SQL statement " + sql);
        }
        return result;
    }

    private class QueryVisitor extends AstVisitor<MockResult, String> {
        @Override
        protected MockResult visitQuerySpecification(final QuerySpecification node, String sql) {
            final Select select = node.getSelect();
            if (!(select.getSelectItems().size() == 1 && select.getSelectItems().get(0) instanceof AllColumns)) {
                throw new RuntimeException("Statement not supported: " + sql);
            }
            assert node.getFrom().isPresent() && node.getFrom().get() instanceof com.facebook.presto.sql.tree.Table;
            final String tableName = ((com.facebook.presto.sql.tree.Table) node.getFrom().get()).getName().toString();
            final List<Field<?>> fields = tables.get(tableName.toUpperCase());
            if (fields == null) {
                throw new RuntimeException("Unknown table: " + tableName);
            }
            final Result<Record> result = dslContext.newResult(fields);
            try {
                dDlogAPI.dumpTable(ddlogRelationName(tableName), (record, l) -> {
                    final Record jooqRecord = dslContext.newRecord(fields);
                    final Object[] returnValue = new Object[fields.size()];
                    for (int i = 0; i < fields.size(); i++) {
                        returnValue[i] = structToValue(fields.get(i), record.getStructField(i));
                    }
                    jooqRecord.fromArray(returnValue);
                    result.add(jooqRecord);
                });
            } catch (final DDlogException e) {
                throw new RuntimeException(e);
            }
            return new MockResult(1, result);
        }

        @Override
        protected MockResult visitQuery(final Query node, final String context) {
            final QuerySpecification specification = (QuerySpecification) node.getQueryBody();
            return visitQuerySpecification(specification, context);
        }

        @Override
        protected MockResult visitInsert(final Insert node, final String sql) {
            try {
                assert node.getQuery().getQueryBody() instanceof Values;
                final Values values = (Values) node.getQuery().getQueryBody();
                final String tableName = node.getTarget().toString();
                final List<Field<?>> fields = tables.get(tableName.toUpperCase());
                final int tableId = dDlogAPI.getTableId(ddlogRelationName(tableName));
                for (final Expression row: values.getRows()) {
                    assert row instanceof Row;
                    final List<Expression> items = ((Row) row).getItems();
                    assert items.size() == fields.size();
                    final DDlogRecord[] recordsArray = new DDlogRecord[items.size()];
                    for (int i = 0; i < items.size(); i++) {
                        recordsArray[i] = parseLiterals.process(items.get(i), fields.get(i).getDataType().nullable());
                    }
                    final DDlogRecord record = DDlogRecord.makeStruct(ddlogTableTypeName(tableName), recordsArray);
                    final DDlogRecCommand command = new DDlogRecCommand(DDlogCommand.Kind.Insert, tableId, record);
                    dDlogAPI.applyUpdates(new DDlogRecCommand[]{command});
                }
                final Result<Record1<Integer>> result = dslContext.newResult(updateCountField);
                final Record1<Integer> resultRecord = dslContext.newRecord(updateCountField);
                resultRecord.setValue(updateCountField, values.getRows().size());
                result.add(resultRecord);
                return new MockResult(1, result);
            } catch (DDlogException e) {
                throw new RuntimeException(e);
            }
        }
    }

    private static class ParseLiterals extends AstVisitor<DDlogRecord, Boolean> {

        @Override
        protected DDlogRecord visitStringLiteral(final StringLiteral node, final Boolean isNullable) {
            try {
                return maybeOption(isNullable, new DDlogRecord(node.getValue()));
            } catch (final DDlogException e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        protected DDlogRecord visitLongLiteral(final LongLiteral node, final Boolean isNullable) {
            return maybeOption(isNullable, new DDlogRecord(node.getValue()));
        }

        @Override
        protected DDlogRecord visitBooleanLiteral(final BooleanLiteral node, final Boolean isNullable) {
            return maybeOption(isNullable, new DDlogRecord(node.getValue()));
        }
    }

    private static String ddlogTableTypeName(final String tableName) {
        return "T" + tableName.toLowerCase();
    }

    private static String ddlogRelationName(final String tableName) {
        return "R" + tableName.toLowerCase();
    }

    private static DDlogRecord maybeOption(final Boolean isNullable, final DDlogRecord record) {
        if (isNullable) {
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

    @Nullable
    private static Object structToValue(final Field<?> field, final DDlogRecord record) {
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
}