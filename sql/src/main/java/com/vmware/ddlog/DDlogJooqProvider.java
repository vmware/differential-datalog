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
import com.facebook.presto.sql.tree.ColumnDefinition;
import com.facebook.presto.sql.tree.CreateTable;
import com.facebook.presto.sql.tree.CreateView;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.Insert;
import com.facebook.presto.sql.tree.LongLiteral;
import com.facebook.presto.sql.tree.Query;
import com.facebook.presto.sql.tree.QuerySpecification;
import com.facebook.presto.sql.tree.Row;
import com.facebook.presto.sql.tree.Select;
import com.facebook.presto.sql.tree.Statement;
import com.facebook.presto.sql.tree.StringLiteral;
import com.facebook.presto.sql.tree.TableElement;
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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.jooq.impl.DSL.field;


/**
 * This class provides a restricted mechanism to make a DDlog program appear like an SQL database that can be
 * queried over a JDBC connection. To initialize, it requires a set of "create table" and "create view" statements
 * to be supplied during initialization. For example:
 *
 *         final DDlogAPI dDlogAPI = new DDlogAPI(1, null, true);
 *
 *         // Initialise the data provider. ddl represents a list of Strings that are SQL DDL statements
 *         // like "create table" and "create view"
 *         MockDataProvider provider = new DDlogJooqProvider(dDlogAPI, ddl);
 *         MockConnection connection = new MockConnection(provider);
 *
 *         // Pass the mock connection to a jOOQ DSLContext:
 *         DSLContext create = DSL.using(connection);
 *
 * After that, the connection that is created with this MockProvider can execute a restricted subset of SQL queries.
 * We assume these queries are of one of the following forms:
 *   A1. "select * from T" where T is a table name which corresponds to a ddlog output relation. By definition,
 *                        T can therefore only be an SQL view for which there is a corresponding
 *                        "create view T as ..." that is passed to the  DdlogJooqProvider.
 *   A2. "insert into T values (<row>)" where T is a base table. That is, there should be a corresponding
 *                                     "create table T..." DDL statement that is passed to the DDlogJooqProvider.
 */
public class DDlogJooqProvider implements MockDataProvider {
    private static final String INTEGER_TYPE = "java.lang.Integer";
    private static final String STRING_TYPE = "java.lang.String";
    private static final String BOOLEAN_TYPE = "java.lang.Boolean";
    private static final String LONG_TYPE = "java.lang.Long";
    private static final String DDLOG_SOME = "ddlog_std::Some";
    private static final String DDLOG_NONE = "ddlog_std::None";
    private final DDlogAPI dDlogAPI;
    private final DSLContext dslContext;
    private final Field<Integer> updateCountField;
    private final Map<String, List<Field<?>>> tablesToFields = new HashMap<>();
    private final Map<String, List<? extends Field<?>>> tablesToPrimaryKeys = new HashMap<>();
    private final SqlParser parser = new SqlParser();
    private final ParsingOptions options = ParsingOptions.builder().build();
    private final QueryVisitor queryVisitor = new QueryVisitor();
    private final ParseLiterals parseLiterals = new ParseLiterals();
    private final TranslateCreateTableDialect translateCreateTableDialect = new TranslateCreateTableDialect();

    public DDlogJooqProvider(final DDlogAPI dDlogAPI, final List<String> sqlStatements) {
        this.dDlogAPI = dDlogAPI;
        this.dslContext = DSL.using("jdbc:h2:mem:");
        this.updateCountField = field("UPDATE_COUNT", Integer.class);

        // We translate DDL statements from the Presto dialect to H2.
        // We then execute these statements in a temporary database so that JOOQ can extract useful metadata
        // that we will use later (for example, the record types for views).
        for (final String sql : sqlStatements) {
            final Statement statement = parser.createStatement(sql, options);
            final String statementInH2Dialect = translateCreateTableDialect.process(statement, sql);
            dslContext.execute(statementInH2Dialect);
        }
        for (final Table<?> table: dslContext.meta().getTables()) {
            if (table.getSchema().getName().equals("PUBLIC")) { // H2-specific assumption
                tablesToFields.put(table.getName(), Arrays.asList(table.fields()));
                if (table.getPrimaryKey() != null) {
                    tablesToPrimaryKeys.put(table.getName(), table.getPrimaryKey().getFields());
                }
            }
        }
    }

    /*
     * All executed SQL queries against a JOOQ connection are received here
     */
    @Override
    public MockResult[] execute(final MockExecuteContext ctx) throws SQLException {
        final String[] batchSql = ctx.batchSQL();
        final MockResult[] mock = new MockResult[batchSql.length];
        try {
            dDlogAPI.transactionStart();
            for (int i = 0; i < batchSql.length; i++) {
                mock[i] = executeOne(batchSql[i]);
            }
            dDlogAPI.transactionCommit();
        } catch (final DDlogException e) {
            throw new RuntimeException(e);
        }
        return mock;
    }

    private MockResult executeOne(final String sql) throws SQLException {
        final Statement statement = parser.createStatement(sql, options);
        final MockResult result = queryVisitor.process(statement, sql);
        if (result == null) {
            throw new SQLException("Could not execute SQL statement " + sql);
        }
        return result;
    }

    /*
     * Visits an SQL query and converts into a JOOQ MockResult type.
     */
    private class QueryVisitor extends AstVisitor<MockResult, String> {
        @Override
        protected MockResult visitQuerySpecification(final QuerySpecification node, String sql) {
            // The checks below encode assumption A1 (see javadoc for the DDlogJooqProvider class)
            final Select select = node.getSelect();
            if (!(select.getSelectItems().size() == 1
                    && select.getSelectItems().get(0) instanceof AllColumns
                    && node.getFrom().isPresent()
                    && node.getFrom().get() instanceof com.facebook.presto.sql.tree.Table)) {
                throw new RuntimeException("Statement not supported: " + sql);
            }
            final String tableName = ((com.facebook.presto.sql.tree.Table) node.getFrom().get()).getName().toString();
            final List<Field<?>> fields = tablesToFields.get(tableName.toUpperCase());
            if (fields == null) {
                throw new RuntimeException(String.format("Unknown table %s queried in statement: %s", tableName, sql));
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
                // The assertions below encode assumption A2 (see javadoc for the DDlogJooqProvider class)
                if (!(node.getQuery().getQueryBody() instanceof Values)) {
                    throw new RuntimeException("Statement not supported: " + sql);
                }
                final Values values = (Values) node.getQuery().getQueryBody();
                final String tableName = node.getTarget().toString();
                final List<Field<?>> fields = tablesToFields.get(tableName.toUpperCase());
                final int tableId = dDlogAPI.getTableId(ddlogRelationName(tableName));
                for (final Expression row: values.getRows()) {
                    if (!(row instanceof Row)) {
                        throw new RuntimeException("Statement not supported: " + sql);
                    }
                    final List<Expression> items = ((Row) row).getItems();
                    if (items.size() != fields.size()) {
                        throw new RuntimeException(
                                String.format("Incorrect row size for insertion into table %s: %s", tableName, sql));
                    }
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

    /*
     * Translates literals into corresponding DDlogRecord instances
     */
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

    private static class TranslateCreateTableDialect extends AstVisitor<String, String> {
        @Override
        protected String visitCreateTable(final CreateTable node, final String sql) {
            final List<String> primaryKeyColumns = new ArrayList<>();
            final List<String> columnsToCreate = new ArrayList<>();
            for (final TableElement element: node.getElements()) {
                if (element instanceof ColumnDefinition) {
                    final ColumnDefinition cd = (ColumnDefinition) element;
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
                    columnsToCreate.add(String.format("%s %s", cd.getName().getValue(), cd.getType()));
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

    /*
     * This corresponds to the naming convention followed by the SQL -> DDlog compiler
     */
    private static String ddlogTableTypeName(final String tableName) {
        return "T" + tableName.toLowerCase();
    }

    /*
     * This corresponds to the naming convention followed by the SQL -> DDlog compiler
     */
    private static String ddlogRelationName(final String tableName) {
        return "R" + tableName.toLowerCase();
    }

    /*
     * The SQL -> DDlog compiler represents nullable fields as ddlog Option<> types. We therefore
     * wrap DDlogRecords if needed.
     */
    private static DDlogRecord maybeOption(final Boolean isNullable, final DDlogRecord record) {
        if (isNullable) {
            try {
                final DDlogRecord[] arr = new DDlogRecord[1];
                arr[0] = record;
                return DDlogRecord.makeStruct(DDLOG_SOME, arr);
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
        if (record.isStruct() && record.getStructName().equals(DDLOG_NONE)) {
            return null;
        }
        if (record.isStruct() && record.getStructName().equals(DDLOG_SOME)) {
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
                throw new RuntimeException("Unknown datatype " + cls.getName());
        }
    }
}