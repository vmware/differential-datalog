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
import com.facebook.presto.sql.tree.ComparisonExpression;
import com.facebook.presto.sql.tree.Delete;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.Identifier;
import com.facebook.presto.sql.tree.Insert;
import com.facebook.presto.sql.tree.Literal;
import com.facebook.presto.sql.tree.LogicalBinaryExpression;
import com.facebook.presto.sql.tree.LongLiteral;
import com.facebook.presto.sql.tree.Parameter;
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
import org.jooq.DataType;
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
import java.sql.Types;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

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
 *   A3. "delete from T where P1 = A and P2 = B..." where T is a base table and P1, P2... are columns in T's
 *                                         primary key. That is, there should be a corresponding
 *                                        "create table T..." DDL statement that is passed to the DDlogJooqProvider
 *                                         where P1, P2... etc are columns in T's primary key.
 */
public final class DDlogJooqProvider implements MockDataProvider {
    private static final String DDLOG_SOME = "ddlog_std::Some";
    private static final String DDLOG_NONE = "ddlog_std::None";
    private static final Object[] DEFAULT_BINDING = new Object[0];
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
    private final Map<String, Set<Record>> materializedViews = new ConcurrentHashMap<>();

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
            final Object[][] bindings = ctx.batchBindings();
            for (int i = 0; i < batchSql.length; i++) {
                final Object[] binding = bindings != null && bindings.length > i ? bindings[i] : DEFAULT_BINDING;
                final QueryContext context = new QueryContext(batchSql[i], binding);
                mock[i] = executeOne(context);
            }
            dDlogAPI.transactionCommitDumpChanges(this::onChange);
        } catch (final DDlogException | RuntimeException e) {
            try {
                dDlogAPI.transactionRollback();
            } catch (DDlogException rollbackFailed) {
                throw new RuntimeException(rollbackFailed);
            }
        }
        return mock;
    }

    private void onChange(final DDlogCommand<DDlogRecord> command) {
        try {
            final int relationId = command.relid();
            final String relationName = dDlogAPI.getTableName(relationId);
            final String tableName = relationNameToTableName(relationName);
            final List<Field<?>> fields = tablesToFields.get(tableName);
            final DDlogRecord record = command.value();
            final Record jooqRecord = dslContext.newRecord(fields);
            for (int i = 0; i < fields.size(); i++) {
                structToValue(fields.get(i), record.getStructField(i), jooqRecord);
            }
            final Set<Record> materializedView = materializedViews.computeIfAbsent(tableName, (k) -> new LinkedHashSet<>());
            switch (command.kind()) {
                case Insert:
                    materializedView.add(jooqRecord);
                    break;
                case DeleteKey:
                    throw new RuntimeException("Did not expect DeleteKey command type");
                case DeleteVal:
                    materializedView.remove(jooqRecord);
                    break;
            }
        } catch (DDlogException ex) {
            throw new RuntimeException(ex);
        }
    }

    private MockResult executeOne(final QueryContext context) throws SQLException {
        final Statement statement = parser.createStatement(context.sql(), options);
        final MockResult result = queryVisitor.process(statement, context);
        if (result == null) {
            throw new SQLException("Could not execute SQL statement " + context);
        }
        return result;
    }

    /*
     * Visits an SQL query and converts into a JOOQ MockResult type.
     */
    private class QueryVisitor extends AstVisitor<MockResult, QueryContext> {
        @Override
        protected MockResult visitQuerySpecification(final QuerySpecification node, final QueryContext context) {
            // The checks below encode assumption A1 (see javadoc for the DDlogJooqProvider class)
            final Select select = node.getSelect();
            if (!(select.getSelectItems().size() == 1
                    && select.getSelectItems().get(0) instanceof AllColumns
                    && node.getFrom().isPresent()
                    && node.getFrom().get() instanceof com.facebook.presto.sql.tree.Table)) {
                throw new RuntimeException("Statement not supported: " + context.sql());
            }
            final String tableName = ((com.facebook.presto.sql.tree.Table) node.getFrom().get()).getName().toString();
            final List<Field<?>> fields = tablesToFields.get(tableName.toUpperCase());
            if (fields == null) {
                throw new RuntimeException(String.format("Unknown table %s queried in statement: %s", tableName,
                                           context.sql()));
            }
            final Result<Record> result = dslContext.newResult(fields);
            result.addAll(materializedViews.computeIfAbsent(tableName.toUpperCase(), (k) -> new LinkedHashSet<>()));
            return new MockResult(1, result);
        }

        @Override
        protected MockResult visitQuery(final Query node, final QueryContext context) {
            final QuerySpecification specification = (QuerySpecification) node.getQueryBody();
            return visitQuerySpecification(specification, context);
        }

        @Override
        protected MockResult visitInsert(final Insert node, final QueryContext context) {
            try {
                // The assertions below encode assumption A2 (see javadoc for the DDlogJooqProvider class)
                if (!(node.getQuery().getQueryBody() instanceof Values)) {
                    throw new RuntimeException("Statement not supported: " + context.sql());
                }
                final Values values = (Values) node.getQuery().getQueryBody();
                final String tableName = node.getTarget().toString();
                final List<Field<?>> fields = tablesToFields.get(tableName.toUpperCase());
                final int tableId = dDlogAPI.getTableId(ddlogRelationName(tableName));
                for (final Expression row: values.getRows()) {
                    if (!(row instanceof Row)) {
                        throw new RuntimeException("Statement not supported: " + context.sql());
                    }
                    final List<Expression> items = ((Row) row).getItems();
                    if (items.size() != fields.size()) {
                        final String error = String.format("Incorrect row size for insertion into table %s. " +
                                                   "Please specify all the table's fields in their declared order: %s",
                                                    tableName, context.sql());
                        throw new RuntimeException(error);
                    }
                    final DDlogRecord[] recordsArray = new DDlogRecord[items.size()];
                    if (context.hasBinding()) {
                        // Is a statement with bound variables
                        for (int i = 0; i < items.size(); i++) {
                            final boolean isNullableField = fields.get(i).getDataType().nullable();
                            final DDlogRecord record = toValue(fields.get(i), context.nextBinding());
                            recordsArray[i] = maybeOption(isNullableField, record);
                        }
                    }
                    else {
                        // need to parse literals into DDLogRecords
                        for (int i = 0; i < items.size(); i++) {
                            final boolean isNullableField = fields.get(i).getDataType().nullable();
                            recordsArray[i] = parseLiterals.process(items.get(i), isNullableField);
                        }
                    }
                    final DDlogRecord record = DDlogRecord.makeStruct(ddlogTableTypeName(tableName), recordsArray);
                    final DDlogRecCommand command = new DDlogRecCommand(DDlogCommand.Kind.Insert, tableId, record);
                    dDlogAPI.applyUpdates(new DDlogRecCommand[]{command});
                }
                final Result<Record1<Integer>> result = dslContext.newResult(updateCountField);
                final Record1<Integer> resultRecord = dslContext.newRecord(updateCountField);
                resultRecord.setValue(updateCountField, values.getRows().size());
                result.add(resultRecord);
                return new MockResult(values.getRows().size(), result);
            } catch (DDlogException e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        protected MockResult visitDelete(final Delete node, final QueryContext context) {
            // The assertions below, and in the ParseWhereClauseForDeletes visitor encode assumption A3
            // (see javadoc for the DDlogJooqProvider class)
            final String tableName = node.getTable().getName().toString();
            if (!node.getWhere().isPresent()) {
                throw new RuntimeException("Delete queries without where clauses are unsupported: " + context.sql());
            }
            try {
                final Expression where = node.getWhere().get();
                final ParseWhereClauseForDeletes visitor = new ParseWhereClauseForDeletes(tableName);
                visitor.process(where, context);
                final DDlogRecord[] matchExpression = visitor.matchExpressions;
                final DDlogRecord record = matchExpression.length > 1 ? DDlogRecord.makeTuple(matchExpression)
                                                                      : matchExpression[0];

                final int tableId = dDlogAPI.getTableId(ddlogRelationName(tableName));
                final DDlogRecCommand command = new DDlogRecCommand(DDlogCommand.Kind.DeleteKey, tableId, record);
                dDlogAPI.applyUpdates(new DDlogRecCommand[]{command});
            } catch (final DDlogException e) {
                throw new RuntimeException(e);
            }
            final Result<Record1<Integer>> result = dslContext.newResult(updateCountField);
            final Record1<Integer> resultRecord = dslContext.newRecord(updateCountField);
            resultRecord.setValue(updateCountField, 1);
            result.add(resultRecord);
            return new MockResult(1, result);
        }
    }

    private class ParseWhereClauseForDeletes extends AstVisitor<Void, QueryContext> {
        final DDlogRecord[] matchExpressions;
        final String tableName;

        public ParseWhereClauseForDeletes(final String tableName) {
            this.tableName = tableName;
            matchExpressions = new DDlogRecord[tablesToPrimaryKeys.get(tableName.toUpperCase()).size()];
        }

        @Override
        protected Void visitLogicalBinaryExpression(final LogicalBinaryExpression node, final QueryContext context) {
            if (!node.getOperator().equals(LogicalBinaryExpression.Operator.AND)) {
                throw new RuntimeException("Only equality-based comparisons on " +
                        "all (not some) primary-key columns are allowed: " + node);
            }
            return super.visitLogicalBinaryExpression(node, context);
        }

        @Override
        protected Void visitComparisonExpression(final ComparisonExpression node, final QueryContext context) {
            final Expression left = node.getLeft();
            final Expression right = node.getRight();
            if (context.hasBinding()) {
                if (left instanceof Identifier && right instanceof Parameter) {
                    setMatchExpression((Identifier) left, context.nextBinding());
                    return null;
                } else if (right instanceof Identifier && left instanceof Parameter) {
                    setMatchExpression((Identifier) right, context.nextBinding());
                    return null;
                }
            } else {
                if (left instanceof Identifier && right instanceof Literal) {
                    setMatchExpression((Identifier) left, (Literal) right);
                    return null;
                } else if (right instanceof Identifier && left instanceof Literal) {
                    setMatchExpression((Identifier) right, (Literal) left);
                    return null;
                }
            }
            throw new RuntimeException("Unexpected comparison expression: "+ node);
        }

        private void setMatchExpression(final Identifier identifier, final Literal literal) {
            final List<? extends Field<?>> fields = tablesToPrimaryKeys.get(tableName.toUpperCase());

            /*
             * The match-expressions correspond to each column in the primary key, in the same
             * order as the primary key declaration in the SQL create table statement.
             */
            for (int i = 0; i < fields.size(); i++) {
                if (fields.get(i).getUnqualifiedName().last().equalsIgnoreCase(identifier.getValue())) {
                    matchExpressions[i] = parseLiterals.process(literal, fields.get(i).getDataType().nullable());
                    return;
                }
            }
            throw new RuntimeException(String.format("Field %s being queried is not a primary key in table %s",
                                                      identifier, tableName));
        }

        private void setMatchExpression(final Identifier identifier, final Object parameter) {
            final List<? extends Field<?>> fields = tablesToPrimaryKeys.get(tableName.toUpperCase());

            /*
             * The match-expressions correspond to each column in the primary key, in the same
             * order as the primary key declaration in the SQL create table statement.
             */
            for (int i = 0; i < fields.size(); i++) {
                if (fields.get(i).getUnqualifiedName().last().equalsIgnoreCase(identifier.getValue())) {
                    matchExpressions[i] = toValue(fields.get(i), parameter);
                    return;
                }
            }
            throw new RuntimeException(String.format("Field %s being queried is not a primary key in table %s",
                    identifier, tableName));
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
     * This corresponds to the naming convention followed by the SQL -> DDlog compiler
     */
    private static String relationNameToTableName(final String relationName) {
        return relationName.substring(1).toUpperCase();
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
    private static void structToValue(final Field<?> field, final DDlogRecord record, final Record jooqRecord) throws DDlogException {
        final boolean isStruct = record.isStruct();
        if (isStruct) {
            final String structName = record.getStructName();
            if (structName.equals(DDLOG_NONE)) {
                jooqRecord.setValue(field, null);
                return;
            }
            if (structName.equals(DDLOG_SOME)) {
                setValue(field, record.getStructField(0), jooqRecord);
                return;
            }
        }
        setValue(field, record, jooqRecord);
    }

    private static DDlogRecord toValue(final Field<?> field, final Object in) {
        final DataType<?> dataType = field.getDataType();
        switch (dataType.getSQLType()) {
            case Types.BOOLEAN:
                return new DDlogRecord((boolean) in);
            case Types.INTEGER:
                return new DDlogRecord((int) in);
            case Types.BIGINT:
                return new DDlogRecord((long) in);
            case Types.VARCHAR:
                try {
                    return new DDlogRecord((String) in);
                } catch (final DDlogException e) {
                    throw new RuntimeException("Could not create String DDlogRecord for object: " + in);
                }
            default:
                throw new RuntimeException("Unknown datatype " + field);
        }
    }

    private static void setValue(final Field<?> field, final DDlogRecord in, final Record out) {
        final DataType<?> dataType = field.getDataType();
        switch (dataType.getSQLType()) {
            case Types.BOOLEAN:
                out.setValue((Field<Boolean>) field, in.getBoolean());
                return;
            case Types.INTEGER:
                out.setValue((Field<Integer>) field, in.getInt().intValue());
                return;
            case Types.BIGINT:
                out.setValue((Field<Long>) field, in.getInt().longValue());
                return;
            case Types.VARCHAR:
                out.setValue((Field<String>) field, in.getString());
                return;
            default:
                throw new RuntimeException("Unknown datatype " + field);
        }
    }

    private static final class QueryContext {
        final String sql;
        final Object[] binding;
        int bindingIndex = 0;

        QueryContext(final String sql, final Object[] binding) {
            this.sql = sql;
            this.binding = binding;
        }

        public String sql() {
            return sql;
        }

        public Object nextBinding() {
            final Object ret = binding[bindingIndex];
            bindingIndex++;
            return ret;
        }

        public boolean hasBinding() {
            return binding.length > 0;
        }
    }
}