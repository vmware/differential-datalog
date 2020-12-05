/*
 * Copyright 2018-2020 VMware, Inc. All Rights Reserved.
 * SPDX-License-Identifier: BSD-2
 */
package com.vmware.ddlog;

import com.facebook.presto.sql.parser.ParsingOptions;
import com.facebook.presto.sql.tree.Statement;
import ddlogapi.DDlogAPI;
import ddlogapi.DDlogCommand;
import ddlogapi.DDlogException;
import ddlogapi.DDlogRecCommand;
import ddlogapi.DDlogRecord;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlDelete;
import org.apache.calcite.sql.SqlDynamicParam;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlInsert;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.SqlUpdate;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.util.SqlBasicVisitor;
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

import java.sql.SQLException;
import java.sql.Types;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
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
 *  A4. "update T set C1 = A, C2 = B where P1 = A..." where T is a base table and P1, P2... are columns in T's
 *                                          primary key. That is, there should be a corresponding
 *                                          "create table T..." DDL statement that is passed to the DDlogJooqProvider
 *                                          where P1, P2... etc are columns in T's primary key. C1, C2 etc can
 *                                          be any column in T.
 */
public final class DDlogJooqProvider implements MockDataProvider {
    private static final String DDLOG_SOME = "ddlog_std::Some";
    private static final String DDLOG_NONE = "ddlog_std::None";
    private static final Object[] DEFAULT_BINDING = new Object[0];
    private static final ParseLiterals PARSE_LITERALS = new ParseLiterals();
    private final DDlogAPI dDlogAPI;
    private final DSLContext dslContext;
    private final Field<Integer> updateCountField;
    private final Map<String, List<Field<?>>> tablesToFields = new HashMap<>();
    private final Map<String, List<? extends Field<?>>> tablesToPrimaryKeys = new HashMap<>();
    private final Map<String, Set<Record>> materializedViews = new ConcurrentHashMap<>();

    public DDlogJooqProvider(final DDlogAPI dDlogAPI, final List<String> sqlStatements) {
        this.dDlogAPI = dDlogAPI;
        this.dslContext = DSL.using("jdbc:h2:mem:");
        this.updateCountField = field("UPDATE_COUNT", Integer.class);

        // We translate DDL statements from the Presto dialect to H2.
        // We then execute these statements in a temporary database so that JOOQ can extract useful metadata
        // that we will use later (for example, the record types for views).
        final com.facebook.presto.sql.parser.SqlParser parser = new com.facebook.presto.sql.parser.SqlParser();
        final ParsingOptions options = ParsingOptions.builder().build();
        final TranslateCreateTableDialect translateCreateTableDialect = new TranslateCreateTableDialect();
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
    public MockResult[] execute(final MockExecuteContext ctx) {
        final String[] batchSql = ctx.batchSQL();
        final MockResult[] mock = new MockResult[batchSql.length];
        try {
            dDlogAPI.transactionStart();
            final Object[][] bindings = ctx.batchBindings();
            for (int i = 0; i < batchSql.length; i++) {
                final Object[] binding = bindings != null && bindings.length > i ? bindings[i] : DEFAULT_BINDING;
                final QueryContext context = new QueryContext(batchSql[i], binding);
                final SqlParser parser = SqlParser.create(batchSql[i]);
                final SqlNode sqlNode = parser.parseStmt();
                mock[i] = sqlNode.accept(new QueryVisitor(context));
            }
            dDlogAPI.transactionCommitDumpChanges(this::onChange);
        } catch (final DDlogException | SqlParseException e) {
            rollback();
        }
        return mock;
    }

    public Result<Record> fetchTable(final String tableName) {
        final List<Field<?>> fields = tablesToFields.get(tableName.toUpperCase());
        if (fields == null) {
            throw new DDlogJooqProviderException(String.format("Unknown table %s queried", tableName));
        }
        final Result<Record> result = dslContext.newResult(fields);
        result.addAll(materializedViews.computeIfAbsent(tableName.toUpperCase(), (k) -> new LinkedHashSet<>()));
        return result;
    }

    private void rollback() {
        try {
            dDlogAPI.transactionRollback();
        } catch (final DDlogException e) {
            throw new DDlogJooqProviderException(e);
        }
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

    private final class QueryVisitor extends SqlBasicVisitor<MockResult> {
        private final QueryContext context;

        QueryVisitor(final QueryContext context) {
            this.context = context;
        }

        @Override
        public MockResult visit(final SqlCall call) {
            switch (call.getKind()) {
                case SELECT:
                    return visitSelect(call);
                case INSERT:
                    return visitInsert(call);
                case DELETE:
                    return visitDelete(call);
                case UPDATE:
                    return visitUpdate(call);
                default:
                    return exception(call.toString());
            }
        }

        private MockResult visitSelect(final SqlCall call) {
            // The checks below encode assumption A1 (see javadoc for the DDlogJooqProvider class)
            final SqlSelect select = (SqlSelect) call;
            if (!(select.getSelectList().size() == 1
                    && select.getSelectList().get(0).toString().equals("*"))) {
                return exception("Statement not supported: " + context.sql());
            }
            final String tableName = ((SqlIdentifier) select.getFrom()).getSimple();
            final Result<Record> result = fetchTable(tableName);
            return new MockResult(1, result);
        }

        private MockResult visitInsert(final SqlCall call) {
            // The assertions below, and in the ParseWhereClauseForDeletes visitor encode assumption A2
            // (see javadoc for the DDlogJooqProvider class)
            final SqlInsert insert = (SqlInsert) call;
            if (insert.getSource().getKind() != SqlKind.VALUES) {
                return exception(call.toString());
            }
            final SqlNode[] values = ((SqlBasicCall) insert.getSource()).getOperands();
            final String tableName = ((SqlIdentifier) insert.getTargetTable()).getSimple();
            final List<Field<?>> fields = tablesToFields.get(tableName.toUpperCase());
            final int tableId = dDlogAPI.getTableId(ddlogRelationName(tableName));
            for (final SqlNode value: values) {
                if (value.getKind() != SqlKind.ROW) {
                    return exception(call.toString());
                }
                final SqlNode[] rowElements = ((SqlBasicCall) value).operands;
                final DDlogRecord[] recordsArray = new DDlogRecord[rowElements.length];
                if (context.hasBinding()) {
                    // Is a statement with bound variables
                    for (int i = 0; i < rowElements.length; i++) {
                        final boolean isNullableField = fields.get(i).getDataType().nullable();
                        final DDlogRecord record = toValue(fields.get(i), context.nextBinding());
                        recordsArray[i] = maybeOption(isNullableField, record);
                    }
                }
                else {
                    // need to parse literals into DDLogRecords
                    for (int i = 0; i < rowElements.length; i++) {
                        final boolean isNullableField = fields.get(i).getDataType().nullable();
                        final DDlogRecord result = rowElements[i].accept(PARSE_LITERALS);
                        recordsArray[i] = maybeOption(isNullableField, result);
                    }
                }
                try {
                    final DDlogRecord record = DDlogRecord.makeStruct(ddlogTableTypeName(tableName), recordsArray);
                    final DDlogRecCommand command = new DDlogRecCommand(DDlogCommand.Kind.Insert, tableId, record);
                    dDlogAPI.applyUpdates(new DDlogRecCommand[]{command});
                } catch (final DDlogException e) {
                    return exception(e);
                }
            }
            final Result<Record1<Integer>> result = dslContext.newResult(updateCountField);
            final Record1<Integer> resultRecord = dslContext.newRecord(updateCountField);
            resultRecord.setValue(updateCountField, values.length);
            result.add(resultRecord);
            return new MockResult(values.length, result);
        }

        private MockResult visitDelete(final SqlCall call) {
            // The assertions below, and in the ParseWhereClauseForDeletes visitor encode assumption A3
            // (see javadoc for the DDlogJooqProvider class)
            final SqlDelete delete = (SqlDelete) call;
            final String tableName = ((SqlIdentifier) delete.getTargetTable()).getSimple();
            if (delete.getCondition() == null) {
                return exception("Delete queries without where clauses are unsupported: " + context.sql());
            }
            try {
                final SqlBasicCall where = (SqlBasicCall) delete.getCondition();
                final List<? extends Field<?>> pkFields = tablesToPrimaryKeys.get(tableName.toUpperCase());
                final DDlogRecord record = matchExpressionFromWhere(where, pkFields, context);
                final int tableId = dDlogAPI.getTableId(ddlogRelationName(tableName));
                final DDlogRecCommand command = new DDlogRecCommand(DDlogCommand.Kind.DeleteKey, tableId, record);
                dDlogAPI.applyUpdates(new DDlogRecCommand[]{command});
            } catch (final DDlogException e) {
                return exception(e);
            }
            final Result<Record1<Integer>> result = dslContext.newResult(updateCountField);
            final Record1<Integer> resultRecord = dslContext.newRecord(updateCountField);
            resultRecord.setValue(updateCountField, 1);
            result.add(resultRecord);
            return new MockResult(1, result);
        }

        private MockResult visitUpdate(final SqlCall call) {
            // The assertions below, and in the ParseWhereClauseForDeletes visitor encode assumption A4
            // (see javadoc for the DDlogJooqProvider class)
            final SqlUpdate update = (SqlUpdate) call;
            final String tableName = ((SqlIdentifier) update.getTargetTable()).getSimple();
            if (update.getCondition() == null) {
                return exception("Delete queries without where clauses are unsupported: " + context.sql());
            }
            try {
                final List<? extends Field<?>> allFields = tablesToFields.get(tableName.toUpperCase());
                final int numColumnsToUpdate = update.getTargetColumnList().size();
                final DDlogRecord[] updatedValues = new DDlogRecord[numColumnsToUpdate];
                final String[] columnsToUpdate = new String[numColumnsToUpdate];
                for (int i = 0; i < numColumnsToUpdate; i++) {
                    final String columnName = ((SqlIdentifier) update.getTargetColumnList().get(i)).getSimple()
                            .toLowerCase();
                    final Field<?> field = allFields.stream()
                            .filter(f -> f.getUnqualifiedName().last().equalsIgnoreCase(columnName))
                            .findFirst()
                            .get();
                    final boolean isNullableField = field.getDataType().nullable();
                    final DDlogRecord valueToUpdateTo = context.hasBinding()
                            ? toValue(field, context.nextBinding())
                            : update.getSourceExpressionList().accept(PARSE_LITERALS);
                    final DDlogRecord maybeWrapped = maybeOption(isNullableField, valueToUpdateTo);
                    updatedValues[i] = maybeWrapped;
                    columnsToUpdate[i] = columnName;
                }

                final SqlBasicCall where = (SqlBasicCall) update.getCondition();
                final List<? extends Field<?>> pkFields = tablesToPrimaryKeys.get(tableName.toUpperCase());
                final DDlogRecord key = matchExpressionFromWhere(where, pkFields, context);

                final DDlogRecord updateRecord = DDlogRecord.makeNamedStruct("", columnsToUpdate, updatedValues);
                final int tableId = dDlogAPI.getTableId(ddlogRelationName(tableName));
                final DDlogRecCommand command = new DDlogRecCommand(DDlogCommand.Kind.Modify, tableId, key, updateRecord);
                dDlogAPI.applyUpdates(new DDlogRecCommand[]{command});
            } catch (final DDlogException e) {
                return exception(e);
            }
            final Result<Record1<Integer>> result = dslContext.newResult(updateCountField);
            final Record1<Integer> resultRecord = dslContext.newRecord(updateCountField);
            resultRecord.setValue(updateCountField, 1);
            result.add(resultRecord);
            return new MockResult(1, result);
        }
    }

    private static DDlogRecord matchExpressionFromWhere(final SqlBasicCall where,
                                                        final List<? extends Field<?>> pkFields,
                                                        final QueryContext context) throws DDlogException {
        final WhereClauseToMatchExpression visitor = new WhereClauseToMatchExpression(pkFields, context);
        final DDlogRecord[] matchExpression = where.accept(visitor);
        return matchExpression.length > 1 ? DDlogRecord.makeTuple(matchExpression)
                                                              : matchExpression[0];
    }

    private static final class WhereClauseToMatchExpression extends SqlBasicVisitor<DDlogRecord[]> {
        private final DDlogRecord[] matchExpressions;
        private final QueryContext context;
        private final List<? extends Field<?>> pkFields;

        public WhereClauseToMatchExpression(final List<? extends Field<?>> pkFields, final QueryContext context) {
            this.context = context;
            this.pkFields = pkFields;
            this.matchExpressions = new DDlogRecord[pkFields.size()];
        }

        @Override
        public DDlogRecord[] visit(final SqlCall call) {
            final SqlBasicCall expr = (SqlBasicCall) call;
            switch (expr.getOperator().getKind()) {
                case AND:
                    return super.visit(call);
                case EQUALS:
                    final SqlNode left = expr.getOperands()[0];
                    final SqlNode right = expr.getOperands()[1];
                    if (context.hasBinding()) {
                        if (left instanceof SqlIdentifier && right instanceof SqlDynamicParam) {
                            return setMatchExpression((SqlIdentifier) left, context.nextBinding());
                        } else if (right instanceof SqlIdentifier && left instanceof SqlDynamicParam) {
                            return setMatchExpression((SqlIdentifier) right, context.nextBinding());
                        }
                    } else {
                        if (left instanceof SqlIdentifier && right instanceof SqlLiteral) {
                            return setMatchExpression((SqlIdentifier) left, (SqlLiteral) right);
                        } else if (right instanceof SqlIdentifier && left instanceof SqlLiteral) {
                            return setMatchExpression((SqlIdentifier) right, (SqlLiteral) left);
                        }
                    }
                    throw new DDlogJooqProviderException("Unexpected comparison expression: " + call);
                default:
                    throw new DDlogJooqProviderException("Unsupported expression in where clause "+ call);
            }
        }

        private DDlogRecord[] setMatchExpression(final SqlIdentifier identifier, final SqlLiteral literal) {
            /*
             * The match-expressions correspond to each column in the primary key, in the same
             * order as the primary key declaration in the SQL create table statement.
             */
            for (int i = 0; i < pkFields.size(); i++) {
                if (pkFields.get(i).getUnqualifiedName().last().equalsIgnoreCase(identifier.getSimple())) {
                    final boolean isNullable = pkFields.get(i).getDataType().nullable();
                    matchExpressions[i] = maybeOption(isNullable, literal.accept(PARSE_LITERALS));
                    return matchExpressions;
                }
            }
            throw new DDlogJooqProviderException(String.format("Field %s being queried is not a primary key",
                                                 identifier));
        }

        private DDlogRecord[] setMatchExpression(final SqlIdentifier identifier, final Object parameter) {
            /*
             * The match-expressions correspond to each column in the primary key, in the same
             * order as the primary key declaration in the SQL create table statement.
             */
            for (int i = 0; i < pkFields.size(); i++) {
                if (pkFields.get(i).getUnqualifiedName().last().equalsIgnoreCase(identifier.getSimple())) {
                    matchExpressions[i] = toValue(pkFields.get(i), parameter);
                    return matchExpressions;
                }
            }
            throw new DDlogJooqProviderException(String.format("Field %s being queried is not a primary key",
                                                 identifier));
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
                return DDlogRecord.makeStruct(DDLOG_SOME, record);
            } catch (final DDlogException e) {
                throw new DDlogJooqProviderException(e);
            }
        } else {
            return record;
        }
    }

    private static void structToValue(final Field<?> field, final DDlogRecord record, final Record jooqRecord) throws DDlogException {
        final boolean isStruct = record.isStruct();
        if (isStruct) {
            final String structName = record.getStructName();
            if (structName.equals(DDLOG_NONE)) {
                jooqRecord.setValue(field, null);
                return;
            }
            if (structName.equals(DDLOG_SOME)) {
                setValue(field, getStructField(record, 0), jooqRecord);
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
                    throw new DDlogJooqProviderException("Could not create String DDlogRecord for object: " + in);
                }
            default:
                throw new DDlogJooqProviderException("Unknown datatype " + field);
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
                throw new DDlogJooqProviderException("Unknown datatype " + field);
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

    private String getTableName(final int relationId) {
        try {
            return dDlogAPI.getTableName(relationId);
        } catch (final DDlogException e) {
            throw new DDlogJooqProviderException(e);
        }
    }

    private static DDlogRecord getStructField(final DDlogRecord record, final int fieldIndex) {
        try {
            return record.getStructField(fieldIndex);
        } catch (final DDlogException e) {
            throw new DDlogJooqProviderException(e);
        }
    }

    public static final class DDlogJooqProviderException extends RuntimeException {
        private DDlogJooqProviderException(final Throwable e) {
            super(e);
        }

        private DDlogJooqProviderException(final String msg) {
            super(msg);
        }
    }

    private static MockResult exception(final String msg) {
        return new MockResult(new SQLException(msg));
    }

    private static MockResult exception(final Throwable e) {
        return new MockResult(new SQLException(e));
    }
}