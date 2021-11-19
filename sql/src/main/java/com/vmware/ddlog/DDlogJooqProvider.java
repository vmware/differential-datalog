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

import com.vmware.ddlog.util.sql.CreateIndexParser;
import com.vmware.ddlog.util.sql.H2SqlStatement;
import ddlogapi.*;
import org.apache.calcite.sql.*;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.util.SqlBasicVisitor;
import org.jooq.*;
import org.jooq.Record;
import org.jooq.impl.DSL;
import org.jooq.tools.jdbc.MockDataProvider;
import org.jooq.tools.jdbc.MockExecuteContext;
import org.jooq.tools.jdbc.MockResult;

import java.sql.SQLException;
import java.sql.Types;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

import static org.jooq.impl.DSL.field;

/**
 * This class provides a restricted mechanism to make a DDlog program appear like an SQL database that can be
 * queried over a JDBC connection, which only takes SQL statements in the H2 dialect.
 * To initialize, it requires a set of "create table" and "create view" statements
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
 *
 * If the appropriate indexes have been created in the DDlog program during initialization, then the following queries
 * are also supported:
 *   I1. "select * from T where P1 = A and P2 = B..." where T is a base table and P1, P2... are columns in some index
 *                                          on T. That is, there should be a corresponding "create index T_idx on T ..."
 *                                          DDL statement that is passed to the DDlogJooqProvider
 *                                          where P1, P2... etc are columns in T_idx.
 *   I2. "delete from T where P1 = A and P2 = B..." where T is a base table and P1, P2... are columns in some index
 *                                          on T. That is, there should be a corresponding "create index T_idx on T ..."
 *                                          DDL statement that is passed to the DDlogJooqProvider
 *                                          where P1, P2... etc are columns in T_idx.
 *
 */
public final class DDlogJooqProvider implements MockDataProvider {
    static final boolean debug = false;

    private static final String IDENTITY_VIEW_SUFFIX = "_view";
    private static final String DDLOG_SOME = "ddlog_std::Some";
    private static final String DDLOG_NONE = "ddlog_std::None";
    private static final Object[] DEFAULT_BINDING = new Object[0];
    private static final ParseLiterals PARSE_LITERALS = new ParseLiterals();
    private final DDlogHandle ddlogHandle;
    private final DSLContext dslContext;
    private final Field<Integer> updateCountField;
    //private final Map<String, Table<?>> tablesToJooqTable = new HashMap<>();
    private final Map<DDlogJooqHelper.NormalizedTableName, List<Field<?>>> tablesToFields = new HashMap<>();
    private final Map<DDlogJooqHelper.NormalizedTableName, Map<String, Field<?>>> tablesToFieldMap = new HashMap<>();
    private final Map<DDlogJooqHelper.NormalizedTableName, List<? extends Field<?>>> tablesToPrimaryKeys = new HashMap<>();
    private final Map<DDlogJooqHelper.NormalizedTableName, Set<Record>> materializedViews = new ConcurrentHashMap<>();
    private final Set<DDlogJooqHelper.NormalizedTableName> inputTableNames = new HashSet<>();
    private final DDlogJooqHelper whereHelper;
    public static boolean trace = false;

    public DDlogJooqProvider(final DDlogHandle handle, final List<H2SqlStatement> sqlStatements) {
        this.ddlogHandle = handle;
        this.dslContext = DSL.using("jdbc:h2:mem:");
        this.updateCountField = field("UPDATE_COUNT", Integer.class);

        this.whereHelper = new DDlogJooqHelper(tablesToFields, handle);

        // We execute H2 statements in a temporary database so that JOOQ can extract useful metadata
        // that we will use later (for example, the record types for views).
        for (final H2SqlStatement sql : sqlStatements) {
            String sqlString = sql.getStatement();
            if (CreateIndexParser.isCreateIndexStatement(sqlString)) {
                this.whereHelper.addIndex(sqlString);
            }
            dslContext.execute(sqlString);
        }

        for (final Table<?> table: dslContext.meta().getTables()) {
            if (table.getSchema().getName().equals("PUBLIC")) { // H2-specific assumption
                DDlogJooqHelper.NormalizedTableName tn = new DDlogJooqHelper.NormalizedTableName(table.getName());
                Arrays.stream(table.fields()).forEach(
                        (field) -> tablesToFieldMap.computeIfAbsent(tn,
                                (k) -> new HashMap<>())
                                .put(field.getUnqualifiedName().last().toUpperCase(), field)
                );
                tablesToFields.put(tn, Arrays.asList(table.fields()));
                //tablesToJooqTable.put(table.getName(), table);
                if (table.getPrimaryKey() != null) {
                    tablesToPrimaryKeys.put(tn, table.getPrimaryKey().getFields());
                }
                if (table.getType().equals(TableOptions.TableType.TABLE)) {
                    inputTableNames.add(tn);
                }
            }
        }
        this.whereHelper.seal();
    }

    public static String toIdentityViewName(String inputTableName) {
        return String.format("%s_%s", inputTableName, IDENTITY_VIEW_SUFFIX);
    }

    public DSLContext getDslContext() {
        return dslContext;
    }

    /*
     * All executed SQL queries against a JOOQ connection are received here
     */
    @Override
    public MockResult[] execute(final MockExecuteContext ctx) {
        final String[] batchSql = ctx.batchSQL();
        final MockResult[] mock = new MockResult[batchSql.length];
        int commandIndex = 0;
        try {
            if (trace)
                System.out.println("Staring transaction: " + ctx.sql());
            ddlogHandle.transactionStart();
            if (trace)
                System.out.println("Transaction started");
            final Object[][] bindings = ctx.batchBindings();
            for (commandIndex = 0; commandIndex < batchSql.length; commandIndex++) {
                final Object[] binding = bindings != null && bindings.length > commandIndex ? bindings[commandIndex] : DEFAULT_BINDING;
                final QueryContext context = new QueryContext(batchSql[commandIndex], binding);
                final SqlParser parser = SqlParser.create(batchSql[commandIndex]);
                final SqlNode sqlNode = parser.parseStmt();
                mock[commandIndex] = sqlNode.accept(new QueryVisitor(context));
            }
            ddlogHandle.transactionCommitDumpChanges(this::onChange);
            if (trace)
                System.out.println("Transaction committed");
        } catch (final Exception e) {
            // We really have to catch all exceptions here to rollback, otherwise
            // we could be left with a started and unterminated transaction.
            if (trace)
                System.out.println("Exception: " + e.getMessage());
            rollback();
            // Not clear that this is the result for all remaining commands,
            // but we cannot leave these null either.
            for (; commandIndex < batchSql.length; commandIndex++)
                mock[commandIndex] = exception(e);
        }
        return mock;
    }

    public Result<Record> fetchTable(final String tableName) {
        final DDlogJooqHelper.NormalizedTableName normalized = new DDlogJooqHelper.NormalizedTableName(tableName);
        return fetchTable(normalized);
    }

    private Result<Record> fetchTable(final DDlogJooqHelper.NormalizedTableName tableName) {
        // If the select is on an input table, redirect it to the corresponding view / output relation, if it exists
        // The user of this JooqProvider is responsible for creating these identity views
        final DDlogJooqHelper.NormalizedTableName maybeIdentityView =
                        inputTableNames.contains(tableName) ?
                        new DDlogJooqHelper.NormalizedTableName(DDlogJooqProvider.toIdentityViewName(tableName.name)) :
                        tableName;
        final List<Field<?>> fields = tablesToFields.get(maybeIdentityView);
        if (fields == null) {
            throw new DDlogJooqProviderException(String.format("Table %s does not exist", maybeIdentityView));
        }
        final Result<Record> result = dslContext.newResult(fields);
        result.addAll(materializedViews.computeIfAbsent(maybeIdentityView, (k) -> new LinkedHashSet<>()));
        return result;
    }

    private void rollback() {
        try {
            if (trace)
                System.out.println("Rolling back transaction");
            ddlogHandle.transactionRollback();
            if (trace)
                System.out.println("Transaction rolled back");
        } catch (final DDlogException e) {
            if (trace)
                System.out.println("Failed to rollback transaction: " + e.getMessage());
            throw new DDlogJooqProviderException(e);
        }
    }

    private void onChange(final DDlogCommand<DDlogRecord> command) {
        try {
            if (trace)
                System.out.println("Result: " + command);
            final int relationId = command.relid();
            final String relationName = ddlogHandle.getTableName(relationId);
            final DDlogJooqHelper.NormalizedTableName tableName =
                    new DDlogJooqHelper.NormalizedTableName(ddlogHandle.relationNameToTableName(relationName));
            final List<Field<?>> fields = tablesToFields.get(tableName);
            final DDlogRecord record = command.value();
            final Record jooqRecord = dslContext.newRecord(fields);
            for (int i = 0; i < fields.size(); i++) {
                setValue(fields.get(i), record.getStructField(i), jooqRecord);
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
            if (trace)
                System.out.println("Exception: " + ex);
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
            try {
                switch (call.getKind()) {
                    case SELECT:
                        return visitSelect((SqlSelect) call);
                    case INSERT:
                        return visitInsert((SqlInsert) call);
                    case DELETE:
                        return visitDelete((SqlDelete) call);
                    case UPDATE:
                        return visitUpdate((SqlUpdate) call);
                    default:
                        return exception(call.toString());
                }
            } catch (final DDlogException e) {
                return exception(e);
            }
        }

        private MockResult selectByIndex(String tableName, String[] ids, SqlBasicCall where) throws DDlogException, SQLException {
            final List<DDlogRecord> clonedResults = whereHelper.getEntriesByIndex(tableName, ids, where, context);

            if (clonedResults.size() == 0) {
                // There was nothing to delete
                return emptyMockResult();
            }

            // We want to return everything in clonedResults
            DDlogJooqHelper.NormalizedTableName tn = new DDlogJooqHelper.NormalizedTableName(tableName);
            final List<Field<?>> fields = tablesToFields.get(tn);
            if (fields == null) {
                throw new SQLException(String.format("Table %s does not exist", tableName));
            }
            final Result<Record> result = dslContext.newResult(fields);
            // Now add every record
            for (DDlogRecord r : clonedResults) {
                final Record jooqRecord = dslContext.newRecord(fields);
                for (int i = 0; i < fields.size(); i++) {
                    setValue(fields.get(i), r.getStructField(i), jooqRecord);
                }
                result.add(jooqRecord);
                r.release();
            }
            return new MockResult(1, result);
        }

        private MockResult visitSelect(final SqlSelect select) {
            // The checks below encode assumption A1 (see javadoc for the DDlogJooqProvider class)
            if (select.hasOrderBy() || select.isDistinct() || select.getHaving() != null
                || select.getGroup() != null) {
                return exception("Statement not supported: " + context.sql());
            }

            String tableName = ((SqlIdentifier) select.getFrom()).getSimple();
            final DDlogJooqHelper.NormalizedTableName tn = new DDlogJooqHelper.NormalizedTableName(tableName);

            // Selects must be on all columns of the table, either denoted by * or by an enumeration of all columns
            if (!((select.getSelectList().size() == 1
                    && select.getSelectList().get(0).toString().equals("*")
                    && select.getFrom() instanceof SqlIdentifier) ||
                    whereHelper.sameFields(select.getSelectList(), tableName)
            )
            ) {
                return exception("Statement not supported: " + context.sql());
            }

            if (!select.hasWhere()) {
                try {
                    final Result<Record> result = fetchTable(tn);
                    return new MockResult(1, result);
                } catch (final DDlogJooqProviderException e) {
                    return exception(e.getMessage());
                }
            }
            // If the select has a where clause, try to find an index to service the query.
            try {
                final SqlBasicCall where = (SqlBasicCall) select.getWhere();

                if (tablesToFields.get(tn) == null) {
                    return exception(String.format("Table %s does not exist: ", tn) + context.sql());
                }

                // Get fields requested in the where condition
                String[] ids = whereHelper.getIdsFromWhere(where);

                return selectByIndex(tableName, ids, where);
            } catch (final Exception e) {
                if (e.getMessage().contains("key not found"))
                    return emptyMockResult();
                return exception(e);
            }
        }

        private MockResult visitInsert(final SqlInsert insert) throws DDlogException {
            // The assertions below, and in the ParseWhereClauseForDeletes visitor encode assumption A2
            // (see javadoc for the DDlogJooqProvider class)
            if (insert.getSource().getKind() != SqlKind.VALUES || !(insert.getTargetTable() instanceof SqlIdentifier)) {
                return exception(insert.toString());
            }
            final SqlNode[] values = ((SqlBasicCall) insert.getSource()).getOperands();

            final String tableName = ((SqlIdentifier) insert.getTargetTable()).getSimple();
            DDlogJooqHelper.NormalizedTableName tn = new DDlogJooqHelper.NormalizedTableName(tableName);
            final List<Field<?>> fields = tablesToFields.get(tn);
            if (fields == null) {
                return exception(String.format("Table %s does not exist: ", tn) + context.sql());
            }
            final int tableId = ddlogHandle.getTableId(ddlogHandle.ddlogRelationName(tn.name));
            for (final SqlNode value: values) {
                if (value.getKind() != SqlKind.ROW) {
                    return exception(insert.toString());
                }
                final SqlNode[] rowElements = ((SqlBasicCall) value).operands;
                final DDlogRecord[] recordsArray = new DDlogRecord[rowElements.length];
                if (context.hasBinding()) {
                    // Is a statement with bound variables
                    for (int i = 0; i < rowElements.length; i++) {
                        Field<?> fi = fields.get(i);
                        final boolean isNullableField = fi.getDataType().nullable();
                        final DDlogRecord record = toValue(fi, context.nextBinding());
                        recordsArray[i] = maybeOption(isNullableField, record, fi.getName());
                    }
                } else {
                    // need to parse literals into DDLogRecords
                    for (int i = 0; i < rowElements.length; i++) {
                        Field<?> fi = fields.get(i);
                        final boolean isNullableField = fi.getDataType().nullable();
                        final DDlogRecord result = rowElements[i].accept(PARSE_LITERALS);
                        recordsArray[i] = maybeOption(isNullableField, result, fi.getName());
                    }
                }
                try {
                    final DDlogRecord record = DDlogRecord.makeStruct(ddlogHandle.ddlogTableTypeName(tableName), recordsArray);
                    final DDlogRecCommand command = new DDlogRecCommand(DDlogCommand.Kind.Insert, tableId, record);
                    ddlogHandle.applyUpdates(new DDlogRecCommand[]{command});
                } catch (final DDlogException e) {
                    return exception(e);
                }
            }
            return updateCountFieldResult();
        }

        private MockResult deleteByIndex(String tableName, String[] ids, SqlBasicCall where) throws DDlogException, SQLException {
            final List<DDlogRecord> clonedResults = whereHelper.getEntriesByIndex(tableName, ids, where, context);

            if (clonedResults.size() == 0) {
                // There was nothing to delete
                return emptyMockResult();
            }

            // Delete the records returned
            for (DDlogRecord r : clonedResults) {
                DDlogRecCommand command = new DDlogRecCommand(DDlogCommand.Kind.DeleteVal,
                        ddlogHandle.getTableId(ddlogHandle.ddlogRelationName(tableName)), r);
                ddlogHandle.applyUpdates(new DDlogRecCommand[]{command});
                // Now release the cloned command
                r.release();
            }
            return updateCountFieldResult();
        }

        private MockResult visitDelete(final SqlDelete delete) {
            // The assertions below, and in the ParseWhereClauseForDeletes visitor encode assumption A3
            // (see javadoc for the DDlogJooqProvider class)
            if (delete.getCondition() == null || !(delete.getTargetTable() instanceof SqlIdentifier)) {
                return exception("Delete queries without where clauses are not supported: " + context.sql());
            }

            final String tableName = ((SqlIdentifier) delete.getTargetTable()).getSimple();
            DDlogJooqHelper.NormalizedTableName tn = new DDlogJooqHelper.NormalizedTableName(tableName);
            final SqlBasicCall where = (SqlBasicCall) delete.getCondition();

            if (tablesToFields.get(tn) == null) {
                return exception(String.format("Table %s does not exist: ", tableName) + context.sql());
            }
            // Get fields requested in the where condition
            String[] ids = whereHelper.getIdsFromWhere(where);

            final List<? extends Field<?>> pkFields = tablesToPrimaryKeys.get(tn);

            try {
                // If a primary key for this table exists AND its columns match the columns request in the WHERE clause,
                // then use the primary key
                if (pkFields != null &&
                        DDlogJooqHelper.compareStringArrays(ids,
                                pkFields.stream().map(x -> x.getUnqualifiedName().unquotedName().toString()).toArray(String[]::new))) {
                    final DDlogRecord record = matchExpressionFromWhere(where, pkFields, context);
                    final int tableId = ddlogHandle.getTableId(ddlogHandle.ddlogRelationName(tableName));
                    final DDlogRecCommand command = new DDlogRecCommand(DDlogCommand.Kind.DeleteKey, tableId, record);
                    ddlogHandle.applyUpdates(new DDlogRecCommand[]{command});
                } else {
                    // If there are no PKs for this table, or if the PK given doesn't match the fields requested by this
                    // query, then we have to look in any indexes on the table.
                    return deleteByIndex(tableName, ids, where);
                }
            } catch (final Exception e) {
                if (e.getMessage().contains("key not found"))
                    return emptyMockResult();
                return exception(e);
            }
            return updateCountFieldResult();
        }

        private MockResult visitUpdate(final SqlUpdate update) {
            // The assertions below, and in the ParseWhereClauseForDeletes visitor encode assumption A4
            // (see javadoc for the DDlogJooqProvider class)
            if (update.getCondition() == null || !(update.getTargetTable() instanceof SqlIdentifier)) {
                return exception("Update queries without where clauses are not supported: " + context.sql());
            }
            try {
                final String tableName = ((SqlIdentifier) update.getTargetTable()).getSimple();
                final DDlogJooqHelper.NormalizedTableName tn = new DDlogJooqHelper.NormalizedTableName(tableName);
                final Map<String, Field<?>> allFields = tablesToFieldMap.get(tn);
                if (allFields == null) {
                    return exception(String.format("Table %s does not exist: ", tn) + context.sql());
                }
                final int numColumnsToUpdate = update.getTargetColumnList().size();
                final SqlNodeList targetColumnList = update.getTargetColumnList();
                final DDlogRecord[] updatedValues = new DDlogRecord[numColumnsToUpdate];
                final String[] columnsToUpdate = new String[numColumnsToUpdate];
                for (int i = 0; i < numColumnsToUpdate; i++) {
                    final String columnName = ((SqlIdentifier) targetColumnList.get(i)).getSimple()
                                                                                       .toLowerCase();
                    final Field<?> field = allFields.get(columnName.toUpperCase());
                    if (field == null)
                        return exception("Unknown column: " + columnName);
                    final boolean isNullableField = field.getDataType().nullable();
                    final DDlogRecord valueToUpdateTo = context.hasBinding()
                            ? toValue(field, context.nextBinding())
                            : update.getSourceExpressionList().accept(PARSE_LITERALS);
                    final DDlogRecord maybeWrapped = maybeOption(isNullableField, valueToUpdateTo, columnName);
                    updatedValues[i] = maybeWrapped;
                    columnsToUpdate[i] = columnName;
                }

                final SqlBasicCall where = (SqlBasicCall) update.getCondition();
                final List<? extends Field<?>> pkFields = tablesToPrimaryKeys.get(tn);
                final DDlogRecord key = matchExpressionFromWhere(where, pkFields, context);

                final DDlogRecord updateRecord = DDlogRecord.makeNamedStruct("", columnsToUpdate, updatedValues);
                final int tableId = ddlogHandle.getTableId(ddlogHandle.ddlogRelationName(tableName));
                final DDlogRecCommand command = new DDlogRecCommand(DDlogCommand.Kind.Modify, tableId, key, updateRecord);
                ddlogHandle.applyUpdates(new DDlogRecCommand[]{command});
            } catch (final DDlogException e) {
                return exception(e);
            }
            return updateCountFieldResult();
        }
    }

    static DDlogRecord matchExpressionFromWhere(final SqlBasicCall where,
                                                        final List<? extends Field<?>> pkFields,
                                                        final QueryContext context) throws DDlogException {
        final WhereClauseToMatchExpression visitor = new WhereClauseToMatchExpression(pkFields, context);
        final DDlogRecord[] matchExpression = where.accept(visitor);
        return matchExpression.length > 1 ? DDlogRecord.makeTuple(matchExpression)
                                                              : matchExpression[0];
    }

    /**
     * A visitor for WHERE conditions. Returns a list of DDlogRecords, one each corresponding to each match expression
     * found in the WHERE clause.
     */
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
                    super.visit(call);
                    return matchExpressions;
                case EQUALS: {
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
                }
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
                    matchExpressions[i] = maybeOption(isNullable, literal.accept(PARSE_LITERALS), pkFields.get(i).getName());
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
     * The SQL -> DDlog compiler represents nullable fields as ddlog Option<> types. We therefore
     * wrap DDlogRecords if needed.  Returns None for a nullptr record, or Some{record} otherwise.
     * @param field name of field that will store the value
     */
    private static DDlogRecord maybeOption(final Boolean isNullable, final DDlogRecord record, String field) {
        if (isNullable) {
            try {
                if (record == null)
                    return DDlogRecord.none();
                return DDlogRecord.makeStruct(DDLOG_SOME, record);
            } catch (final DDlogException e) {
                throw new DDlogJooqProviderException(e);
            }
        } else {
            if (record == null)
                throw new DDlogJooqProviderException("NULL value for non-null column " + field);
            return record;
        }
    }

    /** Convert the value 'in' into a DDlogRecord.
     * @param field  Struct field that is being converted.
     * @param in  Value to convert.  May be null.
     * @return    DDlog record representing the value.  Will be null for a null in.
     */
    private static DDlogRecord toValue(final Field<?> field, final Object in) {
        if (in == null)
            return null;
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

    // Return a value based on the DDlog type of the record
    private static Object ddlogRecordToObject(final Field<?> field, final DDlogRecord in) {
        if (in.isBool()) {
            if (debug) {
                assert(field.getDataType().getSQLDataType().equals(Types.BOOLEAN));
            }
            return in.getBoolean();
        } else if (in.isInt()) {
            if (field.getDataType().getSQLType() == Types.BIGINT) {
                return in.getInt().longValue();
            } else {
                if (debug) {
                    assert(field.getDataType().getSQLDataType().equals(Types.INTEGER));
                }
                return in.getInt().intValue();
            }
        } else if (in.isDouble()) {
            if (debug) {
                assert(field.getDataType().getSQLDataType().equals(Types.DOUBLE));
            }
            return in.getDouble();
        } else if (in.isFloat()) {
            if (debug) {
                assert(field.getDataType().getSQLDataType().equals(Types.FLOAT));
            }
            return in.getFloat();
        } else if (in.isString()) {
            if (debug) {
                assert(field.getDataType().getSQLDataType().equals(Types.VARCHAR));
            }
            return in.getString();
        } else if (in.isStruct()) {
            if (in.getStructName().equals(DDLOG_NONE)) {
                return null;
            } else if (in.getStructName().equals(DDLOG_SOME)) {
                try {
                    return ddlogRecordToObject(field, in.getStructField(0));
                } catch (Exception e) {
                    throw new DDlogJooqProviderException(e);
                }
            } else {
                throw new DDlogJooqProviderException("Can't parse this DDlog field of unknown Struct type");
            }
        } else if (in.isVector()) {
            int size = in.getVectorSize();
            List<Object> tmp = new ArrayList<>(size);
            for (int i = 0; i < size; i++) {
                DDlogRecord r = in.getVectorField(i);
                tmp.add(ddlogRecordToObject(field, r));
            }
            return tmp.toArray();
        } else {
            throw new DDlogJooqProviderException("Unknown datatype " + field);
        }
    }

    private static void setValue(final Field<?> field, final DDlogRecord in, final Record out) {
        out.setValue((Field<Object>) field, ddlogRecordToObject(field, in));
    }

    static final class QueryContext {
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

    private static DDlogRecord getStructField(final DDlogRecord record, @SuppressWarnings("SameParameterValue") final int fieldIndex) {
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

        DDlogJooqProviderException(final String msg) {
            super(msg);
        }
    }

    private static MockResult exception(final String msg) {
        return new MockResult(new SQLException(msg));
    }

    private static MockResult exception(final Throwable e) {
        return new MockResult(new SQLException(e));
    }

    private MockResult updateCountFieldResult() {
        final Result<Record1<Integer>> result = dslContext.newResult(updateCountField);
        final Record1<Integer> resultRecord = dslContext.newRecord(updateCountField);
        resultRecord.setValue(updateCountField, 1);
        result.add(resultRecord);
        return new MockResult(1, result);
    }

    private MockResult emptyMockResult() {
        final Result<Record1<Integer>> result = dslContext.newResult(updateCountField);
        return new MockResult(0, result);
    }
}
