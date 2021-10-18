package com.vmware.ddlog;

import com.google.common.collect.ImmutableList;
import com.vmware.ddlog.ir.DDlogIndexDeclaration;
import com.vmware.ddlog.util.sql.ParsedCreateIndex;
import ddlogapi.DDlogAPI;
import ddlogapi.DDlogException;
import ddlogapi.DDlogRecord;
import org.apache.calcite.sql.*;
import org.apache.calcite.sql.util.SqlBasicVisitor;
import org.jooq.Field;

import java.sql.SQLException;
import java.util.*;
import java.util.stream.Collectors;

import static com.vmware.ddlog.DDlogJooqProvider.matchExpressionFromWhere;

/**
 * Helper class to DDlogJooqProvider that contains logic for looking up DDlog indexes and auxiliary functions
 * related to WHERE clauses. *
 */
public class DDlogJooqHelper {
    private final Map<String, List<Field<?>>> tablesToFields;
    private final Map<String, List<ParsedCreateIndex>> tablesToIndexesMap;
    private final Map<String, List<Field<?>>> indexToFieldsMap = new HashMap<>();
    private final DDlogAPI dDlogAPI;

    public DDlogJooqHelper(Map<String, List<Field<?>>> tablesToFields,
                           Map<String, List<ParsedCreateIndex>> tablesToIndexesMap,
                           DDlogAPI ddlogAPI) {
        this.tablesToFields = tablesToFields;
        this.tablesToIndexesMap = tablesToIndexesMap;
        this.dDlogAPI = ddlogAPI;

        // Now construct list of fields for each index
        for (ParsedCreateIndex parsedIndex :
                tablesToIndexesMap.values().stream().flatMap(List::stream).collect(Collectors.toList())) {
            List<Field<?>> tableFields = tablesToFields.get(parsedIndex.getTableName().toUpperCase(Locale.ROOT));
            List<Field<?>> indexFields = new ArrayList<>();
            for (String indexColumn : parsedIndex.getColumns()) {
                for (Field<?> f : tableFields) {
                    if (f.getName().equalsIgnoreCase(indexColumn)) {
                        indexFields.add(f);
                    }
                }
            }
            indexToFieldsMap.put(parsedIndex.getIndexName(), indexFields);
        }
    }

    // Check for case-insensitive string equality
    public static boolean compareStringArrays(String[] ary1, String[] ary2) {
        if (ary1.length != ary2.length) {
            return false;
        }
        for (int i = 0; i < ary1.length; i++) {
            if (!ary1[i].equalsIgnoreCase(ary2[i])) {
                return false;
            }
        }
        return true;
    }

    public boolean sameFields(SqlNodeList list, String tableName) {
        String[] tableFields = tablesToFields.get(tableName).stream().map(Field::getName).toArray(String[]::new);
        String[] queryFields =
                list.getList().stream().map(x -> {
                    // Just in case the identifier is compound, we get the last name in the names list,
                    // rather than using getSimple()
                    ImmutableList<String> names = ((SqlIdentifier) x).names;
                    return names.get(names.size() - 1);
                }).toArray(String[]::new);
        return compareStringArrays(tableFields, queryFields);
    }

    public String[] getIdsFromWhere(SqlBasicCall where) {
        WhereClauseGetIdentifiers getIdentifiers = new WhereClauseGetIdentifiers();
        return where.accept(getIdentifiers).toArray(new String[0]);

    }

    public List<DDlogRecord> getEntriesByIndex(String tableName,
                                               String[] ids,
                                               SqlBasicCall where, DDlogJooqProvider.QueryContext context)
            throws DDlogException, SQLException {
        final List<DDlogRecord> clonedResults = new ArrayList<>();

        List<ParsedCreateIndex> indexes = tablesToIndexesMap.get(tableName);
        if (indexes == null) {
            throw new SQLException(String.format("Cannot delete from table %s: no appropriate primary key" +
                    "and no indexes", tableName));
        }
        // Look through the indexes to see if there's a matching index
        ParsedCreateIndex matchingIndex = null;
        for (ParsedCreateIndex id : indexes) {
            // Check for equality
            if (!DDlogJooqHelper.compareStringArrays(id.getColumns(), ids)) {
                continue;
            }
            matchingIndex = id;
            break;
        }
        if (matchingIndex == null) {
            throw new SQLException(String.format("Cannot delete from table %s: no appropriate index",
                    tableName));
        }
        // Use the matching index to fetch the row, and then delete it
        final DDlogRecord indexKey =
                matchExpressionFromWhere(where, indexToFieldsMap.get(matchingIndex.getIndexName()), context);

        dDlogAPI.queryIndex(
                DDlogIndexDeclaration.indexName(matchingIndex.getIndexName()),
                indexKey,
                x -> clonedResults.add(x.clone())
        );

        return clonedResults;
    }

    /**
     * A visitor for WHERE conditions. Returns a list of identifiers (column names) used (matched on) by the condition.
     * Typically, after extracting the list of identifiers, the caller will then pass this list to the
     * WhereClauseToMatchExpression visitor in order to construct the appropriate DDlogRecords.
     *
     * Note that we assume this visitor is only called on WHERE clauses of the form (ID1 = OPERAND1) AND ...
     * because these are the only forms of WHERE conditions supported by this JooqProvider.
     */
    private static final class WhereClauseGetIdentifiers extends SqlBasicVisitor<List<String>> {
        private final List<String> identifiers;

        public WhereClauseGetIdentifiers() {
            this.identifiers = new ArrayList<>();
        }

        @Override
        public List<String> visit(final SqlCall call) {
            final SqlBasicCall expr = (SqlBasicCall) call;
            switch (expr.getOperator().getKind()) {
                case AND:
                    for (SqlNode node : expr.getOperands()) {
                        visit((SqlBasicCall) node);
                    }
                    return identifiers;
                case EQUALS: {
                    final SqlNode left = expr.getOperands()[0];
                    final SqlNode right = expr.getOperands()[1];
                    if (left instanceof SqlIdentifier) {
                        // Add left to the columns
                        identifiers.add(left.toString().toUpperCase(Locale.ROOT));
                    } else if (right instanceof SqlIdentifier) {
                        // add right to the identifiers
                        identifiers.add(right.toString().toUpperCase(Locale.ROOT));
                    }
                    return identifiers;
                }
                default:
                    throw new DDlogJooqProvider.DDlogJooqProviderException(
                            "WhereClauseGetIdentifiers: Unsupported expression in where clause " + call);
            }
        }
    }
}