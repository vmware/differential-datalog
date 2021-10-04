package com.vmware.ddlog.util.sql;

public class ParsedCreateIndex {
    private final String indexName;
    private final String tableName;
    private final String[] columns;

    ParsedCreateIndex(String indexName, String tableName, String[] columns) {
        this.indexName = indexName;
        this.tableName = tableName;
        this.columns = columns;
    }

    public String getIndexName() {
        return this.indexName;
    }

    public String getTableName() {
        return this.tableName;
    }

    public String[] getColumns() {
        return this.columns;
    }
}
