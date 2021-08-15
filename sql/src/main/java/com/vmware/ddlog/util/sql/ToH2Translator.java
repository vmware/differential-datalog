package com.vmware.ddlog.util.sql;

/**
 * Base abstract class for translating DDL statements from a SQL dialect to the H2 dialect,
 * which is used in DDlogJooqProvider.
 */
public abstract class ToH2Translator {
    /**
     * Translates given SQL statement to H2 dialect.
     * @param sql
     * @return
     */
    public abstract String toH2(String sql);
}
