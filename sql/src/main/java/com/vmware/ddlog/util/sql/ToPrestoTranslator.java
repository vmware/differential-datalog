package com.vmware.ddlog.util.sql;

/**
 * Base abstract class for translators from any SQL dialect to the Presto dialect, which is used in Translator.
 */
public abstract class ToPrestoTranslator {
    /**
     * Translates given SQL statement to Presto dialect.
     * @param sql
     * @return
     */
    public abstract String toPresto(String sql);

    /**
     * Return what is equivalent to a PrestoToPrestoTranslator, which simply returns the SQL statement.
     * This method can be used to fetch a translator used when the user already passes SQL in the Presto dialect.
     * @return
     */
    public static ToPrestoTranslator noopTranslator() {
        return new ToPrestoTranslator() {
            @Override
            public String toPresto(String sql) {
                return sql;
            }
        };
    }
}