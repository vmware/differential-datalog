package com.vmware.ddlog.util.sql;

import org.apache.calcite.avatica.util.Casing;
import org.apache.calcite.sql.parser.SqlAbstractParserImpl;
import org.apache.calcite.sql.parser.ddl.SqlDdlParserImpl;

import java.io.StringReader;

public class CalciteUtils {

    /**
     * Initialize and return a Calcite DDL Parser.
     * @param sql  Statement to initialize parser with.
     */
    public static SqlAbstractParserImpl createCalciteParser(String sql) {
        SqlAbstractParserImpl ret = SqlDdlParserImpl.FACTORY.getParser(new StringReader(sql));

        /*
         * Currently, SqlDdlParserImpl doesn't have a constructor that accepts config parameters,
         * so we have to set them manually.
         */
        ret.setUnquotedCasing(Casing.TO_LOWER);
        ret.setQuotedCasing(Casing.TO_LOWER);
        ret.setIdentifierMaxLength(100);

        return ret;
    }
}
