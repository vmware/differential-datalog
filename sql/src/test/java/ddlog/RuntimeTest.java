package ddlog;

import org.jooq.DSLContext;
import org.jooq.SQLDialect;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;

import static java.sql.DriverManager.getConnection;
import static org.jooq.impl.DSL.using;

/**
 * Unit test for simple Translator.
 */
public class RuntimeTest
{
    @Test
    public void test() {
        final DSLContext conn = setup();
        final String createStatement = "create table t1(column1 integer, column2 varchar(36), column3 boolean)";
        final String viewStatement = "create view v1 as select * from t1 where column1 = 10";

        // Wrapper around a jdbc connection
        conn.execute(createStatement);
        conn.execute(viewStatement);

        Translator.translateSql(createStatement);
        Translator.translateSql(viewStatement);
    }

    /*
     * Sets up an H2 in-memory database that we use for all tests.
     */
    private DSLContext setup() {
        final Properties properties = new Properties();
        properties.setProperty("foreign_keys", "true");
        try {
            // Create a fresh database
            final String connectionURL = "jdbc:h2:mem:;create=true";
            final Connection conn = getConnection(connectionURL, properties);
            return using(conn, SQLDialect.H2);
        } catch (final SQLException e) {
            throw new RuntimeException(e);
        }
    }
}