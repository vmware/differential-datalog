package ddlog;

import org.jooq.DSLContext;
import org.jooq.Field;
import org.jooq.SQLDialect;
import org.jooq.Table;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;

import java.sql.DriverManager;
import org.jooq.impl.DSL;

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
        //System.out.println(getTablesAndFields(conn));
        //System.out.println(getRecordTypesByTable(conn));
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
            final Connection conn = DriverManager.getConnection(connectionURL, properties);
            conn.setSchema("PUBLIC");
            return DSL.using(conn, SQLDialect.H2);
        } catch (final SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private Map<Table<?>, List<Field<?>>> getTablesAndFields(final DSLContext conn) {
        final List<Table<?>> tables = conn.meta().getTables();
        final Map<Table<?>, List<Field<?>>> tablesToFields = new HashMap<>();
        tables.forEach(
                t -> tablesToFields.put(t, t.fieldStream().collect(Collectors.toList()))
        );
        return tablesToFields;
    }

    private Map<Table<?>, List<Class<?>>> getRecordTypesByTable(final DSLContext conn) {
        final Map<Table<?>, List<Field<?>>> tablesToFields = getTablesAndFields(conn);
        return tablesToFields.entrySet().stream()
                .collect(Collectors.toMap(Map.Entry::getKey,
                                          entry -> entry.getValue()
                                                        .stream()
                                                        .map(Field::getType)
                                                        .collect(Collectors.toList())));
    }
}
