package ddlog;

import ddlogapi.*;
import org.jooq.DSLContext;
import org.jooq.SQLDialect;
import org.jooq.impl.DSL;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

public class DynamicTest extends BaseQueriesTest {
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

    @Test
    public void testDynamic() {
        final DSLContext conn = setup();
        final String createStatement = "create table t1(column1 integer, column2 varchar(36), column3 boolean)";
        final String viewStatement = "create view v1 as select * from t1 where column1 = 10";
        conn.execute(createStatement);
        conn.execute(viewStatement);
    }

    //@Test
    public void testDynamicLoading() throws IOException, DDlogException {
        String ddlogProgram = "import sql\n" + // not needed, but we test the libraries too
                "import sqlop\n" +
                "input relation R(v: bit<16>)\n" +
                "output relation O(v: bit<16>)\n" +
                "O(v) :- R(v).";
        File file = this.writeProgramToFile(ddlogProgram);
        DDlogAPI api = null;
        boolean success = DDlogAPI.compileDDlogProgram(file.toString(), true, "../lib", "./lib");
        if (success) {
            api = DDlogAPI.loadDDlog();
        }
        if (api == null)
            throw new RuntimeException("Could not load program");

        DDlogRecord field = new DDlogRecord(10);
        DDlogRecord[] fields = { field };
        DDlogRecord record = DDlogRecord.makeStruct("R", fields);
        int id = api.getTableId("R");
        Assert.assertTrue(id < 10);
        DDlogRecCommand command = new DDlogRecCommand(
                DDlogCommand.Kind.Insert, id, record);
        DDlogRecCommand[] ca = new DDlogRecCommand[1];
        ca[0] = command;

        System.err.println("Executing " + command.toString());
        api.transactionStart();
        api.applyUpdates(ca);
        api.transactionCommitDumpChanges(s -> Assert.assertEquals("From 0 Insert O{10}", s.toString()));
        api.stop();
    }
}
