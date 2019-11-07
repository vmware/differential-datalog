package ddlog;

import com.facebook.presto.sql.SqlFormatter;
import com.facebook.presto.sql.parser.ParsingOptions;
import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.tree.Statement;
import com.vmware.ddlog.ir.DDlogIRNode;
import com.vmware.ddlog.ir.DDlogProgram;
import com.vmware.ddlog.translator.Translator;
import org.jooq.DSLContext;
import org.jooq.SQLDialect;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Optional;
import java.util.Properties;

import java.sql.DriverManager;
import org.jooq.impl.DSL;
import org.junit.Assert;
import org.junit.Test;

/**
 * Unit test for simple Translator.
 */
public class RuntimeTest {
    @Test
    public void testDynamic() {
        final DSLContext conn = setup();
        final String createStatement = "create table t1(column1 integer, column2 varchar(36), column3 boolean)";
        final String viewStatement = "create view v1 as select * from t1 where column1 = 10";
        conn.execute(createStatement);
        conn.execute(viewStatement);
    }

    @Test
    public void testFormat() {
        final String createStatement = "create table t1(column1 integer, column2 varchar(36), column3 boolean)";
        SqlParser parser = new SqlParser();
        Statement create = parser.createStatement(createStatement, new ParsingOptions());
        String stat = SqlFormatter.formatSql(create, Optional.empty());
        Assert.assertEquals("CREATE TABLE t1 (\n" +
                "   column1 integer,\n" +
                "   column2 varchar(36),\n" +
                "   column3 boolean\n)", stat);

        final String viewStatement = "create view v1 as select * from t1 where column1 = 10";
        Statement view = parser.createStatement(viewStatement, new ParsingOptions());
        stat = SqlFormatter.formatSql(view, Optional.empty());
        Assert.assertEquals("CREATE VIEW v1 AS\n" +
                "SELECT *\n" +
                "FROM\n" +
                "  t1\n" +
                "WHERE (column1 = 10)\n", stat);
    }

    @Test
    public void testExpression() {
        Translator t = new Translator(null);
        DDlogIRNode node = t.translateExpression("true");
        String s = node.toString();
        Assert.assertEquals("true", s);

        node = t.translateExpression("'some string'");
        s = node.toString();
        Assert.assertEquals("'some string'", s);

        node = t.translateExpression("((1 + 2) >= 0) or ((1 - 3) <= 0)");
        s = node.toString();
        Assert.assertEquals("(((64'd1 + 64'd2) >= 64'd0) or ((64'd1 - 64'd3) <= 64'd0))", s);

        node = t.translateExpression("CASE 0\n" +
                "      WHEN 1 THEN 'One'\n" +
                "      WHEN 2 THEN 'Two'\n" +
                "      ELSE 'Other'\n" +
                "END");
        s = node.toString();
        Assert.assertEquals("if (64'd0 == 64'd1){\n" +
                "'One'} else {\n" +
                "if (64'd0 == 64'd2){\n" +
                "'Two'} else {\n" +
                "'Other'}}", s);
    }

    @Test
    public void test() {
        String createStatement = "create table t1(column1 integer, column2 varchar(36), column3 boolean)";
        Translator t = new Translator(null);
        DDlogIRNode create = t.translateSqlStatement(createStatement);
        Assert.assertNotNull(create);
        String s = create.toString();
        Assert.assertNotNull(s);
        Assert.assertEquals("input relation Rt1[t1{column1:signed<64>, column2:string, column3:bool}]", s);

        String viewStatement = "create view v1 as select * from t1";
        DDlogIRNode view = t.translateSqlStatement(viewStatement);
        Assert.assertNotNull(view);
        s = view.toString();
        Assert.assertNotNull(s);
        Assert.assertEquals("Rv1[v0] :- Rt1[v0].", s);

        viewStatement = "create view v1 as select * from t1 where column1 = 10";
        view = t.translateSqlStatement(viewStatement);
        Assert.assertNotNull(view);
        s = view.toString();
        Assert.assertNotNull(s);
        Assert.assertEquals("Rv1[v0] :- Rt1[v0], v0.column1 == 10.", s);

        DDlogProgram program = t.getDDlogProgram();
        Assert.assertNotNull(program);
        s = program.toString();
        Assert.assertEquals("import sql\n" +
                "input relation Rt1[t1{column1:signed<64>, column2:string, column3:bool}]", s);
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
}
