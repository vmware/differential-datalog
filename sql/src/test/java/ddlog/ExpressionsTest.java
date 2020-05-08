package ddlog;

import com.facebook.presto.sql.SqlFormatter;
import com.facebook.presto.sql.parser.ParsingOptions;
import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.tree.Statement;
import com.vmware.ddlog.ir.DDlogIRNode;
import com.vmware.ddlog.translator.Translator;
import org.junit.Assert;
import org.junit.Test;

import java.util.Optional;

public class ExpressionsTest {
    @Test
    public void testFormat() {
        final String createStatement = "create table t1(column1 integer, column2 varchar(36), column3 boolean)";
        SqlParser parser = new SqlParser();
        ParsingOptions options = ParsingOptions.builder().build();
        Statement create = parser.createStatement(createStatement, options);
        String stat = SqlFormatter.formatSql(create, Optional.empty());
        Assert.assertEquals("CREATE TABLE t1 (\n" +
                "   column1 integer,\n" +
                "   column2 varchar(36),\n" +
                "   column3 boolean\n)", stat);

        final String viewStatement = "create view v1 as select * from t1 where column1 = 10";
        Statement view = parser.createStatement(viewStatement, options);
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
        Assert.assertEquals("\"some string\"", s);

        node = t.translateExpression("((1 + 2) >= 0) or ((1 - 3) <= 0)");
        s = node.toString();
        Assert.assertEquals("(((64'sd1 + 64'sd2) >= 64'sd0) or ((64'sd1 - 64'sd3) <= 64'sd0))", s);

        node = t.translateExpression("CASE 0\n" +
                "      WHEN 1 THEN 'One'\n" +
                "      WHEN 2 THEN 'Two'\n" +
                "      ELSE 'Other'\n" +
                "END");
        s = node.toString();
        Assert.assertEquals("if ((64'sd0 == 64'sd1)) {\n" +
                "\"One\"} else {\n" +
                "if ((64'sd0 == 64'sd2)) {\n" +
                "\"Two\"} else {\n" +
                "\"Other\"}}", s);
    }
}
