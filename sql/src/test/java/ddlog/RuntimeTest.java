package ddlog;

import com.facebook.presto.sql.SqlFormatter;
import com.facebook.presto.sql.parser.ParsingOptions;
import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.tree.Statement;
import com.vmware.ddlog.ir.DDlogIRNode;
import com.vmware.ddlog.ir.DDlogProgram;
import com.vmware.ddlog.translator.Translator;
import org.h2.store.fs.FileUtils;
import org.jooq.DSLContext;
import org.jooq.SQLDialect;

import java.io.*;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Optional;
import java.util.Properties;

import java.sql.DriverManager;
import java.util.stream.Collectors;

import org.jooq.impl.DSL;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Unit test for simple Translator.
 */
@SuppressWarnings("FieldCanBeLocal")
public class RuntimeTest {
    // These strings are part of almost all expected outputs
    private final String imports = "import sql\nimport sqlop\n";
    private final String t1t2 =
            "typedef Tt1 = Tt1{column1:signed<64>, column2:string, column3:bool}\n" +
            "typedef Tt2 = Tt2{column1:signed<64>}\n";
    private final String t1t2null =
            "typedef Tt1 = Tt1{column1:Option<signed<64>>, column2:Option<string>, column3:Option<bool>}\n" +
            "typedef Tt2 = Tt2{column1:Option<signed<64>>}\n";

    /**
     * The expected string the generated program starts with.
     * @param withNull  True if the tables can contain nulls.
     */
    private String header(boolean withNull) {
        if (withNull)
            return this.imports + "\n" + this.t1t2null;
        return this.imports + "\n" + this.t1t2;
    }

    /**
     * The expected string for the declared relations
     */
    @SuppressWarnings("unused")
    private String relations(boolean withNull) {
        return "\n" +
               "input relation Rt1[Tt1]\n" +
               "input relation Rt2[Tt2]\n";
    }

    @BeforeClass
    public static void createLibrary() throws FileNotFoundException {
        Translator t = new Translator(null);
        DDlogProgram lib = t.generateLibrary();
        System.out.println("Current directory " + System.getProperty("user.dir"));
        lib.toFile("lib/sqlop.dl");
    }

    @Test
    public void testDynamic() {
        final DSLContext conn = setup();
        final String createStatement = "create table t1(column1 integer, column2 varchar(36), column3 boolean)";
        final String viewStatement = "create view v1 as select * from t1 where column1 = 10";
        conn.execute(createStatement);
        conn.execute(viewStatement);
    }

    private Translator createInputTables(boolean withNulls) {
        String nulls = withNulls ? "" : " not null";
        String createStatement = "create table t1(column1 integer " + nulls + ",\n" +
                " column2 varchar(36) " + nulls + ",\n" +
                " column3 boolean " + nulls + ")";
        Translator t = new Translator(null);
        DDlogIRNode create = t.translateSqlStatement(createStatement);
        Assert.assertNotNull(create);
        String s = create.toString();
        Assert.assertNotNull(s);
        Assert.assertEquals("input relation Rt1[Tt1]", s);

        createStatement = "create table t2(column1 integer " + nulls + ")";
        create = t.translateSqlStatement(createStatement);
        Assert.assertNotNull(create);
        s = create.toString();
        Assert.assertNotNull(s);
        Assert.assertEquals("input relation Rt2[Tt2]", s);

        return t;
    }

    private void compiledDDlog(String program) {
        try {
            File tmp = File.createTempFile("program", ".dl");
            BufferedWriter bw = new BufferedWriter(new FileWriter(tmp));
            bw.write(program);
            bw.close();
            Process process = Runtime.getRuntime().exec("ddlog -i " + tmp.toString() + " -L../lib -L./lib");
            int exitCode = process.waitFor();
            if (exitCode != 0) {
                InputStream errorStream = process.getErrorStream();
                String result = new BufferedReader(new InputStreamReader(errorStream))
                        .lines().collect(Collectors.joining("\n"));
                System.out.println(result);
                System.out.println(program);
            }
            Assert.assertEquals(0, exitCode);
            boolean b = tmp.delete();
            Assert.assertTrue(b);
            String basename = tmp.getName();
            basename = basename.substring(0, basename.lastIndexOf('.'));
            String tempDir = System.getProperty("java.io.tmpdir");
            String dir = tempDir + "/" + basename + "_ddlog";
            FileUtils.deleteRecursive(dir, false);
        } catch (IOException | InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    private void testTranslation(String query, String program, boolean withNulls) {
        Translator t = this.createInputTables(withNulls);
        DDlogIRNode view = t.translateSqlStatement(query);
        Assert.assertNotNull(view);
        String s = view.toString();
        Assert.assertNotNull(s);
        DDlogProgram ddprogram = t.getDDlogProgram();
        Assert.assertNotNull(ddprogram);
        s = ddprogram.toString();
        Assert.assertEquals(program, s);
        this.compiledDDlog(s);
    }

    private void testTranslation(String query, String program) {
        this.testTranslation(query, program, false);
    }

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
        Assert.assertEquals("if (64'sd0 == 64'sd1){\n" +
                "\"One\"} else {\n" +
                "if (64'sd0 == 64'sd2){\n" +
                "\"Two\"} else {\n" +
                "\"Other\"}}", s);
    }

    @Test
    public void testWhen() {
        String query = "create view v1 as SELECT DISTINCT CASE WHEN column1 = 1 THEN 1 WHEN 1 < column1 THEN 2 ELSE 3 END FROM t1";
        String program =
                this.header(false) +
                        "typedef Ttmp0 = Ttmp0{col1:signed<64>}\n" +
                        this.relations(false) +
                        "relation Rtmp0[Ttmp0]\n" +
                        "output relation Rv1[Ttmp0]\n" +
                        "Rv1[v3] :- Rt1[v0],var v2 = Ttmp0{.col1 = if (v0.column1 == 64'sd1){\n" +
                        "64'sd1} else {\n" +
                        "if (64'sd1 < v0.column1){\n" +
                        "64'sd2} else {\n" +
                        "64'sd3}}},var v3 = v2.";
        testTranslation(query, program);
    }

    @Test
    public void testWhenWNull() {
        String query = "create view v1 as SELECT DISTINCT CASE WHEN column1 = 1 THEN 1 WHEN 1 < column1 THEN 2 ELSE 3 END FROM t1";
        String program =
            this.header(true) +
                "typedef Ttmp0 = Ttmp0{col1:signed<64>}\n" +
                this.relations(true) +
                "relation Rtmp0[Ttmp0]\n" +
                "output relation Rv1[Ttmp0]\n" +
                "Rv1[v3] :- Rt1[v0],var v2 = Ttmp0{.col1 = if unwrapBool(a_eq_NR(v0.column1, 64'sd1)){\n" +
                "64'sd1} else {\n" +
                "if unwrapBool(a_lt_RN(64'sd1, v0.column1)){\n" +
                "64'sd2} else {\n" +
                "64'sd3}}},var v3 = v2.";
        testTranslation(query, program, true);
    }

    @Test
    public void testAlias() {
        String query = "create view v1 as SELECT DISTINCT t2.column1 FROM t1 AS t2";
        String program = this.header(false) +
                "typedef Ttmp0 = Ttmp0{column1:signed<64>}\n" +
                this.relations(false) +
                "relation Rtmp0[Ttmp0]\n" +
                "output relation Rv1[Ttmp0]\n" +
                "Rv1[v2] :- Rt1[v0],var v1 = Ttmp0{.column1 = v0.column1},var v2 = v1.";
        testTranslation(query, program);
    }

    @Test
    public void testAliasWNull() {
        String query = "create view v1 as SELECT DISTINCT t2.column1 FROM t1 AS t2";
        String program = this.header(true) +
            "typedef Ttmp0 = Ttmp0{column1:Option<signed<64>>}\n" +
            this.relations(true) +
            "relation Rtmp0[Ttmp0]\n" +
            "output relation Rv1[Ttmp0]\n" +
            "Rv1[v2] :- Rt1[v0],var v1 = Ttmp0{.column1 = v0.column1},var v2 = v1.";
        testTranslation(query, program, true);
    }

    @Test
    public void testScope() {
        String query = "create view v1 as SELECT DISTINCT t1.column1 FROM t1";
        String program = this.header(false) +
                "typedef Ttmp0 = Ttmp0{column1:signed<64>}\n" +
                this.relations(false) +
                "relation Rtmp0[Ttmp0]\n" +
                "output relation Rv1[Ttmp0]\n" +
                "Rv1[v2] :- Rt1[v0],var v1 = Ttmp0{.column1 = v0.column1},var v2 = v1.";
        testTranslation(query, program);
    }

    @Test
    public void testScopeWNulls() {
        String query = "create view v1 as SELECT DISTINCT t1.column1 FROM t1";
        String program = this.header(true) +
            "typedef Ttmp0 = Ttmp0{column1:Option<signed<64>>}\n" +
            this.relations(true) +
            "relation Rtmp0[Ttmp0]\n" +
            "output relation Rv1[Ttmp0]\n" +
            "Rv1[v2] :- Rt1[v0],var v1 = Ttmp0{.column1 = v0.column1},var v2 = v1.";
        testTranslation(query, program, true);
    }

    @Test
    public void testImplicitJoin() {
        String query = "create view v1 as SELECT DISTINCT * FROM t1, t2";
        String program = this.header(false) +
                "typedef Ttmp0 = Ttmp0{column1:signed<64>, column2:string, column3:bool, column10:signed<64>}\n" +
                this.relations(false) +
                "output relation Rv1[Ttmp0]\n" +
                "Rv1[v3] :- Rt1[v0],Rt2[v1],true,var v2 = Ttmp0{.column1 = v0.column1,.column2 = v0.column2,.column3 = v0.column3,.column10 = v1.column1},var v3 = v2.";
        testTranslation(query, program);
    }

    @Test
    public void testJoinStar() {
        String query = "create view v1 as SELECT DISTINCT * FROM t1 JOIN t2 ON t1.column1 = t2.column1";
        String program = this.header(false) +
                "typedef Ttmp0 = Ttmp0{column1:signed<64>, column2:string, column3:bool, column10:signed<64>}\n" +
                this.relations(false) +
                "output relation Rv1[Ttmp0]\n" +
                "Rv1[v3] :- Rt1[v0],Rt2[v1],(v0.column1 == v1.column1),true,var v2 = Ttmp0{.column1 = v0.column1,.column2 = v0.column2,.column3 = v0.column3,.column10 = v1.column1},var v3 = v2.";
        testTranslation(query, program);
    }

    @Test
    public void testJoinStarWNull() {
        String query = "create view v1 as SELECT DISTINCT * FROM t1 JOIN t2 ON t1.column1 = t2.column1";
        String program = this.header(true) +
                "typedef Ttmp0 = Ttmp0{column1:Option<signed<64>>, column2:Option<string>, column3:Option<bool>, column10:Option<signed<64>>}\n" +
                this.relations(true) +
                "output relation Rv1[Ttmp0]\n" +
                "Rv1[v3] :- Rt1[v0],Rt2[v1],unwrapBool(a_eq_NN(v0.column1, v1.column1)),true,var v2 = Ttmp0{.column1 = v0.column1,.column2 = v0.column2,.column3 = v0.column3,.column10 = v1.column1},var v3 = v2.";
        testTranslation(query, program, true);
    }

    @Test
    public void testNaturalJoin() {
        String query = "create view v1 as SELECT DISTINCT * FROM t1 NATURAL JOIN t2";
        String program = this.header(false) +
                "typedef Ttmp0 = Ttmp0{column1:signed<64>, column2:string, column3:bool}\n" +
                this.relations(false) +
                "output relation Rv1[Ttmp0]\n" +
                "Rv1[v3] :- Rt1[v0],Rt2[v1],(true and (v0.column1 == v1.column1)),var v2 = Ttmp0{.column1 = v0.column1,.column2 = v0.column2,.column3 = v0.column3},var v3 = v2.";
        testTranslation(query, program);
    }

    @Test
    public void testJoin() {
        String query = "create view v1 as SELECT DISTINCT t0.column1, t1.column3 FROM t1 AS t0 JOIN t1 ON t0.column2 = t1.column2";
        String program = this.header(false) +
                "typedef Ttmp0 = Ttmp0{column1:signed<64>, column2:string, column3:bool, column10:signed<64>, column20:string, column30:bool}\n" +
                "typedef Ttmp1 = Ttmp1{column1:signed<64>, column3:bool}\n" +
                this.relations(false) +
                "relation Rtmp1[Ttmp1]\n" +
                "output relation Rv1[Ttmp1]\n" +
                "Rv1[v4] :- Rt1[v0],Rt1[v1],(v0.column2 == v1.column2),true," +
                "var v2 = Ttmp0{.column1 = v0.column1,.column2 = v0.column2,.column3 = v0.column3,.column10 = v1.column1,.column20 = v1.column2,.column30 = v1.column3}," +
                "var v3 = Ttmp1{.column1 = v0.column1,.column3 = v1.column3},var v4 = v3.";
        testTranslation(query, program);
    }

    @Test
    public void testCrossJoin() {
        String query = "create view v1 as SELECT DISTINCT * FROM t1 CROSS JOIN t2";
        String program = this.header(false) +
                "typedef Ttmp0 = Ttmp0{column1:signed<64>, column2:string, column3:bool, column10:signed<64>}\n" +
                this.relations(false) +
                "output relation Rv1[Ttmp0]\n" +
                "Rv1[v3] :- Rt1[v0],Rt2[v1],true,var v2 = Ttmp0{.column1 = v0.column1,.column2 = v0.column2,.column3 = v0.column3,.column10 = v1.column1},var v3 = v2.";
        testTranslation(query, program);
    }

    @Test
    public void testCrossJoinWNull() {
        String query = "create view v1 as SELECT DISTINCT * FROM t1 CROSS JOIN t2";
        String program = this.header(true) +
                "typedef Ttmp0 = Ttmp0{column1:Option<signed<64>>, column2:Option<string>, column3:Option<bool>, column10:Option<signed<64>>}\n" +
                this.relations(true) +
                "output relation Rv1[Ttmp0]\n" +
                "Rv1[v3] :- Rt1[v0],Rt2[v1],true,var v2 = Ttmp0{.column1 = v0.column1,.column2 = v0.column2,.column3 = v0.column3,.column10 = v1.column1},var v3 = v2.";
        testTranslation(query, program, true);
    }

    @Test
    public void testMultiJoin() {
        String query = "create view v1 as SELECT DISTINCT *\n" +
                "    FROM t1,\n" +
                "         (SELECT DISTINCT column1 AS a FROM t1) b,\n" +
                "         (SELECT DISTINCT column2 AS c FROM t1) c,\n" +
                "         (SELECT DISTINCT column3 AS d FROM t1) d";
        String program = this.header(false) +
                "typedef Ttmp0 = Ttmp0{a:signed<64>}\n" +
                "typedef Ttmp2 = Ttmp2{column1:signed<64>, column2:string, column3:bool, a:signed<64>}\n" +
                "typedef Ttmp3 = Ttmp3{c:string}\n" +
                "typedef Ttmp5 = Ttmp5{column1:signed<64>, column2:string, column3:bool, a:signed<64>, c:string}\n" +
                "typedef Ttmp6 = Ttmp6{d:bool}\n" +
                "typedef Ttmp8 = Ttmp8{column1:signed<64>, column2:string, column3:bool, a:signed<64>, c:string, d:bool}\n" +
                this.relations(false) +
                "relation Rtmp0[Ttmp0]\n" +
                "relation Rtmp1[Ttmp0]\n" +
                "relation Rtmp3[Ttmp3]\n" +
                "relation Rtmp4[Ttmp3]\n" +
                "relation Rtmp6[Ttmp6]\n" +
                "relation Rtmp7[Ttmp6]\n" +
                "output relation Rv1[Ttmp8]\n" +
                "Rtmp1[v3] :- Rt1[v1],var v2 = Ttmp0{.a = v1.column1},var v3 = v2.\n" +
                "Rtmp4[v7] :- Rt1[v5],var v6 = Ttmp3{.c = v5.column2},var v7 = v6.\n" +
                "Rtmp7[v11] :- Rt1[v9],var v10 = Ttmp6{.d = v9.column3},var v11 = v10.\n" +
                "Rv1[v13] :- Rt1[v0],Rtmp1[v3],true," +
                "var v4 = Ttmp2{.column1 = v0.column1,.column2 = v0.column2,.column3 = v0.column3,.a = v3.a}," +
                "Rtmp4[v7],true," +
                "var v8 = Ttmp5{.column1 = v4.column1,.column2 = v4.column2,.column3 = v4.column3,.a = v4.a,.c = v7.c},Rtmp7[v11],true,var v12 = Ttmp8{.column1 = v8.column1,.column2 = v8.column2,.column3 = v8.column3,.a = v8.a,.c = v8.c,.d = v11.d}," +
                "var v13 = v12.";
        testTranslation(query, program);
    }

    //@Test
    public void testMax() {
        // TODO
        String query = "create view v1 as SELECT DISTINCT MAX(CASE WHEN column2 = 'foo' THEN column1 ELSE 0 END) FROM t1";
        String program = "";
        testTranslation(query, program);
    }

    @Test
    public void testBetween() {
        String query = "create view v1 as SELECT DISTINCT column1, column2 FROM t1 WHERE column1 BETWEEN -1 and 10";
        String program = this.header(false) +
                "typedef Ttmp0 = Ttmp0{column1:signed<64>, column2:string}\n" +
                this.relations(false) +
                "relation Rtmp0[Ttmp0]\n" +
                "output relation Rv1[Ttmp0]\n" +
                "Rv1[v2] :- Rt1[v0],(((- 64'sd1) <= v0.column1) and (v0.column1 <= 64'sd10)),var v1 = Ttmp0{.column1 = v0.column1,.column2 = v0.column2},var v2 = v1.";
        this.testTranslation(query, program);
    }

    @Test
    public void testBetweenWNulls() {
        String query = "create view v1 as SELECT DISTINCT column1, column2 FROM t1 WHERE column1 BETWEEN -1 and 10";
        String program = this.header(true) +
            "typedef Ttmp0 = Ttmp0{column1:Option<signed<64>>, column2:Option<string>}\n" +
            this.relations(true) +
            "relation Rtmp0[Ttmp0]\n" +
            "output relation Rv1[Ttmp0]\n" +
            "Rv1[v2] :- Rt1[v0],unwrapBool(b_and_NN(a_lte_RN((- 64'sd1), v0.column1), a_lte_NR(v0.column1, 64'sd10))),var v1 = Ttmp0{.column1 = v0.column1,.column2 = v0.column2},var v2 = v1.";
        this.testTranslation(query, program, true);
    }

    @Test
    public void testSubstr() {
        String query = "create view v1 as SELECT DISTINCT SUBSTR(column2, 3, 5) FROM t1";
        String program = this.header(false) +
                "typedef Ttmp0 = Ttmp0{col1:string}\n" +
                this.relations(false) +
                "relation Rtmp0[Ttmp0]\n" +
                "output relation Rv1[Ttmp0]\n" +
                "Rv1[v3] :- Rt1[v0],var v2 = Ttmp0{.col1 = substr(v0.column2, 64'sd3, 64'sd5)},var v3 = v2.";
        testTranslation(query, program);
    }

    @Test
    public void testSubstrWNull() {
        String query = "create view v1 as SELECT DISTINCT SUBSTR(column2, 3, 5) FROM t1";
        String program = this.header(true) +
            "typedef Ttmp0 = Ttmp0{col1:Option<string>}\n" +
            this.relations(true) +
            "relation Rtmp0[Ttmp0]\n" +
            "output relation Rv1[Ttmp0]\n" +
            "Rv1[v3] :- Rt1[v0],var v2 = Ttmp0{.col1 = substr_N(v0.column2, 64'sd3, 64'sd5)},var v3 = v2.";
        testTranslation(query, program, true);
    }

    @Test
    public void testSelectWithNulls() {
        String query = "create view v1 as select distinct column1, column2 from t1";
        String program =
                this.header(true) +
                "typedef Ttmp0 = Ttmp0{column1:Option<signed<64>>, column2:Option<string>}\n" +
                this.relations(true) +
                "relation Rtmp0[Ttmp0]\n" +
                "output relation Rv1[Ttmp0]\n" +
                "Rv1[v2] :- Rt1[v0],var v1 = Ttmp0{.column1 = v0.column1,.column2 = v0.column2},var v2 = v1.";
        testTranslation(query, program, true);
    }

    @Test
    public void testSelect() {
        String query = "create view v1 as select distinct column1, column2 from t1";
        String program = this.header(false) +
            "typedef Ttmp0 = Ttmp0{column1:signed<64>, column2:string}\n" +
            this.relations(false) +
            "relation Rtmp0[Ttmp0]\n" +
            "output relation Rv1[Ttmp0]\n" +
            "Rv1[v2] :- Rt1[v0],var v1 = Ttmp0{.column1 = v0.column1,.column2 = v0.column2},var v2 = v1.";
        testTranslation(query, program);
    }

    @Test
    public void testSelectWNull() {
        String query = "create view v1 as select distinct column1, column2 from t1";
        String program = this.header(true) +
            "typedef Ttmp0 = Ttmp0{column1:Option<signed<64>>, column2:Option<string>}\n" +
            this.relations(true) +
            "relation Rtmp0[Ttmp0]\n" +
            "output relation Rv1[Ttmp0]\n" +
            "Rv1[v2] :- Rt1[v0],var v1 = Ttmp0{.column1 = v0.column1,.column2 = v0.column2},var v2 = v1.";
        testTranslation(query, program, true);
    }

    @Test
    public void testSimple() {
        String query = "create view v1 as select distinct * from t1";
        String program = this.header(false) +
                this.relations(false) +
                "output relation Rv1[Tt1]\n" +
                "Rv1[v1] :- Rt1[v0],var v1 = v0.";
        testTranslation(query, program);
    }

    @Test
    public void testSimpleWNull() {
        String query = "create view v1 as select distinct * from t1";
        String program = this.header(true) +
            this.relations(true) +
            "output relation Rv1[Tt1]\n" +
            "Rv1[v1] :- Rt1[v0],var v1 = v0.";
        testTranslation(query, program, true);
    }

    @Test
    public void testSimple1() {
        String query = "create view v2 as select distinct * from t1 where column1 = 10";
        String program = this.header(false) +
                this.relations(false) +
                "output relation Rv2[Tt1]\n" +
                "Rv2[v1] :- Rt1[v0],(v0.column1 == 64'sd10),var v1 = v0.";
        testTranslation(query, program);
    }

    @Test
    public void testSimple1WNulls() {
        String query = "create view v2 as select distinct * from t1 where column1 = 10";
        String program = this.header(true) +
            this.relations(true) +
            "output relation Rv2[Tt1]\n" +
            "Rv2[v1] :- Rt1[v0],unwrapBool(a_eq_NR(v0.column1, 64'sd10)),var v1 = v0.";
        testTranslation(query, program, true);
    }

    @Test
    public void testSimple2() {
        String query = "create view v3 as select distinct * from t1 where column1 = 10 and column2 = 'something'";
        String program = this.header(false) +
                this.relations(false) +
                "output relation Rv3[Tt1]\n" +
                "Rv3[v1] :- Rt1[v0],((v0.column1 == 64'sd10) and (v0.column2 == \"something\")),var v1 = v0.";
        testTranslation(query, program);
    }

    @Test
    public void testSimple2WNulls() {
        String query = "create view v3 as select distinct * from t1 where column1 = 10 and column2 = 'something'";
        String program = this.header(true) +
            this.relations(true) +
            "output relation Rv3[Tt1]\n" +
            "Rv3[v1] :- Rt1[v0],unwrapBool(b_and_NN(a_eq_NR(v0.column1, 64'sd10), s_eq_NR(v0.column2, \"something\"))),var v1 = v0.";
        testTranslation(query, program, true);
    }

    @Test
    public void testNested() {
        String query = "create view v4 as select distinct * from (select distinct * from t1 where column1 = 10) where column2 = 'something'";
        String program = this.header(false) +
                this.relations(false) +
                "relation Rtmp0[Tt1]\n" +
                "output relation Rv4[Tt1]\n" +
                "Rtmp0[v1] :- Rt1[v0],(v0.column1 == 64'sd10),var v1 = v0.\n" +
                "Rv4[v2] :- Rtmp0[v1],(v1.column2 == \"something\"),var v2 = v1.";
        testTranslation(query, program);
    }

    @Test
    public void testNestedWNull() {
        String query = "create view v4 as select distinct * from (select distinct * from t1 where column1 = 10) where column2 = 'something'";
        String program = this.header(true) +
            this.relations(true) +
            "relation Rtmp0[Tt1]\n" +
            "output relation Rv4[Tt1]\n" +
            "Rtmp0[v1] :- Rt1[v0],unwrapBool(a_eq_NR(v0.column1, 64'sd10)),var v1 = v0.\n" +
            "Rv4[v2] :- Rtmp0[v1],unwrapBool(s_eq_NR(v1.column2, \"something\")),var v2 = v1.";
        testTranslation(query, program, true);
    }

    @Test
    public void testWeave() {
        Translator t = new Translator(null);
        String node_info = "create table node_info\n" +
                "(\n" +
                "  name varchar(36) not null /*primary key*/,\n" +
                "  unschedulable boolean not null,\n" +
                "  out_of_disk boolean not null,\n" +
                "  memory_pressure boolean not null,\n" +
                "  disk_pressure boolean not null,\n" +
                "  pid_pressure boolean not null,\n" +
                "  ready boolean not null,\n" +
                "  network_unavailable boolean not null,\n" +
                "  cpu_capacity bigint not null,\n" +
                "  memory_capacity bigint not null,\n" +
                "  ephemeral_storage_capacity bigint not null,\n" +
                "  pods_capacity bigint not null,\n" +
                "  cpu_allocatable bigint not null,\n" +
                "  memory_allocatable bigint not null,\n" +
                "  ephemeral_storage_allocatable bigint not null,\n" +
                "  pods_allocatable bigint not null\n" +
                ")";
        DDlogIRNode create = t.translateSqlStatement(node_info);
        Assert.assertNotNull(create);

        String pod_info =
                "create table pod_info\n" +
                "(\n" +
                "  pod_name varchar(100) not null /*primary key*/,\n" +
                "  status varchar(36) not null,\n" +
                "  node_name varchar(36) /*null*/,\n" +
                "  namespace varchar(100) not null,\n" +
                "  cpu_request bigint not null,\n" +
                "  memory_request bigint not null,\n" +
                "  ephemeral_storage_request bigint not null,\n" +
                "  pods_request bigint not null,\n" +
                "  owner_name varchar(100) not null,\n" +
                "  creation_timestamp varchar(100) not null,\n" +
                "  priority integer not null,\n" +
                "  schedulerName varchar(50),\n" +
                "  has_node_selector_labels boolean not null,\n" +
                "  has_pod_affinity_requirements boolean not null\n" +
                ")";
        create = t.translateSqlStatement(pod_info);
        Assert.assertNotNull(create);

        String  pod_ports_request =
                "-- This table tracks the \"ContainerPorts\" fields of each pod.\n" +
                "-- It is used to enforce the PodFitsHostPorts constraint.\n" +
                "create table pod_ports_request\n" +
                "(\n" +
                "  pod_name varchar(100) not null,\n" +
                "  host_ip varchar(100) not null,\n" +
                "  host_port integer not null,\n" +
                "  host_protocol varchar(10) not null\n" +
                "  /*,foreign key(pod_name) references pod_info(pod_name) on delete cascade\n*/" +
                ")";
        create = t.translateSqlStatement(pod_ports_request);
        Assert.assertNotNull(create);

        String container_host_ports =
                "-- This table tracks the set of hostports in use at each node.\n" +
                "-- Also used to enforce the PodFitsHostPorts constraint.\n" +
                "create table container_host_ports\n" +
                "(\n" +
                "  pod_name varchar(100) not null,\n" +
                "  node_name varchar(36) not null,\n" +
                "  host_ip varchar(100) not null,\n" +
                "  host_port integer not null,\n" +
                "  host_protocol varchar(10) not null/*,\n" +
                "  foreign key(pod_name) references pod_info(pod_name) on delete cascade,\n" +
                "  foreign key(node_name) references node_info(name) on delete cascade\n*/" +
                ")";
        create = t.translateSqlStatement(container_host_ports);
        Assert.assertNotNull(create);

        String pod_node_selector_labels =
                "-- Tracks the set of node selector labels per pod.\n" +
                "create table pod_node_selector_labels\n" +
                "(\n" +
                "  pod_name varchar(100) not null,\n" +
                "  term integer not null,\n" +
                "  match_expression integer not null,\n" +
                "  num_match_expressions integer not null,\n" +
                "  label_key varchar(100) not null,\n" +
                "  label_operator varchar(12) not null,\n" +
                "  label_value varchar(36) /*null*/\n" +
                "  /*, foreign key(pod_name) references pod_info(pod_name) on delete cascade\n */" +
                ")";
        create = t.translateSqlStatement(pod_node_selector_labels);
        Assert.assertNotNull(create);

        String pod_affinity_match_expressions =
                "-- Tracks the set of pod affinity match expressions.\n" +
                "create table pod_affinity_match_expressions\n" +
                "(\n" +
                "  pod_name varchar(100) not null,\n" +
                "  label_selector integer not null,\n" +
                "  match_expression integer not null,\n" +
                "  num_match_expressions integer not null,\n" +
                "  label_key varchar(100) not null,\n" +
                "  label_operator varchar(30) not null,\n" +
                "  label_value varchar(36) not null,\n" +
                "  topology_key varchar(100) not null\n" +
                "  /*, foreign key(pod_name) references pod_info(pod_name) on delete cascade\n */" +
                ")";
        create = t.translateSqlStatement(pod_affinity_match_expressions);
        Assert.assertNotNull(create);

        String pod_anti_affinity_match_expressions =
                "-- Tracks the set of pod anti-affinity match expressions.\n" +
                "create table pod_anti_affinity_match_expressions\n" +
                "(\n" +
                "  pod_name varchar(100) not null,\n" +
                "  label_key varchar(100) not null,\n" +
                "  label_operator varchar(12) not null,\n" +
                "  label_value varchar(36) not null,\n" +
                "  topology_key varchar(100) not null/*,\n" +
                "  foreign key(pod_name) references pod_info(pod_name) on delete cascade\n*/" +
                ")";
        create = t.translateSqlStatement(pod_anti_affinity_match_expressions);
        Assert.assertNotNull(create);

        String pod_labels =
                "-- Tracks the set of labels per pod, and indicates if\n" +
                "-- any of them are also node selector labels\n" +
                "create table pod_labels\n" +
                "(\n" +
                "  pod_name varchar(100) not null,\n" +
                "  label_key varchar(100) not null,\n" +
                "  label_value varchar(36) not null/*,\n" +
                "  foreign key(pod_name) references pod_info(pod_name) on delete cascade\n*/" +
                ")";
        create = t.translateSqlStatement(pod_labels);
        Assert.assertNotNull(create);

        String node_labels =
                "-- Tracks the set of labels per node\n" +
                "create table node_labels\n" +
                "(\n" +
                "  node_name varchar(36) not null,\n" +
                "  label_key varchar(100) not null,\n" +
                "  label_value varchar(36) not null/*,\n" +
                "  foreign key(node_name) references node_info(name) on delete cascade\n*/" +
                ")";
        create = t.translateSqlStatement(node_labels);
        Assert.assertNotNull(create);

        String volume_labels =
                "-- Volume labels\n" +
                "create table volume_labels\n" +
                "(\n" +
                "  volume_name varchar(36) not null,\n" +
                "  pod_name varchar(100) not null,\n" +
                "  label_key varchar(100) not null,\n" +
                "  label_value varchar(36) not null/*,\n" +
                "  foreign key(pod_name) references pod_info(pod_name) on delete cascade\n*/" +
                ")";
        create = t.translateSqlStatement(volume_labels);
        Assert.assertNotNull(create);

        String pod_by_service =
                "-- For pods that have ports exposed\n" +
                "create table pod_by_service\n" +
                "(\n" +
                "  pod_name varchar(100) not null,\n" +
                "  service_name varchar(100) not null/*,\n" +
                "  foreign key(pod_name) references pod_info(pod_name) on delete cascade\n*/" +
                ")";
        create = t.translateSqlStatement(pod_by_service);
        Assert.assertNotNull(create);

        String service_affinity_labels =
                "-- Service affinity labels\n" +
                "create table service_affinity_labels\n" +
                "(\n" +
                "  label_key varchar(100) not null\n" +
                ")";
        create = t.translateSqlStatement(service_affinity_labels);
        Assert.assertNotNull(create);

        String labels_to_check_for_presence =
                "-- Labels present on node\n" +
                "create table labels_to_check_for_presence\n" +
                "(\n" +
                "  label_key varchar(100) not null,\n" +
                "  present boolean not null\n" +
                ")";
        create = t.translateSqlStatement(labels_to_check_for_presence);
        Assert.assertNotNull(create);

        String node_taints =
                "-- Node taints\n" +
                "create table node_taints\n" +
                "(\n" +
                "  node_name varchar(36) not null,\n" +
                "  taint_key varchar(100) not null,\n" +
                "  taint_value varchar(100),\n" +
                "  taint_effect varchar(100) not null/*,\n" +
                "  foreign key(node_name) references node_info(name) on delete cascade\n*/" +
                ")";
        create = t.translateSqlStatement(node_taints);
        Assert.assertNotNull(create);

        String pod_taints =
                "-- Pod taints.\n" +
                "create table pod_tolerations\n" +
                "(\n" +
                "  pod_name varchar(100) not null,\n" +
                "  tolerations_key varchar(100),\n" +
                "  tolerations_value varchar(100),\n" +
                "  tolerations_effect varchar(100),\n" +
                "  tolerations_operator varchar(100)/*,\n" +
                "  foreign key(pod_name) references pod_info(pod_name) on delete cascade\n*/" +
                ")";
        create = t.translateSqlStatement(pod_taints);
        Assert.assertNotNull(create);

        String node_images =
                "-- Tracks the set of node images that are already\n" +
                "-- available at a node\n" +
                "create table node_images\n" +
                "(\n" +
                "  node_name varchar(36) not null,\n" +
                "  image_name varchar(200) not null,\n" +
                "  image_size bigint not null/*,\n" +
                "  foreign key(node_name) references node_info(name) on delete cascade\n*/" +
                ")";
        create = t.translateSqlStatement(node_images);
        Assert.assertNotNull(create);

        String pod_images =
                "-- Tracks the container images required by each pod\n" +
                "create table pod_images\n" +
                "(\n" +
                "  pod_name varchar(100) not null,\n" +
                "  image_name varchar(200) not null/*,\n" +
                "  foreign key(pod_name) references pod_info(pod_name) on delete cascade\n*/" +
                ")";
        create = t.translateSqlStatement(pod_images);
        Assert.assertNotNull(create);

        String pods_to_assign_no_limit =
                "-- Select all pods that need to be scheduled.\n" +
                "-- We also indicate boolean values to check whether\n" +
                "-- a pod has node selector or pod affinity labels,\n" +
                "-- and whether pod affinity rules already yields some subset of\n" +
                "-- nodes that we can assign pods to.\n" +
                "create view pods_to_assign_no_limit as\n" +
                "select DISTINCT\n" +
                "  pod_name,\n" +
                "  status,\n" +
                "  node_name as controllable__node_name,\n" +
                "  namespace,\n" +
                "  cpu_request,\n" +
                "  memory_request,\n" +
                "  ephemeral_storage_request,\n" +
                "  pods_request,\n" +
                "  owner_name,\n" +
                "  creation_timestamp,\n" +
                "  has_node_selector_labels,\n" +
                "  has_pod_affinity_requirements\n" +
                "from pod_info\n" +
                "where status = 'Pending' and node_name is null and schedulerName = 'dcm-scheduler'\n" +
                "/*order by creation_timestamp*/\n";
        create = t.translateSqlStatement(pods_to_assign_no_limit);
        Assert.assertNotNull(create);

        String batch_size =
                "\n" +
                "-- This view is updated dynamically to change the limit. This\n" +
                "-- pattern is required because there is no clean way to enforce\n" +
                "-- a dynamic \"LIMIT\" clause.\n" +
                "create table batch_size\n" +
                "(\n" +
                "  pendingPodsLimit integer not null /*primary key*/\n" +
                ")";
        create = t.translateSqlStatement(batch_size);
        Assert.assertNotNull(create);

        String pods_to_assign =
                "create view pods_to_assign as\n" +
                "select DISTINCT * from pods_to_assign_no_limit /*limit 100*/\n";
        create = t.translateSqlStatement(pods_to_assign);
        Assert.assertNotNull(create);

        String pods_with_port_request =
                "-- Pods with port requests\n" +
                "create view pods_with_port_requests as\n" +
                "select DISTINCT pods_to_assign.controllable__node_name as controllable__node_name,\n" +
                "       pod_ports_request.host_port as host_port,\n" +
                "       pod_ports_request.host_ip as host_ip,\n" +
                "       pod_ports_request.host_protocol as host_protocol\n" +
                "from pods_to_assign\n" +
                "join pod_ports_request\n" +
                "     on pod_ports_request.pod_name = pods_to_assign.pod_name";
        create = t.translateSqlStatement(pods_with_port_request);
        Assert.assertNotNull(create);

        /*
        String pod_node_selectors =
                "-- Pod node selectors\n" +
                "create view pod_node_selector_matches as\n" +
                "select DISTINCT pods_to_assign.pod_name as pod_name,\n" +
                "       node_labels.node_name as node_name\n" +
                "from pods_to_assign\n" +
                "join pod_node_selector_labels\n" +
                "     on pods_to_assign.pod_name = pod_node_selector_labels.pod_name\n" +
                "join node_labels\n" +
                "        on\n" +
                "           (pod_node_selector_labels.label_operator = 'In'\n" +
                "            and pod_node_selector_labels.label_key = node_labels.label_key\n" +
                "            and pod_node_selector_labels.label_value = node_labels.label_value)\n" +
                "        or (pod_node_selector_labels.label_operator = 'Exists'\n" +
                "            and pod_node_selector_labels.label_key = node_labels.label_key)\n" +
                "        or (pod_node_selector_labels.label_operator = 'NotIn')\n" +
                "        or (pod_node_selector_labels.label_operator = 'DoesNotExist')\n" +
                "group by pods_to_assign.pod_name,  node_labels.node_name, pod_node_selector_labels.term,\n" +
                "         pod_node_selector_labels.label_operator, pod_node_selector_labels.num_match_expressions\n" +
                "having case pod_node_selector_labels.label_operator\n" +
                "            when 'NotIn'\n" +
                "                 then not(any(pod_node_selector_labels.label_key = node_labels.label_key\n" +
                "                              and pod_node_selector_labels.label_value = node_labels.label_value))\n" +
                "            when 'DoesNotExist'\n" +
                "                 then not(any(pod_node_selector_labels.label_key = node_labels.label_key))\n" +
                "            else count(distinct match_expression) = pod_node_selector_labels.num_match_expressions\n" +
                "       end";
        create = t.translateSqlStatement(pod_node_selectors);
        Assert.assertNotNull(create);

        // indexes are ignored
        String indexes =
                "create index pod_info_idx on pod_info (status, node_name);\n" +
                "create index pod_node_selector_labels_fk_idx on pod_node_selector_labels (pod_name);\n" +
                "create index node_labels_idx on node_labels (label_key, label_value);\n" +
                "create index pod_affinity_match_expressions_idx on pod_affinity_match_expressions (pod_name);\n" +
                "create index pod_labels_idx on pod_labels (label_key, label_value);\n";
        create = t.translateSqlStatement(pod_node_selectors);
        Assert.assertNotNull(create);

        String inter_pod_affinity =
                "-- Inter pod affinity\n" +
                "create view inter_pod_affinity_matches_inner as\n" +
                "select pods_to_assign.pod_name as pod_name,\n" +
                "       pod_labels.pod_name as matches,\n" +
                "       pod_info.node_name as node_name\n" +
                "from pods_to_assign\n" +
                "join pod_affinity_match_expressions\n" +
                "     on pods_to_assign.pod_name = pod_affinity_match_expressions.pod_name\n" +
                "join pod_labels\n" +
                "        on (pod_affinity_match_expressions.label_operator = 'In'\n" +
                "            and pod_affinity_match_expressions.label_key = pod_labels.label_key\n" +
                "            and pod_affinity_match_expressions.label_value = pod_labels.label_value)\n" +
                "        or (pod_affinity_match_expressions.label_operator = 'Exists'\n" +
                "            and pod_affinity_match_expressions.label_key = pod_labels.label_key)\n" +
                "        or (pod_affinity_match_expressions.label_operator = 'NotIn')\n" +
                "        or (pod_affinity_match_expressions.label_operator = 'DoesNotExist')\n" +
                "join pod_info\n" +
                "        on pod_labels.pod_name = pod_info.pod_name\n" +
                "group by pods_to_assign.pod_name,  pod_labels.pod_name, pod_affinity_match_expressions.label_selector,\n" +
                "         pod_affinity_match_expressions.topology_key, pod_affinity_match_expressions.label_operator,\n" +
                "         pod_affinity_match_expressions.num_match_expressions, pod_info.node_name\n" +
                "having case pod_affinity_match_expressions.label_operator\n" +
                "             when 'NotIn'\n" +
                "                  then not(any(pod_affinity_match_expressions.label_key = pod_labels.label_key\n" +
                "                               and pod_affinity_match_expressions.label_value = pod_labels.label_value))\n" +
                "             when 'DoesNotExist'\n" +
                "                  then not(any(pod_affinity_match_expressions.label_key = pod_labels.label_key))\n" +
                "             else count(distinct match_expression) = pod_affinity_match_expressions.num_match_expressions\n" +
                "       end";
        create = t.translateSqlStatement(inter_pod_affinity);
        Assert.assertNotNull(create);

        String inter_pod_affinity_matches =
                "create view inter_pod_affinity_matches as\n" +
                "select *, count(*) over (partition by pod_name) as num_matches from inter_pod_affinity_matches_inner";
        create = t.translateSqlStatement(inter_pod_affinity_matches);
        Assert.assertNotNull(create);

        String spare_capacity =
                "-- Spare capacity\n" +
                "create view spare_capacity_per_node as\n" +
                "select node_info.name as name,\n" +
                "       cast(node_info.cpu_allocatable - sum(pod_info.cpu_request) as integer) as cpu_remaining,\n" +
                "       cast(node_info.memory_allocatable - sum(pod_info.memory_request) as integer) as memory_remaining,\n" +
                "       cast(node_info.pods_allocatable - sum(pod_info.pods_request) as integer) as pods_remaining\n" +
                "from node_info\n" +
                "join pod_info\n" +
                "     on pod_info.node_name = node_info.name and pod_info.node_name != 'null'\n" +
                "group by node_info.name, node_info.cpu_allocatable,\n" +
                "         node_info.memory_allocatable, node_info.pods_allocatable";
        create = t.translateSqlStatement(spare_capacity);
        Assert.assertNotNull(create);

        String view_pods_that_tolerate_node_taints =
                "-- Taints and tolerations\n" +
                "create view pods_that_tolerate_node_taints as\n" +
                "select pods_to_assign.pod_name as pod_name,\n" +
                "       A.node_name as node_name\n" +
                "from pods_to_assign\n" +
                "join pod_tolerations\n" +
                "     on pods_to_assign.pod_name = pod_tolerations.pod_name\n" +
                "join (select *, count(*) over (partition by node_name) as num_taints from node_taints) as A\n" +
                "     on pod_tolerations.tolerations_key = A.taint_key\n" +
                "     and (pod_tolerations.tolerations_effect = null\n" +
                "          or pod_tolerations.tolerations_effect = A.taint_effect)\n" +
                "     and (pod_tolerations.tolerations_operator = 'Exists'\n" +
                "          or pod_tolerations.tolerations_value = A.taint_value)\n" +
                "group by pod_tolerations.pod_name, A.node_name, A.num_taints\n" +
                "having count(*) = A.num_taints";
        create = t.translateSqlStatement(view_pods_that_tolerate_node_taints);
        Assert.assertNotNull(create);
        */

        String nodes_that_have_tollerations =
                "create view nodes_that_have_tolerations as\n" +
                "select distinct node_name from node_taints";
        create = t.translateSqlStatement(nodes_that_have_tollerations);
        Assert.assertNotNull(create);

        DDlogProgram program = t.getDDlogProgram();
        String p = program.toString();
        Assert.assertNotNull(p);
        String expected = imports + "\n" +
            "typedef Tnode_info = Tnode_info{name:string, unschedulable:bool, out_of_disk:bool, memory_pressure:bool, disk_pressure:bool, pid_pressure:bool, ready:bool, network_unavailable:bool, cpu_capacity:bigint, memory_capacity:bigint, ephemeral_storage_capacity:bigint, pods_capacity:bigint, cpu_allocatable:bigint, memory_allocatable:bigint, ephemeral_storage_allocatable:bigint, pods_allocatable:bigint}\n" +
            "typedef Tpod_info = Tpod_info{pod_name:string, status:string, node_name:Option<string>, namespace:string, cpu_request:bigint, memory_request:bigint, ephemeral_storage_request:bigint, pods_request:bigint, owner_name:string, creation_timestamp:string, priority:signed<64>, schedulerName:Option<string>, has_node_selector_labels:bool, has_pod_affinity_requirements:bool}\n" +
            "typedef Tpod_ports_request = Tpod_ports_request{pod_name:string, host_ip:string, host_port:signed<64>, host_protocol:string}\n" +
            "typedef Tcontainer_host_ports = Tcontainer_host_ports{pod_name:string, node_name:string, host_ip:string, host_port:signed<64>, host_protocol:string}\n" +
            "typedef Tpod_node_selector_labels = Tpod_node_selector_labels{pod_name:string, term:signed<64>, match_expression:signed<64>, num_match_expressions:signed<64>, label_key:string, label_operator:string, label_value:Option<string>}\n" +
            "typedef Tpod_affinity_match_expressions = Tpod_affinity_match_expressions{pod_name:string, label_selector:signed<64>, match_expression:signed<64>, num_match_expressions:signed<64>, label_key:string, label_operator:string, label_value:string, topology_key:string}\n" +
            "typedef Tpod_anti_affinity_match_expressions = Tpod_anti_affinity_match_expressions{pod_name:string, label_key:string, label_operator:string, label_value:string, topology_key:string}\n" +
            "typedef Tpod_labels = Tpod_labels{pod_name:string, label_key:string, label_value:string}\n" +
            "typedef Tnode_labels = Tnode_labels{node_name:string, label_key:string, label_value:string}\n" +
            "typedef Tvolume_labels = Tvolume_labels{volume_name:string, pod_name:string, label_key:string, label_value:string}\n" +
            "typedef Tpod_by_service = Tpod_by_service{pod_name:string, service_name:string}\n" +
            "typedef Tservice_affinity_labels = Tservice_affinity_labels{label_key:string}\n" +
            "typedef Tlabels_to_check_for_presence = Tlabels_to_check_for_presence{label_key:string, present:bool}\n" +
            "typedef Tnode_taints = Tnode_taints{node_name:string, taint_key:string, taint_value:Option<string>, taint_effect:string}\n" +
            "typedef Tpod_tolerations = Tpod_tolerations{pod_name:string, tolerations_key:Option<string>, tolerations_value:Option<string>, tolerations_effect:Option<string>, tolerations_operator:Option<string>}\n" +
            "typedef Tnode_images = Tnode_images{node_name:string, image_name:string, image_size:bigint}\n" +
            "typedef Tpod_images = Tpod_images{pod_name:string, image_name:string}\n" +
            "typedef Ttmp0 = Ttmp0{pod_name:string, status:string, controllable__node_name:Option<string>, namespace:string, cpu_request:bigint, memory_request:bigint, ephemeral_storage_request:bigint, pods_request:bigint, owner_name:string, creation_timestamp:string, has_node_selector_labels:bool, has_pod_affinity_requirements:bool}\n" +
            "typedef Tbatch_size = Tbatch_size{pendingPodsLimit:signed<64>}\n" +
            "typedef Ttmp1 = Ttmp1{pod_name:string, status:string, controllable__node_name:Option<string>, namespace:string, cpu_request:bigint, memory_request:bigint, ephemeral_storage_request:bigint, pods_request:bigint, owner_name:string, creation_timestamp:string, has_node_selector_labels:bool, has_pod_affinity_requirements:bool, pod_name0:string, host_ip:string, host_port:signed<64>, host_protocol:string}\n" +
            "typedef Ttmp2 = Ttmp2{controllable__node_name:Option<string>, host_port:signed<64>, host_ip:string, host_protocol:string}\n" +
            "typedef Ttmp3 = Ttmp3{node_name:string}\n" +
            "\n" +
            "input relation Rnode_info[Tnode_info]\n" +
            "input relation Rpod_info[Tpod_info]\n" +
            "input relation Rpod_ports_request[Tpod_ports_request]\n" +
            "input relation Rcontainer_host_ports[Tcontainer_host_ports]\n" +
            "input relation Rpod_node_selector_labels[Tpod_node_selector_labels]\n" +
            "input relation Rpod_affinity_match_expressions[Tpod_affinity_match_expressions]\n" +
            "input relation Rpod_anti_affinity_match_expressions[Tpod_anti_affinity_match_expressions]\n" +
            "input relation Rpod_labels[Tpod_labels]\n" +
            "input relation Rnode_labels[Tnode_labels]\n" +
            "input relation Rvolume_labels[Tvolume_labels]\n" +
            "input relation Rpod_by_service[Tpod_by_service]\n" +
            "input relation Rservice_affinity_labels[Tservice_affinity_labels]\n" +
            "input relation Rlabels_to_check_for_presence[Tlabels_to_check_for_presence]\n" +
            "input relation Rnode_taints[Tnode_taints]\n" +
            "input relation Rpod_tolerations[Tpod_tolerations]\n" +
            "input relation Rnode_images[Tnode_images]\n" +
            "input relation Rpod_images[Tpod_images]\n" +
            "relation Rtmp0[Ttmp0]\n" +
            "output relation Rpods_to_assign_no_limit[Ttmp0]\n" +
            "input relation Rbatch_size[Tbatch_size]\n" +
            "output relation Rpods_to_assign[Ttmp0]\n" +
            "relation Rtmp2[Ttmp2]\n" +
            "output relation Rpods_with_port_requests[Ttmp2]\n" +
            "relation Rtmp3[Ttmp3]\n" +
            "output relation Rnodes_that_have_tolerations[Ttmp3]\n" +
            "Rpods_to_assign_no_limit[v2] :- Rpod_info[v0],unwrapBool(b_and_RN(((v0.status == \"Pending\") and isNull(v0.node_name)), s_eq_NR(v0.schedulerName, \"dcm-scheduler\"))),var v1 = Ttmp0{.pod_name = v0.pod_name,.status = v0.status,.controllable__node_name = v0.node_name,.namespace = v0.namespace,.cpu_request = v0.cpu_request,.memory_request = v0.memory_request,.ephemeral_storage_request = v0.ephemeral_storage_request,.pods_request = v0.pods_request,.owner_name = v0.owner_name,.creation_timestamp = v0.creation_timestamp,.has_node_selector_labels = v0.has_node_selector_labels,.has_pod_affinity_requirements = v0.has_pod_affinity_requirements},var v2 = v1.\n" +
            "Rpods_to_assign[v1] :- Rpods_to_assign_no_limit[v0],var v1 = v0.\n" +
            "Rpods_with_port_requests[v4] :- Rpods_to_assign[v0],Rpod_ports_request[v1],(v1.pod_name == v0.pod_name),true,var v2 = Ttmp1{.pod_name = v0.pod_name,.status = v0.status,.controllable__node_name = v0.controllable__node_name,.namespace = v0.namespace,.cpu_request = v0.cpu_request,.memory_request = v0.memory_request,.ephemeral_storage_request = v0.ephemeral_storage_request,.pods_request = v0.pods_request,.owner_name = v0.owner_name,.creation_timestamp = v0.creation_timestamp,.has_node_selector_labels = v0.has_node_selector_labels,.has_pod_affinity_requirements = v0.has_pod_affinity_requirements,.pod_name0 = v1.pod_name,.host_ip = v1.host_ip,.host_port = v1.host_port,.host_protocol = v1.host_protocol},var v3 = Ttmp2{.controllable__node_name = v0.controllable__node_name,.host_port = v1.host_port,.host_ip = v1.host_ip,.host_protocol = v1.host_protocol},var v4 = v3.\n" +
            "Rnodes_that_have_tolerations[v2] :- Rnode_taints[v0],var v1 = Ttmp3{.node_name = v0.node_name},var v2 = v1.";
        Assert.assertEquals(expected, p);
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
