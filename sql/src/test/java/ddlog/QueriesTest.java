package ddlog;

import com.vmware.ddlog.ir.DDlogIRNode;
import com.vmware.ddlog.ir.DDlogProgram;
import com.vmware.ddlog.translator.TranslationException;
import com.vmware.ddlog.translator.Translator;
import org.h2.store.fs.FileUtils;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.*;
import java.util.stream.Collectors;

/**
 * Unit test for simple Translator.
 */
@SuppressWarnings("FieldCanBeLocal")
public class QueriesTest {
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
            tmp.deleteOnExit();
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
        //System.out.println(s);
        Assert.assertEquals(program, s);
        this.compiledDDlog(s);
    }

    private void testTranslation(String query, String program) {
        this.testTranslation(query, program, false);
    }

    @Test
    public void testGroupBy() {
        String query = "create view v1 as SELECT COUNT(*) FROM t1 GROUP BY column2";
        String program = this.header(false) +
                "typedef Ttmp0 = Ttmp0{col4:signed<64>}\n" +
                "function agg1(g1: Group<(Tt1)>):Ttmp0 =\n" +
                "var first3 = true;\n" +
                "(var count5 = 64'sd0);\n" +
                "(for (i2 in g1) {\n" +
                "var v0 = i2;\n" +
                "(count5 = if first3 {\n" +
                "64'sd1} else {\n" +
                "agg_count_R(count5, 64'sd1)});\n" +
                "(first3 = false)}\n" +
                ");\n" +
                "(Ttmp0{.col4 = count5})" +
                this.relations(false) +
                "relation Rtmp0[Ttmp0]\n" +
                "output relation Rv1[Ttmp0]\n" +
                "Rv1[v8] :- Rt1[v0],var g7 = v0.column2,var v6 = Aggregate((g7), agg1((v0))),var v8 = v6.";
        this.testTranslation(query, program);
    }

    @Test
    public void testGroupBy1() {
        String query = "create view v1 as SELECT column2, COUNT(*) FROM t1 GROUP BY column2";
        String program = this.header(false) +
                "typedef Ttmp0 = Ttmp0{column2:string, col5:signed<64>}\n" +
                "typedef Tagg12 = Tagg12{col5:signed<64>}\n" +
                "function agg1(g2: Group<(Tt1)>):Tagg12 =\n" +
                "var first4 = true;\n" +
                "(var count6 = 64'sd0);\n" +
                "(for (i3 in g2) {\n" +
                "var v0 = i3;\n" +
                "(count6 = if first4 {\n" +
                "64'sd1} else {\n" +
                "agg_count_R(count6, 64'sd1)});\n" +
                "(first4 = false)}\n" +
                ");\n" +
                "(Tagg12{.col5 = count6})" +
                this.relations(false) +
                "relation Rtmp0[Ttmp0]\n" +
                "output relation Rv1[Ttmp0]\n" +
                "Rv1[v9] :- Rt1[v0],var gb1 = v0.column2,var aggResult8 = Aggregate((gb1), agg1((v0))),var v7 = Ttmp0{.column2 = gb1,.col5 = aggResult8.col5},var v9 = v7.";
        this.testTranslation(query, program);
    }

    @Test
    public void testTwoAggregations() {
        String query = "create view v1 as SELECT MIN(column1) + MAX(column1) FROM t1";
        String program = this.header(false) +
                "typedef Ttmp0 = Ttmp0{col4:signed<64>}\n" +
                "function agg1(g1: Group<'K, (Tt1)>):Ttmp0 =\n" +
                "var first3 = true;\n" +
                "(var min6 = 64'sd0);\n" +
                "(var max8 = 64'sd0);\n" +
                "(for (i2 in g1) {\n" +
                "var v0 = i2;\n" +
                "(var incr5 = v0.column1);\n" +
                "(min6 = if first3 {\n" +
                "incr5} else {\n" +
                "agg_min_R(min6, incr5)});\n" +
                "(var incr7 = v0.column1);\n" +
                "(max8 = if first3 {\n" +
                "incr7} else {\n" +
                "agg_max_R(max8, incr7)});\n" +
                "(first3 = false)}\n" +
                ");\n" +
                "(Ttmp0{.col4 = (min6 + max8)})" +
                this.relations(false) +
                "relation Rtmp0[Ttmp0]\n" +
                "output relation Rv1[Ttmp0]\n" +
                "Rv1[v10] :- Rt1[v0],var v9 = Aggregate((), agg1((v0))),var v10 = v9.";
        this.testTranslation(query, program);
    }

    @Test
    public void testStringAggregate() {
        String query = "create view v1 as SELECT MIN(column2) FROM t1";
        String program = this.header(false) +
                "typedef Ttmp0 = Ttmp0{col4:string}\n" +
                "function agg1(g1: Group<'K, (Tt1)>):Ttmp0 =\n" +
                "var first3 = true;\n" +
                "(var min6 = \"\");\n" +
                "(for (i2 in g1) {\n" +
                "var v0 = i2;\n" +
                "(var incr5 = v0.column2);\n" +
                "(min6 = if first3 {\n" +
                "incr5} else {\n" +
                "agg_min_R(min6, incr5)});\n" +
                "(first3 = false)}\n" +
                ");\n" +
                "(Ttmp0{.col4 = min6})" +
                this.relations(false) +
                "relation Rtmp0[Ttmp0]\n" +
                "output relation Rv1[Ttmp0]\n" +
                "Rv1[v8] :- Rt1[v0],var v7 = Aggregate((), agg1((v0))),var v8 = v7.";
        this.testTranslation(query, program);
    }

    @Test
    public void testParallelAggregates() {
        String query = "create view v1 as SELECT COUNT(column1), SUM(column1) FROM t1";
        String program = this.header(false) +
                "typedef Ttmp0 = Ttmp0{col4:signed<64>, col7:signed<64>}\n" +
                "function agg1(g1: Group<'K, (Tt1)>):Ttmp0 =\n" +
                "var first3 = true;\n" +
                "(var count6 = 64'sd0);\n" +
                "(var sum9 = 64'sd0);\n" +
                "(for (i2 in g1) {\n" +
                "var v0 = i2;\n" +
                "(var incr5 = v0.column1);\n" +
                "(count6 = if first3 {\n" +
                "64'sd1} else {\n" +
                "agg_count_R(count6, incr5)});\n" +
                "(var incr8 = v0.column1);\n" +
                "(sum9 = if first3 {\n" +
                "64'sd0} else {\n" +
                "agg_sum_R(sum9, incr8)});\n" +
                "(first3 = false)}\n" +
                ");\n" +
                "(Ttmp0{.col4 = count6,.col7 = sum9})" +
                this.relations(false) +
                "relation Rtmp0[Ttmp0]\n" +
                "output relation Rv1[Ttmp0]\n" +
                "Rv1[v11] :- Rt1[v0],var v10 = Aggregate((), agg1((v0))),var v11 = v10.";
        this.testTranslation(query, program);
    }

    @Test
    public void testCount() {
        String query = "create view v1 as SELECT COUNT(*) FROM t1";
        String program = this.header(false) +
                "typedef Ttmp0 = Ttmp0{col4:signed<64>}\n" +
                "function agg1(g1: Group<'K, (Tt1)>):Ttmp0 =\n" +
                "var first3 = true;\n" +
                "(var count5 = 64'sd0);\n" +
                "(for (i2 in g1) {\n" +
                "var v0 = i2;\n" +
                "(count5 = if first3 {\n" +
                "64'sd1} else {\n" +
                "agg_count_R(count5, 64'sd1)});\n" +
                "(first3 = false)}\n" +
                ");\n" +
                "(Ttmp0{.col4 = count5})" +
                this.relations(false) +
                "relation Rtmp0[Ttmp0]\n" +
                "output relation Rv1[Ttmp0]\n" +
                "Rv1[v7] :- Rt1[v0],var v6 = Aggregate((), agg1((v0))),var v7 = v6.";
        this.testTranslation(query, program);
    }

    @Test
    public void testCountJoin() {
        String query = "create view v1 as SELECT COUNT(t1.column2) FROM t1 JOIN t2 ON t1.column1 = t2.column1";
        String program = this.header(false) +
                "typedef Ttmp0 = Ttmp0{column1:signed<64>, column2:string, column3:bool, column10:signed<64>}\n" +
                "typedef Ttmp1 = Ttmp1{col6:signed<64>}\n" +
                "function agg2(g3: Group<'K, (Tt1,Tt2)>):Ttmp1 =\n" +
                "var first5 = true;\n" +
                "(var count8 = 64'sd0);\n" +
                "(for (i4 in g3) {\n" +
                "var v0 = i4.0;\n" +
                "(var v1 = i4.1);\n" +
                "(var incr7 = v0.column2);\n" +
                "(count8 = if first5 {\n" +
                "64'sd1} else {\n" +
                "agg_count_R(count8, incr7)});\n" +
                "(first5 = false)}\n" +
                ");\n" +
                "(Ttmp1{.col6 = count8})" +
                this.relations(false) +
                "relation Rtmp1[Ttmp1]\n" +
                "output relation Rv1[Ttmp1]\n" +
                "Rv1[v10] :- Rt1[v0],Rt2[v1],(v0.column1 == v1.column1),true,var v2 = Ttmp0{.column1 = v0.column1,.column2 = v0.column2,.column3 = v0.column3,.column10 = v1.column1},var v9 = Aggregate((), agg2((v0, v1))),var v10 = v9.";
        this.testTranslation(query, program);
    }

    @Test
    public void testCountColumn() {
        String query = "create view v1 as SELECT COUNT(column1) FROM t1";
        String program = this.header(false) +
                "typedef Ttmp0 = Ttmp0{col4:signed<64>}\n" +
                "function agg1(g1: Group<'K, (Tt1)>):Ttmp0 =\n" +
                "var first3 = true;\n" +
                "(var count6 = 64'sd0);\n" +
                "(for (i2 in g1) {\n" +
                "var v0 = i2;\n" +
                "(var incr5 = v0.column1);\n" +
                "(count6 = if first3 {\n" +
                "64'sd1} else {\n" +
                "agg_count_R(count6, incr5)});\n" +
                "(first3 = false)}\n" +
                ");\n" +
                "(Ttmp0{.col4 = count6})" +
                this.relations(false) +
                "relation Rtmp0[Ttmp0]\n" +
                "output relation Rv1[Ttmp0]\n" +
                "Rv1[v8] :- Rt1[v0],var v7 = Aggregate((), agg1((v0))),var v8 = v7.";
        this.testTranslation(query, program);
    }

    @Test
    public void testCountColumnWNull() {
        String query = "create view v1 as SELECT COUNT(column1) FROM t1";
        String program = this.header(true) +
                "typedef Ttmp0 = Ttmp0{col4:signed<64>}\n" +
                "function agg1(g1: Group<'K, (Tt1)>):Ttmp0 =\n" +
                "var first3 = true;\n" +
                "(var count6 = 64'sd0);\n" +
                "(for (i2 in g1) {\n" +
                "var v0 = i2;\n" +
                "(var incr5 = v0.column1);\n" +
                "(count6 = if first3 {\n" +
                "isNullAsInt(incr5)} else {\n" +
                "agg_count_N(count6, incr5)});\n" +
                "(first3 = false)}\n" +
                ");\n" +
                "(Ttmp0{.col4 = count6})" +
                this.relations(true) +
                "relation Rtmp0[Ttmp0]\n" +
                "output relation Rv1[Ttmp0]\n" +
                "Rv1[v8] :- Rt1[v0],var v7 = Aggregate((), agg1((v0))),var v8 = v7.";
        this.testTranslation(query, program, true);
    }

    @Test
    public void testAvg() {
        String query = "create view v1 as SELECT AVG(column1) FROM t1";
        String program = this.header(false) +
                "typedef Ttmp0 = Ttmp0{col4:signed<64>}\n" +
                "function agg1(g1: Group<'K, (Tt1)>):Ttmp0 =\n" +
                "var first3 = true;\n" +
                "(var avg6 = (64'sd0, 64'sd0));\n" +
                "(for (i2 in g1) {\n" +
                "var v0 = i2;\n" +
                "(var incr5 = v0.column1);\n" +
                "(avg6 = if first3 {\n" +
                "(64'sd1, incr5)} else {\n" +
                "agg_avg_R(avg6, incr5)});\n" +
                "(first3 = false)}\n" +
                ");\n" +
                "(Ttmp0{.col4 = avg(avg6.1, avg6.0)})" +
                this.relations(false) +
                "relation Rtmp0[Ttmp0]\n" +
                "output relation Rv1[Ttmp0]\n" +
                "Rv1[v8] :- Rt1[v0],var v7 = Aggregate((), agg1((v0))),var v8 = v7.";
        this.testTranslation(query, program);
    }

    @Test
    public void testAvgWNull() {
        String query = "create view v1 as SELECT AVG(column1) FROM t1";
        String program = this.header(true) +
                "typedef Ttmp0 = Ttmp0{col4:signed<64>}\n" +
                "function agg1(g1: Group<'K, (Tt1)>):Ttmp0 =\n" +
                "var first3 = true;\n" +
                "(var avg6 = (64'sd0, 64'sd0));\n" +
                "(for (i2 in g1) {\n" +
                "var v0 = i2;\n" +
                "(var incr5 = v0.column1);\n" +
                "(avg6 = if first3 {\n" +
                "(isNullAsInt(incr5), unwrapNull(incr5))} else {\n" +
                "agg_avg_N(avg6, incr5)});\n" +
                "(first3 = false)}\n" +
                ");\n" +
                "(Ttmp0{.col4 = avg(avg6.1, avg6.0)})" +
                this.relations(true) +
                "relation Rtmp0[Ttmp0]\n" +
                "output relation Rv1[Ttmp0]\n" +
                "Rv1[v8] :- Rt1[v0],var v7 = Aggregate((), agg1((v0))),var v8 = v7.";
        this.testTranslation(query, program, true);
    }

    @Test
    public void testCountWNull() {
        String query = "create view v1 as SELECT COUNT(*) FROM t1";
        String program = this.header(true) +
                "typedef Ttmp0 = Ttmp0{col4:signed<64>}\n" +
                "function agg1(g1: Group<'K, (Tt1)>):Ttmp0 =\n" +
                "var first3 = true;\n" +
                "(var count5 = 64'sd0);\n" +
                "(for (i2 in g1) {\n" +
                "var v0 = i2;\n" +
                "(count5 = if first3 {\n" +
                "64'sd1} else {\n" +
                "agg_count_R(count5, 64'sd1)});\n" +
                "(first3 = false)}\n" +
                ");\n" +
                "(Ttmp0{.col4 = count5})" +
                this.relations(true) +
                "relation Rtmp0[Ttmp0]\n" +
                "output relation Rv1[Ttmp0]\n" +
                "Rv1[v7] :- Rt1[v0],var v6 = Aggregate((), agg1((v0))),var v7 = v6.";
        this.testTranslation(query, program, true);
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
                        "Rv1[v3] :- Rt1[v0],var v2 = Ttmp0{.col1 = if (v0.column1 == 64'sd1) {\n" +
                        "64'sd1} else {\n" +
                        "if (64'sd1 < v0.column1) {\n" +
                        "64'sd2} else {\n" +
                        "64'sd3}}},var v3 = v2.";
        this.testTranslation(query, program);
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
                "Rv1[v3] :- Rt1[v0],var v2 = Ttmp0{.col1 = if unwrapBool(a_eq_NR(v0.column1, 64'sd1)) {\n" +
                "64'sd1} else {\n" +
                "if unwrapBool(a_lt_RN(64'sd1, v0.column1)) {\n" +
                "64'sd2} else {\n" +
                "64'sd3}}},var v3 = v2.";
        this.testTranslation(query, program, true);
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
        this.testTranslation(query, program);
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
        this.testTranslation(query, program, true);
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
        this.testTranslation(query, program);
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
        this.testTranslation(query, program, true);
    }

    @Test
    public void testImplicitJoin() {
        String query = "create view v1 as SELECT DISTINCT * FROM t1, t2";
        String program = this.header(false) +
                "typedef Ttmp0 = Ttmp0{column1:signed<64>, column2:string, column3:bool, column10:signed<64>}\n" +
                this.relations(false) +
                "output relation Rv1[Ttmp0]\n" +
                "Rv1[v3] :- Rt1[v0],Rt2[v1],true,var v2 = Ttmp0{.column1 = v0.column1,.column2 = v0.column2,.column3 = v0.column3,.column10 = v1.column1},var v3 = v2.";
        this.testTranslation(query, program);
    }

    @Test
    public void testJoinStar() {
        String query = "create view v1 as SELECT DISTINCT * FROM t1 JOIN t2 ON t1.column1 = t2.column1";
        String program = this.header(false) +
                "typedef Ttmp0 = Ttmp0{column1:signed<64>, column2:string, column3:bool, column10:signed<64>}\n" +
                this.relations(false) +
                "output relation Rv1[Ttmp0]\n" +
                "Rv1[v3] :- Rt1[v0],Rt2[v1],(v0.column1 == v1.column1),true,var v2 = Ttmp0{.column1 = v0.column1,.column2 = v0.column2,.column3 = v0.column3,.column10 = v1.column1},var v3 = v2.";
        this.testTranslation(query, program);
    }

    @Test
    public void testJoinStarWNull() {
        String query = "create view v1 as SELECT DISTINCT * FROM t1 JOIN t2 ON t1.column1 = t2.column1";
        String program = this.header(true) +
                "typedef Ttmp0 = Ttmp0{column1:Option<signed<64>>, column2:Option<string>, column3:Option<bool>, column10:Option<signed<64>>}\n" +
                this.relations(true) +
                "output relation Rv1[Ttmp0]\n" +
                "Rv1[v3] :- Rt1[v0],Rt2[v1],unwrapBool(a_eq_NN(v0.column1, v1.column1)),true,var v2 = Ttmp0{.column1 = v0.column1,.column2 = v0.column2,.column3 = v0.column3,.column10 = v1.column1},var v3 = v2.";
        this.testTranslation(query, program, true);
    }

    @Test
    public void testNaturalJoin() {
        String query = "create view v1 as SELECT DISTINCT * FROM t1 NATURAL JOIN t2";
        String program = this.header(false) +
                "typedef Ttmp0 = Ttmp0{column1:signed<64>, column2:string, column3:bool}\n" +
                this.relations(false) +
                "output relation Rv1[Ttmp0]\n" +
                "Rv1[v3] :- Rt1[v0],Rt2[v1],(true and (v0.column1 == v1.column1)),var v2 = Ttmp0{.column1 = v0.column1,.column2 = v0.column2,.column3 = v0.column3},var v3 = v2.";
        this.testTranslation(query, program);
    }

    @Test
    public void testNaturalJoinWhere() {
        String query = "create view v1 as SELECT DISTINCT * FROM t1 NATURAL JOIN t2 WHERE column3";
        String program = this.header(false) +
                "typedef Ttmp0 = Ttmp0{column1:signed<64>, column2:string, column3:bool}\n" +
                this.relations(false) +
                "output relation Rv1[Ttmp0]\n" +
                "Rv1[v3] :- Rt1[v0],Rt2[v1],(true and (v0.column1 == v1.column1))," +
                "var v2 = Ttmp0{.column1 = v0.column1,.column2 = v0.column2,.column3 = v0.column3},v0.column3,var v3 = v2.";
        this.testTranslation(query, program);
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
        this.testTranslation(query, program);
    }

    @Test
    public void testCrossJoin() {
        String query = "create view v1 as SELECT DISTINCT * FROM t1 CROSS JOIN t2";
        String program = this.header(false) +
                "typedef Ttmp0 = Ttmp0{column1:signed<64>, column2:string, column3:bool, column10:signed<64>}\n" +
                this.relations(false) +
                "output relation Rv1[Ttmp0]\n" +
                "Rv1[v3] :- Rt1[v0],Rt2[v1],true,var v2 = Ttmp0{.column1 = v0.column1,.column2 = v0.column2,.column3 = v0.column3,.column10 = v1.column1},var v3 = v2.";
        this.testTranslation(query, program);
    }

    @Test
    public void testCrossJoinWNull() {
        String query = "create view v1 as SELECT DISTINCT * FROM t1 CROSS JOIN t2";
        String program = this.header(true) +
                "typedef Ttmp0 = Ttmp0{column1:Option<signed<64>>, column2:Option<string>, column3:Option<bool>, column10:Option<signed<64>>}\n" +
                this.relations(true) +
                "output relation Rv1[Ttmp0]\n" +
                "Rv1[v3] :- Rt1[v0],Rt2[v1],true,var v2 = Ttmp0{.column1 = v0.column1,.column2 = v0.column2,.column3 = v0.column3,.column10 = v1.column1},var v3 = v2.";
        this.testTranslation(query, program, true);
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
        this.testTranslation(query, program);
    }

    @Test
    public void testMaxCase() {
        String query = "create view v1 as SELECT MAX(CASE WHEN column2 = 'foo' THEN column1 ELSE 0 END) FROM t1";
        String program = this.header(false) +
                "typedef Ttmp0 = Ttmp0{col4:signed<64>}\n" +
                "function agg1(g1: Group<'K, (Tt1)>):Ttmp0 =\n" +
                "var first3 = true;\n" +
                "(var max6 = 64'sd0);\n" +
                "(for (i2 in g1) {\n" +
                "var v0 = i2;\n" +
                "(var incr5 = if (v0.column2 == \"foo\") {\n" +
                "v0.column1} else {\n" +
                "64'sd0});\n" +
                "(max6 = if first3 {\n" +
                "incr5} else {\n" +
                "agg_max_R(max6, incr5)});\n" +
                "(first3 = false)}\n" +
                ");\n" +
                "(Ttmp0{.col4 = max6})" +
                this.relations(false) +
                "relation Rtmp0[Ttmp0]\n" +
                "output relation Rv1[Ttmp0]\n" +
                "Rv1[v8] :- Rt1[v0],var v7 = Aggregate((), agg1((v0))),var v8 = v7.";
        this.testTranslation(query, program);
    }

    @Test
    public void testAbs() {
        String query = "create view v1 as SELECT DISTINCT ABS(column1) FROM t1";
        String program = this.header(false) +
                "typedef Ttmp0 = Ttmp0{col1:signed<64>}\n" +
                this.relations(false) +
                "relation Rtmp0[Ttmp0]\n" +
                "output relation Rv1[Ttmp0]\n" +
                "Rv1[v3] :- Rt1[v0],var v2 = Ttmp0{.col1 = abs(v0.column1)},var v3 = v2.";
        this.testTranslation(query, program);
    }

    @Test
    public void testAbsWNull() {
        String query = "create view v1 as SELECT DISTINCT ABS(column1) FROM t1";
        String program = this.header(true) +
                "typedef Ttmp0 = Ttmp0{col1:Option<signed<64>>}\n" +
                this.relations(true) +
                "relation Rtmp0[Ttmp0]\n" +
                "output relation Rv1[Ttmp0]\n" +
                "Rv1[v3] :- Rt1[v0],var v2 = Ttmp0{.col1 = abs_N(v0.column1)},var v3 = v2.";
        this.testTranslation(query, program, true);
    }

    @Test
    public void testMax() {
        String query = "create view v1 as SELECT MAX(column1) FROM t1";
        String program = this.header(false) +
                "typedef Ttmp0 = Ttmp0{col4:signed<64>}\n" +
                "function agg1(g1: Group<'K, (Tt1)>):Ttmp0 =\n" +
                "var first3 = true;\n" +
                "(var max6 = 64'sd0);\n" +
                "(for (i2 in g1) {\n" +
                "var v0 = i2;\n" +
                "(var incr5 = v0.column1);\n" +
                "(max6 = if first3 {\n" +
                "incr5} else {\n" +
                "agg_max_R(max6, incr5)});\n" +
                "(first3 = false)}\n" +
                ");\n" +
                "(Ttmp0{.col4 = max6})" +
                this.relations(false) +
                "relation Rtmp0[Ttmp0]\n" +
                "output relation Rv1[Ttmp0]\n" +
                "Rv1[v8] :- Rt1[v0],var v7 = Aggregate((), agg1((v0))),var v8 = v7.";
        this.testTranslation(query, program);
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
        this.testTranslation(query, program);
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
        this.testTranslation(query, program, true);
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
        this.testTranslation(query, program, true);
    }

    @Test(expected = TranslationException.class)
    public void testMixAggregate() {
        String query = "create view v1 as select max(column1), column2 from t1";
        this.testTranslation(query, "");
    }

    @Test(expected = TranslationException.class)
    public void testMixAggregate1() {
        String query = "create view v1 as select min(column1) + column2 from t1";
        this.testTranslation(query, "");
    }

    @Test(expected = TranslationException.class)
    public void testMixAggregate2() {
        String query = "create view v1 as select min(min(column1)) from t1";
        this.testTranslation(query, "");
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
        this.testTranslation(query, program);
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
        this.testTranslation(query, program, true);
    }

    @Test
    public void testSimple() {
        String query = "create view v1 as select distinct * from t1";
        String program = this.header(false) +
                this.relations(false) +
                "output relation Rv1[Tt1]\n" +
                "Rv1[v1] :- Rt1[v0],var v1 = v0.";
        this.testTranslation(query, program);
    }

    @Test
    public void testSimpleWNull() {
        String query = "create view v1 as select distinct * from t1";
        String program = this.header(true) +
            this.relations(true) +
            "output relation Rv1[Tt1]\n" +
            "Rv1[v1] :- Rt1[v0],var v1 = v0.";
        this.testTranslation(query, program, true);
    }

    @Test
    public void testSimple1() {
        String query = "create view v2 as select distinct * from t1 where column1 = 10";
        String program = this.header(false) +
                this.relations(false) +
                "output relation Rv2[Tt1]\n" +
                "Rv2[v1] :- Rt1[v0],(v0.column1 == 64'sd10),var v1 = v0.";
        this.testTranslation(query, program);
    }

    @Test
    public void testSimple1WNulls() {
        String query = "create view v2 as select distinct * from t1 where column1 = 10";
        String program = this.header(true) +
            this.relations(true) +
            "output relation Rv2[Tt1]\n" +
            "Rv2[v1] :- Rt1[v0],unwrapBool(a_eq_NR(v0.column1, 64'sd10)),var v1 = v0.";
        this.testTranslation(query, program, true);
    }

    @Test
    public void testSimple2() {
        String query = "create view v3 as select distinct * from t1 where column1 = 10 and column2 = 'something'";
        String program = this.header(false) +
                this.relations(false) +
                "output relation Rv3[Tt1]\n" +
                "Rv3[v1] :- Rt1[v0],((v0.column1 == 64'sd10) and (v0.column2 == \"something\")),var v1 = v0.";
        this.testTranslation(query, program);
    }

    @Test
    public void testSimple2WNulls() {
        String query = "create view v3 as select distinct * from t1 where column1 = 10 and column2 = 'something'";
        String program = this.header(true) +
            this.relations(true) +
            "output relation Rv3[Tt1]\n" +
            "Rv3[v1] :- Rt1[v0],unwrapBool(b_and_NN(a_eq_NR(v0.column1, 64'sd10), s_eq_NR(v0.column2, \"something\"))),var v1 = v0.";
        this.testTranslation(query, program, true);
    }

    @Test
    public void testNested() {
        String query = "create view v4 as select distinct * from (select distinct * from t1 where column1 = 10) where column2 = 'something'";
        String program = this.header(false) +
                this.relations(false) +
                "relation Rtmp1[Tt1]\n" +
                "output relation Rv4[Tt1]\n" +
                "Rtmp1[v1] :- Rt1[v0],(v0.column1 == 64'sd10),var v1 = v0.\n" +
                "Rv4[v2] :- Rtmp1[v1],(v1.column2 == \"something\"),var v2 = v1.";
        this.testTranslation(query, program);
    }

    @Test
    public void testNestedWNull() {
        String query = "create view v4 as select distinct * from (select distinct * from t1 where column1 = 10) where column2 = 'something'";
        String program = this.header(true) +
            this.relations(true) +
            "relation Rtmp1[Tt1]\n" +
            "output relation Rv4[Tt1]\n" +
            "Rtmp1[v1] :- Rt1[v0],unwrapBool(a_eq_NR(v0.column1, 64'sd10)),var v1 = v0.\n" +
            "Rv4[v2] :- Rtmp1[v1],unwrapBool(s_eq_NR(v1.column2, \"something\")),var v2 = v1.";
        this.testTranslation(query, program, true);
    }
}
