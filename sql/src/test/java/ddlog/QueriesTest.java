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
            "typedef Tt1 = Tt1{column1:signed<64>, column2:string, column3:bool, column4:double}\n" +
            "typedef Tt2 = Tt2{column1:signed<64>}\n";
    private final String t1t2null =
            "typedef Tt1 = Tt1{column1:Option<signed<64>>, column2:Option<string>, column3:Option<bool>, column4:Option<double>}\n" +
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
                " column3 boolean " + nulls + ",\n" +
                " column4 real " + nulls + ")";
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
        String query = "create view v0 as SELECT COUNT(*) FROM t1 GROUP BY column2";
        String program = this.header(false) +
                "typedef TRtmp = TRtmp{col:signed<64>}\n" +
                "function agg(g: Group<string, Tt1>):TRtmp =\n" +
                "(var gb) = group_key(g);\n" +
                "(var count = 64'sd0: signed<64>);\n" +
                "(for (i in g) {\n" +
                "var v = i;\n" +
                "(count = agg_count_R(count, 64'sd1))}\n" +
                ");\n" +
                "(TRtmp{.col = count})" +
                this.relations(false) +
                "relation Rtmp[TRtmp]\n" +
                "output relation Rv0[TRtmp]\n" +
                "Rv0[v1] :- Rt1[v],var gb = v.column2,var aggResult = Aggregate((gb), agg((v)))," +
                "var v0 = TRtmp{.col = aggResult.col},var v1 = v0.";
        this.testTranslation(query, program);
    }

    @Test
    public void testGroupBy1() {
        String query = "create view v0 as SELECT column2, COUNT(*) FROM t1 GROUP BY column2";
        String program = this.header(false) +
                "typedef TRtmp = TRtmp{column2:string, col:signed<64>}\n" +
                "typedef Tagg = Tagg{col:signed<64>}\n" +
                "function agg(g: Group<string, Tt1>):Tagg =\n" +
                "(var gb) = group_key(g);\n" +
                "(var count = 64'sd0: signed<64>);\n" +
                "(for (i in g) {\n" +
                "var v = i;\n" +
                "(count = agg_count_R(count, 64'sd1))}\n" +
                ");\n" +
                "(Tagg{.col = count})" +
                this.relations(false) +
                "relation Rtmp[TRtmp]\n" +
                "output relation Rv0[TRtmp]\n" +
                "Rv0[v1] :- Rt1[v],var gb = v.column2,var aggResult = Aggregate((gb), agg((v)))," +
                "var v0 = TRtmp{.column2 = gb,.col = aggResult.col},var v1 = v0.";
        this.testTranslation(query, program);
    }

    @Test
    public void testGroupBy2() {
        String query = "create view v0 as SELECT column2, column3, COUNT(*), SUM(column1) FROM t1 GROUP BY column2, column3";
        String program = this.header(false) +
                "typedef TRtmp = TRtmp{column2:string, column3:bool, col:signed<64>, col1:signed<64>}\n" +
                "typedef Tagg = Tagg{col:signed<64>, col1:signed<64>}\n" +
                "function agg(g: Group<(string, bool), Tt1>):Tagg =\n" +
                "(var gb, var gb0) = group_key(g);\n" +
                "(var count = 64'sd0: signed<64>);\n" +
                "(var sum = 64'sd0: signed<64>);\n" +
                "(for (i in g) {\n" +
                "var v = i;\n" +
                "(count = agg_count_R(count, 64'sd1));\n" +
                "(var incr = v.column1);\n" +
                "(sum = agg_sum_signed_R(sum, incr))}\n" +
                ");\n" +
                "(Tagg{.col = count,.col1 = sum})" +
                this.relations(false) +
                "relation Rtmp[TRtmp]\n" +
                "output relation Rv0[TRtmp]\n" +
                "Rv0[v3] :- Rt1[v],var gb = v.column2,var gb0 = v.column3," +
                "var aggResult = Aggregate((gb, gb0), agg((v)))," +
                "var v2 = TRtmp{.column2 = gb,.column3 = gb0,.col = aggResult.col,.col1 = aggResult.col1},var v3 = v2.";
        this.testTranslation(query, program);
    }

    @Test
    public void testGroupByNull1() {
        String query = "create view v0 as SELECT column2, COUNT(*) FROM t1 GROUP BY column2";
        String program = this.header(true) +
                "typedef TRtmp = TRtmp{column2:Option<string>, col:signed<64>}\n" +
                "typedef Tagg = Tagg{col:signed<64>}\n" +
                "function agg(g: Group<Option<string>, Tt1>):Tagg =\n" +
                "(var gb) = group_key(g);\n" +
                "(var count = 64'sd0: signed<64>);\n" +
                "(for (i in g) {\n" +
                "var v = i;\n" +
                "(count = agg_count_R(count, 64'sd1))}\n" +
                ");\n" +
                "(Tagg{.col = count})" +
                this.relations(true) +
                "relation Rtmp[TRtmp]\n" +
                "output relation Rv0[TRtmp]\n" +
                "Rv0[v1] :- Rt1[v],var gb = v.column2,var aggResult = Aggregate((gb), agg((v)))," +
                "var v0 = TRtmp{.column2 = gb,.col = aggResult.col},var v1 = v0.";
        this.testTranslation(query, program, true);
    }

    @Test
    public void testMixAggregateGroupBy() {
        String query = "create view v0 as SELECT column2, SUM(column1) FROM t1 GROUP BY column2";
        String program = this.header(false) +
                "typedef TRtmp = TRtmp{column2:string, col:signed<64>}\n" +
                "typedef Tagg = Tagg{col:signed<64>}\n" +
                "function agg(g: Group<string, Tt1>):Tagg =\n" +
                "(var gb) = group_key(g);\n" +
                "(var sum = 64'sd0: signed<64>);\n" +
                "(for (i in g) {\n" +
                "var v = i;\n" +
                "(var incr = v.column1);\n" +
                "(sum = agg_sum_signed_R(sum, incr))}\n" +
                ");\n" +
                "(Tagg{.col = sum})" +
                this.relations(false) +
                "relation Rtmp[TRtmp]\n" +
                "output relation Rv0[TRtmp]\n" +
                "Rv0[v1] :- Rt1[v],var gb = v.column2,var aggResult = Aggregate((gb), agg((v))),var v0 = TRtmp{.column2 = gb,.col = aggResult.col},var v1 = v0.";
        this.testTranslation(query, program);
    }

    @Test
    public void testMixAggregateGroupByNull() {
        String query = "create view v0 as SELECT column2, SUM(column1) FROM t1 GROUP BY column2";
        String program = this.header(true) +
                "typedef TRtmp = TRtmp{column2:Option<string>, col:Option<signed<64>>}\n" +
                "typedef Tagg = Tagg{col:Option<signed<64>>}\n" +
                "function agg(g: Group<Option<string>, Tt1>):Tagg =\n" +
                "(var gb) = group_key(g);\n" +
                "(var sum = None{}: Option<signed<64>>);\n" +
                "(for (i in g) {\n" +
                "var v = i;\n" +
                "(var incr = v.column1);\n" +
                "(sum = agg_sum_signed_N(sum, incr))}\n" +
                ");\n" +
                "(Tagg{.col = sum})" +
                this.relations(true) +
                "relation Rtmp[TRtmp]\n" +
                "output relation Rv0[TRtmp]\n" +
                "Rv0[v1] :- Rt1[v],var gb = v.column2,var aggResult = Aggregate((gb), agg((v))),var v0 = TRtmp{.column2 = gb,.col = aggResult.col},var v1 = v0.";
        this.testTranslation(query, program, true);
    }

    @Test
    public void testHaving() {
        String query = "create view v0 as SELECT COUNT(column2) FROM t1 GROUP BY column1 HAVING COUNT(column2) > 2";
        String program = this.header(false) +
                "typedef TRtmp = TRtmp{col:signed<64>}\n" +
                "typedef Tagg = Tagg{col:signed<64>, col0:bool}\n" +
                "function agg(g: Group<signed<64>, Tt1>):Tagg =\n" +
                "(var gb) = group_key(g);\n" +
                "(var count = 64'sd0: signed<64>);\n" +
                "(for (i in g) {\n" +
                "var v = i;\n" +
                "(var incr = v.column2);\n" +
                "(count = agg_count_R(count, incr))}\n" +
                ");\n" +
                "(Tagg{.col = count,.col0 = (count > 64'sd2)})" +
                this.relations(false) +
                "relation Rtmp[TRtmp]\n" +
                "output relation Rv0[TRtmp]\n" +
                "Rv0[v2] :- Rt1[v],var gb = v.column1,var aggResult = Aggregate((gb), agg((v)))," +
                "var v1 = TRtmp{.col = aggResult.col},aggResult.col0,var v2 = v1.";
        this.testTranslation(query, program);
    }

    @Test
    public void testHaving1() {
        String query = "create view v0 as SELECT COUNT(column2) FROM t1 GROUP BY column1 HAVING COUNT(column2) > 2 and column1 = 3";
        String program = this.header(false) +
                "typedef TRtmp = TRtmp{col:signed<64>}\n" +
                "typedef Tagg = Tagg{col:signed<64>, col0:bool}\n" +
                "function agg(g: Group<signed<64>, Tt1>):Tagg =\n" +
                "(var gb) = group_key(g);\n" +
                "(var count = 64'sd0: signed<64>);\n" +
                "(for (i in g) {\n" +
                "var v = i;\n" +
                "(var incr = v.column2);\n" +
                "(count = agg_count_R(count, incr))}\n" +
                ");\n" +
                "(Tagg{.col = count,.col0 = ((count > 64'sd2) and (gb == 64'sd3))})" +
                this.relations(false) +
                "relation Rtmp[TRtmp]\n" +
                "output relation Rv0[TRtmp]\n" +
                "Rv0[v2] :- Rt1[v],var gb = v.column1,var aggResult = Aggregate((gb), agg((v)))," +
                "var v1 = TRtmp{.col = aggResult.col},aggResult.col0,var v2 = v1.";
        this.testTranslation(query, program);
    }

    @Test
    public void testHaving2() {
        String query = "create view v0 as SELECT SUM(column1) FROM t1 GROUP BY column2 HAVING COUNT(*) > 2";
        String program = this.header(false) +
                "typedef TRtmp = TRtmp{col:signed<64>}\n" +
                "typedef Tagg = Tagg{col:signed<64>, col0:bool}\n" +
                "function agg(g: Group<string, Tt1>):Tagg =\n" +
                "(var gb) = group_key(g);\n" +
                "(var sum = 64'sd0: signed<64>);\n" +
                "(var count = 64'sd0: signed<64>);\n" +
                "(for (i in g) {\n" +
                "var v = i;\n" +
                "(var incr = v.column1);\n" +
                "(sum = agg_sum_signed_R(sum, incr));\n" +
                "(count = agg_count_R(count, 64'sd1))}\n" +
                ");\n" +
                "(Tagg{.col = sum,.col0 = (count > 64'sd2)})" +
                this.relations(false) +
                "relation Rtmp[TRtmp]\n" +
                "output relation Rv0[TRtmp]\n" +
                "Rv0[v2] :- Rt1[v],var gb = v.column2,var aggResult = Aggregate((gb), agg((v)))," +
                "var v1 = TRtmp{.col = aggResult.col},aggResult.col0,var v2 = v1.";
        this.testTranslation(query, program);
    }

    @Test
    public void testHaving3() {
        String query = "create view v0 as SELECT column2, SUM(column1) FROM t1 GROUP BY column2 HAVING COUNT(DISTINCT column3) > 1";
        String program = this.header(false) +
                "typedef TRtmp = TRtmp{column2:string, col:signed<64>}\n" +
                "typedef Tagg = Tagg{col:signed<64>, col0:bool}\n" +
                "function agg(g: Group<string, Tt1>):Tagg =\n" +
                "(var gb) = group_key(g);\n" +
                "(var sum = 64'sd0: signed<64>);\n" +
                "(var count_distinct = set_empty(): Set<bool>);\n" +
                "(for (i in g) {\n" +
                "var v = i;\n" +
                "(var incr = v.column1);\n" +
                "(sum = agg_sum_signed_R(sum, incr));\n" +
                "(var incr1 = v.column3);\n" +
                "(set_insert(count_distinct, incr1))}\n" +
                ");\n" +
                "(Tagg{.col = sum,.col0 = (set_size(count_distinct) as signed<64> > 64'sd1)})" +
                this.relations(false) +
                "relation Rtmp[TRtmp]\n" +
                "output relation Rv0[TRtmp]\n" +
                "Rv0[v3] :- Rt1[v],var gb = v.column2,var aggResult = Aggregate((gb), agg((v)))," +
                "var v2 = TRtmp{.column2 = gb,.col = aggResult.col},aggResult.col0,var v3 = v2.";
        this.testTranslation(query, program);
    }

    @Test
    public void testAny() {
        String query = "create view v0 as SELECT ANY(column3) FROM t1";
        String program = this.header(false) +
                "typedef TRtmp = TRtmp{col:bool}\n" +
                "function agg(g: Group<(), Tt1>):TRtmp =\n" +
                "var any = false: bool;\n" +
                "(for (i in g) {\n" +
                "var v = i;\n" +
                "(var incr = v.column3);\n" +
                "(any = agg_any_R(any, incr))}\n" +
                ");\n" +
                "(TRtmp{.col = any})" +
                this.relations(false) +
                "relation Rtmp[TRtmp]\n" +
                "output relation Rv0[TRtmp]\n" +
                "Rv0[v1] :- Rt1[v],var aggResult = Aggregate((), agg((v))),var v0 = aggResult,var v1 = v0.";
        this.testTranslation(query, program);
    }

    @Test
    public void testEvery() {
        String query = "create view v0 as SELECT EVERY(column3) FROM t1";
        String program = this.header(false) +
                "typedef TRtmp = TRtmp{col:bool}\n" +
                "function agg(g: Group<(), Tt1>):TRtmp =\n" +
                "var every = true: bool;\n" +
                "(for (i in g) {\n" +
                "var v = i;\n" +
                "(var incr = v.column3);\n" +
                "(every = agg_every_R(every, incr))}\n" +
                ");\n" +
                "(TRtmp{.col = every})" +
                this.relations(false) +
                "relation Rtmp[TRtmp]\n" +
                "output relation Rv0[TRtmp]\n" +
                "Rv0[v1] :- Rt1[v],var aggResult = Aggregate((), agg((v))),var v0 = aggResult,var v1 = v0.";
        this.testTranslation(query, program);
    }

    @Test
    public void testCountDistinct() {
        String query = "create view v0 as SELECT COUNT(DISTINCT column1) FROM t1";
        String program = this.header(false) +
                "typedef TRtmp = TRtmp{col:signed<64>}\n" +
                "function agg(g: Group<(), Tt1>):TRtmp =\n" +
                "var count_distinct = set_empty(): Set<signed<64>>;\n" +
                "(for (i in g) {\n" +
                "var v = i;\n" +
                "(var incr = v.column1);\n" +
                "(set_insert(count_distinct, incr))}\n" +
                ");\n" +
                "(TRtmp{.col = set_size(count_distinct) as signed<64>})" +
                this.relations(false) +
                "relation Rtmp[TRtmp]\n" +
                "output relation Rv0[TRtmp]\n" +
                "Rv0[v1] :- Rt1[v],var aggResult = Aggregate((), agg((v))),var v0 = aggResult,var v1 = v0.";
        this.testTranslation(query, program);
    }

    @Test
    public void testCountDistinctWNulls() {
        String query = "create view v0 as SELECT COUNT(DISTINCT column1) FROM t1";
        String program = this.header(true) +
                "typedef TRtmp = TRtmp{col:signed<64>}\n" +
                "function agg(g: Group<(), Tt1>):TRtmp =\n" +
                "var count_distinct = set_empty(): Set<signed<64>>;\n" +
                "(for (i in g) {\n" +
                "var v = i;\n" +
                "(var incr = v.column1);\n" +
                "(insert_non_null(count_distinct, incr))}\n" +
                ");\n" +
                "(TRtmp{.col = set_size(count_distinct) as signed<64>})" +
                this.relations(true) +
                "relation Rtmp[TRtmp]\n" +
                "output relation Rv0[TRtmp]\n" +
                "Rv0[v1] :- Rt1[v],var aggResult = Aggregate((), agg((v))),var v0 = aggResult,var v1 = v0.";
        this.testTranslation(query, program, true);
    }

    @Test
    public void testSumDistinct() {
        String query = "create view v0 as SELECT SUM(DISTINCT column1) FROM t1";
        String program = this.header(false) +
                "typedef TRtmp = TRtmp{col:signed<64>}\n" +
                "function agg(g: Group<(), Tt1>):TRtmp =\n" +
                "var sum_distinct = set_empty(): Set<signed<64>>;\n" +
                "(for (i in g) {\n" +
                "var v = i;\n" +
                "(var incr = v.column1);\n" +
                "(set_insert(sum_distinct, incr))}\n" +
                ");\n" +
                "(TRtmp{.col = set_signed_sum(sum_distinct)})" +
                this.relations(false) +
                "relation Rtmp[TRtmp]\n" +
                "output relation Rv0[TRtmp]\n" +
                "Rv0[v1] :- Rt1[v],var aggResult = Aggregate((), agg((v))),var v0 = aggResult,var v1 = v0.";
        this.testTranslation(query, program);
    }

    @Test
    public void testMinDistinct() {
        String query = "create view v0 as SELECT MIN(DISTINCT column1) FROM t1";
        String program = this.header(false) +
                "typedef TRtmp = TRtmp{col:signed<64>}\n" +
                "function agg(g: Group<(), Tt1>):TRtmp =\n" +
                "var min = (true, 64'sd0): (bool, signed<64>);\n" +
                "(for (i in g) {\n" +
                "var v = i;\n" +
                "(var incr = v.column1);\n" +
                "(min = agg_min_R(min, incr))}\n" +
                ");\n" +
                "(TRtmp{.col = min.1})" +
                this.relations(false) +
                "relation Rtmp[TRtmp]\n" +
                "output relation Rv0[TRtmp]\n" +
                "Rv0[v1] :- Rt1[v],var aggResult = Aggregate((), agg((v))),var v0 = aggResult,var v1 = v0.";
        this.testTranslation(query, program);
    }

    @Test
    public void testGroupByExpression() {
        String query = "create view v0 as SELECT substr(column2, 0, 1), COUNT(*) FROM t1 GROUP BY substr(column2, 0, 1)";
        String program = this.header(false) +
                "typedef TRtmp = TRtmp{col:string, col0:signed<64>}\n" +
                "typedef Tagg = Tagg{col0:signed<64>}\n" +
                "function agg(g: Group<string, Tt1>):Tagg =\n" +
                "(var gb) = group_key(g);\n" +
                "(var count = 64'sd0: signed<64>);\n" +
                "(for (i in g) {\n" +
                "var v = i;\n" +
                "(count = agg_count_R(count, 64'sd1))}\n" +
                ");\n" +
                "(Tagg{.col0 = count})" +
                this.relations(false) +
                "relation Rtmp[TRtmp]\n" +
                "output relation Rv0[TRtmp]\n" +
                "Rv0[v2] :- Rt1[v],var gb = substr(v.column2, 64'sd0, 64'sd1)," +
                "var aggResult = Aggregate((gb), agg((v))),var v1 = TRtmp{.col = gb,.col0 = aggResult.col0},var v2 = v1.";
        this.testTranslation(query, program);
    }

    @Test
    public void testExpressionOfGroupBy() {
        String query = "create view v0 as SELECT substr(column2, 0, 1), COUNT(*) FROM t1 GROUP BY column2";
        String program = this.header(false) +
                "typedef TRtmp = TRtmp{col:string, col0:signed<64>}\n" +
                "function agg(g: Group<string, Tt1>):TRtmp =\n" +
                "(var gb) = group_key(g);\n" +
                "(var count = 64'sd0: signed<64>);\n" +
                "(for (i in g) {\n" +
                "var v = i;\n" +
                "(count = agg_count_R(count, 64'sd1))}\n" +
                ");\n" +
                "(TRtmp{.col = substr(gb, 64'sd0, 64'sd1),.col0 = count})" +
                this.relations(false) +
                "relation Rtmp[TRtmp]\n" +
                "output relation Rv0[TRtmp]\n" +
                "Rv0[v2] :- Rt1[v],var gb = v.column2," +
                "var aggResult = Aggregate((gb), agg((v))),var v1 = TRtmp{.col = aggResult.col,.col0 = aggResult.col0},var v2 = v1.";
        this.testTranslation(query, program);
    }

    @Test
    public void testTwoAggregations() {
        String query = "create view v0 as SELECT MIN(column1) + MAX(column1) FROM t1";
        String program = this.header(false) +
                "typedef TRtmp = TRtmp{col:signed<64>}\n" +
                "function agg(g: Group<(), Tt1>):TRtmp =\n" +
                "var min = (true, 64'sd0): (bool, signed<64>);\n" +
                "(var max = (true, 64'sd0): (bool, signed<64>));\n" +
                "(for (i in g) {\n" +
                "var v = i;\n" +
                "(var incr = v.column1);\n" +
                "(min = agg_min_R(min, incr));\n" +
                "(var incr0 = v.column1);\n" +
                "(max = agg_max_R(max, incr0))}\n" +
                ");\n" +
                "(TRtmp{.col = (min.1 + max.1)})" +
                this.relations(false) +
                "relation Rtmp[TRtmp]\n" +
                "output relation Rv0[TRtmp]\n" +
                "Rv0[v2] :- Rt1[v],var aggResult = Aggregate((), agg((v))),var v1 = aggResult,var v2 = v1.";
        this.testTranslation(query, program);
    }

    @Test
    public void testStringAggregate() {
        String query = "create view v0 as SELECT MIN(column2) FROM t1";
        String program = this.header(false) +
                "typedef TRtmp = TRtmp{col:string}\n" +
                "function agg(g: Group<(), Tt1>):TRtmp =\n" +
                "var min = (true, \"\"): (bool, string);\n" +
                "(for (i in g) {\n" +
                "var v = i;\n" +
                "(var incr = v.column2);\n" +
                "(min = agg_min_R(min, incr))}\n" +
                ");\n" +
                "(TRtmp{.col = min.1})" +
                this.relations(false) +
                "relation Rtmp[TRtmp]\n" +
                "output relation Rv0[TRtmp]\n" +
                "Rv0[v1] :- Rt1[v],var aggResult = Aggregate((), agg((v))),var v0 = aggResult,var v1 = v0.";
        this.testTranslation(query, program);
    }

    @Test
    public void testParallelAggregates() {
        String query = "create view v0 as SELECT COUNT(column1), SUM(column1) FROM t1";
        String program = this.header(false) +
                "typedef TRtmp = TRtmp{col:signed<64>, col0:signed<64>}\n" +
                "function agg(g: Group<(), Tt1>):TRtmp =\n" +
                "var count = 64'sd0: signed<64>;\n" +
                "(var sum = 64'sd0: signed<64>);\n" +
                "(for (i in g) {\n" +
                "var v = i;\n" +
                "(var incr = v.column1);\n" +
                "(count = agg_count_R(count, incr));\n" +
                "(var incr1 = v.column1);\n" +
                "(sum = agg_sum_signed_R(sum, incr1))}\n" +
                ");\n" +
                "(TRtmp{.col = count,.col0 = sum})" +
                this.relations(false) +
                "relation Rtmp[TRtmp]\n" +
                "output relation Rv0[TRtmp]\n" +
                "Rv0[v3] :- Rt1[v],var aggResult = Aggregate((), agg((v))),var v2 = aggResult,var v3 = v2.";
        this.testTranslation(query, program);
    }

    @Test
    public void testCount() {
        String query = "create view v0 as SELECT COUNT(*) FROM t1";
        String program = this.header(false) +
                "typedef TRtmp = TRtmp{col:signed<64>}\n" +
                "function agg(g: Group<(), Tt1>):TRtmp =\n" +
                "var count = 64'sd0: signed<64>;\n" +
                "(for (i in g) {\n" +
                "var v = i;\n" +
                "(count = agg_count_R(count, 64'sd1))}\n" +
                ");\n" +
                "(TRtmp{.col = count})" +
                this.relations(false) +
                "relation Rtmp[TRtmp]\n" +
                "output relation Rv0[TRtmp]\n" +
                "Rv0[v1] :- Rt1[v],var aggResult = Aggregate((), agg((v))),var v0 = aggResult,var v1 = v0.";
        this.testTranslation(query, program);
    }

    @Test
    public void testRedundantCount() {
        String query = "create view v0 as SELECT COUNT(*), COUNT(*) FROM t1";
        String program = this.header(false) +
                "typedef TRtmp = TRtmp{col:signed<64>, col0:signed<64>}\n" +
                "function agg(g: Group<(), Tt1>):TRtmp =\n" +
                "var count = 64'sd0: signed<64>;\n" +
                "(for (i in g) {\n" +
                "var v = i;\n" +
                "(count = agg_count_R(count, 64'sd1))}\n" +
                ");\n" +
                "(TRtmp{.col = count,.col0 = count})" +
                this.relations(false) +
                "relation Rtmp[TRtmp]\n" +
                "output relation Rv0[TRtmp]\n" +
                "Rv0[v2] :- Rt1[v],var aggResult = Aggregate((), agg((v))),var v1 = aggResult,var v2 = v1.";
        this.testTranslation(query, program);
    }

    @Test
    public void testCountJoin() {
        String query = "create view v0 as SELECT COUNT(t1.column2) FROM t1 JOIN t2 ON t1.column1 = t2.column1";
        String program = this.header(false) +
                "typedef Ttmp = Ttmp{column1:signed<64>, column2:string, column3:bool, column4:double, column10:signed<64>}\n" +
                "typedef TRtmp = TRtmp{col:signed<64>}\n" +
                "function agg(g: Group<(), (Tt1, Tt2)>):TRtmp =\n" +
                "var count = 64'sd0: signed<64>;\n" +
                "(for (i in g) {\n" +
                "var v = i.0;\n" +
                "(var v0 = i.1);\n" +
                "(var incr = v.column2);\n" +
                "(count = agg_count_R(count, incr))}\n" +
                ");\n" +
                "(TRtmp{.col = count})" +
                this.relations(false) +
                "relation Rtmp[TRtmp]\n" +
                "output relation Rv0[TRtmp]\n" +
                "Rv0[v3] :- Rt1[v],Rt2[v0],(v.column1 == v0.column1),true," +
                "var v1 = Ttmp{.column1 = v.column1,.column2 = v.column2,.column3 = v.column3,.column4 = v.column4,.column10 = v0.column1}," +
                "var aggResult = Aggregate((), agg((v, v0))),var v2 = aggResult,var v3 = v2.";
        this.testTranslation(query, program);
    }

    @Test
    public void testCountColumn() {
        String query = "create view v0 as SELECT COUNT(column1) FROM t1";
        String program = this.header(false) +
                "typedef TRtmp = TRtmp{col:signed<64>}\n" +
                "function agg(g: Group<(), Tt1>):TRtmp =\n" +
                "var count = 64'sd0: signed<64>;\n" +
                "(for (i in g) {\n" +
                "var v = i;\n" +
                "(var incr = v.column1);\n" +
                "(count = agg_count_R(count, incr))}\n" +
                ");\n" +
                "(TRtmp{.col = count})" +
                this.relations(false) +
                "relation Rtmp[TRtmp]\n" +
                "output relation Rv0[TRtmp]\n" +
                "Rv0[v1] :- Rt1[v],var aggResult = Aggregate((), agg((v))),var v0 = aggResult,var v1 = v0.";
        this.testTranslation(query, program);
    }

    @Test
    public void testCountColumnWNull() {
        String query = "create view v0 as SELECT COUNT(column1) FROM t1";
        String program = this.header(true) +
                "typedef TRtmp = TRtmp{col:Option<signed<64>>}\n" +
                "function agg(g: Group<(), Tt1>):TRtmp =\n" +
                "var count = None{}: Option<signed<64>>;\n" +
                "(for (i in g) {\n" +
                "var v = i;\n" +
                "(var incr = v.column1);\n" +
                "(count = agg_count_N(count, incr))}\n" +
                ");\n" +
                "(TRtmp{.col = count})" +
                this.relations(true) +
                "relation Rtmp[TRtmp]\n" +
                "output relation Rv0[TRtmp]\n" +
                "Rv0[v1] :- Rt1[v],var aggResult = Aggregate((), agg((v))),var v0 = aggResult,var v1 = v0.";
        this.testTranslation(query, program, true);
    }

    @Test
    public void testAvg() {
        String query = "create view v0 as SELECT AVG(column1) FROM t1";
        String program = this.header(false) +
                "typedef TRtmp = TRtmp{col:signed<64>}\n" +
                "function agg(g: Group<(), Tt1>):TRtmp =\n" +
                "var avg = (64'sd0, 64'sd0): (signed<64>, signed<64>);\n" +
                "(for (i in g) {\n" +
                "var v = i;\n" +
                "(var incr = v.column1);\n" +
                "(avg = agg_avg_signed_R(avg, incr))}\n" +
                ");\n" +
                "(TRtmp{.col = avg_signed_R(avg)})" +
                this.relations(false) +
                "relation Rtmp[TRtmp]\n" +
                "output relation Rv0[TRtmp]\n" +
                "Rv0[v1] :- Rt1[v],var aggResult = Aggregate((), agg((v))),var v0 = aggResult,var v1 = v0.";
        this.testTranslation(query, program);
    }

    @Test
    public void testAvgDouble() {
        String query = "create view v0 as SELECT AVG(column4) FROM t1";
        String program = this.header(false) +
                "typedef TRtmp = TRtmp{col:double}\n" +
                "function agg(g: Group<(), Tt1>):TRtmp =\n" +
                "var avg = (64'f0.0, 64'f0.0): (double, double);\n" +
                "(for (i in g) {\n" +
                "var v = i;\n" +
                "(var incr = v.column4);\n" +
                "(avg = agg_avg_double_R(avg, incr))}\n" +
                ");\n" +
                "(TRtmp{.col = avg_double_R(avg)})" +
                this.relations(false) +
                "relation Rtmp[TRtmp]\n" +
                "output relation Rv0[TRtmp]\n" +
                "Rv0[v1] :- Rt1[v],var aggResult = Aggregate((), agg((v))),var v0 = aggResult,var v1 = v0.";
        this.testTranslation(query, program);
    }

    @Test
    public void testAvgWNull() {
        String query = "create view v0 as SELECT AVG(column1) FROM t1";
        String program = this.header(true) +
                "typedef TRtmp = TRtmp{col:Option<signed<64>>}\n" +
                "function agg(g: Group<(), Tt1>):TRtmp =\n" +
                "var avg = None{}: Option<(signed<64>, signed<64>)>;\n" +
                "(for (i in g) {\n" +
                "var v = i;\n" +
                "(var incr = v.column1);\n" +
                "(avg = agg_avg_signed_N(avg, incr))}\n" +
                ");\n" +
                "(TRtmp{.col = avg_signed_N(avg)})" +
                this.relations(true) +
                "relation Rtmp[TRtmp]\n" +
                "output relation Rv0[TRtmp]\n" +
                "Rv0[v1] :- Rt1[v],var aggResult = Aggregate((), agg((v))),var v0 = aggResult,var v1 = v0.";
        this.testTranslation(query, program, true);
    }

    @Test
    public void testCountWNull() {
        String query = "create view v0 as SELECT COUNT(*) FROM t1";
        String program = this.header(true) +
                "typedef TRtmp = TRtmp{col:signed<64>}\n" +
                "function agg(g: Group<(), Tt1>):TRtmp =\n" +
                "var count = 64'sd0: signed<64>;\n" +
                "(for (i in g) {\n" +
                "var v = i;\n" +
                "(count = agg_count_R(count, 64'sd1))}\n" +
                ");\n" +
                "(TRtmp{.col = count})" +
                this.relations(true) +
                "relation Rtmp[TRtmp]\n" +
                "output relation Rv0[TRtmp]\n" +
                "Rv0[v1] :- Rt1[v],var aggResult = Aggregate((), agg((v))),var v0 = aggResult,var v1 = v0.";
        this.testTranslation(query, program, true);
    }

    @Test
    public void testWhen() {
        String query = "create view v0 as SELECT DISTINCT CASE WHEN column1 = 1 THEN 1 WHEN 1 < column1 THEN 2 ELSE 3 END FROM t1";
        String program =
                this.header(false) +
                        "typedef TRtmp = TRtmp{col:signed<64>}\n" +
                        this.relations(false) +
                        "relation Rtmp[TRtmp]\n" +
                        "output relation Rv0[TRtmp]\n" +
                        "Rv0[v1] :- Rt1[v],var v0 = TRtmp{.col = if (v.column1 == 64'sd1) {\n" +
                        "64'sd1} else {\n" +
                        "if (64'sd1 < v.column1) {\n" +
                        "64'sd2} else {\n" +
                        "64'sd3}}},var v1 = v0.";
        this.testTranslation(query, program);
    }

    @Test
    public void testWhenWNull() {
        String query = "create view v0 as SELECT DISTINCT CASE WHEN column1 = 1 THEN 1 WHEN 1 < column1 THEN 2 ELSE 3 END FROM t1";
        String program =
            this.header(true) +
                "typedef TRtmp = TRtmp{col:signed<64>}\n" +
                this.relations(true) +
                "relation Rtmp[TRtmp]\n" +
                "output relation Rv0[TRtmp]\n" +
                "Rv0[v1] :- Rt1[v],var v0 = TRtmp{.col = if unwrapBool(a_eq_NR(v.column1, 64'sd1)) {\n" +
                "64'sd1} else {\n" +
                "if unwrapBool(a_lt_RN(64'sd1, v.column1)) {\n" +
                "64'sd2} else {\n" +
                "64'sd3}}},var v1 = v0.";
        this.testTranslation(query, program, true);
    }

    @Test
    public void testAlias() {
        String query = "create view v0 as SELECT DISTINCT t2.column1 FROM t1 AS t2";
        String program = this.header(false) +
                "typedef TRtmp = TRtmp{column1:signed<64>}\n" +
                this.relations(false) +
                "relation Rtmp[TRtmp]\n" +
                "output relation Rv0[TRtmp]\n" +
                "Rv0[v1] :- Rt1[v],var v0 = TRtmp{.column1 = v.column1},var v1 = v0.";
        this.testTranslation(query, program);
    }

    @Test
    public void testAliasWNull() {
        String query = "create view v0 as SELECT DISTINCT t2.column1 FROM t1 AS t2";
        String program = this.header(true) +
            "typedef TRtmp = TRtmp{column1:Option<signed<64>>}\n" +
            this.relations(true) +
            "relation Rtmp[TRtmp]\n" +
            "output relation Rv0[TRtmp]\n" +
            "Rv0[v1] :- Rt1[v],var v0 = TRtmp{.column1 = v.column1},var v1 = v0.";
        this.testTranslation(query, program, true);
    }

    @Test
    public void testScope() {
        String query = "create view v0 as SELECT DISTINCT t1.column1 FROM t1";
        String program = this.header(false) +
                "typedef TRtmp = TRtmp{column1:signed<64>}\n" +
                this.relations(false) +
                "relation Rtmp[TRtmp]\n" +
                "output relation Rv0[TRtmp]\n" +
                "Rv0[v1] :- Rt1[v],var v0 = TRtmp{.column1 = v.column1},var v1 = v0.";
        this.testTranslation(query, program);
    }

    @Test
    public void testScopeWNulls() {
        String query = "create view v0 as SELECT DISTINCT t1.column1 FROM t1";
        String program = this.header(true) +
            "typedef TRtmp = TRtmp{column1:Option<signed<64>>}\n" +
            this.relations(true) +
            "relation Rtmp[TRtmp]\n" +
            "output relation Rv0[TRtmp]\n" +
            "Rv0[v1] :- Rt1[v],var v0 = TRtmp{.column1 = v.column1},var v1 = v0.";
        this.testTranslation(query, program, true);
    }

    @Test
    public void testImplicitJoin() {
        String query = "create view v0 as SELECT DISTINCT * FROM t1, t2";
        String program = this.header(false) +
                "typedef Ttmp = Ttmp{column1:signed<64>, column2:string, column3:bool, column4:double, column10:signed<64>}\n" +
                this.relations(false) +
                "output relation Rv0[Ttmp]\n" +
                "Rv0[v2] :- Rt1[v],Rt2[v0],true," +
                "var v1 = Ttmp{.column1 = v.column1,.column2 = v.column2,.column3 = v.column3,.column4 = v.column4,.column10 = v0.column1}," +
                "var v2 = v1.";
        this.testTranslation(query, program);
    }

    @Test
    public void testJoinStar() {
        String query = "create view v0 as SELECT DISTINCT * FROM t1 JOIN t2 ON t1.column1 = t2.column1";
        String program = this.header(false) +
                "typedef Ttmp = Ttmp{column1:signed<64>, column2:string, column3:bool, column4:double, column10:signed<64>}\n" +
                this.relations(false) +
                "output relation Rv0[Ttmp]\n" +
                "Rv0[v2] :- Rt1[v],Rt2[v0],(v.column1 == v0.column1),true," +
                "var v1 = Ttmp{.column1 = v.column1,.column2 = v.column2,.column3 = v.column3,.column4 = v.column4,.column10 = v0.column1},var v2 = v1.";
        this.testTranslation(query, program);
    }

    @Test
    public void testJoinStarWNull() {
        String query = "create view v0 as SELECT DISTINCT * FROM t1 JOIN t2 ON t1.column1 = t2.column1";
        String program = this.header(true) +
                "typedef Ttmp = Ttmp{column1:Option<signed<64>>, column2:Option<string>, column3:Option<bool>, column4:Option<double>, column10:Option<signed<64>>}\n" +
                this.relations(true) +
                "output relation Rv0[Ttmp]\n" +
                "Rv0[v2] :- Rt1[v],Rt2[v0],unwrapBool(a_eq_NN(v.column1, v0.column1)),true," +
                "var v1 = Ttmp{.column1 = v.column1,.column2 = v.column2,.column3 = v.column3,.column4 = v.column4,.column10 = v0.column1},var v2 = v1.";
        this.testTranslation(query, program, true);
    }

    @Test
    public void testNaturalJoin() {
        String query = "create view v0 as SELECT DISTINCT * FROM t1 NATURAL JOIN t2";
        String program = this.header(false) +
                "typedef Ttmp = Ttmp{column1:signed<64>, column2:string, column3:bool, column4:double}\n" +
                this.relations(false) +
                "output relation Rv0[Ttmp]\n" +
                "Rv0[v2] :- Rt1[v],Rt2[v0],(true and (v.column1 == v0.column1))," +
                "var v1 = Ttmp{.column1 = v.column1,.column2 = v.column2,.column3 = v.column3,.column4 = v.column4},var v2 = v1.";
        this.testTranslation(query, program);
    }

    @Test
    public void testNaturalJoinWhere() {
        String query = "create view v0 as SELECT DISTINCT * FROM t1 NATURAL JOIN t2 WHERE column3";
        String program = this.header(false) +
                "typedef Ttmp = Ttmp{column1:signed<64>, column2:string, column3:bool, column4:double}\n" +
                this.relations(false) +
                "output relation Rv0[Ttmp]\n" +
                "Rv0[v2] :- Rt1[v],Rt2[v0],(true and (v.column1 == v0.column1))," +
                "var v1 = Ttmp{.column1 = v.column1,.column2 = v.column2,.column3 = v.column3,.column4 = v.column4},v.column3,var v2 = v1.";
        this.testTranslation(query, program);
    }

    @Test
    public void testJoin() {
        String query = "create view v0 as SELECT DISTINCT t0.column1, t1.column3 FROM t1 AS t0 JOIN t1 ON t0.column2 = t1.column2";
        String program = this.header(false) +
                "typedef Ttmp = Ttmp{column1:signed<64>, column2:string, column3:bool, column4:double, " +
                "column10:signed<64>, column20:string, column30:bool, column40:double}\n" +
                "typedef TRtmp = TRtmp{column1:signed<64>, column3:bool}\n" +
                this.relations(false) +
                "relation Rtmp[TRtmp]\n" +
                "output relation Rv0[TRtmp]\n" +
                "Rv0[v3] :- Rt1[v],Rt1[v0],(v.column2 == v0.column2),true," +
                "var v1 = Ttmp{.column1 = v.column1,.column2 = v.column2,.column3 = v.column3,.column4 = v.column4," +
                ".column10 = v0.column1,.column20 = v0.column2,.column30 = v0.column3,.column40 = v0.column4}," +
                "var v2 = TRtmp{.column1 = v.column1,.column3 = v0.column3},var v3 = v2.";
        this.testTranslation(query, program);
    }

    @Test
    public void testCrossJoin() {
        String query = "create view v0 as SELECT DISTINCT * FROM t1 CROSS JOIN t2";
        String program = this.header(false) +
                "typedef Ttmp = Ttmp{column1:signed<64>, column2:string, column3:bool, column4:double, column10:signed<64>}\n" +
                this.relations(false) +
                "output relation Rv0[Ttmp]\n" +
                "Rv0[v2] :- Rt1[v],Rt2[v0],true,var v1 = Ttmp{.column1 = v.column1,.column2 = v.column2,.column3 = v.column3,.column4 = v.column4,.column10 = v0.column1}," +
                "var v2 = v1.";
        this.testTranslation(query, program);
    }

    @Test
    public void testCrossJoinWNull() {
        String query = "create view v0 as SELECT DISTINCT * FROM t1 CROSS JOIN t2";
        String program = this.header(true) +
                "typedef Ttmp = Ttmp{column1:Option<signed<64>>, column2:Option<string>, column3:Option<bool>, column4:Option<double>, column10:Option<signed<64>>}\n" +
                this.relations(true) +
                "output relation Rv0[Ttmp]\n" +
                "Rv0[v2] :- Rt1[v],Rt2[v0],true,var v1 = Ttmp{.column1 = v.column1,.column2 = v.column2,.column3 = v.column3,.column4 = v.column4,.column10 = v0.column1}," +
                "var v2 = v1.";
        this.testTranslation(query, program, true);
    }

    @Test
    public void testMultiJoin() {
        String query = "create view v0 as SELECT DISTINCT *\n" +
                "    FROM t1,\n" +
                "         (SELECT DISTINCT column1 AS a FROM t1) b,\n" +
                "         (SELECT DISTINCT column2 AS c FROM t1) c,\n" +
                "         (SELECT DISTINCT column3 AS d FROM t1) d";
        String program = this.header(false) +
                "typedef TRtmp = TRtmp{a:signed<64>}\n" +
                "typedef Ttmp = Ttmp{column1:signed<64>, column2:string, column3:bool, column4:double, a:signed<64>}\n" +
                "typedef TRtmp1 = TRtmp1{c:string}\n" +
                "typedef Ttmp3 = Ttmp3{column1:signed<64>, column2:string, column3:bool, column4:double, a:signed<64>, c:string}\n" +
                "typedef TRtmp4 = TRtmp4{d:bool}\n" +
                "typedef Ttmp6 = Ttmp6{column1:signed<64>, column2:string, column3:bool, column4:double, a:signed<64>, c:string, d:bool}\n" +
                this.relations(false) +
                "relation Rtmp[TRtmp]\n" +
                "relation Rtmp0[TRtmp]\n" +
                "relation Rtmp1[TRtmp1]\n" +
                "relation Rtmp2[TRtmp1]\n" +
                "relation Rtmp4[TRtmp4]\n" +
                "relation Rtmp5[TRtmp4]\n" +
                "output relation Rv0[Ttmp6]\n" +
                "Rtmp0[v2] :- Rt1[v0],var v1 = TRtmp{.a = v0.column1},var v2 = v1.\n" +
                "Rtmp2[v6] :- Rt1[v4],var v5 = TRtmp1{.c = v4.column2},var v6 = v5.\n" +
                "Rtmp5[v10] :- Rt1[v8],var v9 = TRtmp4{.d = v8.column3},var v10 = v9.\n" +
                "Rv0[v12] :- Rt1[v],Rtmp0[v2],true," +
                "var v3 = Ttmp{.column1 = v.column1,.column2 = v.column2,.column3 = v.column3,.column4 = v.column4,.a = v2.a}," +
                "Rtmp2[v6],true," +
                "var v7 = Ttmp3{.column1 = v3.column1,.column2 = v3.column2,.column3 = v3.column3,.column4 = v3.column4,.a = v3.a,.c = v6.c}," +
                "Rtmp5[v10],true," +
                "var v11 = Ttmp6{.column1 = v7.column1,.column2 = v7.column2,.column3 = v7.column3,.column4 = v7.column4,.a = v7.a,.c = v7.c,.d = v10.d}," +
                "var v12 = v11.";
        this.testTranslation(query, program);
    }

    @Test
    public void testMaxCase() {
        String query = "create view v0 as SELECT MAX(CASE WHEN column2 = 'foo' THEN column1 ELSE 0 END) FROM t1";
        String program = this.header(false) +
                "typedef TRtmp = TRtmp{col:signed<64>}\n" +
                "function agg(g: Group<(), Tt1>):TRtmp =\n" +
                "var max = (true, 64'sd0): (bool, signed<64>);\n" +
                "(for (i in g) {\n" +
                "var v = i;\n" +
                "(var incr = if (v.column2 == \"foo\") {\n" +
                "v.column1} else {\n" +
                "64'sd0});\n" +
                "(max = agg_max_R(max, incr))}\n" +
                ");\n" +
                "(TRtmp{.col = max.1})" +
                this.relations(false) +
                "relation Rtmp[TRtmp]\n" +
                "output relation Rv0[TRtmp]\n" +
                "Rv0[v1] :- Rt1[v],var aggResult = Aggregate((), agg((v))),var v0 = aggResult,var v1 = v0.";
        this.testTranslation(query, program);
    }

    @Test
    public void testAbs() {
        String query = "create view v0 as SELECT DISTINCT ABS(column1) FROM t1";
        String program = this.header(false) +
                "typedef TRtmp = TRtmp{col:signed<64>}\n" +
                this.relations(false) +
                "relation Rtmp[TRtmp]\n" +
                "output relation Rv0[TRtmp]\n" +
                "Rv0[v1] :- Rt1[v],var v0 = TRtmp{.col = abs(v.column1)},var v1 = v0.";
        this.testTranslation(query, program);
    }

    @Test
    public void testAbsWNull() {
        String query = "create view v0 as SELECT DISTINCT ABS(column1) FROM t1";
        String program = this.header(true) +
                "typedef TRtmp = TRtmp{col:Option<signed<64>>}\n" +
                this.relations(true) +
                "relation Rtmp[TRtmp]\n" +
                "output relation Rv0[TRtmp]\n" +
                "Rv0[v1] :- Rt1[v],var v0 = TRtmp{.col = abs_N(v.column1)},var v1 = v0.";
        this.testTranslation(query, program, true);
    }

    @Test
    public void testMax() {
        String query = "create view v0 as SELECT MAX(column1) FROM t1";
        String program = this.header(false) +
                "typedef TRtmp = TRtmp{col:signed<64>}\n" +
                "function agg(g: Group<(), Tt1>):TRtmp =\n" +
                "var max = (true, 64'sd0): (bool, signed<64>);\n" +
                "(for (i in g) {\n" +
                "var v = i;\n" +
                "(var incr = v.column1);\n" +
                "(max = agg_max_R(max, incr))}\n" +
                ");\n" +
                "(TRtmp{.col = max.1})" +
                this.relations(false) +
                "relation Rtmp[TRtmp]\n" +
                "output relation Rv0[TRtmp]\n" +
                "Rv0[v1] :- Rt1[v],var aggResult = Aggregate((), agg((v))),var v0 = aggResult,var v1 = v0.";
        this.testTranslation(query, program);
    }

    @Test
    public void testBetween() {
        String query = "create view v0 as SELECT DISTINCT column1, column2 FROM t1 WHERE column1 BETWEEN -1 and 10";
        String program = this.header(false) +
                "typedef TRtmp = TRtmp{column1:signed<64>, column2:string}\n" +
                this.relations(false) +
                "relation Rtmp[TRtmp]\n" +
                "output relation Rv0[TRtmp]\n" +
                "Rv0[v1] :- Rt1[v],(((- 64'sd1) <= v.column1) and (v.column1 <= 64'sd10)),var v0 = TRtmp{.column1 = v.column1,.column2 = v.column2},var v1 = v0.";
        this.testTranslation(query, program);
    }

    @Test
    public void testBetweenWNulls() {
        String query = "create view v0 as SELECT DISTINCT column1, column2 FROM t1 WHERE column1 BETWEEN -1 and 10";
        String program = this.header(true) +
            "typedef TRtmp = TRtmp{column1:Option<signed<64>>, column2:Option<string>}\n" +
            this.relations(true) +
            "relation Rtmp[TRtmp]\n" +
            "output relation Rv0[TRtmp]\n" +
            "Rv0[v1] :- Rt1[v],unwrapBool(b_and_NN(a_lte_RN((- 64'sd1), v.column1), a_lte_NR(v.column1, 64'sd10))),var v0 = TRtmp{.column1 = v.column1,.column2 = v.column2},var v1 = v0.";
        this.testTranslation(query, program, true);
    }

    @Test
    public void testSubstr() {
        String query = "create view v0 as SELECT DISTINCT SUBSTR(column2, 3, 5) FROM t1";
        String program = this.header(false) +
                "typedef TRtmp = TRtmp{col:string}\n" +
                this.relations(false) +
                "relation Rtmp[TRtmp]\n" +
                "output relation Rv0[TRtmp]\n" +
                "Rv0[v1] :- Rt1[v],var v0 = TRtmp{.col = substr(v.column2, 64'sd3, 64'sd5)},var v1 = v0.";
        this.testTranslation(query, program);
    }

    @Test
    public void testSubstrWNull() {
        String query = "create view v0 as SELECT DISTINCT SUBSTR(column2, 3, 5) FROM t1";
        String program = this.header(true) +
            "typedef TRtmp = TRtmp{col:Option<string>}\n" +
            this.relations(true) +
            "relation Rtmp[TRtmp]\n" +
            "output relation Rv0[TRtmp]\n" +
            "Rv0[v1] :- Rt1[v],var v0 = TRtmp{.col = substr_N(v.column2, 64'sd3, 64'sd5)},var v1 = v0.";
        this.testTranslation(query, program, true);
    }

    @Test
    public void testSelectWithNulls() {
        String query = "create view v0 as select distinct column1, column2 from t1";
        String program =
                this.header(true) +
                "typedef TRtmp = TRtmp{column1:Option<signed<64>>, column2:Option<string>}\n" +
                this.relations(true) +
                "relation Rtmp[TRtmp]\n" +
                "output relation Rv0[TRtmp]\n" +
                "Rv0[v1] :- Rt1[v],var v0 = TRtmp{.column1 = v.column1,.column2 = v.column2},var v1 = v0.";
        this.testTranslation(query, program, true);
    }

    @Test(expected = TranslationException.class)
    public void testMixAggregate() {
        String query = "create view v0 as select max(column1), column2 from t1";
        this.testTranslation(query, "");
    }

    @Test(expected = TranslationException.class)
    public void testMixAggregate1() {
        String query = "create view v0 as select min(column1) + column2 from t1";
        this.testTranslation(query, "");
    }

    @Test(expected = TranslationException.class)
    public void testMixAggregate2() {
        String query = "create view v0 as select min(min(column1)) from t1";
        this.testTranslation(query, "");
    }

    @Test
    public void testSelect() {
        String query = "create view v0 as select distinct column1, column2 from t1";
        String program = this.header(false) +
            "typedef TRtmp = TRtmp{column1:signed<64>, column2:string}\n" +
            this.relations(false) +
            "relation Rtmp[TRtmp]\n" +
            "output relation Rv0[TRtmp]\n" +
            "Rv0[v1] :- Rt1[v],var v0 = TRtmp{.column1 = v.column1,.column2 = v.column2},var v1 = v0.";
        this.testTranslation(query, program);
    }

    @Test
    public void testSelectWNull() {
        String query = "create view v0 as select distinct column1, column2 from t1";
        String program = this.header(true) +
            "typedef TRtmp = TRtmp{column1:Option<signed<64>>, column2:Option<string>}\n" +
            this.relations(true) +
            "relation Rtmp[TRtmp]\n" +
            "output relation Rv0[TRtmp]\n" +
            "Rv0[v1] :- Rt1[v],var v0 = TRtmp{.column1 = v.column1,.column2 = v.column2},var v1 = v0.";
        this.testTranslation(query, program, true);
    }

    @Test
    public void testSimple() {
        String query = "create view v0 as select distinct * from t1";
        String program = this.header(false) +
                this.relations(false) +
                "output relation Rv0[Tt1]\n" +
                "Rv0[v0] :- Rt1[v],var v0 = v.";
        this.testTranslation(query, program);
    }

    @Test
    public void testSimpleWNull() {
        String query = "create view v0 as select distinct * from t1";
        String program = this.header(true) +
            this.relations(true) +
            "output relation Rv0[Tt1]\n" +
            "Rv0[v0] :- Rt1[v],var v0 = v.";
        this.testTranslation(query, program, true);
    }

    @Test
    public void testSimple1() {
        String query = "create view v1 as select distinct * from t1 where column1 = 10";
        String program = this.header(false) +
                this.relations(false) +
                "output relation Rv1[Tt1]\n" +
                "Rv1[v0] :- Rt1[v],(v.column1 == 64'sd10),var v0 = v.";
        this.testTranslation(query, program);
    }

    @Test
    public void testSimple1WNulls() {
        String query = "create view v1 as select distinct * from t1 where column1 = 10";
        String program = this.header(true) +
            this.relations(true) +
            "output relation Rv1[Tt1]\n" +
            "Rv1[v0] :- Rt1[v],unwrapBool(a_eq_NR(v.column1, 64'sd10)),var v0 = v.";
        this.testTranslation(query, program, true);
    }

    @Test
    public void testSimple2() {
        String query = "create view v2 as select distinct * from t1 where column1 = 10 and column2 = 'something'";
        String program = this.header(false) +
                this.relations(false) +
                "output relation Rv2[Tt1]\n" +
                "Rv2[v0] :- Rt1[v],((v.column1 == 64'sd10) and (v.column2 == \"something\")),var v0 = v.";
        this.testTranslation(query, program);
    }

    @Test
    public void testSimple2WNulls() {
        String query = "create view v2 as select distinct * from t1 where column1 = 10 and column2 = 'something'";
        String program = this.header(true) +
            this.relations(true) +
            "output relation Rv2[Tt1]\n" +
            "Rv2[v0] :- Rt1[v],unwrapBool(b_and_NN(a_eq_NR(v.column1, 64'sd10), s_eq_NR(v.column2, \"something\"))),var v0 = v.";
        this.testTranslation(query, program, true);
    }

    @Test
    public void testNested() {
        String query = "create view v3 as select distinct * from (select distinct * from t1 where column1 = 10) where column2 = 'something'";
        String program = this.header(false) +
                this.relations(false) +
                "relation Rtmp0[Tt1]\n" +
                "output relation Rv3[Tt1]\n" +
                "Rtmp0[v0] :- Rt1[v],(v.column1 == 64'sd10),var v0 = v.\n" +
                "Rv3[v1] :- Rtmp0[v0],(v0.column2 == \"something\"),var v1 = v0.";
        this.testTranslation(query, program);
    }

    @Test
    public void testNestedWNull() {
        String query = "create view v3 as select distinct * from (select distinct * from t1 where column1 = 10) where column2 = 'something'";
        String program = this.header(true) +
            this.relations(true) +
            "relation Rtmp0[Tt1]\n" +
            "output relation Rv3[Tt1]\n" +
            "Rtmp0[v0] :- Rt1[v],unwrapBool(a_eq_NR(v.column1, 64'sd10)),var v0 = v.\n" +
            "Rv3[v1] :- Rtmp0[v0],unwrapBool(s_eq_NR(v0.column2, \"something\")),var v1 = v0.";
        this.testTranslation(query, program, true);
    }
}
