package ddlog;

import org.junit.Test;

public class GroupbyTest extends BaseQueriesTest {
    @Test
    public void testGroupBy() {
        String query = "create view v0 as SELECT COUNT(*) FROM t1 GROUP BY column2";
        String program = this.header(false) +
                "typedef TRtmp = TRtmp{col:signed<64>}\n" +
                "function agg(g: Group<string, Tt1>):TRtmp {\n" +
                "(var gb) = group_key(g);\n" +
                "(var count = 64'sd0: signed<64>);\n" +
                "(for (i in g) {\n" +
                "var v = i;\n" +
                "(count = agg_count_R(count, 64'sd1))}\n" +
                ");\n" +
                "(TRtmp{.col = count})\n}\n" +
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
                "function agg(g: Group<string, Tt1>):Tagg {\n" +
                "(var gb) = group_key(g);\n" +
                "(var count = 64'sd0: signed<64>);\n" +
                "(for (i in g) {\n" +
                "var v = i;\n" +
                "(count = agg_count_R(count, 64'sd1))}\n" +
                ");\n" +
                "(Tagg{.col = count})\n}\n" +
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
                "function agg(g: Group<(string, bool), Tt1>):Tagg {\n" +
                "(var gb, var gb0) = group_key(g);\n" +
                "(var count = 64'sd0: signed<64>);\n" +
                "(var sum = 64'sd0: signed<64>);\n" +
                "(for (i in g) {\n" +
                "var v = i;\n" +
                "(count = agg_count_R(count, 64'sd1));\n" +
                "(var incr = v.column1);\n" +
                "(sum = agg_sum_signed_R(sum, incr))}\n" +
                ");\n" +
                "(Tagg{.col = count,.col1 = sum})\n}\n" +
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
                "function agg(g: Group<Option<string>, Tt1>):Tagg {\n" +
                "(var gb) = group_key(g);\n" +
                "(var count = 64'sd0: signed<64>);\n" +
                "(for (i in g) {\n" +
                "var v = i;\n" +
                "(count = agg_count_R(count, 64'sd1))}\n" +
                ");\n" +
                "(Tagg{.col = count})\n}\n" +
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
                "function agg(g: Group<string, Tt1>):Tagg {\n" +
                "(var gb) = group_key(g);\n" +
                "(var sum = 64'sd0: signed<64>);\n" +
                "(for (i in g) {\n" +
                "var v = i;\n" +
                "(var incr = v.column1);\n" +
                "(sum = agg_sum_signed_R(sum, incr))}\n" +
                ");\n" +
                "(Tagg{.col = sum})\n}\n" +
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
                "function agg(g: Group<Option<string>, Tt1>):Tagg {\n" +
                "(var gb) = group_key(g);\n" +
                "(var sum = None{}: Option<signed<64>>);\n" +
                "(for (i in g) {\n" +
                "var v = i;\n" +
                "(var incr = v.column1);\n" +
                "(sum = agg_sum_signed_N(sum, incr))}\n" +
                ");\n" +
                "(Tagg{.col = sum})\n}\n" +
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
                "function agg(g: Group<signed<64>, Tt1>):Tagg {\n" +
                "(var gb) = group_key(g);\n" +
                "(var count = 64'sd0: signed<64>);\n" +
                "(for (i in g) {\n" +
                "var v = i;\n" +
                "(var incr = v.column2);\n" +
                "(count = agg_count_R(count, incr))}\n" +
                ");\n" +
                "(Tagg{.col = count,.col0 = (count > 64'sd2)})\n}\n" +
                this.relations(false) +
                "relation Rtmp[TRtmp]\n" +
                "output relation Rv0[TRtmp]\n" +
                "Rv0[v2] :- Rt1[v],var gb = v.column1,var aggResult = Aggregate((gb), agg((v)))," +
                "var v1 = TRtmp{.col = aggResult.col},aggResult.col0,var v2 = v1.";
        this.testTranslation(query, program);
    }

    @Test
    public void testHavingNull() {
        String query = "create view v0 as SELECT COUNT(column2) FROM t1 GROUP BY column1 HAVING ANY(column3)";
        String program = this.header(true) +
                "typedef TRtmp = TRtmp{col:Option<signed<64>>}\n" +
                "typedef Tagg = Tagg{col:Option<signed<64>>, col0:Option<bool>}\n" +
                "function agg(g: Group<Option<signed<64>>, Tt1>):Tagg {\n" +
                "(var gb) = group_key(g);\n" +
                "(var count = None{}: Option<signed<64>>);\n" +
                "(var any = Some{false}: Option<bool>);\n" +
                "(for (i in g) {\n" +
                "var v = i;\n" +
                "(var incr = v.column2);\n" +
                "(count = agg_count_N(count, incr));\n" +
                "(var incr1 = v.column3);\n" +
                "(any = agg_any_N(any, incr1))}\n" +
                ");\n" +
                "(Tagg{.col = count,.col0 = any})\n}\n" +
                this.relations(true) +
                "relation Rtmp[TRtmp]\n" +
                "output relation Rv0[TRtmp]\n" +
                "Rv0[v3] :- Rt1[v],var gb = v.column1,var aggResult = Aggregate((gb), agg((v)))," +
                "var v2 = TRtmp{.col = aggResult.col},unwrapBool(aggResult.col0),var v3 = v2.";
        this.testTranslation(query, program, true);
    }

    @Test
    public void testHaving1() {
        String query = "create view v0 as SELECT COUNT(column2) FROM t1 GROUP BY column1 HAVING COUNT(column2) > 2 and column1 = 3";
        String program = this.header(false) +
                "typedef TRtmp = TRtmp{col:signed<64>}\n" +
                "typedef Tagg = Tagg{col:signed<64>, col0:bool}\n" +
                "function agg(g: Group<signed<64>, Tt1>):Tagg {\n" +
                "(var gb) = group_key(g);\n" +
                "(var count = 64'sd0: signed<64>);\n" +
                "(for (i in g) {\n" +
                "var v = i;\n" +
                "(var incr = v.column2);\n" +
                "(count = agg_count_R(count, incr))}\n" +
                ");\n" +
                "(Tagg{.col = count,.col0 = ((count > 64'sd2) and (gb == 64'sd3))})\n}\n" +
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
                "function agg(g: Group<string, Tt1>):Tagg {\n" +
                "(var gb) = group_key(g);\n" +
                "(var sum = 64'sd0: signed<64>);\n" +
                "(var count = 64'sd0: signed<64>);\n" +
                "(for (i in g) {\n" +
                "var v = i;\n" +
                "(var incr = v.column1);\n" +
                "(sum = agg_sum_signed_R(sum, incr));\n" +
                "(count = agg_count_R(count, 64'sd1))}\n" +
                ");\n" +
                "(Tagg{.col = sum,.col0 = (count > 64'sd2)})\n}\n" +
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
                "function agg(g: Group<string, Tt1>):Tagg {\n" +
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
                "(Tagg{.col = sum,.col0 = (set_size(count_distinct) as signed<64> > 64'sd1)})\n}\n" +
                this.relations(false) +
                "relation Rtmp[TRtmp]\n" +
                "output relation Rv0[TRtmp]\n" +
                "Rv0[v3] :- Rt1[v],var gb = v.column2,var aggResult = Aggregate((gb), agg((v)))," +
                "var v2 = TRtmp{.column2 = gb,.col = aggResult.col},aggResult.col0,var v3 = v2.";
        this.testTranslation(query, program);
    }

    @Test
    public void testGroupByExpression() {
        String query = "create view v0 as SELECT substr(column2, 0, 1), COUNT(*) FROM t1 GROUP BY substr(column2, 0, 1)";
        String program = this.header(false) +
                "typedef TRtmp = TRtmp{col:string, col0:signed<64>}\n" +
                "typedef Tagg = Tagg{col0:signed<64>}\n" +
                "function agg(g: Group<string, Tt1>):Tagg {\n" +
                "(var gb) = group_key(g);\n" +
                "(var count = 64'sd0: signed<64>);\n" +
                "(for (i in g) {\n" +
                "var v = i;\n" +
                "(count = agg_count_R(count, 64'sd1))}\n" +
                ");\n" +
                "(Tagg{.col0 = count})\n}\n" +
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
                "function agg(g: Group<string, Tt1>):TRtmp {\n" +
                "(var gb) = group_key(g);\n" +
                "(var count = 64'sd0: signed<64>);\n" +
                "(for (i in g) {\n" +
                "var v = i;\n" +
                "(count = agg_count_R(count, 64'sd1))}\n" +
                ");\n" +
                "(TRtmp{.col = substr(gb, 64'sd0, 64'sd1),.col0 = count})\n}\n" +
                this.relations(false) +
                "relation Rtmp[TRtmp]\n" +
                "output relation Rv0[TRtmp]\n" +
                "Rv0[v2] :- Rt1[v],var gb = v.column2," +
                "var aggResult = Aggregate((gb), agg((v))),var v1 = TRtmp{.col = aggResult.col,.col0 = aggResult.col0},var v2 = v1.";
        this.testTranslation(query, program);
    }

}