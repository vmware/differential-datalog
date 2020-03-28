package ddlog;

import org.junit.Test;

/**
 * Unit test for simple Translator.
 */
public class AggregatesTest extends BaseQueriesTest {
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
}
