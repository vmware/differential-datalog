/*
 * Copyright (c) 2021 VMware, Inc.
 * SPDX-License-Identifier: MIT
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 *
 */

package ddlog;

import org.junit.Test;

import java.util.Arrays;

/**
 * Unit test for simple Translator.
 */
public class AggregatesTest extends BaseQueriesTest {
    @Test
    public void testAny() {
        String query = "create view v0 as SELECT ANY(column3) AS a FROM t1";
        String program = this.header(false) +
                "typedef TRtmp = TRtmp{a:bool}\n" +
                "function agg(g: Group<(), TRt1>):TRtmp {\n" +
                "var any = false: bool;\n" +
                "(for ((i, _) in g) {\n" +
                "var v = i;\n" +
                "(var incr = v.column3);\n" +
                "(any = agg_any_R(any, incr))}\n" +
                ");\n" +
                "(TRtmp{.a = any})\n}\n" +
                this.relations(false) +
                "relation Rtmp[TRtmp]\n" +
                "output relation Rv0[TRtmp]\n" +
                "Rv0[v1] :- Rt1[v],var groupResult = (v).group_by(()),var aggResult = agg(groupResult),var v0 = aggResult,var v1 = v0.";
        this.testTranslation(query, program);
    }

    @Test
    public void testAnyWNull() {
        String query = "create view v0 as SELECT ANY(column3) AS a FROM t1";
        String program = this.header(true) +
                "typedef TRtmp = TRtmp{a:Option<bool>}\n" +
                "function agg(g: Group<(), TRt1>):TRtmp {\n" +
                "var any = Some{false}: Option<bool>;\n" +
                "(for ((i, _) in g) {\n" +
                "var v = i;\n" +
                "(var incr = v.column3);\n" +
                "(any = agg_any_N(any, incr))}\n" +
                ");\n" +
                "(TRtmp{.a = any})\n}\n" +
                this.relations(true) +
                "relation Rtmp[TRtmp]\n" +
                "output relation Rv0[TRtmp]\n" +
                "Rv0[v1] :- Rt1[v],var groupResult = (v).group_by(()),var aggResult = agg(groupResult),var v0 = aggResult,var v1 = v0.";
        this.testTranslation(query, program, true);
    }

    @Test
    public void testAnyWNullNeg() {
        String query = "create view v0 as SELECT NOT ANY(column3) AS a FROM t1";
        String program = this.header(true) +
                "typedef TRtmp = TRtmp{a:Option<bool>}\n" +
                "function agg(g: Group<(), TRt1>):TRtmp {\n" +
                "var any = Some{false}: Option<bool>;\n" +
                "(for ((i, _) in g) {\n" +
                "var v = i;\n" +
                "(var incr = v.column3);\n" +
                "(any = agg_any_N(any, incr))}\n" +
                ");\n" +
                "(TRtmp{.a = b_not_N(any)})\n}\n" +
                this.relations(true) +
                "relation Rtmp[TRtmp]\n" +
                "output relation Rv0[TRtmp]\n" +
                "Rv0[v1] :- Rt1[v],var groupResult = (v).group_by(()),var aggResult = agg(groupResult),var v0 = aggResult,var v1 = v0.";
        this.testTranslation(query, program, true);
    }

    @Test
    public void testEvery() {
        String query = "create view v0 as SELECT EVERY(column3) AS e FROM t1";
        String program = this.header(false) +
                "typedef TRtmp = TRtmp{e:bool}\n" +
                "function agg(g: Group<(), TRt1>):TRtmp {\n" +
                "var every = true: bool;\n" +
                "(for ((i, _) in g) {\n" +
                "var v = i;\n" +
                "(var incr = v.column3);\n" +
                "(every = agg_every_R(every, incr))}\n" +
                ");\n" +
                "(TRtmp{.e = every})\n}\n" +
                this.relations(false) +
                "relation Rtmp[TRtmp]\n" +
                "output relation Rv0[TRtmp]\n" +
                "Rv0[v1] :- Rt1[v],var groupResult = (v).group_by(()),var aggResult = agg(groupResult),var v0 = aggResult,var v1 = v0.";
        this.testTranslation(query, program);
    }

    @Test
    public void testCountDistinct() {
        String query = "create view v0 as SELECT COUNT(DISTINCT column1) AS ct FROM t1";
        String program = this.header(false) +
                "typedef TRtmp = TRtmp{ct:signed<64>}\n" +
                "function agg(g: Group<(), TRt1>):TRtmp {\n" +
                "var count_distinct = set_empty(): Set<signed<64>>;\n" +
                "(for ((i, _) in g) {\n" +
                "var v = i;\n" +
                "(var incr = v.column1);\n" +
                "(set_insert(count_distinct, incr))}\n" +
                ");\n" +
                "(TRtmp{.ct = set_size(count_distinct) as signed<64>})\n}\n" +
                this.relations(false) +
                "relation Rtmp[TRtmp]\n" +
                "output relation Rv0[TRtmp]\n" +
                "Rv0[v1] :- Rt1[v],var groupResult = (v).group_by(()),var aggResult = agg(groupResult),var v0 = aggResult,var v1 = v0.";
        this.testTranslation(query, program);
    }

    @Test
    public void testCountDistinctWNulls() {
        String query = "create view v0 as SELECT COUNT(DISTINCT column1) AS ct FROM t1";
        String program = this.header(true) +
                "typedef TRtmp = TRtmp{ct:signed<64>}\n" +
                "function agg(g: Group<(), TRt1>):TRtmp {\n" +
                "var count_distinct = set_empty(): Set<signed<64>>;\n" +
                "(for ((i, _) in g) {\n" +
                "var v = i;\n" +
                "(var incr = v.column1);\n" +
                "(insert_non_null(count_distinct, incr))}\n" +
                ");\n" +
                "(TRtmp{.ct = set_size(count_distinct) as signed<64>})\n}\n" +
                this.relations(true) +
                "relation Rtmp[TRtmp]\n" +
                "output relation Rv0[TRtmp]\n" +
                "Rv0[v1] :- Rt1[v],var groupResult = (v).group_by(()),var aggResult = agg(groupResult),var v0 = aggResult,var v1 = v0.";
        this.testTranslation(query, program, true);
    }

    @Test
    public void testSumDistinct() {
        String query = "create view v0 as SELECT SUM(DISTINCT column1) AS sum FROM t1";
        String program = this.header(false) +
                "typedef TRtmp = TRtmp{sum:signed<64>}\n" +
                "function agg(g: Group<(), TRt1>):TRtmp {\n" +
                "var sum_distinct = set_empty(): Set<signed<64>>;\n" +
                "(for ((i, _) in g) {\n" +
                "var v = i;\n" +
                "(var incr = v.column1);\n" +
                "(set_insert(sum_distinct, incr))}\n" +
                ");\n" +
                "(TRtmp{.sum = set_signed_sum(sum_distinct)})\n}\n" +
                this.relations(false) +
                "relation Rtmp[TRtmp]\n" +
                "output relation Rv0[TRtmp]\n" +
                "Rv0[v1] :- Rt1[v],var groupResult = (v).group_by(()),var aggResult = agg(groupResult),var v0 = aggResult,var v1 = v0.";
        this.testTranslation(query, program);
    }

    @Test
    public void testMinDistinct() {
        String query = "create view v0 as SELECT MIN(DISTINCT column1) AS min FROM t1";
        String program = this.header(false) +
                "typedef TRtmp = TRtmp{min:signed<64>}\n" +
                "function agg(g: Group<(), TRt1>):TRtmp {\n" +
                "var min = (true, 64'sd0): (bool, signed<64>);\n" +
                "(for ((i, _) in g) {\n" +
                "var v = i;\n" +
                "(var incr = v.column1);\n" +
                "(min = agg_min_R(min, incr))}\n" +
                ");\n" +
                "(TRtmp{.min = min.1})\n}\n" +
                this.relations(false) +
                "relation Rtmp[TRtmp]\n" +
                "output relation Rv0[TRtmp]\n" +
                "Rv0[v1] :- Rt1[v],var groupResult = (v).group_by(()),var aggResult = agg(groupResult),var v0 = aggResult,var v1 = v0.";
        this.testTranslation(query, program);
    }

    @Test
    public void testTwoAggregations() {
        String query = "create view v0 as SELECT MIN(column1) + MAX(column1) AS total FROM t1";
        String program = this.header(false) +
                "typedef TRtmp = TRtmp{total:signed<64>}\n" +
                "function agg(g: Group<(), TRt1>):TRtmp {\n" +
                "var min = (true, 64'sd0): (bool, signed<64>);\n" +
                "(var max = (true, 64'sd0): (bool, signed<64>));\n" +
                "(for ((i, _) in g) {\n" +
                "var v = i;\n" +
                "(var incr = v.column1);\n" +
                "(min = agg_min_R(min, incr));\n" +
                "(var incr0 = v.column1);\n" +
                "(max = agg_max_R(max, incr0))}\n" +
                ");\n" +
                "(TRtmp{.total = (min.1 + max.1)})\n}\n" +
                this.relations(false) +
                "relation Rtmp[TRtmp]\n" +
                "output relation Rv0[TRtmp]\n" +
                "Rv0[v1] :- Rt1[v],var groupResult = (v).group_by(()),var aggResult = agg(groupResult),var v0 = aggResult,var v1 = v0.";
        this.testTranslation(query, program);
    }

    @Test
    public void testStringAggregate() {
        String query = "create view v0 as SELECT MIN(column2) AS min FROM t1";
        String program = this.header(false) +
                "typedef TRtmp = TRtmp{min:istring}\n" +
                "function agg(g: Group<(), TRt1>):TRtmp {\n" +
                "var min = (true, i\"\"): (bool, istring);\n" +
                "(for ((i, _) in g) {\n" +
                "var v = i;\n" +
                "(var incr = v.column2);\n" +
                "(min = agg_min_R(min, incr))}\n" +
                ");\n" +
                "(TRtmp{.min = min.1})\n}\n" +
                this.relations(false) +
                "relation Rtmp[TRtmp]\n" +
                "output relation Rv0[TRtmp]\n" +
                "Rv0[v1] :- Rt1[v],var groupResult = (v).group_by(()),var aggResult = agg(groupResult),var v0 = aggResult,var v1 = v0.";
        this.testTranslation(query, program);
    }

    @Test
    public void testParallelAggregates() {
        String query = "create view v0 as SELECT COUNT(column1) AS ct, SUM(column1) AS sum FROM t1";
        String program = this.header(false) +
                "typedef TRtmp = TRtmp{ct:signed<64>, sum:signed<64>}\n" +
                "function agg(g: Group<(), TRt1>):TRtmp {\n" +
                "var count = 64'sd0: signed<64>;\n" +
                "(var sum = 64'sd0: signed<64>);\n" +
                "(for ((i, _) in g) {\n" +
                "var v = i;\n" +
                "(var incr = v.column1);\n" +
                "(count = agg_count_R(count, incr));\n" +
                "(var incr0 = v.column1);\n" +
                "(sum = agg_sum_signed_R(sum, incr0))}\n" +
                ");\n" +
                "(TRtmp{.ct = count,.sum = sum})\n}\n" +
                this.relations(false) +
                "relation Rtmp[TRtmp]\n" +
                "output relation Rv0[TRtmp]\n" +
                "Rv0[v1] :- Rt1[v],var groupResult = (v).group_by(()),var aggResult = agg(groupResult),var v0 = aggResult,var v1 = v0.";
        this.testTranslation(query, program);
    }

    @Test
    public void testCount() {
        String query = "create view v0 as SELECT COUNT(*) AS ct FROM t1";
        String program = this.header(false) +
                "typedef TRtmp = TRtmp{ct:signed<64>}\n" +
                "function agg(g: Group<(), TRt1>):TRtmp {\n" +
                "var count = 64'sd0: signed<64>;\n" +
                "(for ((i, _) in g) {\n" +
                "var v = i;\n" +
                "(count = agg_count_R(count, 64'sd1))}\n" +
                ");\n" +
                "(TRtmp{.ct = count})\n}\n" +
                this.relations(false) +
                "relation Rtmp[TRtmp]\n" +
                "output relation Rv0[TRtmp]\n" +
                "Rv0[v1] :- Rt1[v],var groupResult = (v).group_by(()),var aggResult = agg(groupResult),var v0 = aggResult,var v1 = v0.";
        this.testTranslation(query, program);
    }

    @Test
    public void testTwoQueriesCount() {
        String query0 = "create view v0 as SELECT COUNT(*) as ct FROM t1";
        String query1 = "create view v1 as SELECT COUNT(*) as ct FROM t1";
        String program = this.header(false) +
                "typedef TRtmp = TRtmp{ct:signed<64>}\n" +
                "function agg(g: Group<(), TRt1>):TRtmp {\n" +
                "var count = 64'sd0: signed<64>;\n" +
                "(for ((i, _) in g) {\n" +
                "var v = i;\n" +
                "(count = agg_count_R(count, 64'sd1))}\n" +
                ");\n" +
                "(TRtmp{.ct = count})\n}\n\n" +
                "function agg0(g: Group<(), TRt1>):TRtmp {\n" +
                "var count = 64'sd0: signed<64>;\n" +
                "(for ((i, _) in g) {\n" +
                "var v = i;\n" +
                "(count = agg_count_R(count, 64'sd1))}\n" +
                ");\n" +
                "(TRtmp{.ct = count})\n}\n" +
                this.relations(false) +
                "relation Rtmp[TRtmp]\n" +
                "output relation Rv0[TRtmp]\n" +
                "relation Rtmp0[TRtmp]\n" +
                "output relation Rv1[TRtmp]\n" +
                "Rv0[v1] :- Rt1[v],var groupResult = (v).group_by(()),var aggResult = agg(groupResult),var v0 = aggResult,var v1 = v0.\n" +
                "Rv1[v1] :- Rt1[v],var groupResult = (v).group_by(()),var aggResult = agg0(groupResult),var v0 = aggResult,var v1 = v0.";
        this.testTranslation(Arrays.asList(query0, query1), program, false);
    }

    @Test
    public void testRedundantCount() {
        String query = "create view v0 as SELECT COUNT(*) AS ct0, COUNT(*) AS ct1 FROM t1";
        String program = this.header(false) +
                "typedef TRtmp = TRtmp{ct0:signed<64>, ct1:signed<64>}\n" +
                "function agg(g: Group<(), TRt1>):TRtmp {\n" +
                "var count = 64'sd0: signed<64>;\n" +
                "(for ((i, _) in g) {\n" +
                "var v = i;\n" +
                "(count = agg_count_R(count, 64'sd1))}\n" +
                ");\n" +
                "(TRtmp{.ct0 = count,.ct1 = count})\n}\n" +
                this.relations(false) +
                "relation Rtmp[TRtmp]\n" +
                "output relation Rv0[TRtmp]\n" +
                "Rv0[v1] :- Rt1[v],var groupResult = (v).group_by(()),var aggResult = agg(groupResult),var v0 = aggResult,var v1 = v0.";
        this.testTranslation(query, program);
    }

   @Test
    public void testCountColumn() {
        String query = "create view v0 as SELECT COUNT(column1) AS ct FROM t1";
        String program = this.header(false) +
                "typedef TRtmp = TRtmp{ct:signed<64>}\n" +
                "function agg(g: Group<(), TRt1>):TRtmp {\n" +
                "var count = 64'sd0: signed<64>;\n" +
                "(for ((i, _) in g) {\n" +
                "var v = i;\n" +
                "(var incr = v.column1);\n" +
                "(count = agg_count_R(count, incr))}\n" +
                ");\n" +
                "(TRtmp{.ct = count})\n}\n" +
                this.relations(false) +
                "relation Rtmp[TRtmp]\n" +
                "output relation Rv0[TRtmp]\n" +
                "Rv0[v1] :- Rt1[v],var groupResult = (v).group_by(()),var aggResult = agg(groupResult),var v0 = aggResult,var v1 = v0.";
        this.testTranslation(query, program);
    }

    @Test
    public void testCountColumnWNull() {
        String query = "create view v0 as SELECT COUNT(column1) AS ct FROM t1";
        String program = this.header(true) +
                "typedef TRtmp = TRtmp{ct:Option<signed<64>>}\n" +
                "function agg(g: Group<(), TRt1>):TRtmp {\n" +
                "var count = None{}: Option<signed<64>>;\n" +
                "(for ((i, _) in g) {\n" +
                "var v = i;\n" +
                "(var incr = v.column1);\n" +
                "(count = agg_count_N(count, incr))}\n" +
                ");\n" +
                "(TRtmp{.ct = count})\n}\n" +
                this.relations(true) +
                "relation Rtmp[TRtmp]\n" +
                "output relation Rv0[TRtmp]\n" +
                "Rv0[v1] :- Rt1[v],var groupResult = (v).group_by(()),var aggResult = agg(groupResult),var v0 = aggResult,var v1 = v0.";
        this.testTranslation(query, program, true);
    }

    @Test
    public void testAvg() {
        String query = "create view v0 as SELECT AVG(column1) AS avg FROM t1";
        String program = this.header(false) +
                "typedef TRtmp = TRtmp{avg:signed<64>}\n" +
                "function agg(g: Group<(), TRt1>):TRtmp {\n" +
                "var avg = (64'sd0, 64'sd0): (signed<64>, signed<64>);\n" +
                "(for ((i, _) in g) {\n" +
                "var v = i;\n" +
                "(var incr = v.column1);\n" +
                "(avg = agg_avg_signed_R(avg, incr))}\n" +
                ");\n" +
                "(TRtmp{.avg = avg_signed_R(avg)})\n}\n" +
                this.relations(false) +
                "relation Rtmp[TRtmp]\n" +
                "output relation Rv0[TRtmp]\n" +
                "Rv0[v1] :- Rt1[v],var groupResult = (v).group_by(()),var aggResult = agg(groupResult),var v0 = aggResult,var v1 = v0.";
        this.testTranslation(query, program);
    }

    @Test
    public void testAvgDouble() {
        String query = "create view v0 as SELECT AVG(column4) AS avg FROM t1";
        String program = this.header(false) +
                "typedef TRtmp = TRtmp{avg:double}\n" +
                "function agg(g: Group<(), TRt1>):TRtmp {\n" +
                "var avg = (64'f0.0, 64'f0.0): (double, double);\n" +
                "(for ((i, _) in g) {\n" +
                "var v = i;\n" +
                "(var incr = v.column4);\n" +
                "(avg = agg_avg_double_R(avg, incr))}\n" +
                ");\n" +
                "(TRtmp{.avg = avg_double_R(avg)})\n}\n" +
                this.relations(false) +
                "relation Rtmp[TRtmp]\n" +
                "output relation Rv0[TRtmp]\n" +
                "Rv0[v1] :- Rt1[v],var groupResult = (v).group_by(()),var aggResult = agg(groupResult),var v0 = aggResult,var v1 = v0.";
        this.testTranslation(query, program);
    }

    @Test
    public void testAvgWNull() {
        String query = "create view v0 as SELECT AVG(column1) AS avg FROM t1";
        String program = this.header(true) +
                "typedef TRtmp = TRtmp{avg:Option<signed<64>>}\n" +
                "function agg(g: Group<(), TRt1>):TRtmp {\n" +
                "var avg = None{}: Option<(signed<64>, signed<64>)>;\n" +
                "(for ((i, _) in g) {\n" +
                "var v = i;\n" +
                "(var incr = v.column1);\n" +
                "(avg = agg_avg_signed_N(avg, incr))}\n" +
                ");\n" +
                "(TRtmp{.avg = avg_signed_N(avg)})\n}\n" +
                this.relations(true) +
                "relation Rtmp[TRtmp]\n" +
                "output relation Rv0[TRtmp]\n" +
                "Rv0[v1] :- Rt1[v],var groupResult = (v).group_by(()),var aggResult = agg(groupResult),var v0 = aggResult,var v1 = v0.";
        this.testTranslation(query, program, true);
    }

    @Test
    public void testCountWNull() {
        String query = "create view v0 as SELECT COUNT(*) AS ct FROM t1";
        String program = this.header(true) +
                "typedef TRtmp = TRtmp{ct:signed<64>}\n" +
                "function agg(g: Group<(), TRt1>):TRtmp {\n" +
                "var count = 64'sd0: signed<64>;\n" +
                "(for ((i, _) in g) {\n" +
                "var v = i;\n" +
                "(count = agg_count_R(count, 64'sd1))}\n" +
                ");\n" +
                "(TRtmp{.ct = count})\n}\n" +
                this.relations(true) +
                "relation Rtmp[TRtmp]\n" +
                "output relation Rv0[TRtmp]\n" +
                "Rv0[v1] :- Rt1[v],var groupResult = (v).group_by(()),var aggResult = agg(groupResult),var v0 = aggResult,var v1 = v0.";
        this.testTranslation(query, program, true);
    }

    @Test
    public void testMaxCase() {
        String query = "create view v0 as SELECT MAX(CASE WHEN column2 = 'foo' THEN column1 ELSE 0 END) AS m FROM t1";
        String program = this.header(false) +
                "typedef TRtmp = TRtmp{m:signed<64>}\n" +
                "function agg(g: Group<(), TRt1>):TRtmp {\n" +
                "var max = (true, 64'sd0): (bool, signed<64>);\n" +
                "(for ((i, _) in g) {\n" +
                "var v = i;\n" +
                "(var incr = if ((v.column2 == i\"foo\")) {\n" +
                "v.column1} else {\n" +
                "64'sd0});\n" +
                "(max = agg_max_R(max, incr))}\n" +
                ");\n" +
                "(TRtmp{.m = max.1})\n}\n" +
                this.relations(false) +
                "relation Rtmp[TRtmp]\n" +
                "output relation Rv0[TRtmp]\n" +
                "Rv0[v1] :- Rt1[v],var groupResult = (v).group_by(()),var aggResult = agg(groupResult),var v0 = aggResult,var v1 = v0.";
        this.testTranslation(query, program);
    }

   @Test
    public void testMax() {
        String query = "create view v0 as SELECT MAX(column1) AS m FROM t1";
        String program = this.header(false) +
                "typedef TRtmp = TRtmp{m:signed<64>}\n" +
                "function agg(g: Group<(), TRt1>):TRtmp {\n" +
                "var max = (true, 64'sd0): (bool, signed<64>);\n" +
                "(for ((i, _) in g) {\n" +
                "var v = i;\n" +
                "(var incr = v.column1);\n" +
                "(max = agg_max_R(max, incr))}\n" +
                ");\n" +
                "(TRtmp{.m = max.1})\n}\n" +
                this.relations(false) +
                "relation Rtmp[TRtmp]\n" +
                "output relation Rv0[TRtmp]\n" +
                "Rv0[v1] :- Rt1[v],var groupResult = (v).group_by(()),var aggResult = agg(groupResult),var v0 = aggResult,var v1 = v0.";
        this.testTranslation(query, program);
    }

    @Test
    public void arrayAggTest() {
        String query = "create view v1 as select array_agg(column2) from t1";
        String program = this.header(false) +
                "typedef TRtmp = TRtmp{col0:Ref<Vec<istring>>}\n" +
                "function agg(g: Group<(), TRt1>):TRtmp {\n" +
                "var array_agg = vec_empty(): Vec<istring>;\n" +
                "(for ((i, _) in g) {\n" +
                "var v = i;\n" +
                "(var incr = v.column2);\n" +
                "(vec_push(array_agg, incr))}\n" +
                ");\n" +
                "(TRtmp{.col0 = array_agg.ref_new()})\n" +
                "}\n" +
                this.relations(false) +
                "relation Rtmp[TRtmp]\n" +
                "output relation Rv1[TRtmp]\n" +
                "Rv1[v1] :- Rt1[v],var groupResult = (v).group_by(()),var aggResult = agg(groupResult),var v0 = aggResult,var v1 = v0.";
        this.testTranslation(query, program);
    }

    @Test
    public void arrayLengthTest() {
        String query = "create view v1 as select array_length(array_agg(column2)) from t1";
        String program = this.header(false) +
                "typedef TRtmp = TRtmp{col0:signed<64>}\n" +
                "function agg(g: Group<(), TRt1>):TRtmp {\n" +
                "var array_agg = vec_empty(): Vec<istring>;\n" +
                "(for ((i, _) in g) {\n" +
                "var v = i;\n" +
                "(var incr = v.column2);\n" +
                "(vec_push(array_agg, incr))}\n" +
                ");\n" +
                "(TRtmp{.col0 = sql_array_length(array_agg.ref_new())})\n" +
                "}\n" +
                this.relations(false) +
                "relation Rtmp[TRtmp]\n" +
                "output relation Rv1[TRtmp]\n" +
                "Rv1[v1] :- Rt1[v],var groupResult = (v).group_by(()),var aggResult = agg(groupResult),var v0 = aggResult,var v1 = v0.";
        this.testTranslation(query, program);
    }
}
