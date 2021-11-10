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
import java.util.List;

public class SimpleQueriesTest extends BaseQueriesTest {
    @Test
    public void arrayTest() {
        String query = "create table a(column1 integer not null, column2 integer not null, column3 integer array not null)";
        String program = this.header(false) +
                "typedef Ta = Ta{column1:signed<64>, column2:signed<64>, column3:Vec<signed<64>>}\n" +
                this.relations(false) +
                "input relation Ra[Ta]\n";
        this.testTranslation(query, program);
    }

    @Test
    public void inTest() {
        String query = "create view v0 as select distinct column1 from t1 where column2 IN ('a', 'b')";
        String program = this.header(false) +
                this.relations(false) +
                "relation Rtmp[Tt2]\n" +
                "output relation Rv0[Tt2]\n" +
                "Rv0[v1] :- Rt1[v],vec_contains(vec_push_imm(vec_push_imm(vec_empty(), \"a\"), \"b\"), v.column2)," +
                "var v0 = Tt2{.column1 = v.column1},var v1 = v0.";
        this.testTranslation(query, program);
    }

    @Test
    public void keyTest() {
        String query = "create table a(column1 integer not null with (primary_key = true),\n" +
                " column2 integer not null with (primary_key = true)\n" +
                ", column3 integer array not null with (primary_key = true))";
        String program = "import fp\n" +
                "import time\n" +
                "import sql\n" +
                "import sqlop\n" +
                "\n" +
                "typedef Tt1 = Tt1{column1:signed<64>, column2:string, column3:bool, column4:double}\n" +
                "typedef Tt2 = Tt2{column1:signed<64>}\n" +
                "typedef Tt3 = Tt3{d:Date, t:Time, dt:DateTime}\n" +
                "typedef Tt4 = Tt4{column1:Option<signed<64>>, column2:Option<string>}\n" +
                "typedef Ta = Ta{column1:signed<64>, column2:signed<64>, column3:Vec<signed<64>>}\n" +
                "\n" +
                "input relation Rt1[Tt1]\n" +
                "input relation Rt2[Tt2]\n" +
                "input relation Rt3[Tt3]\n" +
                "input relation Rt4[Tt4]\n" +
                "input relation Ra[Ta] primary key (row) (row.column1, row.column2, row.column3)\n";
        this.testTranslation(query, program, false);
    }

    @Test
    public void inArrayTest() {
        List<String> queries = Arrays.asList(
                "create table a(column1 integer not null, column2 integer not null, column3 integer array not null)",
                "create view v0 as select distinct column1 from a where array_contains(column3, column1)");
        String program = this.header(false) +
                "typedef Ta = Ta{column1:signed<64>, column2:signed<64>, column3:Vec<signed<64>>}\n" +
                this.relations(false) +
                "input relation Ra[Ta]\n" +
                "relation Rtmp[Tt2]\n" +
                "output relation Rv0[Tt2]\n" +
                "Rv0[v1] :- Ra[v],sql_array_contains(v.column3, v.column1),var v0 = Tt2{.column1 = v.column1},var v1 = v0.";
        this.testTranslation(queries, program, false);
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
    public void testSelectStar() {
        String query = "create view v0 as select DISTINCT *, column1 as C1 from t1";
        String program = this.header(false) +
                "typedef TRtmp = TRtmp{" +
                "column1:signed<64>, column2:string, column3:bool, column4:double, c1:signed<64>}\n" +
                this.relations(false) +
                "relation Rtmp[TRtmp]\n" +
                "output relation Rv0[TRtmp]\n" +
                "Rv0[v1] :- Rt1[v],var v0 = TRtmp{" +
                ".column1 = v.column1,.column2 = v.column2,.column3 = v.column3,.column4 = v.column4,.c1 = v.column1}," +
                "var v1 = v0.";
        this.testTranslation(query, program);
    }

    @Test
    public void test2SelectStar() {
        String query0 = "create view v0 as select DISTINCT *, column1 as C1 from t1";
        String query1 = "create view v1 as select DISTINCT *, column1 as C1 from t1";
        String program = this.header(false) +
                "typedef TRtmp = TRtmp{" +
                "column1:signed<64>, column2:string, column3:bool, column4:double, c1:signed<64>}\n" +
                this.relations(false) +
                "relation Rtmp[TRtmp]\n" +
                "output relation Rv0[TRtmp]\n" +
                "relation Rtmp0[TRtmp]\n" +
                "output relation Rv1[TRtmp]\n" +
                "Rv0[v1] :- Rt1[v],var v0 = TRtmp{" +
                ".column1 = v.column1,.column2 = v.column2,.column3 = v.column3,.column4 = v.column4,.c1 = v.column1}," +
                "var v1 = v0.\n" +
                "Rv1[v1] :- Rt1[v],var v0 = TRtmp{" +
                ".column1 = v.column1,.column2 = v.column2,.column3 = v.column3,.column4 = v.column4,.c1 = v.column1}," +
                "var v1 = v0.";
        this.testTranslation(Arrays.asList(query0, query1), program, false);
    }

    @Test
    public void testSelectWNull() {
        String query = "create view v0 as select distinct column1, column2 from t1";
        String program = this.header(true) +
            this.relations(true) +
            "relation Rtmp[Tt4]\n" +
            "output relation Rv0[Tt4]\n" +
            "Rv0[v1] :- Rt1[v],var v0 = Tt4{.column1 = v.column1,.column2 = v.column2},var v1 = v0.";
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
    public void testWhen() {
        String query = "create view v0 as SELECT DISTINCT CASE WHEN column1 = 1 THEN 1 WHEN 1 < column1 THEN 2 ELSE 3 END AS i FROM t1";
        String program =
                this.header(false) +
                        "typedef TRtmp = TRtmp{i:signed<64>}\n" +
                        this.relations(false) +
                        "relation Rtmp[TRtmp]\n" +
                        "output relation Rv0[TRtmp]\n" +
                        "Rv0[v1] :- Rt1[v],var v0 = TRtmp{.i = if ((v.column1 == 64'sd1)) {\n" +
                        "64'sd1} else {\n" +
                        "if ((64'sd1 < v.column1)) {\n" +
                        "64'sd2} else {\n" +
                        "64'sd3}}},var v1 = v0.";
        this.testTranslation(query, program);
    }

    @Test
    public void testNULL() {
        String query = "create view v0 as SELECT DISTINCT CASE WHEN column1 = 1 THEN NULL ELSE 3 END AS i FROM t1";
        String program =
            this.header(true) +
                "typedef TRtmp = TRtmp{i:Option<signed<64>>}\n" +
                this.relations(true) +
                "relation Rtmp[TRtmp]\n" +
                "output relation Rv0[TRtmp]\n" +
                "Rv0[v1] :- Rt1[v],var v0 = TRtmp{.i = if (unwrapBool(a_eq_NR(v.column1, 64'sd1))) {\n" +
                "None{}: Option<signed<64>>} else {\n" +
                "Some{.x = 64'sd3}}},var v1 = v0.";
        this.testTranslation(query, program, true);
    }

    @Test
    public void testCaseWNull() {
        String query = "create view v0 as SELECT DISTINCT CASE WHEN column1 = 1 THEN 1 WHEN 1 < column1 THEN 2 ELSE 3 END AS i FROM t1";
        String program =
                this.header(true) +
                        "typedef TRtmp = TRtmp{i:signed<64>}\n" +
                        this.relations(true) +
                        "relation Rtmp[TRtmp]\n" +
                        "output relation Rv0[TRtmp]\n" +
                        "Rv0[v1] :- Rt1[v],var v0 = TRtmp{.i = if (unwrapBool(a_eq_NR(v.column1, 64'sd1))) {\n" +
                        "64'sd1} else {\n" +
                        "if (unwrapBool(a_lt_RN(64'sd1, v.column1))) {\n" +
                        "64'sd2} else {\n" +
                        "64'sd3}}},var v1 = v0.";
        this.testTranslation(query, program, true);
    }

    @Test
    public void testAlias() {
        String query = "create view v0 as SELECT DISTINCT t2.column1 FROM t1 AS t2";
        String program = this.header(false) +
                this.relations(false) +
                "relation Rtmp[Tt2]\n" +
                "output relation Rv0[Tt2]\n" +
                "Rv0[v1] :- Rt1[v],var v0 = Tt2{.column1 = v.column1},var v1 = v0.";
        this.testTranslation(query, program);
    }

    @Test
    public void testAliasWNull() {
        String query = "create view v0 as SELECT DISTINCT t2.column1 FROM t1 AS t2";
        String program = this.header(true) +
            this.relations(true) +
            "relation Rtmp[Tt2]\n" +
            "output relation Rv0[Tt2]\n" +
            "Rv0[v1] :- Rt1[v],var v0 = Tt2{.column1 = v.column1},var v1 = v0.";
        this.testTranslation(query, program, true);
    }

    @Test
    public void testScope() {
        String query = "create view v0 as SELECT DISTINCT t1.column1 FROM t1";
        String program = this.header(false) +
                this.relations(false) +
                "relation Rtmp[Tt2]\n" +
                "output relation Rv0[Tt2]\n" +
                "Rv0[v1] :- Rt1[v],var v0 = Tt2{.column1 = v.column1},var v1 = v0.";
        this.testTranslation(query, program);
    }

    @Test
    public void testLimit() {
        String query = "create view v0 as SELECT DISTINCT * FROM t1 LIMIT 10";
        String program = this.header(false) +
                this.relations(false) +
                "relation Rlimit[Tt1]\n" +
                "output relation Rv0[Tt1]\n" +
                "Rlimit[v0] :- Rt1[v],var v0 = v.\n" +
                "Rv0[v1] :- Rlimit[v0],var g = v0.group_by(()),var agg = limit(g, 10),var limited = FlatMap(agg),var v1 = limited.";
        this.testTranslation(query, program);
    }

    @Test
    public void testScopeWNulls() {
        String query = "create view v0 as SELECT DISTINCT t1.column1 FROM t1";
        String program = this.header(true) +
            this.relations(true) +
            "relation Rtmp[Tt2]\n" +
            "output relation Rv0[Tt2]\n" +
            "Rv0[v1] :- Rt1[v],var v0 = Tt2{.column1 = v.column1},var v1 = v0.";
        this.testTranslation(query, program, true);
    }
    @Test
    public void testAbs() {
        String query = "create view v0 as SELECT DISTINCT ABS(column1) AS i FROM t1";
        String program = this.header(false) +
                "typedef TRtmp = TRtmp{i:signed<64>}\n" +
                this.relations(false) +
                "relation Rtmp[TRtmp]\n" +
                "output relation Rv0[TRtmp]\n" +
                "Rv0[v1] :- Rt1[v],var v0 = TRtmp{.i = sql_abs(v.column1)},var v1 = v0.";
        this.testTranslation(query, program);
    }

    @Test
    public void testAbsWNull() {
        String query = "create view v0 as SELECT DISTINCT ABS(column1) AS i FROM t1";
        String program = this.header(true) +
                "typedef TRtmp = TRtmp{i:Option<signed<64>>}\n" +
                this.relations(true) +
                "relation Rtmp[TRtmp]\n" +
                "output relation Rv0[TRtmp]\n" +
                "Rv0[v1] :- Rt1[v],var v0 = TRtmp{.i = sql_abs_N(v.column1)},var v1 = v0.";
        this.testTranslation(query, program, true);
    }

    @Test
    public void duplicatedColumnTest() {
        String query = "CREATE VIEW v AS SELECT DISTINCT column3 tmp, column2 gb1, column2 column2 FROM t1";
        String translation = this.header(false) +
                "typedef TRtmp = TRtmp{tmp:bool, gb1:string, column2:string}\n" +
                this.relations(false) +
                "relation Rtmp[TRtmp]\n" +
                "output relation Rv[TRtmp]\n" +
                "Rv[v1] :- Rt1[v],var v0 = TRtmp{.tmp = v.column3,.gb1 = v.column2,.column2 = v.column2},var v1 = v0.";
        this.testTranslation(query, translation);
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
            this.relations(true) +
            "relation Rtmp[Tt4]\n" +
            "output relation Rv0[Tt4]\n" +
            "Rv0[v1] :- Rt1[v],unwrapBool(b_and_NN(a_lte_RN((- 64'sd1), v.column1), a_lte_NR(v.column1, 64'sd10))),var v0 = Tt4{.column1 = v.column1,.column2 = v.column2},var v1 = v0.";
        this.testTranslation(query, program, true);
    }

    @Test
    public void testSubstr() {
        String query = "create view v0 as SELECT DISTINCT SUBSTR(column2, 3, 5) AS s FROM t1";
        String program = this.header(false) +
                "typedef TRtmp = TRtmp{s:string}\n" +
                this.relations(false) +
                "relation Rtmp[TRtmp]\n" +
                "output relation Rv0[TRtmp]\n" +
                "Rv0[v1] :- Rt1[v],var v0 = TRtmp{.s = sql_substr(v.column2, 64'sd3, 64'sd5)},var v1 = v0.";
        this.testTranslation(query, program);
    }

    @Test
    public void testSubstrWNull() {
        String query = "create view v0 as SELECT DISTINCT SUBSTR(column2, 3, 5) AS s FROM t1";
        String program = this.header(true) +
            "typedef TRtmp = TRtmp{s:Option<string>}\n" +
            this.relations(true) +
            "relation Rtmp[TRtmp]\n" +
            "output relation Rv0[TRtmp]\n" +
            "Rv0[v1] :- Rt1[v],var v0 = TRtmp{.s = sql_substr_N(v.column2, 64'sd3, 64'sd5)},var v1 = v0.";
        this.testTranslation(query, program, true);
    }

    @Test
    public void testSelectWithNulls() {
        String query = "create view v0 as select distinct column1, column2 from t1";
        String program =
                this.header(true) +
                this.relations(true) +
                "relation Rtmp[Tt4]\n" +
                "output relation Rv0[Tt4]\n" +
                "Rv0[v1] :- Rt1[v],var v0 = Tt4{.column1 = v.column1,.column2 = v.column2},var v1 = v0.";
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
    public void testConcat() {
        String query = "create view v3 as select distinct concat(t1.column1, t1.column2, t1.column3) as c from t1";
        String program = this.header(false) +
                "typedef TRtmp = TRtmp{c:string}\n" +
                this.relations(false) +
                "relation Rtmp[TRtmp]\n" +
                "output relation Rv3[TRtmp]\n" +
                "Rv3[v1] :- Rt1[v],var v0 = TRtmp{.c = sql_concat(sql_concat([|${v.column1}|], v.column2), [|${v.column3}|])},var v1 = v0.";
        this.testTranslation(query, program);
    }

    @Test
    public void testConcatWNull() {
        String query = "create view v3 as select distinct concat(t1.column1, t1.column2, t1.column3) as c from t1";
        String program = this.header(true) +
                "typedef TRtmp = TRtmp{c:Option<string>}\n" +
                this.relations(true) +
                "relation Rtmp[TRtmp]\n" +
                "output relation Rv3[TRtmp]\n" +
                "Rv3[v1] :- Rt1[v],var v0 = TRtmp{.c = sql_concat_N(sql_concat_N(match(v.column1) {None{}: Option<signed<64>> -> None{}: Option<string>,\n" +
                "Some{.x = var x} -> Some{.x = [|${x}|]}\n" +
                "}, v.column2), match(v.column3) {None{}: Option<bool> -> None{}: Option<string>,\n" +
                "Some{.x = var x} -> Some{.x = [|${x}|]}\n" +
                "})},var v1 = v0.";
        this.testTranslation(query, program, true);
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

    @Test
    public void testIndex1() {
        String query = "create index idx_name on t1 (column1, column2)";
        String program = this.header(true) +
                this.relations(true) + "\n" +
                "index Iidx_name(column1:Option<signed<64>>,column2:Option<string>) on Rt1[Tt1{column1,column2,_,_}]";
        this.testIndexTranslation(query, program, true);
    }

    @Test
    public void testIndex2() {
        String query = "create index idx_name on t1 (column1)";
        String program = this.header(true) +
                this.relations(true) + "\n" +
                "index Iidx_name(column1:Option<signed<64>>) on Rt1[Tt1{column1,_,_,_}]";
        this.testIndexTranslation(query, program, true);
    }

    @Test
    public void testIndexException() {
        String query = "create index idx_name on non_existent_table (column1)";
        try {
            this.testIndexTranslation(query, "junk program", true);
        } catch (Exception e) {
            assert e.getMessage().equals("Cannot find base table that index refers to");
        }
    }
}