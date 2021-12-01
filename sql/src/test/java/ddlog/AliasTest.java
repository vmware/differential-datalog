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

// Tests for renaming
public class AliasTest extends BaseQueriesTest {
    @Test
    public void testAliasWNull() {
        String query = "create view v0 as SELECT DISTINCT t2.column1 FROM t1 AS t2";
        String program = this.header(true) +
                this.relations(true) +
                "output relation Rv0[TRt2]\n" +
                "Rv0[v1] :- Rt1[v],var v0 = TRt2{.column1 = v.column1},var v1 = v0.";
        this.testTranslation(query, program, true);
    }

    @Test
    public void sameAliasTest() {
        String query = "create view v0 as SELECT DISTINCT t1.column1 as new_column " +
                "from t1 as t1 " +
                "JOIN t2 as alias2 ON t1.column1 = alias2.column1";
        String program = this.header(false) +
                "typedef TRalias2 = TRalias2{new_column:signed<64>}\n" +
                this.relations(false) +
                "output relation Rv0[TRalias2]\n" +
                "Rv0[v3] :- Rt1[TRt1{.column1 = column1,.column2 = column2,.column3 = column3,.column4 = column4}],Rt2[TRt2{.column1 = column1}],var v1 = TRt1{.column1 = column1,.column2 = column2,.column3 = column3,.column4 = column4},var v2 = TRalias2{.new_column = v1.column1},var v3 = v2.";
        this.testTranslation(query, program);
    }

    @Test
    public void testAlias1() {
        String query = "create view v0 as SELECT DISTINCT t2.column1 AS x, t2.column2 as y FROM t1 AS t2";
        String program = this.header(false) +
                "typedef TRt20 = TRt20{x:signed<64>, y:string}\n" +
                this.relations(false) +
                "output relation Rv0[TRt20]\n" +
                "Rv0[v1] :- Rt1[v],var v0 = TRt20{.x = v.column1,.y = v.column2},var v1 = v0.";
        this.testTranslation(query, program);
    }

    @Test
    public void testScope() {
        String query = "create view v0 as SELECT DISTINCT t1.column1 FROM t1";
        String program = this.header(false) +
                this.relations(false) +
                "output relation Rv0[TRt2]\n" +
                "Rv0[v1] :- Rt1[v],var v0 = TRt2{.column1 = v.column1},var v1 = v0.";
        this.testTranslation(query, program);
    }

    @Test
    public void testScopeWNulls() {
        String query = "create view v0 as SELECT DISTINCT t1.column1 FROM t1";
        String program = this.header(true) +
                this.relations(true) +
                "output relation Rv0[TRt2]\n" +
                "Rv0[v1] :- Rt1[v],var v0 = TRt2{.column1 = v.column1},var v1 = v0.";
        this.testTranslation(query, program, true);
    }

    @Test
    public void testNested3() {
        String query = "CREATE VIEW v3 AS (SELECT DISTINCT Y.c FROM " +
                "((SELECT DISTINCT column1 AS c FROM t2 AS X) AS Y))";
        String program = this.header(false) +
                "typedef TX = TX{c:signed<64>}\n" +
                this.relations(false) +
                "relation Y[TX]\n" +
                "relation Rtmp0[TX]\n" +
                "output relation Rv3[TX]\n" +
                "Y[v1] :- Rt2[v],var v0 = TX{.c = v.column1},var v1 = v0.\n" +
                "Rtmp0[v3] :- Y[v1],var v2 = TX{.c = v1.c},var v3 = v2.\n" +
                "Rv3[v4] :- Rtmp0[v3],var v4 = v3.";
        this.testTranslation(query, program);
    }

    @Test
    public void testNested2() {
        String query = "CREATE VIEW v3 AS (SELECT DISTINCT column1 FROM (t2 AS X) AS Y)";
        String program = this.header(false) +
                this.relations(false) +
                "relation Y[TRt2]\n" +
                "output relation Rv3[TRt2]\n" +
                "Y[v1] :- Rt2[v],var v0 = TRt2{.column1 = v.column1},var v1 = v0.\n" +
                "Rv3[v2] :- Y[v1],var v2 = v1.";
        this.testTranslation(query, program);
    }

    @Test
    public void testNestedAlias() {
        String query = "CREATE VIEW v3 AS (SELECT DISTINCT Y.column1 FROM ((t2 AS X) AS Y))";
        String program = this.header(false) +
                this.relations(false) +
                "relation Y[TRt2]\n" +
                "output relation Rv3[TRt2]\n" +
                "Y[v1] :- Rt2[v],var v0 = TRt2{.column1 = v.column1},var v1 = v0.\n" +
                "Rv3[v2] :- Y[v1],var v2 = v1.";
        this.testTranslation(query, program);
    }

    @Test
    public void testNested1() {
        String query = "CREATE VIEW v3 AS SELECT DISTINCT X.c FROM (SELECT DISTINCT column1 AS c FROM t2 AS X)";
        String program = this.header(false) +
                "typedef TX = TX{c:signed<64>}\n" +
                this.relations(false) +
                "relation Rtmp[TX]\n" +
                "output relation Rv3[TX]\n" +
                "Rtmp[v1] :- Rt2[v],var v0 = TX{.c = v.column1},var v1 = v0.\n" +
                "Rv3[v3] :- Rtmp[v1],var v2 = TX{.c = v1.c},var v3 = v2.";
        this.testTranslation(query, program);
    }

    @Test
    public void testSelectStar() {
        String query = "create view v0 as select DISTINCT *, column1 as C1 from t1";
        String program = this.header(false) +
                "typedef TRtmp = TRtmp{column1:signed<64>, column2:string, column3:bool, column4:double, c1:signed<64>}\n" +
                this.relations(false) +
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
                "output relation Rv0[TRtmp]\n" +
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
    public void testAlias() {
        String query = "create view v0 as SELECT DISTINCT t2.column1 FROM (t1 AS t2)";
        String program = this.header(false) +
                this.relations(false) +
                "output relation Rv0[TRt2]\n" +
                "Rv0[v1] :- Rt1[v],var v0 = TRt2{.column1 = v.column1},var v1 = v0.";
        this.testTranslation(query, program);
    }
}
