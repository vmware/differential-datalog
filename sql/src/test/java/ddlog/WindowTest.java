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

public class WindowTest extends BaseQueriesTest {
    @Test
    public void windowSimpleTest() {
        String query = "create view v1 as\n" +
                "select DISTINCT *, count(column3) over (partition by column2) as c2 from t1";
        String translation = this.header(false) +
                "typedef TRtmp = TRtmp{tmp:bool, gb:istring, column1:signed<64>, column2:istring, column3:bool, column4:double}\n" +
                "typedef TRtmp0 = TRtmp0{gb:istring, count:signed<64>}\n" +
                "typedef Tagg = Tagg{count:signed<64>}\n" +
                "typedef Ttmp = Ttmp{tmp:bool, gb:istring, column1:signed<64>, column2:istring, column3:bool, column4:double, gb0:istring, count:signed<64>}\n" +
                "typedef TRtmp1 = TRtmp1{column1:signed<64>, column2:istring, column3:bool, column4:double, c2:signed<64>}\n" +
                "function agg(g: Group<istring, TRtmp>):Tagg {\n" +
                "(var gb0) = group_key(g);\n" +
                "(var count0 = 64'sd0: signed<64>);\n" +
                "(for ((i, _) in g) {\n" +
                "var v3 = i;\n" +
                "(var incr = v3.tmp);\n" +
                "(count0 = agg_count_R(count0, incr))}\n" +
                ");\n" +
                "(Tagg{.count = count0})\n" +
                "}\n" +
                this.relations(false) +
                "relation Roverinput[TRtmp]\n" +
                "relation Rover[TRtmp0]\n" +
                "output relation Rv1[TRtmp1]\n" +
                "Roverinput[v2] :- Rt1[v0],var v1 = TRtmp{.tmp = v0.column3,.gb = v0.column2,.column1 = v0.column1," +
                ".column2 = v0.column2,.column3 = v0.column3,.column4 = v0.column4},var v2 = v1.\n" +
                "Rover[v5] :- Roverinput[v3],var gb0 = v3.gb,var groupResult = (v3).group_by((gb0)),var aggResult = agg(groupResult),var v4 = TRtmp0{.gb = gb0,.count = aggResult.count},var v5 = v4.\n" +
                "Rv1[v10] :- Roverinput[TRtmp{.tmp = tmp0,.gb = gb1,.column1 = column1,.column2 = column2,.column3 = column3,.column4 = column4}],Rover[TRtmp0{.gb = gb1,.count = count1}],var v8 = Ttmp{.tmp = tmp0,.gb = gb1,.column1 = column1,.column2 = column2,.column3 = column3,.column4 = column4,.gb0 = gb1,.count = count1},var v9 = TRtmp1{.column1 = v8.column1,.column2 = v8.column2,.column3 = v8.column3,.column4 = v8.column4,.c2 = v8.count},var v10 = v9.";
        this.testTranslation(query, translation);
    }

    @Test
    public void windowAliasTest() {
        String query = "create view v0 as SELECT DISTINCT alias1.column1 as new_column, " +
                "AVG(alias2.column1) OVER (PARTITION BY alias1.column1) as problem " +
                "from t1 as alias1 " +
                "JOIN t4 as alias2 ON alias1.column1 = alias2.column1";
        String program = this.header(false) +
                "typedef Ttmp = Ttmp{column1:signed<64>, column2:istring, column3:bool, column4:double, column10:Option<signed<64>>, column20:Option<istring>}\n" +
                "typedef TRalias2 = TRalias2{tmp:Option<signed<64>>, gb:signed<64>, new_column:signed<64>}\n" +
                "typedef TRtmp = TRtmp{gb:signed<64>, avg:Option<signed<64>>}\n" +
                "typedef Tagg = Tagg{avg:Option<signed<64>>}\n" +
                "typedef Ttmp0 = Ttmp0{tmp:Option<signed<64>>, gb:signed<64>, new_column:signed<64>, gb0:signed<64>, avg:Option<signed<64>>}\n" +
                "typedef TRtmp0 = TRtmp0{new_column:signed<64>, problem:Option<signed<64>>}\n" +
                "function agg(g: Group<signed<64>, TRalias2>):Tagg {\n" +
                "(var gb0) = group_key(g);\n" +
                "(var avg0 = Some{.x = (64'sd0, 64'sd0)}: Option<(signed<64>, signed<64>)>);\n" +
                "(for ((i, _) in g) {\n" +
                "var v7 = i;\n" +
                "(var incr = v7.tmp);\n" +
                "(avg0 = agg_avg_signed_N(avg0, incr))}\n" +
                ");\n" +
                "(Tagg{.avg = avg_signed_N(avg0)})\n" +
                "}\n" +
                this.relations(false) +
                "relation Roverinput[TRalias2]\n" +
                "relation Rover[TRtmp]\n" +
                "output relation Rv0[TRtmp0]\n" +
                "Roverinput[v6] :- Rt1[TRt1{.column1 = column11,.column2 = column21,.column3 = column30,.column4 = column40}],Rt4[TRt4{.column1 = Some{.x = column11},.column2 = column22}],var v4 = Ttmp{.column1 = column11,.column2 = column21,.column3 = column30,.column4 = column40,.column10 = Some{.x = column11},.column20 = column22},var v5 = TRalias2{.tmp = v4.column10,.gb = v4.column1,.new_column = v4.column1},var v6 = v5.\n" +
                "Rover[v9] :- Roverinput[v7],var gb0 = v7.gb,var groupResult = (v7).group_by((gb0)),var aggResult = agg(groupResult),var v8 = TRtmp{.gb = gb0,.avg = aggResult.avg},var v9 = v8.\n" +
                "Rv0[v14] :- Roverinput[TRalias2{.tmp = tmp0,.gb = gb1,.new_column = new_column}],Rover[TRtmp{.gb = gb1,.avg = avg1}],var v12 = Ttmp0{.tmp = tmp0,.gb = gb1,.new_column = new_column,.gb0 = gb1,.avg = avg1},var v13 = TRtmp0{.new_column = v12.new_column,.problem = v12.avg},var v14 = v13.";
        this.testTranslation(query, program);
    }

    @Test
    public void windowExpressionTest() {
        String query = "create view v1 as\n" +
                "select DISTINCT *, 3 + count(column3) over (partition by column2) as c2 from t1";
        String translation = this.header(false) +
                "typedef TRtmp = TRtmp{tmp:bool, gb:istring, column1:signed<64>, column2:istring, column3:bool, column4:double}\n" +
                "typedef TRtmp0 = TRtmp0{gb:istring, count:signed<64>}\n" +
                "typedef Tagg = Tagg{count:signed<64>}\n" +
                "typedef Ttmp = Ttmp{tmp:bool, gb:istring, column1:signed<64>, column2:istring, column3:bool, column4:double, gb0:istring, count:signed<64>}\n" +
                "typedef TRtmp1 = TRtmp1{column1:signed<64>, column2:istring, column3:bool, column4:double, c2:signed<64>}\n" +
                "function agg(g: Group<istring, TRtmp>):Tagg {\n" +
                "(var gb0) = group_key(g);\n" +
                "(var count0 = 64'sd0: signed<64>);\n" +
                "(for ((i, _) in g) {\n" +
                "var v3 = i;\n" +
                "(var incr = v3.tmp);\n" +
                "(count0 = agg_count_R(count0, incr))}\n" +
                ");\n" +
                "(Tagg{.count = count0})\n" +
                "}\n" +
                this.relations(false) +
                "relation Roverinput[TRtmp]\n" +
                "relation Rover[TRtmp0]\n" +
                "output relation Rv1[TRtmp1]\n" +
                "Roverinput[v2] :- Rt1[v0],var v1 = TRtmp{.tmp = v0.column3,.gb = v0.column2,.column1 = v0.column1," +
                ".column2 = v0.column2,.column3 = v0.column3,.column4 = v0.column4},var v2 = v1.\n" +
                "Rover[v5] :- Roverinput[v3],var gb0 = v3.gb,var groupResult = (v3).group_by((gb0)),var aggResult = agg(groupResult),var v4 = TRtmp0{.gb = gb0,.count = aggResult.count},var v5 = v4.\n" +
                "Rv1[v10] :- Roverinput[TRtmp{.tmp = tmp0,.gb = gb1,.column1 = column1,.column2 = column2,.column3 = column3,.column4 = column4}],Rover[TRtmp0{.gb = gb1,.count = count1}],var v8 = Ttmp{.tmp = tmp0,.gb = gb1,.column1 = column1,.column2 = column2,.column3 = column3,.column4 = column4,.gb0 = gb1,.count = count1},var v9 = TRtmp1{.column1 = v8.column1,.column2 = v8.column2,.column3 = v8.column3,.column4 = v8.column4,.c2 = (64'sd3 + v8.count)},var v10 = v9.";
        this.testTranslation(query, translation);
    }

    @Test
    public void windowOfExpressionTest() {
        String query = "create view v1 as\n" +
                "select DISTINCT *, 3 + count(substr(column2, 3, 3)) over (partition by column3) as c2 from t1";
        String translation = this.header(false) +
                "typedef TRtmp = TRtmp{tmp:istring, gb:bool, column1:signed<64>, column2:istring, column3:bool, column4:double}\n" +
                "typedef TRtmp0 = TRtmp0{gb:bool, count:signed<64>}\n" +
                "typedef Tagg = Tagg{count:signed<64>}\n" +
                "typedef Ttmp = Ttmp{tmp:istring, gb:bool, column1:signed<64>, column2:istring, column3:bool, column4:double, gb0:bool, count:signed<64>}\n" +
                "typedef TRtmp1 = TRtmp1{column1:signed<64>, column2:istring, column3:bool, column4:double, c2:signed<64>}\n" +
                "function agg(g: Group<bool, TRtmp>):Tagg {\n" +
                "(var gb0) = group_key(g);\n" +
                "(var count0 = 64'sd0: signed<64>);\n" +
                "(for ((i, _) in g) {\n" +
                "var v3 = i;\n" +
                "(var incr = v3.tmp);\n" +
                "(count0 = agg_count_R(count0, incr))}\n" +
                ");\n" +
                "(Tagg{.count = count0})\n" +
                "}\n" +
                this.relations(false) +
                "relation Roverinput[TRtmp]\n" +
                "relation Rover[TRtmp0]\n" +
                "output relation Rv1[TRtmp1]\n" +
                "Roverinput[v2] :- Rt1[v0],var v1 = TRtmp{.tmp = sql_substr(v0.column2, 64'sd3, 64'sd3),.gb = v0.column3," +
                ".column1 = v0.column1,.column2 = v0.column2,.column3 = v0.column3,.column4 = v0.column4},var v2 = v1.\n" +
                "Rover[v5] :- Roverinput[v3],var gb0 = v3.gb,var groupResult = (v3).group_by((gb0)),var aggResult = agg(groupResult),var v4 = TRtmp0{.gb = gb0,.count = aggResult.count},var v5 = v4.\n" +
                "Rv1[v10] :- Roverinput[TRtmp{.tmp = tmp0,.gb = gb1,.column1 = column1,.column2 = column2,.column3 = column3,.column4 = column4}],Rover[TRtmp0{.gb = gb1,.count = count1}],var v8 = Ttmp{.tmp = tmp0,.gb = gb1,.column1 = column1,.column2 = column2,.column3 = column3,.column4 = column4,.gb0 = gb1,.count = count1},var v9 = TRtmp1{.column1 = v8.column1,.column2 = v8.column2,.column3 = v8.column3,.column4 = v8.column4,.c2 = (64'sd3 + v8.count)},var v10 = v9.";
        this.testTranslation(query, translation);
    }

    @Test
    public void windowTwoWindowsTest() {
        String query = "create view v1 as\n" +
                "select DISTINCT count(column3) over (partition by column2) + COUNT(column2) over (partition by column3) as X from t1";
        String translation = this.header(false) +
                "typedef TRtmp = TRtmp{tmp:bool, tmp0:istring, gb:istring, gb0:bool}\n" +
                "typedef TRtmp0 = TRtmp0{gb:istring, count:signed<64>}\n" +
                "typedef Tagg = Tagg{count:signed<64>}\n" +
                "typedef TRtmp1 = TRtmp1{gb0:bool, count0:signed<64>}\n" +
                "typedef Tagg0 = Tagg0{count0:signed<64>}\n" +
                "typedef Ttmp = Ttmp{tmp:bool, tmp0:istring, gb:istring, gb0:bool, gb1:istring, count:signed<64>}\n" +
                "typedef Ttmp0 = Ttmp0{tmp:bool, tmp0:istring, gb:istring, gb0:bool, gb1:istring, count:signed<64>, gb00:bool, count0:signed<64>}\n" +
                "typedef TRtmp3 = TRtmp3{x:signed<64>}\n" +
                "function agg(g: Group<istring, TRtmp>):Tagg {\n" +
                "(var gb1) = group_key(g);\n" +
                "(var count1 = 64'sd0: signed<64>);\n" +
                "(for ((i, _) in g) {\n" +
                "var v3 = i;\n" +
                "(var incr = v3.tmp);\n" +
                "(count1 = agg_count_R(count1, incr))}\n" +
                ");\n" +
                "(Tagg{.count = count1})\n" +
                "}\n" +
                "\n" +
                "function agg0(g0: Group<bool, TRtmp>):Tagg0 {\n" +
                "(var gb2) = group_key(g0);\n" +
                "(var count2 = 64'sd0: signed<64>);\n" +
                "(for ((i0, _) in g0) {\n" +
                "var v6 = i0;\n" +
                "(var incr0 = v6.tmp0);\n" +
                "(count2 = agg_count_R(count2, incr0))}\n" +
                ");\n" +
                "(Tagg0{.count0 = count2})\n" +
                "}\n" +
                this.relations(false) +
                "relation Roverinput[TRtmp]\n" +
                "relation Rover[TRtmp0]\n" +
                "relation Rover0[TRtmp1]\n" +
                "relation Rtmp2[Ttmp]\n" +
                "output relation Rv1[TRtmp3]\n" +
                "Roverinput[v2] :- Rt1[v0],var v1 = TRtmp{.tmp = v0.column3,.tmp0 = v0.column2,.gb = v0.column2,.gb0 = v0.column3},var v2 = v1.\n" +
                "Rover[v5] :- Roverinput[v3],var gb1 = v3.gb,var groupResult = (v3).group_by((gb1)),var aggResult = agg(groupResult),var v4 = TRtmp0{.gb = gb1,.count = aggResult.count},var v5 = v4.\n" +
                "Rover0[v8] :- Roverinput[v6],var gb2 = v6.gb0,var groupResult0 = (v6).group_by((gb2)),var aggResult0 = agg0(groupResult0),var v7 = TRtmp1{.gb0 = gb2,.count0 = aggResult0.count0},var v8 = v7.\n" +
                "Rtmp2[v13] :- Roverinput[TRtmp{.tmp = tmp1,.tmp0 = tmp00,.gb = gb3,.gb0 = gb00}],Rover[TRtmp0{.gb = gb3,.count = count3}],var v11 = Ttmp{.tmp = tmp1,.tmp0 = tmp00,.gb = gb3,.gb0 = gb00,.gb1 = gb3,.count = count3},var v13 = v11.\n" +
                "Rv1[v16] :- Rtmp2[Ttmp{.tmp = tmp2,.tmp0 = tmp01,.gb = gb5,.gb0 = gb01,.gb1 = gb10,.count = count4}],Rover0[TRtmp1{.gb0 = gb01,.count0 = count00}],var v14 = Ttmp0{.tmp = tmp2,.tmp0 = tmp01,.gb = gb5,.gb0 = gb01,.gb1 = gb10,.count = count4,.gb00 = gb01,.count0 = count00},var v15 = TRtmp3{.x = (v14.count + v14.count0)},var v16 = v15.";
        this.testTranslation(query, translation);
    }

    @Test
    public void windowGroupByTest() {
        String query = "create view v1 as\n" +
                "SELECT DISTINCT column2, SUM(column1) AS s, MIN(AVG(column1)) OVER (PARTITION by column3) AS min FROM t1 GROUP BY column2, column3";
        String translation = this.header(false) +
                "typedef TRtmp = TRtmp{tmp:signed<64>, gb1:bool, column2:istring, s:signed<64>}\n" +
                "typedef Tagg = Tagg{tmp:signed<64>, s:signed<64>}\n" +
                "typedef TRtmp0 = TRtmp0{gb1:bool, min:signed<64>}\n" +
                "typedef Tagg0 = Tagg0{min:signed<64>}\n" +
                "typedef Ttmp = Ttmp{tmp:signed<64>, gb1:bool, column2:istring, s:signed<64>, gb10:bool, min:signed<64>}\n" +
                "typedef TRtmp1 = TRtmp1{column2:istring, s:signed<64>, min:signed<64>}\n" +
                "function agg(g: Group<(istring, bool), TRt1>):Tagg {\n" +
                "(var gb2, var gb3) = group_key(g);\n" +
                "(var avg = (64'sd0, 64'sd0): (signed<64>, signed<64>));\n" +
                "(var sum = 64'sd0: signed<64>);\n" +
                "(for ((i, _) in g) {\n" +
                "var v0 = i;\n" +
                "(var incr = v0.column1);\n" +
                "(avg = agg_avg_signed_R(avg, incr));\n" +
                "(var incr0 = v0.column1);\n" +
                "(sum = agg_sum_signed_R(sum, incr0))}\n" +
                ");\n" +
                "(Tagg{.tmp = avg_signed_R(avg),.s = sum})\n" +
                "}\n" +
                "\n" +
                "function agg0(g0: Group<bool, TRtmp>):Tagg0 {\n" +
                "(var gb4) = group_key(g0);\n" +
                "(var min0 = (true, 64'sd0): (bool, signed<64>));\n" +
                "(for ((i0, _) in g0) {\n" +
                "var v3 = i0;\n" +
                "(var incr1 = v3.tmp);\n" +
                "(min0 = agg_min_R(min0, incr1))}\n" +
                ");\n" +
                "(Tagg0{.min = min0.1})\n" +
                "}\n" +
                this.relations(false) +
                "relation Roverinput[TRtmp]\n" +
                "relation Rover[TRtmp0]\n" +
                "output relation Rv1[TRtmp1]\n" +
                "Roverinput[v2] :- Rt1[v0],var gb2 = v0.column2,var gb3 = v0.column3,var groupResult = (v0).group_by((gb2, gb3)),var aggResult = agg(groupResult),var v1 = TRtmp{.column2 = gb2,.gb1 = gb3,.tmp = aggResult.tmp,.s = aggResult.s},var v2 = v1.\n" +
                "Rover[v5] :- Roverinput[v3],var gb4 = v3.gb1,var groupResult0 = (v3).group_by((gb4)),var aggResult0 = agg0(groupResult0),var v4 = TRtmp0{.gb1 = gb4,.min = aggResult0.min},var v5 = v4.\n" +
                "Rv1[v10] :- Roverinput[TRtmp{.tmp = tmp0,.gb1 = gb10,.column2 = column2,.s = s}],Rover[TRtmp0{.gb1 = gb10,.min = min1}],var v8 = Ttmp{.tmp = tmp0,.gb1 = gb10,.column2 = column2,.s = s,.gb10 = gb10,.min = min1},var v9 = TRtmp1{.column2 = v8.column2,.s = v8.s,.min = v8.min},var v10 = v9.";
        this.testTranslation(query, translation);
    }

    @Test
    public void windowGroupByTest2() {
        String query = "CREATE VIEW v0 as SELECT DISTINCT column2, COUNT(column3) OVER (PARTITION BY column2) from t1 GROUP BY column2,column3";
        String translation = this.header(false) +
                "typedef TRtmp = TRtmp{tmp:bool, gb1:istring, column2:istring}\n" +
                "typedef Tagg = Tagg{}\n" +
                "typedef TRtmp0 = TRtmp0{gb1:istring, count:signed<64>}\n" +
                "typedef Tagg0 = Tagg0{count:signed<64>}\n" +
                "typedef Ttmp = Ttmp{tmp:bool, gb1:istring, column2:istring, gb10:istring, count:signed<64>}\n" +
                "typedef TRtmp1 = TRtmp1{column2:istring, count:signed<64>}\n" +
                "function agg0(g0: Group<istring, TRtmp>):Tagg0 {\n" +
                "(var gb4) = group_key(g0);\n" +
                "(var count0 = 64'sd0: signed<64>);\n" +
                "(for ((i0, _) in g0) {\n" +
                "var v3 = i0;\n" +
                "(var incr = v3.tmp);\n" +
                "(count0 = agg_count_R(count0, incr))}\n" +
                ");\n" +
                "(Tagg0{.count = count0})\n" +
                "}\n" +
                this.relations(false) +
                "relation Roverinput[TRtmp]\n" +
                "relation Rover[TRtmp0]\n" +
                "output relation Rv0[TRtmp1]\n" +
                "Roverinput[v2] :- Rt1[v0],var gb2 = v0.column2,var gb3 = v0.column3,var groupResult = (v0).group_by((gb2, gb3)),var v1 = TRtmp{.gb1 = gb2,.column2 = gb2,.tmp = gb3},var v2 = v1.\n" +
                "Rover[v5] :- Roverinput[v3],var gb4 = v3.gb1,var groupResult0 = (v3).group_by((gb4)),var aggResult0 = agg0(groupResult0),var v4 = TRtmp0{.gb1 = gb4,.count = aggResult0.count},var v5 = v4.\n" +
                "Rv0[v10] :- Roverinput[TRtmp{.tmp = tmp0,.gb1 = gb10,.column2 = column2}],Rover[TRtmp0{.gb1 = gb10,.count = count1}],var v8 = Ttmp{.tmp = tmp0,.gb1 = gb10,.column2 = column2,.gb10 = gb10,.count = count1},var v9 = TRtmp1{.column2 = v8.column2,.count = v8.count},var v10 = v9.";
        this.testTranslation(query, translation);
    }

    @Test
    public void nestedAggTest() {
        String query = "CREATE VIEW v0 as SELECT column2, SUM(column1), SUM(SUM(column1)) OVER (PARTITION BY column3) FROM t1 GROUP BY column2, column3";
        String translation = this.header(false) +
                "typedef TRtmp = TRtmp{tmp:signed<64>, gb1:bool, column2:istring, col:signed<64>}\n" +
                "typedef Tagg = Tagg{tmp:signed<64>, col:signed<64>}\n" +
                "typedef TRtmp0 = TRtmp0{gb1:bool, sum:signed<64>}\n" +
                "typedef Tagg0 = Tagg0{sum:signed<64>}\n" +
                "typedef Ttmp = Ttmp{tmp:signed<64>, gb1:bool, column2:istring, col:signed<64>, gb10:bool, sum:signed<64>}\n" +
                "typedef TRtmp1 = TRtmp1{column2:istring, col:signed<64>, sum:signed<64>}\n" +
                "function agg(g: Group<(istring, bool), TRt1>):Tagg {\n" +
                "(var gb2, var gb3) = group_key(g);\n" +
                "(var sum0 = 64'sd0: signed<64>);\n" +
                "(for ((i, _) in g) {\n" +
                "var v0 = i;\n" +
                "(var incr = v0.column1);\n" +
                "(sum0 = agg_sum_signed_R(sum0, incr))}\n" +
                ");\n" +
                "(Tagg{.tmp = sum0,.col = sum0})\n" +
                "}\n" +
                "\n" +
                "function agg0(g0: Group<bool, TRtmp>):Tagg0 {\n" +
                "(var gb4) = group_key(g0);\n" +
                "(var sum1 = 64'sd0: signed<64>);\n" +
                "(for ((i0, _) in g0) {\n" +
                "var v3 = i0;\n" +
                "(var incr0 = v3.tmp);\n" +
                "(sum1 = agg_sum_signed_R(sum1, incr0))}\n" +
                ");\n" +
                "(Tagg0{.sum = sum1})\n" +
                "}\n" +
                this.relations(false) +
                "relation Roverinput[TRtmp]\n" +
                "relation Rover[TRtmp0]\n" +
                "output relation Rv0[TRtmp1]\n" +
                "Roverinput[v2] :- Rt1[v0],var gb2 = v0.column2,var gb3 = v0.column3,var groupResult = (v0).group_by((gb2, gb3)),var aggResult = agg(groupResult),var v1 = TRtmp{.column2 = gb2,.gb1 = gb3,.tmp = aggResult.tmp,.col = aggResult.col},var v2 = v1.\n" +
                "Rover[v5] :- Roverinput[v3],var gb4 = v3.gb1,var groupResult0 = (v3).group_by((gb4)),var aggResult0 = agg0(groupResult0),var v4 = TRtmp0{.gb1 = gb4,.sum = aggResult0.sum},var v5 = v4.\n" +
                "Rv0[v10] :- Roverinput[TRtmp{.tmp = tmp0,.gb1 = gb10,.column2 = column2,.col = col0}],Rover[TRtmp0{.gb1 = gb10,.sum = sum2}],var v8 = Ttmp{.tmp = tmp0,.gb1 = gb10,.column2 = column2,.col = col0,.gb10 = gb10,.sum = sum2},var v9 = TRtmp1{.column2 = v8.column2,.col = v8.col,.sum = v8.sum},var v10 = v9.";
        this.testTranslation(query, translation);
    }

    @Test
    public void joinWindowTest() {
        String query = "CREATE VIEW v0 as SELECT DISTINCT t1.column2, AVG(t2.column1) OVER (PARTITION by t1.column3) AS avg\n" +
                "         FROM t1 JOIN t2 ON t1.column1 = t2.column1\n";
        String translation = this.header(false) +
                "typedef Ttmp = Ttmp{column1:signed<64>, column2:istring, column3:bool, column4:double, column10:signed<64>}\n" +
                "typedef TRtmp = TRtmp{tmp:signed<64>, gb:bool, column2:istring}\n" +
                "typedef TRtmp0 = TRtmp0{gb:bool, avg:signed<64>}\n" +
                "typedef Tagg = Tagg{avg:signed<64>}\n" +
                "typedef Ttmp0 = Ttmp0{tmp:signed<64>, gb:bool, column2:istring, gb0:bool, avg:signed<64>}\n" +
                "typedef TRtmp1 = TRtmp1{column2:istring, avg:signed<64>}\n" +
                "function agg(g: Group<bool, TRtmp>):Tagg {\n" +
                "(var gb0) = group_key(g);\n" +
                "(var avg0 = (64'sd0, 64'sd0): (signed<64>, signed<64>));\n" +
                "(for ((i, _) in g) {\n" +
                "var v7 = i;\n" +
                "(var incr = v7.tmp);\n" +
                "(avg0 = agg_avg_signed_R(avg0, incr))}\n" +
                ");\n" +
                "(Tagg{.avg = avg_signed_R(avg0)})\n" +
                "}\n" +
                this.relations(false) +
                "relation Roverinput[TRtmp]\n" +
                "relation Rover[TRtmp0]\n" +
                "output relation Rv0[TRtmp1]\n" +
                "Roverinput[v6] :- Rt1[TRt1{.column1 = column11,.column2 = column20,.column3 = column30,.column4 = column40}],Rt2[TRt2{.column1 = column11}],var v4 = Ttmp{.column1 = column11,.column2 = column20,.column3 = column30,.column4 = column40,.column10 = column11},var v5 = TRtmp{.tmp = v4.column10,.gb = v4.column3,.column2 = v4.column2},var v6 = v5.\n" +
                "Rover[v9] :- Roverinput[v7],var gb0 = v7.gb,var groupResult = (v7).group_by((gb0)),var aggResult = agg(groupResult),var v8 = TRtmp0{.gb = gb0,.avg = aggResult.avg},var v9 = v8.\n" +
                "Rv0[v14] :- Roverinput[TRtmp{.tmp = tmp0,.gb = gb1,.column2 = column21}],Rover[TRtmp0{.gb = gb1,.avg = avg1}],var v12 = Ttmp0{.tmp = tmp0,.gb = gb1,.column2 = column21,.gb0 = gb1,.avg = avg1},var v13 = TRtmp1{.column2 = v12.column2,.avg = v12.avg},var v14 = v13.";
        this.testTranslation(query, translation);
    }

    @Test
    public void joinWindowWithAliasTest() {
        String query = "CREATE VIEW v0 as SELECT DISTINCT t1.column2 as test_alias, AVG(t2.column1) OVER (PARTITION by t1.column3) AS avg\n" +
                "         FROM t1 JOIN t2 ON t1.column1 = t2.column1\n";
        String translation = this.header(false) +
                "typedef Ttmp = Ttmp{column1:signed<64>, column2:istring, column3:bool, column4:double, column10:signed<64>}\n" +
                "typedef TRtmp = TRtmp{tmp:signed<64>, gb:bool, test_alias:istring}\n" +
                "typedef TRtmp0 = TRtmp0{gb:bool, avg:signed<64>}\n" +
                "typedef Tagg = Tagg{avg:signed<64>}\n" +
                "typedef Ttmp0 = Ttmp0{tmp:signed<64>, gb:bool, test_alias:istring, gb0:bool, avg:signed<64>}\n" +
                "typedef TRtmp1 = TRtmp1{test_alias:istring, avg:signed<64>}\n" +
                "function agg(g: Group<bool, TRtmp>):Tagg {\n" +
                "(var gb0) = group_key(g);\n" +
                "(var avg0 = (64'sd0, 64'sd0): (signed<64>, signed<64>));\n" +
                "(for ((i, _) in g) {\n" +
                "var v7 = i;\n" +
                "(var incr = v7.tmp);\n" +
                "(avg0 = agg_avg_signed_R(avg0, incr))}\n" +
                ");\n" +
                "(Tagg{.avg = avg_signed_R(avg0)})\n" +
                "}\n" +
                this.relations(false) +
                "relation Roverinput[TRtmp]\n" +
                "relation Rover[TRtmp0]\n" +
                "output relation Rv0[TRtmp1]\n" +
                "Roverinput[v6] :- Rt1[TRt1{.column1 = column11,.column2 = column20,.column3 = column30,.column4 = column40}],Rt2[TRt2{.column1 = column11}],var v4 = Ttmp{.column1 = column11,.column2 = column20,.column3 = column30,.column4 = column40,.column10 = column11},var v5 = TRtmp{.tmp = v4.column10,.gb = v4.column3,.test_alias = v4.column2},var v6 = v5.\n" +
                "Rover[v9] :- Roverinput[v7],var gb0 = v7.gb,var groupResult = (v7).group_by((gb0)),var aggResult = agg(groupResult),var v8 = TRtmp0{.gb = gb0,.avg = aggResult.avg},var v9 = v8.\n" +
                "Rv0[v14] :- Roverinput[TRtmp{.tmp = tmp0,.gb = gb1,.test_alias = test_alias}],Rover[TRtmp0{.gb = gb1,.avg = avg1}],var v12 = Ttmp0{.tmp = tmp0,.gb = gb1,.test_alias = test_alias,.gb0 = gb1,.avg = avg1},var v13 = TRtmp1{.test_alias = v12.test_alias,.avg = v12.avg},var v14 = v13.";
        this.testTranslation(query, translation);
    }

    @Test
    public void joinWindowWithAliasOrderTest() {
        String query = "CREATE VIEW v0 as SELECT DISTINCT AVG(t2.column1) OVER (PARTITION by t1.column3) AS avg, t1.column2 as test_alias\n" +
                "         FROM t1 JOIN t2 ON t1.column1 = t2.column1\n";
        String translation = this.header(false) +
                "typedef Ttmp = Ttmp{column1:signed<64>, column2:istring, column3:bool, column4:double, column10:signed<64>}\n" +
                "typedef TRtmp = TRtmp{tmp:signed<64>, gb:bool, test_alias:istring}\n" +
                "typedef TRtmp0 = TRtmp0{gb:bool, avg:signed<64>}\n" +
                "typedef Tagg = Tagg{avg:signed<64>}\n" +
                "typedef Ttmp0 = Ttmp0{tmp:signed<64>, gb:bool, test_alias:istring, gb0:bool, avg:signed<64>}\n" +
                "typedef TRtmp1 = TRtmp1{avg:signed<64>, test_alias:istring}\n" +
                "function agg(g: Group<bool, TRtmp>):Tagg {\n" +
                "(var gb0) = group_key(g);\n" +
                "(var avg0 = (64'sd0, 64'sd0): (signed<64>, signed<64>));\n" +
                "(for ((i, _) in g) {\n" +
                "var v7 = i;\n" +
                "(var incr = v7.tmp);\n" +
                "(avg0 = agg_avg_signed_R(avg0, incr))}\n" +
                ");\n" +
                "(Tagg{.avg = avg_signed_R(avg0)})\n" +
                "}\n" +
                this.relations(false) +
                "relation Roverinput[TRtmp]\n" +
                "relation Rover[TRtmp0]\n" +
                "output relation Rv0[TRtmp1]\n" +
                "Roverinput[v6] :- Rt1[TRt1{.column1 = column11,.column2 = column20,.column3 = column30,.column4 = column40}],Rt2[TRt2{.column1 = column11}],var v4 = Ttmp{.column1 = column11,.column2 = column20,.column3 = column30,.column4 = column40,.column10 = column11},var v5 = TRtmp{.tmp = v4.column10,.gb = v4.column3,.test_alias = v4.column2},var v6 = v5.\n" +
                "Rover[v9] :- Roverinput[v7],var gb0 = v7.gb,var groupResult = (v7).group_by((gb0)),var aggResult = agg(groupResult),var v8 = TRtmp0{.gb = gb0,.avg = aggResult.avg},var v9 = v8.\n" +
                "Rv0[v14] :- Roverinput[TRtmp{.tmp = tmp0,.gb = gb1,.test_alias = test_alias}],Rover[TRtmp0{.gb = gb1,.avg = avg1}],var v12 = Ttmp0{.tmp = tmp0,.gb = gb1,.test_alias = test_alias,.gb0 = gb1,.avg = avg1},var v13 = TRtmp1{.avg = v12.avg,.test_alias = v12.test_alias},var v14 = v13.";

        this.testTranslation(query, translation);
    }
}
