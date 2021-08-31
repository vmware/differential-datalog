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
                "typedef TRtmp = TRtmp{tmp:bool, gb:string, column1:signed<64>, column2:string, column3:bool, column4:double}\n" +
                "typedef TRtmp0 = TRtmp0{gb:string, count:signed<64>}\n" +
                "typedef Tagg = Tagg{count:signed<64>}\n" +
                "typedef Ttmp = Ttmp{tmp:bool, gb:string, column1:signed<64>, column2:string, column3:bool, column4:double, count:signed<64>}\n" +
                "typedef TRtmp1 = TRtmp1{column1:signed<64>, column2:string, column3:bool, column4:double, c2:signed<64>}\n" +
                "function agg(g: Group<string, TRtmp>):Tagg {\n" +
                "(var gb4) = group_key(g);\n" +
                "(var count5 = 64'sd0: signed<64>);\n" +
                "(for ((i, _) in g) {\n" +
                "var v3 = i;\n" +
                "(var incr = v3.tmp);\n" +
                "(count5 = agg_count_R(count5, incr))}\n" +
                ");\n" +
                "(Tagg{.count = count5})\n" +
                "}\n" +
                this.relations(false) +
                "relation Rtmp[TRtmp]\n" +
                "relation Roverinput[TRtmp]\n" +
                "relation Rtmp0[TRtmp0]\n" +
                "relation Rover[TRtmp0]\n" +
                "relation Rtmp1[TRtmp1]\n" +
                "output relation Rv1[TRtmp1]\n" +
                "Roverinput[v2] :- Rt1[v0],var v1 = TRtmp{.tmp = v0.column3,.gb = v0.column2,.column1 = v0.column1," +
                ".column2 = v0.column2,.column3 = v0.column3,.column4 = v0.column4},var v2 = v1.\n" +
                "Rover[v7] :- Roverinput[v3],var gb4 = v3.gb,var groupResult = (v3).group_by((gb4)),var aggResult = agg(groupResult)," +
                "var v6 = TRtmp0{.gb = gb4,.count = aggResult.count},var v7 = v6.\n" +
                "Rv1[v12] :- Roverinput[v8],Rover[v9],(true and (v8.gb == v9.gb))," +
                "var v10 = Ttmp{.tmp = v8.tmp,.gb = v8.gb,.column1 = v8.column1,.column2 = v8.column2," +
                ".column3 = v8.column3,.column4 = v8.column4,.count = v9.count}," +
                "var v11 = TRtmp1{.column1 = v8.column1,.column2 = v8.column2,.column3 = v8.column3," +
                ".column4 = v8.column4,.c2 = v9.count},var v12 = v11.";
        this.testTranslation(query, translation);
    }

    @Test
    public void windowExpressionTest() {
        String query = "create view v1 as\n" +
                "select DISTINCT *, 3 + count(column3) over (partition by column2) as c2 from t1";
        String translation = this.header(false) +
                "typedef TRtmp = TRtmp{tmp:bool, gb:string, column1:signed<64>, column2:string, column3:bool, column4:double}\n" +
                "typedef TRtmp0 = TRtmp0{gb:string, count:signed<64>}\n" +
                "typedef Tagg = Tagg{count:signed<64>}\n" +
                "typedef Ttmp = Ttmp{tmp:bool, gb:string, column1:signed<64>, column2:string, column3:bool, column4:double, count:signed<64>}\n" +
                "typedef TRtmp1 = TRtmp1{column1:signed<64>, column2:string, column3:bool, column4:double, c2:signed<64>}\n" +
                "function agg(g: Group<string, TRtmp>):Tagg {\n" +
                "(var gb4) = group_key(g);\n" +
                "(var count5 = 64'sd0: signed<64>);\n" +
                "(for ((i, _) in g) {\n" +
                "var v3 = i;\n" +
                "(var incr = v3.tmp);\n" +
                "(count5 = agg_count_R(count5, incr))}\n" +
                ");\n" +
                "(Tagg{.count = count5})\n" +
                "}\n" +
                this.relations(false) +
                "relation Rtmp[TRtmp]\n" +
                "relation Roverinput[TRtmp]\n" +
                "relation Rtmp0[TRtmp0]\n" +
                "relation Rover[TRtmp0]\n" +
                "relation Rtmp1[TRtmp1]\n" +
                "output relation Rv1[TRtmp1]\n" +
                "Roverinput[v2] :- Rt1[v0],var v1 = TRtmp{.tmp = v0.column3,.gb = v0.column2,.column1 = v0.column1," +
                ".column2 = v0.column2,.column3 = v0.column3,.column4 = v0.column4},var v2 = v1.\n" +
                "Rover[v7] :- Roverinput[v3],var gb4 = v3.gb,var groupResult = (v3).group_by((gb4)),var aggResult = agg(groupResult)," +
                "var v6 = TRtmp0{.gb = gb4,.count = aggResult.count},var v7 = v6.\n" +
                "Rv1[v12] :- Roverinput[v8],Rover[v9],(true and (v8.gb == v9.gb))," +
                "var v10 = Ttmp{.tmp = v8.tmp,.gb = v8.gb,.column1 = v8.column1,.column2 = v8.column2," +
                ".column3 = v8.column3,.column4 = v8.column4,.count = v9.count}," +
                "var v11 = TRtmp1{.column1 = v8.column1,.column2 = v8.column2,.column3 = v8.column3,.column4 = v8.column4,.c2 = (64'sd3 + v9.count)}," +
                "var v12 = v11.";
        this.testTranslation(query, translation);
    }

    @Test
    public void windowOfExpressionTest() {
        String query = "create view v1 as\n" +
                "select DISTINCT *, 3 + count(substr(column2, 3, 3)) over (partition by column3) as c2 from t1";
        String translation = this.header(false) +
                "typedef TRtmp = TRtmp{tmp:string, gb:bool, column1:signed<64>, column2:string, column3:bool, column4:double}\n" +
                "typedef TRtmp0 = TRtmp0{gb:bool, count:signed<64>}\n" +
                "typedef Tagg = Tagg{count:signed<64>}\n" +
                "typedef Ttmp = Ttmp{tmp:string, gb:bool, column1:signed<64>, column2:string, column3:bool, column4:double, count:signed<64>}\n" +
                "typedef TRtmp1 = TRtmp1{column1:signed<64>, column2:string, column3:bool, column4:double, c2:signed<64>}\n" +
                "function agg(g: Group<bool, TRtmp>):Tagg {\n" +
                "(var gb4) = group_key(g);\n" +
                "(var count5 = 64'sd0: signed<64>);\n" +
                "(for ((i, _) in g) {\n" +
                "var v3 = i;\n" +
                "(var incr = v3.tmp);\n" +
                "(count5 = agg_count_R(count5, incr))}\n" +
                ");\n" +
                "(Tagg{.count = count5})\n" +
                "}\n" +
                this.relations(false) +
                "relation Rtmp[TRtmp]\n" +
                "relation Roverinput[TRtmp]\n" +
                "relation Rtmp0[TRtmp0]\n" +
                "relation Rover[TRtmp0]\n" +
                "relation Rtmp1[TRtmp1]\n" +
                "output relation Rv1[TRtmp1]\n" +
                "Roverinput[v2] :- Rt1[v0],var v1 = TRtmp{.tmp = sql_substr(v0.column2, 64'sd3, 64'sd3),.gb = v0.column3," +
                ".column1 = v0.column1,.column2 = v0.column2,.column3 = v0.column3,.column4 = v0.column4},var v2 = v1.\n" +
                "Rover[v7] :- Roverinput[v3],var gb4 = v3.gb,var groupResult = (v3).group_by((gb4)),var aggResult = agg(groupResult)," +
                "var v6 = TRtmp0{.gb = gb4,.count = aggResult.count},var v7 = v6.\n" +
                "Rv1[v12] :- Roverinput[v8],Rover[v9],(true and (v8.gb == v9.gb))," +
                "var v10 = Ttmp{.tmp = v8.tmp,.gb = v8.gb,.column1 = v8.column1,.column2 = v8.column2," +
                ".column3 = v8.column3,.column4 = v8.column4,.count = v9.count}," +
                "var v11 = TRtmp1{.column1 = v8.column1,.column2 = v8.column2,.column3 = v8.column3," +
                ".column4 = v8.column4,.c2 = (64'sd3 + v9.count)}," +
                "var v12 = v11.";
        this.testTranslation(query, translation);
    }

    @Test
    public void windowTwoWindowsTest() {
        String query = "create view v1 as\n" +
                "select DISTINCT count(column3) over (partition by column2) + COUNT(column2) over (partition by column3) as X from t1";
        String translation = this.header(false) +
                "typedef TRtmp = TRtmp{tmp:bool, tmp1:string, gb:string, gb0:bool}\n" +
                "typedef TRtmp0 = TRtmp0{gb:string, count:signed<64>}\n" +
                "typedef Tagg = Tagg{count:signed<64>}\n" +
                "typedef TRtmp2 = TRtmp2{gb0:bool, count2:signed<64>}\n" +
                "typedef Tagg3 = Tagg3{count2:signed<64>}\n" +
                "typedef Ttmp = Ttmp{tmp:bool, tmp1:string, gb:string, gb0:bool, count:signed<64>}\n" +
                "typedef Ttmp4 = Ttmp4{tmp:bool, tmp1:string, gb:string, gb0:bool, count:signed<64>, " +
                "count2:signed<64>}\n" +
                "typedef TRtmp5 = TRtmp5{x:signed<64>}\n" +
                "function agg(g: Group<string, TRtmp>):Tagg {\n" +
                "(var gb7) = group_key(g);\n" +
                "(var count8 = 64'sd0: signed<64>);\n" +
                "(for ((i, _) in g) {\n" +
                "var v6 = i;\n" +
                "(var incr = v6.tmp);\n" +
                "(count8 = agg_count_R(count8, incr))}\n" +
                ");\n" +
                "(Tagg{.count = count8})\n" +
                "}\n" +
                "\n" +
                "function agg3(g13: Group<bool, TRtmp>):Tagg3 {\n" +
                "(var gb12) = group_key(g13);\n" +
                "(var count16 = 64'sd0: signed<64>);\n" +
                "(for ((i14, _) in g13) {\n" +
                "var v11 = i14;\n" +
                "(var incr15 = v11.tmp1);\n" +
                "(count16 = agg_count_R(count16, incr15))}\n" +
                ");\n" +
                "(Tagg3{.count2 = count16})\n" +
                "}\n" +
                this.relations(false) +
                "relation Rtmp[TRtmp]\n" +
                "relation Roverinput[TRtmp]\n" +
                "relation Rtmp0[TRtmp0]\n" +
                "relation Rover[TRtmp0]\n" +
                "relation Rtmp2[TRtmp2]\n" +
                "relation Rover1[TRtmp2]\n" +
                "relation Rtmp5[TRtmp5]\n" +
                "output relation Rv1[TRtmp5]\n" +
                "Roverinput[v5] :- Rt1[v3],var v4 = TRtmp{.tmp = v3.column3,.tmp1 = v3.column2,.gb = v3.column2," +
                ".gb0 = v3.column3},var v5 = v4.\n" +
                "Rover[v10] :- Roverinput[v6],var gb7 = v6.gb,var groupResult = (v6).group_by((gb7))," +
                "var aggResult = agg(groupResult),var v9 = TRtmp0{.gb = gb7,.count = aggResult.count},var v10 = v9.\n" +
                "Rover1[v20] :- Roverinput[v11],var gb12 = v11.gb0,var groupResult19 = (v11).group_by((gb12))," +
                "var aggResult18 = agg3(groupResult19),var v17 = TRtmp2{.gb0 = gb12,.count2 = aggResult18.count2},var v20 = v17.\n" +
                "Rv1[v27] :- Roverinput[v21],Rover[v22],(true and (v21.gb == v22.gb))," +
                "var v23 = Ttmp{.tmp = v21.tmp,.tmp1 = v21.tmp1,.gb = v21.gb,.gb0 = v21.gb0,.count = v22.count}," +
                "Rover1[v24],(true and (v23.gb0 == v24.gb0))," +
                "var v25 = Ttmp4{.tmp = v23.tmp,.tmp1 = v23.tmp1,.gb = v23.gb,.gb0 = v23.gb0,.count = v23.count,.count2 = v24.count2}," +
                "var v26 = TRtmp5{.x = (v22.count + v24.count2)},var v27 = v26.";
        this.testTranslation(query, translation);
    }

    @Test
    public void windowGroupByTest() {
        String query = "create view v1 as\n" +
                "SELECT DISTINCT column2, SUM(column1) AS s, MIN(AVG(column1)) OVER (PARTITION by column3) AS min FROM t1 GROUP BY column2, column3";
        String translation = this.header(false) +
                "typedef TRtmp = TRtmp{tmp:signed<64>, gb1:bool, column2:string, s:signed<64>}\n" +
                "typedef Tagg = Tagg{tmp:signed<64>, s:signed<64>}\n" +
                "typedef TRtmp0 = TRtmp0{gb1:bool, min:signed<64>}\n" +
                "typedef Tagg1 = Tagg1{min:signed<64>}\n" +
                "typedef Ttmp = Ttmp{tmp:signed<64>, gb1:bool, column2:string, s:signed<64>, min:signed<64>}\n" +
                "typedef TRtmp2 = TRtmp2{column2:string, s:signed<64>, min:signed<64>}\n" +
                "function agg(g: Group<(string, bool), Tt1>):Tagg {\n" +
                "(var gb3, var gb4) = group_key(g);\n" +
                "(var avg = (64'sd0, 64'sd0): (signed<64>, signed<64>));\n" +
                "(var sum = 64'sd0: signed<64>);\n" +
                "(for ((i, _) in g) {\n" +
                "var v2 = i;\n" +
                "(var incr = v2.column1);\n" +
                "(avg = agg_avg_signed_R(avg, incr));\n" +
                "(var incr5 = v2.column1);\n" +
                "(sum = agg_sum_signed_R(sum, incr5))}\n" +
                ");\n" +
                "(Tagg{.tmp = avg_signed_R(avg),.s = sum})\n" +
                "}\n" +
                "\n" +
                "function agg1(g10: Group<bool, TRtmp>):Tagg1 {\n" +
                "(var gb9) = group_key(g10);\n" +
                "(var min13 = (true, 64'sd0): (bool, signed<64>));\n" +
                "(for ((i11, _) in g10) {\n" +
                "var v8 = i11;\n" +
                "(var incr12 = v8.tmp);\n" +
                "(min13 = agg_min_R(min13, incr12))}\n" +
                ");\n" +
                "(Tagg1{.min = min13.1})\n" +
                "}\n" +
                this.relations(false) +
                "relation Rtmp[TRtmp]\n" +
                "relation Roverinput[TRtmp]\n" +
                "relation Rtmp0[TRtmp0]\n" +
                "relation Rover[TRtmp0]\n" +
                "relation Rtmp2[TRtmp2]\n" +
                "output relation Rv1[TRtmp2]\n" +
                "Roverinput[v7] :- Rt1[v2],var gb3 = v2.column2,var gb4 = v2.column3," +
                "var groupResult = (v2).group_by((gb3, gb4)),var aggResult = agg(groupResult)," +
                "var v6 = TRtmp{.column2 = gb3,.gb1 = gb4,.tmp = aggResult.tmp,.s = aggResult.s},var v7 = v6.\n" +
                "Rover[v17] :- Roverinput[v8],var gb9 = v8.gb1,var groupResult16 = (v8).group_by((gb9))," +
                "var aggResult15 = agg1(groupResult16),var v14 = TRtmp0{.gb1 = gb9,.min = aggResult15.min},var v17 = v14.\n" +
                "Rv1[v22] :- Roverinput[v18],Rover[v19],(true and (v18.gb1 == v19.gb1))," +
                "var v20 = Ttmp{.tmp = v18.tmp,.gb1 = v18.gb1,.column2 = v18.column2,.s = v18.s,.min = v19.min}," +
                "var v21 = TRtmp2{.column2 = v18.column2,.s = v18.s,.min = v19.min},var v22 = v21.";
        this.testTranslation(query, translation);
    }

    @Test
    public void windowGroupByTest2() {
        String query = "CREATE VIEW v0 as SELECT DISTINCT column2, COUNT(column3) OVER (PARTITION BY column2) from t1 GROUP BY column2,column3";
        String translation = this.header(false) +
                "typedef TRtmp = TRtmp{tmp:bool, gb1:string, column2:string}\n" +
                "typedef Tagg = Tagg{}\n" +
                "typedef TRtmp0 = TRtmp0{gb1:string, count:signed<64>}\n" +
                "typedef Tagg1 = Tagg1{count:signed<64>}\n" +
                "typedef Ttmp = Ttmp{tmp:bool, gb1:string, column2:string, count:signed<64>}\n" +
                "typedef TRtmp2 = TRtmp2{column2:string, count:signed<64>}\n" +
                "function agg1(g9: Group<string, TRtmp>):Tagg1 {\n" +
                "(var gb8) = group_key(g9);\n" +
                "(var count11 = 64'sd0: signed<64>);\n" +
                "(for ((i10, _) in g9) {\n" +
                "var v7 = i10;\n" +
                "(var incr = v7.tmp);\n" +
                "(count11 = agg_count_R(count11, incr))}\n" +
                ");\n" +
                "(Tagg1{.count = count11})\n" +
                "}\n" +
                this.relations(false) +
                "relation Rtmp[TRtmp]\n" +
                "relation Roverinput[TRtmp]\n" +
                "relation Rtmp0[TRtmp0]\n" +
                "relation Rover[TRtmp0]\n" +
                "relation Rtmp2[TRtmp2]\n" +
                "output relation Rv0[TRtmp2]\n" +
                "Roverinput[v6] :- Rt1[v2],var gb3 = v2.column2,var gb4 = v2.column3," +
                "var groupResult = (v2).group_by((gb3, gb4))," +
                "var v5 = TRtmp{.gb1 = gb3,.column2 = gb3,.tmp = gb4},var v6 = v5.\n" +
                "Rover[v15] :- Roverinput[v7],var gb8 = v7.gb1,var groupResult14 = (v7).group_by((gb8))," +
                "var aggResult13 = agg1(groupResult14),var v12 = TRtmp0{.gb1 = gb8,.count = aggResult13.count},var v15 = v12.\n" +
                "Rv0[v20] :- Roverinput[v16],Rover[v17],(true and (v16.gb1 == v17.gb1))," +
                "var v18 = Ttmp{.tmp = v16.tmp,.gb1 = v16.gb1,.column2 = v16.column2,.count = v17.count}," +
                "var v19 = TRtmp2{.column2 = v16.column2,.count = v17.count},var v20 = v19.";
        this.testTranslation(query, translation);
    }

    @Test
    public void nestedAggTest() {
        String query = "SELECT column2, SUM(column1), SUM(SUM(column1)) OVER (PARTITION BY column3) FROM t1 GROUP BY column2, column3";
        String translation = this.header(false) +
                "typedef TRtmp = TRtmp{tmp:signed<64>, gb1:bool, column2:string, col:signed<64>}\n" +
                "typedef Tagg = Tagg{tmp:signed<64>, col:signed<64>}\n" +
                "typedef TRtmp0 = TRtmp0{gb1:bool, sum:signed<64>}\n" +
                "typedef Tagg1 = Tagg1{sum:signed<64>}\n" +
                "typedef Ttmp = Ttmp{tmp:signed<64>, gb1:bool, column2:string, col:signed<64>, sum:signed<64>}\n" +
                "typedef TRtmp2 = TRtmp2{column2:string, col:signed<64>, sum:signed<64>}\n" +
                "function agg(g: Group<(string, bool), Tt1>):Tagg {\n" +
                "(var gb3, var gb4) = group_key(g);\n" +
                "(var sum5 = 64'sd0: signed<64>);\n" +
                "(for ((i, _) in g) {\n" +
                "var v2 = i;\n" +
                "(var incr = v2.column1);\n" +
                "(sum5 = agg_sum_signed_R(sum5, incr))}\n" +
                ");\n" +
                "(Tagg{.tmp = sum5,.col = sum5})\n" +
                "}\n" +
                "\n" +
                "function agg1(g10: Group<bool, TRtmp>):Tagg1 {\n" +
                "(var gb9) = group_key(g10);\n" +
                "(var sum13 = 64'sd0: signed<64>);\n" +
                "(for ((i11, _) in g10) {\n" +
                "var v8 = i11;\n" +
                "(var incr12 = v8.tmp);\n" +
                "(sum13 = agg_sum_signed_R(sum13, incr12))}\n" +
                ");\n" +
                "(Tagg1{.sum = sum13})\n" +
                "}\n" +
                this.relations(false) +
                "relation Rtmp[TRtmp]\n" +
                "relation Roverinput[TRtmp]\n" +
                "relation Rtmp0[TRtmp0]\n" +
                "relation Rover[TRtmp0]\n" +
                "relation Rtmp2[TRtmp2]\n" +
                "Roverinput[v7] :- Rt1[v2],var gb3 = v2.column2,var gb4 = v2.column3,var groupResult = (v2).group_by((gb3, gb4))," +
                "var aggResult = agg(groupResult),var v6 = TRtmp{.column2 = gb3,.gb1 = gb4,.tmp = aggResult.tmp,.col = aggResult.col}," +
                "var v7 = v6.\n" +
                "Rover[v17] :- Roverinput[v8],var gb9 = v8.gb1,var groupResult16 = (v8).group_by((gb9))," +
                "var aggResult15 = agg1(groupResult16),var v14 = TRtmp0{.gb1 = gb9,.sum = aggResult15.sum},var v17 = v14.";
        this.testTranslation(query, translation);
    }
}
