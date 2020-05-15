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
                "(for (i in g) {\n" +
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
                "Rover[v7] :- Roverinput[v3],var gb4 = v3.gb,var aggResult = Aggregate((gb4), agg((v3)))," +
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
                "(for (i in g) {\n" +
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
                "Rover[v7] :- Roverinput[v3],var gb4 = v3.gb,var aggResult = Aggregate((gb4), agg((v3)))," +
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
                "(for (i in g) {\n" +
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
                "Roverinput[v2] :- Rt1[v0],var v1 = TRtmp{.tmp = substr(v0.column2, 64'sd3, 64'sd3),.gb = v0.column3," +
                ".column1 = v0.column1,.column2 = v0.column2,.column3 = v0.column3,.column4 = v0.column4},var v2 = v1.\n" +
                "Rover[v7] :- Roverinput[v3],var gb4 = v3.gb,var aggResult = Aggregate((gb4), agg((v3)))," +
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
                "typedef TRtmp1 = TRtmp1{gb0:bool, count2:signed<64>}\n" +
                "typedef Tagg2 = Tagg2{count2:signed<64>}\n" +
                "typedef Ttmp = Ttmp{tmp:bool, tmp1:string, gb:string, gb0:bool, count:signed<64>}\n" +
                "typedef Ttmp3 = Ttmp3{tmp:bool, tmp1:string, gb:string, gb0:bool, count:signed<64>, count2:signed<64>}\n" +
                "typedef TRtmp4 = TRtmp4{x:signed<64>}\n" +
                "function agg(g: Group<string, TRtmp>):Tagg {\n" +
                "(var gb7) = group_key(g);\n" +
                "(var count8 = 64'sd0: signed<64>);\n" +
                "(for (i in g) {\n" +
                "var v6 = i;\n" +
                "(var incr = v6.tmp);\n" +
                "(count8 = agg_count_R(count8, incr))}\n" +
                ");\n" +
                "(Tagg{.count = count8})\n" +
                "}\n" +
                "\n" +
                "function agg2(g14: Group<bool, TRtmp>):Tagg2 {\n" +
                "(var gb13) = group_key(g14);\n" +
                "(var count17 = 64'sd0: signed<64>);\n" +
                "(for (i15 in g14) {\n" +
                "var v12 = i15;\n" +
                "(var incr16 = v12.tmp1);\n" +
                "(count17 = agg_count_R(count17, incr16))}\n" +
                ");\n" +
                "(Tagg2{.count2 = count17})\n" +
                "}\n" +
                this.relations(false) +
                "relation Rtmp[TRtmp]\n" +
                "relation Roverinput[TRtmp]\n" +
                "relation Rtmp0[TRtmp0]\n" +
                "relation Rover[TRtmp0]\n" +
                "relation Rtmp1[TRtmp1]\n" +
                "relation Rover11[TRtmp1]\n" +
                "relation Rtmp4[TRtmp4]\n" +
                "output relation Rv1[TRtmp4]\n" +
                "Roverinput[v5] :- Rt1[v3],var v4 = TRtmp{.tmp = v3.column3,.tmp1 = v3.column2,.gb = v3.column2,.gb0 = v3.column3}," +
                "var v5 = v4.\n" +
                "Rover[v10] :- Roverinput[v6],var gb7 = v6.gb,var aggResult = Aggregate((gb7), agg((v6)))," +
                "var v9 = TRtmp0{.gb = gb7,.count = aggResult.count},var v10 = v9.\n" +
                "Rover11[v20] :- Roverinput[v12],var gb13 = v12.gb0,var aggResult19 = Aggregate((gb13), agg2((v12)))," +
                "var v18 = TRtmp1{.gb0 = gb13,.count2 = aggResult19.count2},var v20 = v18.\n" +
                "Rv1[v27] :- Roverinput[v21],Rover[v22],(true and (v21.gb == v22.gb))," +
                "var v23 = Ttmp{.tmp = v21.tmp,.tmp1 = v21.tmp1,.gb = v21.gb,.gb0 = v21.gb0,.count = v22.count}," +
                "Rover11[v24],(true and (v23.gb0 == v24.gb0))," +
                "var v25 = Ttmp3{.tmp = v23.tmp,.tmp1 = v23.tmp1,.gb = v23.gb,.gb0 = v23.gb0,.count = v23.count,.count2 = v24.count2}," +
                "var v26 = TRtmp4{.x = (v22.count + v24.count2)},var v27 = v26.";
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
                "(for (i in g) {\n" +
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
                "(for (i11 in g10) {\n" +
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
                "var aggResult = Aggregate((gb3, gb4), agg((v2)))," +
                "var v6 = TRtmp{.column2 = gb3,.gb1 = gb4,.tmp = aggResult.tmp,.s = aggResult.s},var v7 = v6.\n" +
                "Rover[v16] :- Roverinput[v8],var gb9 = v8.gb1,var aggResult15 = Aggregate((gb9), agg1((v8)))," +
                "var v14 = TRtmp0{.gb1 = gb9,.min = aggResult15.min},var v16 = v14.\n" +
                "Rv1[v21] :- Roverinput[v17],Rover[v18],(true and (v17.gb1 == v18.gb1))," +
                "var v19 = Ttmp{.tmp = v17.tmp,.gb1 = v17.gb1,.column2 = v17.column2,.s = v17.s,.min = v18.min}," +
                "var v20 = TRtmp2{.column2 = v17.column2,.s = v17.s,.min = v18.min},var v21 = v20.";
        this.testTranslation(query, translation);
    }
}
