package ddlog;

import org.junit.Test;

public class WindowTest extends BaseQueriesTest {
    @Test
    public void windowSimpleTest() {
        String query = "create view v1 as\n" +
                "select DISTINCT *, count(column3) over (partition by column2) as c2 from t1";
        String translation = this.header(false) +
                "typedef TRtmp = TRtmp{tmp:bool, gb:string}\n" +
                "typedef TRtmp0 = TRtmp0{gb:string, count:signed<64>}\n" +
                "typedef Tagg = Tagg{count:signed<64>}\n" +
                "typedef Ttmp = Ttmp{tmp:bool, gb:string, count:signed<64>}\n" +
                "typedef TRtmp1 = TRtmp1{tmp:bool, gb:string, count:signed<64>, c2:signed<64>}\n" +
                "function agg(g: Group<string, (Tt1, Tt1, TRtmp)>):Tagg {\n" +
                "(var gb4) = group_key(g);\n" +
                "(var count5 = 64'sd0: signed<64>);\n" +
                "(for (i in g) {\n" +
                "var v = i.0;\n" +
                "(var v0 = i.1);\n" +
                "(var v3 = i.2);\n" +
                "(var incr = v0.column3);\n" +
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
                "Roverinput[v2] :- Rt1[v0],var v1 = TRtmp{.tmp = v0.column3,.gb = v0.column2},var v2 = v1.\n" +
                "Rover[v7] :- Roverinput[v3],var gb4 = v3.gb,var aggResult = Aggregate((gb4), agg((v, v0, v3)))," +
                "var v6 = TRtmp0{.gb = gb4,.count = aggResult.count},var v7 = v6.\n" +
                "Rv1[v12] :- Roverinput[v8],Rover[v9],(true and (v8.gb == v9.gb))," +
                "var v10 = Ttmp{.tmp = v8.tmp,.gb = v8.gb,.count = v9.count}," +
                "var v11 = TRtmp1{.tmp = v10.tmp,.gb = v10.gb,.count = v10.count,.c2 = v9.count}," +
                "var v12 = v11.";
        this.testTranslation(query, translation);
    }

    @Test
    public void windowExpressionTest() {
        String query = "create view v1 as\n" +
                "select DISTINCT *, 3 + count(column3) over (partition by column2) as c2 from t1";
        String translation = "";
        this.testTranslation(query, translation);
    }

    @Test
    public void windowOfExpressionTest() {
        String query = "create view v1 as\n" +
                "select DISTINCT *, 3 + count(SUBSTR(column3, 3)) over (partition by column2) as c2 from t1";
        String translation = "";
        this.testTranslation(query, translation);
    }

    @Test
    public void windowTwoWindowsTest() {
        String query = "create view v1 as\n" +
                "select DISTINCT count(column3) over (partition by column2) + COUNT(column2) over (partition by column3) from t1";
        String translation = "";
        this.testTranslation(query, translation);
    }
}
