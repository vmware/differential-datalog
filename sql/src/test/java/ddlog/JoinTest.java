package ddlog;

import org.junit.Test;

public class JoinTest extends BaseQueriesTest {
    @Test
    public void testCountJoin() {
        String query = "create view v0 as SELECT COUNT(t1.column2) FROM t1 JOIN t2 ON t1.column1 = t2.column1";
        String program = this.header(false) +
                "typedef Ttmp = Ttmp{column1:signed<64>, column2:string, column3:bool, column4:double, column10:signed<64>}\n" +
                "typedef TRtmp = TRtmp{col:signed<64>}\n" +
                "function agg(g: Group<(), (Tt1, Tt2)>):TRtmp {\n" +
                "var count = 64'sd0: signed<64>;\n" +
                "(for (i in g) {\n" +
                "var v = i.0;\n" +
                "(var v0 = i.1);\n" +
                "(var incr = v.column2);\n" +
                "(count = agg_count_R(count, incr))}\n" +
                ");\n" +
                "(TRtmp{.col = count})\n}\n" +
                this.relations(false) +
                "relation Rtmp[TRtmp]\n" +
                "output relation Rv0[TRtmp]\n" +
                "Rv0[v3] :- Rt1[v],Rt2[v0],(v.column1 == v0.column1),true," +
                "var v1 = Ttmp{.column1 = v.column1,.column2 = v.column2,.column3 = v.column3,.column4 = v.column4,.column10 = v0.column1}," +
                "var aggResult = Aggregate((), agg((v, v0))),var v2 = aggResult,var v3 = v2.";
        this.testTranslation(query, program);
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
}