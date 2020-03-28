package ddlog;

import org.junit.Test;

public class SimpleQueriesTest extends BaseQueriesTest {
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
    public void testSelectWNull() {
        String query = "create view v0 as select distinct column1, column2 from t1";
        String program = this.header(true) +
            "typedef TRtmp = TRtmp{column1:Option<signed<64>>, column2:Option<string>}\n" +
            this.relations(true) +
            "relation Rtmp[TRtmp]\n" +
            "output relation Rv0[TRtmp]\n" +
            "Rv0[v1] :- Rt1[v],var v0 = TRtmp{.column1 = v.column1,.column2 = v.column2},var v1 = v0.";
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
        String query = "create view v0 as SELECT DISTINCT CASE WHEN column1 = 1 THEN 1 WHEN 1 < column1 THEN 2 ELSE 3 END FROM t1";
        String program =
                this.header(false) +
                        "typedef TRtmp = TRtmp{col:signed<64>}\n" +
                        this.relations(false) +
                        "relation Rtmp[TRtmp]\n" +
                        "output relation Rv0[TRtmp]\n" +
                        "Rv0[v1] :- Rt1[v],var v0 = TRtmp{.col = if (v.column1 == 64'sd1) {\n" +
                        "64'sd1} else {\n" +
                        "if (64'sd1 < v.column1) {\n" +
                        "64'sd2} else {\n" +
                        "64'sd3}}},var v1 = v0.";
        this.testTranslation(query, program);
    }

    @Test
    public void testWhenWNull() {
        String query = "create view v0 as SELECT DISTINCT CASE WHEN column1 = 1 THEN 1 WHEN 1 < column1 THEN 2 ELSE 3 END FROM t1";
        String program =
            this.header(true) +
                "typedef TRtmp = TRtmp{col:signed<64>}\n" +
                this.relations(true) +
                "relation Rtmp[TRtmp]\n" +
                "output relation Rv0[TRtmp]\n" +
                "Rv0[v1] :- Rt1[v],var v0 = TRtmp{.col = if unwrapBool(a_eq_NR(v.column1, 64'sd1)) {\n" +
                "64'sd1} else {\n" +
                "if unwrapBool(a_lt_RN(64'sd1, v.column1)) {\n" +
                "64'sd2} else {\n" +
                "64'sd3}}},var v1 = v0.";
        this.testTranslation(query, program, true);
    }

    @Test
    public void testAlias() {
        String query = "create view v0 as SELECT DISTINCT t2.column1 FROM t1 AS t2";
        String program = this.header(false) +
                "typedef TRtmp = TRtmp{column1:signed<64>}\n" +
                this.relations(false) +
                "relation Rtmp[TRtmp]\n" +
                "output relation Rv0[TRtmp]\n" +
                "Rv0[v1] :- Rt1[v],var v0 = TRtmp{.column1 = v.column1},var v1 = v0.";
        this.testTranslation(query, program);
    }

    @Test
    public void testAliasWNull() {
        String query = "create view v0 as SELECT DISTINCT t2.column1 FROM t1 AS t2";
        String program = this.header(true) +
            "typedef TRtmp = TRtmp{column1:Option<signed<64>>}\n" +
            this.relations(true) +
            "relation Rtmp[TRtmp]\n" +
            "output relation Rv0[TRtmp]\n" +
            "Rv0[v1] :- Rt1[v],var v0 = TRtmp{.column1 = v.column1},var v1 = v0.";
        this.testTranslation(query, program, true);
    }

    @Test
    public void testScope() {
        String query = "create view v0 as SELECT DISTINCT t1.column1 FROM t1";
        String program = this.header(false) +
                "typedef TRtmp = TRtmp{column1:signed<64>}\n" +
                this.relations(false) +
                "relation Rtmp[TRtmp]\n" +
                "output relation Rv0[TRtmp]\n" +
                "Rv0[v1] :- Rt1[v],var v0 = TRtmp{.column1 = v.column1},var v1 = v0.";
        this.testTranslation(query, program);
    }

    @Test
    public void testScopeWNulls() {
        String query = "create view v0 as SELECT DISTINCT t1.column1 FROM t1";
        String program = this.header(true) +
            "typedef TRtmp = TRtmp{column1:Option<signed<64>>}\n" +
            this.relations(true) +
            "relation Rtmp[TRtmp]\n" +
            "output relation Rv0[TRtmp]\n" +
            "Rv0[v1] :- Rt1[v],var v0 = TRtmp{.column1 = v.column1},var v1 = v0.";
        this.testTranslation(query, program, true);
    }
    @Test
    public void testAbs() {
        String query = "create view v0 as SELECT DISTINCT ABS(column1) FROM t1";
        String program = this.header(false) +
                "typedef TRtmp = TRtmp{col:signed<64>}\n" +
                this.relations(false) +
                "relation Rtmp[TRtmp]\n" +
                "output relation Rv0[TRtmp]\n" +
                "Rv0[v1] :- Rt1[v],var v0 = TRtmp{.col = abs(v.column1)},var v1 = v0.";
        this.testTranslation(query, program);
    }

    @Test
    public void testAbsWNull() {
        String query = "create view v0 as SELECT DISTINCT ABS(column1) FROM t1";
        String program = this.header(true) +
                "typedef TRtmp = TRtmp{col:Option<signed<64>>}\n" +
                this.relations(true) +
                "relation Rtmp[TRtmp]\n" +
                "output relation Rv0[TRtmp]\n" +
                "Rv0[v1] :- Rt1[v],var v0 = TRtmp{.col = abs_N(v.column1)},var v1 = v0.";
        this.testTranslation(query, program, true);
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
            "typedef TRtmp = TRtmp{column1:Option<signed<64>>, column2:Option<string>}\n" +
            this.relations(true) +
            "relation Rtmp[TRtmp]\n" +
            "output relation Rv0[TRtmp]\n" +
            "Rv0[v1] :- Rt1[v],unwrapBool(b_and_NN(a_lte_RN((- 64'sd1), v.column1), a_lte_NR(v.column1, 64'sd10))),var v0 = TRtmp{.column1 = v.column1,.column2 = v.column2},var v1 = v0.";
        this.testTranslation(query, program, true);
    }

    @Test
    public void testSubstr() {
        String query = "create view v0 as SELECT DISTINCT SUBSTR(column2, 3, 5) FROM t1";
        String program = this.header(false) +
                "typedef TRtmp = TRtmp{col:string}\n" +
                this.relations(false) +
                "relation Rtmp[TRtmp]\n" +
                "output relation Rv0[TRtmp]\n" +
                "Rv0[v1] :- Rt1[v],var v0 = TRtmp{.col = substr(v.column2, 64'sd3, 64'sd5)},var v1 = v0.";
        this.testTranslation(query, program);
    }

    @Test
    public void testSubstrWNull() {
        String query = "create view v0 as SELECT DISTINCT SUBSTR(column2, 3, 5) FROM t1";
        String program = this.header(true) +
            "typedef TRtmp = TRtmp{col:Option<string>}\n" +
            this.relations(true) +
            "relation Rtmp[TRtmp]\n" +
            "output relation Rv0[TRtmp]\n" +
            "Rv0[v1] :- Rt1[v],var v0 = TRtmp{.col = substr_N(v.column2, 64'sd3, 64'sd5)},var v1 = v0.";
        this.testTranslation(query, program, true);
    }

    @Test
    public void testSelectWithNulls() {
        String query = "create view v0 as select distinct column1, column2 from t1";
        String program =
                this.header(true) +
                "typedef TRtmp = TRtmp{column1:Option<signed<64>>, column2:Option<string>}\n" +
                this.relations(true) +
                "relation Rtmp[TRtmp]\n" +
                "output relation Rv0[TRtmp]\n" +
                "Rv0[v1] :- Rt1[v],var v0 = TRtmp{.column1 = v.column1,.column2 = v.column2},var v1 = v0.";
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
}