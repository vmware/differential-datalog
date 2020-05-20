package ddlog;

import com.vmware.ddlog.translator.TranslationException;
import org.junit.Test;

import java.util.Arrays;

public class SetTests extends BaseQueriesTest {
    @Test
    public void unionTest() {
        String query = "create view v0 as SELECT DISTINCT column1 FROM t1 UNION SELECT DISTINCT column1 FROM t2";
        String program = this.header(false) +
                this.relations(false) +
                "relation Rtmp[Tt2]\n" +
                "relation Runion[Tt2]\n" +
                "relation Rtmp0[Tt2]\n" +
                "output relation Rv0[Tt2]\n" +
                "Runion[v1] :- Rt1[v],var v0 = Tt2{.column1 = v.column1},var v1 = v0.\n" +
                "Runion[v1] :- Rt2[v2],var v3 = Tt2{.column1 = v2.column1},var v1 = v3.\n" +
                "Rv0[v4] :- Runion[v1],var v4 = v1.";
        this.testTranslation(query, program);
    }

    @Test
    public void intersectTest() {
        String query = "create view v0 as SELECT DISTINCT column1 FROM t1 INTERSECT SELECT DISTINCT column1 FROM t2";
        String program = this.header(false) +
                this.relations(false) +
                "relation Rtmp[Tt2]\n" +
                "relation Rintersect[Tt2]\n" +
                "relation Rtmp0[Tt2]\n" +
                "relation Rintersect1[Tt2]\n" +
                "output relation Rv0[Tt2]\n" +
                "Rintersect[v1] :- Rt1[v],var v0 = Tt2{.column1 = v.column1},var v1 = v0.\n" +
                "Rintersect1[v5] :- Rt2[v3],var v4 = Tt2{.column1 = v3.column1},var v5 = v4.\n" +
                "Rv0[v6] :- Rintersect[v2],Rintersect1[v2],var v6 = v2.";
        this.testTranslation(query, program);
    }

    @Test
    public void exceptTest() {
        String query = "create view v0 as SELECT DISTINCT column1 FROM t1 EXCEPT SELECT DISTINCT column1 FROM t2";
        String program = this.header(false) +
                this.relations(false) +
                "relation Rtmp[Tt2]\n" +
                "relation Rtmp0[Tt2]\n" +
                "relation Rexcept[Tt2]\n" +
                "output relation Rv0[Tt2]\n" +
                "Rexcept[v3] :- Rt2[v1],var v2 = Tt2{.column1 = v1.column1},var v3 = v2.\n" +
                "Rv0[v4] :- Rt1[v],var v0 = Tt2{.column1 = v.column1},var v3 = v0,not Rexcept[v3],var v4 = v0.";
        this.testTranslation(query, program);
    }

    @Test(expected = TranslationException.class)
    public void illegalUnionTest() {
        String query = "create view v0 as SELECT DISTINCT column2 FROM t1 UNION SELECT DISTINCT column1 FROM t2";
        this.testTranslation(query, "");
    }

    @Test
    public void unionWNullTest() {
        String query = "create view v0 as SELECT DISTINCT column1 FROM t1 UNION SELECT DISTINCT column1 FROM t2";
        String program = this.header(true) +
                this.relations(true) +
                "relation Rtmp[Tt2]\n" +
                "relation Runion[Tt2]\n" +
                "relation Rtmp0[Tt2]\n" +
                "output relation Rv0[Tt2]\n" +
                "Runion[v1] :- Rt1[v],var v0 = Tt2{.column1 = v.column1},var v1 = v0.\n" +
                "Runion[v1] :- Rt2[v2],var v3 = Tt2{.column1 = v2.column1},var v1 = v3.\n" +
                "Rv0[v4] :- Runion[v1],var v4 = v1.";
        this.testTranslation(query, program, true);
    }

    //@Test
    public void unionMixTest() {
        // TODO: this is currently broken.
        String query0 = "create table t4(column1 integer)";
        String query1 = "create view v0 as SELECT DISTINCT column1 FROM t1 UNION SELECT DISTINCT column1 FROM t4";
        String program = this.header(false) +
                this.relations(false) +
                "relation Rtmp[Tt2]\n" +
                "output relation Runion[Tt2]\n" +
                "relation Rtmp0[Tt2]\n" +
                "output relation Rv0[Tt2]\n" +
                "Runion[v1] :- Rt1[v],var v0 = Tt2{.column1 = v.column1},var v1 = v0.\n" +
                "Runion[v1] :- Rt2[v2],var v3 = Tt2{.column1 = v2.column1},var v1 = v3.\n" +
                "Rv0[v4] :- Runion[v1],var v4 = v1.";
        this.testTranslation(Arrays.asList(query0, query1), program, false);
    }
}
