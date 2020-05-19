package ddlog;

import com.vmware.ddlog.translator.TranslationException;
import org.junit.Test;

public class SetTests extends BaseQueriesTest {
    @Test
    public void unionTest() {
        String query = "create view v0 as SELECT DISTINCT column1 FROM t1 UNION SELECT DISTINCT column1 FROM t2";
        String program = this.header(false) +
                this.relations(false) +
                "relation Rtmp[Tt2]\n" +
                "output relation Runion[Tt2]\n" +
                "relation Rtmp0[Tt2]\n" +
                "output relation Rv0[Tt2]\n" +
                "Runion[v1] :- Rt1[v],var v0 = Tt2{.column1 = v.column1},var v1 = v0.\n" +
                "Runion[v1] :- Rt2[v2],var v3 = Tt2{.column1 = v2.column1},var v1 = v3.\n" +
                "Rv0[v4] :- Runion[v1],var v4 = v1.";
        this.testTranslation(query, program);
    }

    @Test(expected = TranslationException.class)
    public void illegalUnionTest() {
        String query = "create view v0 as SELECT DISTINCT column2 FROM t1 UNION SELECT DISTINCT column1 FROM t2";
        this.testTranslation(query, "");
    }

    @Test
    public void unionTestWNull() {
        String query = "create view v0 as SELECT DISTINCT column1 FROM t1 UNION SELECT DISTINCT column1 FROM t2";
        String program = this.header(true) +
                this.relations(true) +
                "relation Rtmp[Tt2]\n" +
                "output relation Runion[Tt2]\n" +
                "relation Rtmp0[Tt2]\n" +
                "output relation Rv0[Tt2]\n" +
                "Runion[v1] :- Rt1[v],var v0 = Tt2{.column1 = v.column1},var v1 = v0.\n" +
                "Runion[v1] :- Rt2[v2],var v3 = Tt2{.column1 = v2.column1},var v1 = v3.\n" +
                "Rv0[v4] :- Runion[v1],var v4 = v1.";
        this.testTranslation(query, program, true);
    }
}
