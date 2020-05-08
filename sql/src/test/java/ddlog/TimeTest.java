package ddlog;

import org.junit.Test;

/**
 * Tests using time, date, datetime.
 */
public class TimeTest extends BaseQueriesTest {
    @Test
    public void testYearExtract() {
        String query = "create view v0 as SELECT DISTINCT EXTRACT(YEAR FROM t3.d) FROM t3";
        String program = this.header(false) +
                "typedef TRtmp = TRtmp{col:signed<64>}\n" +
                this.relations(false) +
                "relation Rtmp[TRtmp]\n" +
                "output relation Rv0[TRtmp]\n" +
                "Rv0[v1] :- Rt3[v],var v0 = TRtmp{.col = sql_extract_year(v.d)},var v1 = v0.";
        this.testTranslation(query, program);
    }

    @Test
    public void testMonthExtract() {
        String query = "create view v0 as SELECT DISTINCT EXTRACT(MONTH FROM t3.d) FROM t3";
        String program = this.header(false) +
                "typedef TRtmp = TRtmp{col:signed<64>}\n" +
                this.relations(false) +
                "relation Rtmp[TRtmp]\n" +
                "output relation Rv0[TRtmp]\n" +
                "Rv0[v1] :- Rt3[v],var v0 = TRtmp{.col = sql_extract_month(v.d)},var v1 = v0.";
        this.testTranslation(query, program);
    }

    @Test
    public void testHourExtract() {
        String query = "create view v0 as SELECT DISTINCT EXTRACT(HOUR FROM t3.t) FROM t3";
        String program = this.header(false) +
                "typedef TRtmp = TRtmp{col:signed<64>}\n" +
                this.relations(false) +
                "relation Rtmp[TRtmp]\n" +
                "output relation Rv0[TRtmp]\n" +
                "Rv0[v1] :- Rt3[v],var v0 = TRtmp{.col = sql_extract_hour(v.t)},var v1 = v0.";
        this.testTranslation(query, program);
    }
}
