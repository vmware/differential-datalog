package ddlog;

import com.vmware.ddlog.translator.TranslationException;
import org.junit.Test;

/**
 * Test various CAST expressions
 */
public class CastTest extends BaseQueriesTest {
    @Test
    public void testCastIntToFloat() {
        String query = "create view v0 as SELECT DISTINCT CAST(t1.column1 AS FLOAT) FROM t1";
        String program = this.header(false) +
                "typedef TRtmp = TRtmp{col:float}\n" +
                this.relations(false) +
                "relation Rtmp[TRtmp]\n" +
                "output relation Rv0[TRtmp]\n" +
                "Rv0[v1] :- Rt1[v],var v0 = TRtmp{.col = v.column1 as float},var v1 = v0.";
        this.testTranslation(query, program);
    }

    @Test
    public void testCastIntToString() {
        String query = "create view v0 as SELECT DISTINCT CAST(t1.column1 AS VARCHAR) FROM t1";
        String program = this.header(false) +
                "typedef TRtmp = TRtmp{col:string}\n" +
                this.relations(false) +
                "relation Rtmp[TRtmp]\n" +
                "output relation Rv0[TRtmp]\n" +
                "Rv0[v1] :- Rt1[v],var v0 = TRtmp{.col = [|${v.column1}|]},var v1 = v0.";
        this.testTranslation(query, program);
    }

    @Test
    public void testCastStringToInt() {
        String query = "create view v0 as SELECT DISTINCT CAST(t1.column2 AS INT) FROM t1";
        String program = this.header(false) +
                "typedef TRtmp = TRtmp{col:signed<64>}\n" +
                this.relations(false) +
                "relation Rtmp[TRtmp]\n" +
                "output relation Rv0[TRtmp]\n" +
                "Rv0[v1] :- Rt1[v],var v0 = TRtmp{.col = option_unwrap_or_default(parse_dec_i64(v.column2))},var v1 = v0.";
        this.testTranslation(query, program);
    }

    @Test
    public void testCastStringToFloat() {
        String query = "create view v0 as SELECT DISTINCT CAST(t1.column2 AS REAL) FROM t1";
        String program = this.header(false) +
                "typedef TRtmp = TRtmp{col:double}\n" +
                this.relations(false) +
                "relation Rtmp[TRtmp]\n" +
                "output relation Rv0[TRtmp]\n" +
                "Rv0[v1] :- Rt1[v],var v0 = TRtmp{.col = result_unwrap_or_default(parse_d(v.column2))},var v1 = v0.";
        this.testTranslation(query, program);
    }

    @Test
    public void testCastStringToDate() {
        String query = "create view v0 as SELECT DISTINCT CAST(t1.column2 AS DATE) FROM t1";
        String program = this.header(false) +
                "typedef TRtmp = TRtmp{col:Date}\n" +
                this.relations(false) +
                "relation Rtmp[TRtmp]\n" +
                "output relation Rv0[TRtmp]\n" +
                "Rv0[v1] :- Rt1[v],var v0 = TRtmp{.col = result_unwrap_or_default(string2date(v.column2))},var v1 = v0.";
        this.testTranslation(query, program);
    }

    @Test
    public void testCastIntToDate() {
        String query = "create view v0 as SELECT DISTINCT CAST(t1.column1 AS DATE) FROM t1";
        String program = this.header(false) +
                "typedef TRtmp = TRtmp{col:Date}\n" +
                this.relations(false) +
                "relation Rtmp[TRtmp]\n" +
                "output relation Rv0[TRtmp]\n" +
                "Rv0[v1] :- Rt1[v],var v0 = TRtmp{.col = result_unwrap_or_default(string2date([|${v.column1}|]))},var v1 = v0.";
        this.testTranslation(query, program);
    }

    @Test
    public void testCastStringToDateTime() {
        String query = "create view v0 as SELECT DISTINCT CAST(t1.column2 AS TIMESTAMP) FROM t1";
        String program = this.header(false) +
                "typedef TRtmp = TRtmp{col:DateTime}\n" +
                this.relations(false) +
                "relation Rtmp[TRtmp]\n" +
                "output relation Rv0[TRtmp]\n" +
                "Rv0[v1] :- Rt1[v],var v0 = TRtmp{.col = result_unwrap_or_default(string2datetime(v.column2))},var v1 = v0.";
        this.testTranslation(query, program);
    }

    @Test
    public void testCastFloatToInt() {
        String query = "create view v0 as SELECT DISTINCT CAST(t1.column4 AS INTEGER) FROM t1";
        String program = this.header(false) +
                "typedef TRtmp = TRtmp{col:signed<64>}\n" +
                this.relations(false) +
                "relation Rtmp[TRtmp]\n" +
                "output relation Rv0[TRtmp]\n" +
                "Rv0[v1] :- Rt1[v],var v0 = TRtmp{.col = option_unwrap_or_default(int_from_d(v.column4)) as signed<64>},var v1 = v0.";
        this.testTranslation(query, program);
    }

    @Test
    public void testCastDateToString() {
        String query = "create view v0 as SELECT DISTINCT CAST(t3.d AS VARCHAR) FROM t3";
        String program = this.header(false) +
                "typedef TRtmp = TRtmp{col:string}\n" +
                this.relations(false) +
                "relation Rtmp[TRtmp]\n" +
                "output relation Rv0[TRtmp]\n" +
                "Rv0[v1] :- Rt3[v],var v0 = TRtmp{.col = [|${v.d}|]},var v1 = v0.";
        this.testTranslation(query, program);
    }

    @Test
    public void testCastBoolToInt() {
        String query = "create view v0 as SELECT DISTINCT CAST(t1.column3 AS INTEGER) FROM t1";
        String program = this.header(false) +
                "typedef TRtmp = TRtmp{col:signed<64>}\n" +
                this.relations(false) +
                "relation Rtmp[TRtmp]\n" +
                "output relation Rv0[TRtmp]\n" +
                "Rv0[v1] :- Rt1[v],var v0 = TRtmp{.col = if (v.column3) {\n64'sd1} else {\n64'sd0}},var v1 = v0.";
        this.testTranslation(query, program);
    }

    @Test
    public void testCastIntToBool() {
        String query = "create view v0 as SELECT DISTINCT CAST(t1.column1 AS BOOLEAN) FROM t1";
        String program = this.header(false) +
                "typedef TRtmp = TRtmp{col:bool}\n" +
                this.relations(false) +
                "relation Rtmp[TRtmp]\n" +
                "output relation Rv0[TRtmp]\n" +
                "Rv0[v1] :- Rt1[v],var v0 = TRtmp{.col = (v.column1 != 64'sd0)},var v1 = v0.";
        this.testTranslation(query, program);
    }

    @Test(expected = TranslationException.class)
    public void testCastDateToBool() {
        String query = "create view v0 as SELECT DISTINCT CAST(t3.d AS BOOLEAN) FROM t3";
        String program = "";
        this.testTranslation(query, program);
    }
}
