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

import com.vmware.ddlog.translator.TranslationException;
import org.junit.Test;

/**
 * Test various CAST expressions
 */
public class CastTest extends BaseQueriesTest {
    @Test
    public void testCastIntToFloat() {
        String query = "create view v0 as SELECT DISTINCT CAST(t1.column1 AS FLOAT) AS f FROM t1";
        String program = this.header(false) +
                "typedef TRtmp = TRtmp{f:float}\n" +
                this.relations(false) +
                "relation Rtmp[TRtmp]\n" +
                "output relation Rv0[TRtmp]\n" +
                "Rv0[v1] :- Rt1[v],var v0 = TRtmp{.f = v.column1 as float},var v1 = v0.";
        this.testTranslation(query, program);
    }

    @Test
    public void testCastIntToString() {
        String query = "create view v0 as SELECT DISTINCT CAST(t1.column1 AS VARCHAR) AS s FROM t1";
        String program = this.header(false) +
                "typedef TRtmp = TRtmp{s:string}\n" +
                this.relations(false) +
                "relation Rtmp[TRtmp]\n" +
                "output relation Rv0[TRtmp]\n" +
                "Rv0[v1] :- Rt1[v],var v0 = TRtmp{.s = [|${v.column1}|]},var v1 = v0.";
        this.testTranslation(query, program);
    }

    @Test
    public void testCastStringToInt() {
        String query = "create view v0 as SELECT DISTINCT CAST(t1.column2 AS INT) AS i FROM t1";
        String program = this.header(false) +
                "typedef TRtmp = TRtmp{i:signed<64>}\n" +
                this.relations(false) +
                "relation Rtmp[TRtmp]\n" +
                "output relation Rv0[TRtmp]\n" +
                "Rv0[v1] :- Rt1[v],var v0 = TRtmp{.i = option_unwrap_or_default(parse_dec_i64(v.column2))},var v1 = v0.";
        this.testTranslation(query, program);
    }

    @Test
    public void testCastStringToFloat() {
        String query = "create view v0 as SELECT DISTINCT CAST(t1.column2 AS REAL) AS d FROM t1";
        String program = this.header(false) +
                "typedef TRtmp = TRtmp{d:double}\n" +
                this.relations(false) +
                "relation Rtmp[TRtmp]\n" +
                "output relation Rv0[TRtmp]\n" +
                "Rv0[v1] :- Rt1[v],var v0 = TRtmp{.d = result_unwrap_or_default(parse_d(v.column2))},var v1 = v0.";
        this.testTranslation(query, program);
    }

    @Test
    public void testCastStringToDate() {
        String query = "create view v0 as SELECT DISTINCT CAST(t1.column2 AS DATE) AS d FROM t1";
        String program = this.header(false) +
                "typedef TRtmp = TRtmp{d:Date}\n" +
                this.relations(false) +
                "relation Rtmp[TRtmp]\n" +
                "output relation Rv0[TRtmp]\n" +
                "Rv0[v1] :- Rt1[v],var v0 = TRtmp{.d = result_unwrap_or_default(string2date(v.column2))},var v1 = v0.";
        this.testTranslation(query, program);
    }

    @Test
    public void testCastIntToDate() {
        String query = "create view v0 as SELECT DISTINCT CAST(t1.column1 AS DATE) AS d FROM t1";
        String program = this.header(false) +
                "typedef TRtmp = TRtmp{d:Date}\n" +
                this.relations(false) +
                "relation Rtmp[TRtmp]\n" +
                "output relation Rv0[TRtmp]\n" +
                "Rv0[v1] :- Rt1[v],var v0 = TRtmp{.d = result_unwrap_or_default(string2date([|${v.column1}|]))},var v1 = v0.";
        this.testTranslation(query, program);
    }

    @Test
    public void testCastStringToDateTime() {
        String query = "create view v0 as SELECT DISTINCT CAST(t1.column2 AS TIMESTAMP) AS t FROM t1";
        String program = this.header(false) +
                "typedef TRtmp = TRtmp{t:DateTime}\n" +
                this.relations(false) +
                "relation Rtmp[TRtmp]\n" +
                "output relation Rv0[TRtmp]\n" +
                "Rv0[v1] :- Rt1[v],var v0 = TRtmp{.t = result_unwrap_or_default(string2datetime(v.column2))},var v1 = v0.";
        this.testTranslation(query, program);
    }

    @Test
    public void testCastFloatToInt() {
        String query = "create view v0 as SELECT DISTINCT CAST(t1.column4 AS INTEGER) AS i FROM t1";
        String program = this.header(false) +
                "typedef TRtmp = TRtmp{i:signed<64>}\n" +
                this.relations(false) +
                "relation Rtmp[TRtmp]\n" +
                "output relation Rv0[TRtmp]\n" +
                "Rv0[v1] :- Rt1[v],var v0 = TRtmp{.i = option_unwrap_or_default(int_from_d(v.column4)) as signed<64>},var v1 = v0.";
        this.testTranslation(query, program);
    }

    @Test
    public void testCastDateToString() {
        String query = "create view v0 as SELECT DISTINCT CAST(t3.d AS VARCHAR) AS s FROM t3";
        String program = this.header(false) +
                "typedef TRtmp = TRtmp{s:string}\n" +
                this.relations(false) +
                "relation Rtmp[TRtmp]\n" +
                "output relation Rv0[TRtmp]\n" +
                "Rv0[v1] :- Rt3[v],var v0 = TRtmp{.s = [|${v.d}|]},var v1 = v0.";
        this.testTranslation(query, program);
    }

    @Test
    public void testCastBoolToInt() {
        String query = "create view v0 as SELECT DISTINCT CAST(t1.column3 AS INTEGER) AS i FROM t1";
        String program = this.header(false) +
                "typedef TRtmp = TRtmp{i:signed<64>}\n" +
                this.relations(false) +
                "relation Rtmp[TRtmp]\n" +
                "output relation Rv0[TRtmp]\n" +
                "Rv0[v1] :- Rt1[v],var v0 = TRtmp{.i = if (v.column3) {\n64'sd1} else {\n64'sd0}},var v1 = v0.";
        this.testTranslation(query, program);
    }

    @Test
    public void testCastIntToBool() {
        String query = "create view v0 as SELECT DISTINCT CAST(t1.column1 AS BOOLEAN) AS b FROM t1";
        String program = this.header(false) +
                "typedef TRtmp = TRtmp{b:bool}\n" +
                this.relations(false) +
                "relation Rtmp[TRtmp]\n" +
                "output relation Rv0[TRtmp]\n" +
                "Rv0[v1] :- Rt1[v],var v0 = TRtmp{.b = (v.column1 != 64'sd0)},var v1 = v0.";
        this.testTranslation(query, program);
    }

    @Test(expected = TranslationException.class)
    public void testCastDateToBool() {
        String query = "create view v0 as SELECT DISTINCT CAST(t3.d AS BOOLEAN) AS b FROM t3";
        String program = "";
        this.testTranslation(query, program);
    }
    
    @Test
    public void testCastIntToFloatWithNull() {
        String query = "create view v0 as SELECT DISTINCT CAST(t1.column1 AS FLOAT) AS f FROM t1";
        String program = this.header(true) +
                "typedef TRtmp = TRtmp{f:Option<float>}\n" +
                this.relations(true) +
                "relation Rtmp[TRtmp]\n" +
                "output relation Rv0[TRtmp]\n" +
                "Rv0[v1] :- Rt1[v],var v0 = TRtmp{.f = match(v.column1) {None{}: Option<signed<64>> -> None{}: Option<float>,\n" +
                "Some{.x = var x} -> Some{.x = x as float}\n" +
                "}},var v1 = v0.";
        this.testTranslation(query, program, true);
    }

    @Test
    public void testCastIntToStringWithNull() {
        String query = "create view v0 as SELECT DISTINCT CAST(t1.column1 AS VARCHAR) AS s FROM t1";
        String program = this.header(true) +
                "typedef TRtmp = TRtmp{s:Option<string>}\n" +
                this.relations(true) +
                "relation Rtmp[TRtmp]\n" +
                "output relation Rv0[TRtmp]\n" +
                "Rv0[v1] :- Rt1[v],var v0 = TRtmp{.s = match(v.column1) {None{}: Option<signed<64>> -> None{}: Option<string>,\n" +
                "Some{.x = var x} -> Some{.x = [|${x}|]}\n" +
                "}},var v1 = v0.";
        this.testTranslation(query, program, true);
    }

    @Test
    public void testCastStringToIntWithNull() {
        String query = "create view v0 as SELECT DISTINCT CAST(t1.column2 AS INT) AS i FROM t1";
        String program = this.header(true) +
                "typedef TRtmp = TRtmp{i:Option<signed<64>>}\n" +
                this.relations(true) +
                "relation Rtmp[TRtmp]\n" +
                "output relation Rv0[TRtmp]\n" +
                "Rv0[v1] :- Rt1[v],var v0 = TRtmp{.i = match(v.column2) {None{}: Option<string> -> None{}: Option<signed<64>>,\n" +
                "Some{.x = var x} -> Some{.x = option_unwrap_or_default(parse_dec_i64(x))}\n" +
                "}},var v1 = v0.";
        this.testTranslation(query, program, true);
    }

    @Test
    public void testCastStringToFloatWithNull() {
        String query = "create view v0 as SELECT DISTINCT CAST(t1.column2 AS REAL) AS d FROM t1";
        String program = this.header(true) +
                "typedef TRtmp = TRtmp{d:Option<double>}\n" +
                this.relations(true) +
                "relation Rtmp[TRtmp]\n" +
                "output relation Rv0[TRtmp]\n" +
                "Rv0[v1] :- Rt1[v],var v0 = TRtmp{.d = match(v.column2) {None{}: Option<string> -> None{}: Option<double>,\n" +
                "Some{.x = var x} -> Some{.x = result_unwrap_or_default(parse_d(x))}\n" +
                "}},var v1 = v0.";
        this.testTranslation(query, program, true);
    }

    @Test
    public void testCastStringToDateWithNull() {
        String query = "create view v0 as SELECT DISTINCT CAST(t1.column2 AS DATE) AS d FROM t1";
        String program = this.header(true) +
                "typedef TRtmp = TRtmp{d:Option<Date>}\n" +
                this.relations(true) +
                "relation Rtmp[TRtmp]\n" +
                "output relation Rv0[TRtmp]\n" +
                "Rv0[v1] :- Rt1[v],var v0 = TRtmp{.d = match(v.column2) {None{}: Option<string> -> None{}: Option<Date>,\n" +
                "Some{.x = var x} -> Some{.x = result_unwrap_or_default(string2date(x))}\n" +
                "}},var v1 = v0.";
        this.testTranslation(query, program, true);
    }

    @Test
    public void testCastIntToDateWithNull() {
        String query = "create view v0 as SELECT DISTINCT CAST(t1.column1 AS DATE) AS d FROM t1";
        String program = this.header(true) +
                "typedef TRtmp = TRtmp{d:Option<Date>}\n" +
                this.relations(true) +
                "relation Rtmp[TRtmp]\n" +
                "output relation Rv0[TRtmp]\n" +
                "Rv0[v1] :- Rt1[v],var v0 = TRtmp{.d = match(v.column1) {None{}: Option<signed<64>> -> None{}: Option<Date>,\n" +
                "Some{.x = var x} -> Some{.x = result_unwrap_or_default(string2date([|${v.column1}|]))}\n" +
                "}},var v1 = v0.";
        this.testTranslation(query, program, true);
    }

    @Test
    public void testCastStringToDateTimeWithNull() {
        String query = "create view v0 as SELECT DISTINCT CAST(t1.column2 AS TIMESTAMP) AS t FROM t1";
        String program = this.header(true) +
                "typedef TRtmp = TRtmp{t:Option<DateTime>}\n" +
                this.relations(true) +
                "relation Rtmp[TRtmp]\n" +
                "output relation Rv0[TRtmp]\n" +
                "Rv0[v1] :- Rt1[v],var v0 = TRtmp{.t = match(v.column2) {None{}: Option<string> -> None{}: Option<DateTime>,\n" +
                "Some{.x = var x} -> Some{.x = result_unwrap_or_default(string2datetime(x))}\n" +
                "}},var v1 = v0.";
        this.testTranslation(query, program, true);
    }

    @Test
    public void testCastFloatToIntWithNull() {
        String query = "create view v0 as SELECT DISTINCT CAST(t1.column4 AS INTEGER) AS i FROM t1";
        String program = this.header(true) +
                "typedef TRtmp = TRtmp{i:Option<signed<64>>}\n" +
                this.relations(true) +
                "relation Rtmp[TRtmp]\n" +
                "output relation Rv0[TRtmp]\n" +
                "Rv0[v1] :- Rt1[v],var v0 = TRtmp{.i = match(v.column4) {None{}: Option<double> -> None{}: Option<signed<64>>,\n" +
                "Some{.x = var x} -> Some{.x = option_unwrap_or_default(int_from_d(x)) as signed<64>}\n" +
                "}},var v1 = v0.";
        this.testTranslation(query, program, true);
    }

    @Test
    public void testCastDateToStringWithNull() {
        String query = "create view v0 as SELECT DISTINCT CAST(t3.d AS VARCHAR) AS s FROM t3";
        String program = this.header(true) +
                "typedef TRtmp = TRtmp{s:Option<string>}\n" +
                this.relations(true) +
                "relation Rtmp[TRtmp]\n" +
                "output relation Rv0[TRtmp]\n" +
                "Rv0[v1] :- Rt3[v],var v0 = TRtmp{.s = match(v.d) {None{}: Option<Date> -> None{}: Option<string>,\n" +
                "Some{.x = var x} -> Some{.x = [|${x}|]}\n" +
                "}},var v1 = v0.";
        this.testTranslation(query, program, true);
    }

    @Test
    public void testCastBoolToIntWithNull() {
        String query = "create view v0 as SELECT DISTINCT CAST(t1.column3 AS INTEGER) AS i FROM t1";
        String program = this.header(true) +
                "typedef TRtmp = TRtmp{i:Option<signed<64>>}\n" +
                this.relations(true) +
                "relation Rtmp[TRtmp]\n" +
                "output relation Rv0[TRtmp]\n" +
                "Rv0[v1] :- Rt1[v],var v0 = TRtmp{.i = match(v.column3) {None{}: Option<bool> -> None{}: Option<signed<64>>,\n" +
                "Some{.x = var x} -> Some{.x = if (x) {\n" +
                "64'sd1} else {\n" +
                "64'sd0}}\n" +
                "}},var v1 = v0.";
        this.testTranslation(query, program, true);
    }

    @Test
    public void testCastIntToBoolWithNull() {
        String query = "create view v0 as SELECT DISTINCT CAST(t1.column1 AS BOOLEAN) AS b FROM t1";
        String program = this.header(true) +
                "typedef TRtmp = TRtmp{b:Option<bool>}\n" +
                this.relations(true) +
                "relation Rtmp[TRtmp]\n" +
                "output relation Rv0[TRtmp]\n" +
                "Rv0[v1] :- Rt1[v],var v0 = TRtmp{.b = match(v.column1) {None{}: Option<signed<64>> -> None{}: Option<bool>,\n" +
                "Some{.x = var x} -> Some{.x = (x != 64'sd0)}\n" +
                "}},var v1 = v0.";
        this.testTranslation(query, program, true);
    }

    @Test(expected = TranslationException.class)
    public void testCastDateToBoolWithNull() {
        String query = "create view v0 as SELECT DISTINCT CAST(t3.d AS BOOLEAN) AS b FROM t3";
        String program = "";
        this.testTranslation(query, program, true);
    }
}
