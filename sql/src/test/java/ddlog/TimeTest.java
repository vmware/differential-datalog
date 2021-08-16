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

/**
 * Tests using time, date, datetime.
 */
public class TimeTest extends BaseQueriesTest {
    @Test
    public void testYearExtract() {
        String query = "create view v0 as SELECT DISTINCT EXTRACT(YEAR FROM t3.d) AS y FROM t3";
        String program = this.header(false) +
                "typedef TRtmp = TRtmp{y:signed<64>}\n" +
                this.relations(false) +
                "relation Rtmp[TRtmp]\n" +
                "output relation Rv0[TRtmp]\n" +
                "Rv0[v1] :- Rt3[v],var v0 = TRtmp{.y = sql_extract_year(v.d)},var v1 = v0.";
        this.testTranslation(query, program);
    }

    @Test
    public void testMonthExtract() {
        String query = "create view v0 as SELECT DISTINCT EXTRACT(MONTH FROM t3.d) AS m FROM t3";
        String program = this.header(false) +
                "typedef TRtmp = TRtmp{m:signed<64>}\n" +
                this.relations(false) +
                "relation Rtmp[TRtmp]\n" +
                "output relation Rv0[TRtmp]\n" +
                "Rv0[v1] :- Rt3[v],var v0 = TRtmp{.m = sql_extract_month(v.d)},var v1 = v0.";
        this.testTranslation(query, program);
    }

    @Test
    public void testHourExtract() {
        String query = "create view v0 as SELECT DISTINCT EXTRACT(HOUR FROM t3.t) AS h FROM t3";
        String program = this.header(false) +
                "typedef TRtmp = TRtmp{h:signed<64>}\n" +
                this.relations(false) +
                "relation Rtmp[TRtmp]\n" +
                "output relation Rv0[TRtmp]\n" +
                "Rv0[v1] :- Rt3[v],var v0 = TRtmp{.h = sql_extract_hour(v.t)},var v1 = v0.";
        this.testTranslation(query, program);
    }
}
