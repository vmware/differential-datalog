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

public class NestedTest extends BaseQueriesTest {
    @Test
    public void nestedWhereTest() {
        String query = "create view v0 as SELECT DISTINCT column2 FROM t1 WHERE column1 in " +
                "(SELECT DISTINCT column1 FROM t2)";
        String program = this.header(false) +
                "typedef TRtmp0 = TRtmp0{column2:istring}\n" +
                this.relations(false) +
                "relation Rsub[TRt2]\n" +
                "output relation Rv0[TRtmp0]\n" +
                "Rsub[v2] :- Rt2[v0],var v1 = TRt2{.column1 = v0.column1},var v2 = v1.\n" +
                "Rv0[v4] :- Rt1[v],Rsub[TRt2{.column1 = v.column1}],var v3 = TRtmp0{.column2 = v.column2},var v4 = v3.";
        this.testTranslation(query, program);
    }

    @Test
    public void nestedWhere1Test() {
        String query = "create view v0 as SELECT DISTINCT column1 FROM t1 WHERE column1 in " +
                "(SELECT DISTINCT column1 FROM t2)";
        String program = this.header(false) +
                this.relations(false) +
                "relation Rsub[TRt2]\n" +
                "output relation Rv0[TRt2]\n" +
                "Rsub[v2] :- Rt2[v0],var v1 = TRt2{.column1 = v0.column1},var v2 = v1.\n" +
                "Rv0[v4] :- Rt1[v],Rsub[TRt2{.column1 = v.column1}],var v3 = TRt2{.column1 = v.column1},var v4 = v3.";
        this.testTranslation(query, program);
    }

    @Test
    public void nestedWhere2Test() {
        String query = "create view v0 as SELECT DISTINCT column1 FROM t1 WHERE " +
        "column1 IN (SELECT DISTINCT column1 FROM t2) AND column1 IN (SELECT DISTINCT column1 FROM t1)";
        String program = this.header(false) +
                this.relations(false) +
                "relation Rsub[TRt2]\n" +
                "relation Rsub0[TRt2]\n" +
                "output relation Rv0[TRt2]\n" +
                "Rsub[v2] :- Rt2[v0],var v1 = TRt2{.column1 = v0.column1},var v2 = v1.\n" +
                "Rsub0[v5] :- Rt1[v3],var v4 = TRt2{.column1 = v3.column1},var v5 = v4.\n" +
                "Rv0[v7] :- Rt1[v],Rsub[TRt2{.column1 = v.column1}],Rsub0[TRt2{.column1 = v.column1}],var v6 = TRt2{.column1 = v.column1},var v7 = v6.";
        this.testTranslation(query, program);
    }

    @Test
    public void nestedWhere3Test() {
        String query = "create view v0 as SELECT DISTINCT column1 FROM t1 WHERE " +
                "column1 IN (SELECT DISTINCT column1 FROM t2) AND column1 IN (SELECT DISTINCT column1 FROM t1 WHERE column1 > 0)";
        String program = this.header(false) +
                this.relations(false) +
                "relation Rsub[TRt2]\n" +
                "relation Rsub0[TRt2]\n" +
                "output relation Rv0[TRt2]\n" +
                "Rsub[v2] :- Rt2[v0],var v1 = TRt2{.column1 = v0.column1},var v2 = v1.\n" +
                "Rsub0[v5] :- Rt1[v3],(v3.column1 > 64'sd0),var v4 = TRt2{.column1 = v3.column1},var v5 = v4.\n" +
                "Rv0[v7] :- Rt1[v],Rsub[TRt2{.column1 = v.column1}],Rsub0[TRt2{.column1 = v.column1}],var v6 = TRt2{.column1 = v.column1},var v7 = v6.";
        this.testTranslation(query, program);
    }

    /*
    This will require more work.
    @Test
    public void nestedWhere3Test() {
        String query = "create view v0 as SELECT DISTINCT column1 FROM t1 WHERE " +
                "column1 IN (SELECT DISTINCT column1 FROM t2) OR column2 IN (SELECT DISTINCT column2 FROM t1)";
        String program = this.header(false) +
                this.relations(false) +
                "relation Rtmp[Tt2]\n" +
                "relation Rsub[Tt2]\n" +
                "relation Rtmp0[Tt2]\n" +
                "output relation Rv0[Tt2]\n" +
                "Rsub[v2] :- Rt2[v0],var v1 = Tt2{.column1 = v0.column1},var v2 = v1.\n" +
                "Rv0[v4] :- Rt1[v],Rsub[Tt2{v.column1}],var v3 = Tt2{.column1 = v.column1},var v4 = v3.";
        this.testTranslation(query, program);
    }
     */
}
