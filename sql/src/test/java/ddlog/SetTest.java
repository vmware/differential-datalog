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

public class SetTest extends BaseQueriesTest {
    @Test
    public void unionTest() {
        String query = "create view v0 as SELECT DISTINCT column1 FROM t1 UNION SELECT DISTINCT column1 FROM t2";
        String program = this.header(false) +
                this.relations(false) +
                "relation Runion[TRt2]\n" +
                "output relation Rv0[TRt2]\n" +
                "Runion[v3] :- Rt1[v],var v0 = TRt2{.column1 = v.column1},var v3 = v0.\n" +
                "Runion[v3] :- Rt2[v1],var v2 = TRt2{.column1 = v1.column1},var v3 = v2.\n" +
                "Rv0[v4] :- Runion[v3],var v4 = v3.";
        this.testTranslation(query, program);
    }

    @Test
    public void unionMixTest() {
        String query = "create view v0 as SELECT DISTINCT column1 FROM t1 UNION SELECT DISTINCT column1 FROM t4";
        String program = this.header(false) +
                "typedef TRtmp0 = TRtmp0{column1:Option<signed<64>>}\n" +
                this.relations(false) +
                "relation Rsource[TRt2]\n" +
                "relation Runion[TRtmp0]\n" +
                "output relation Rv0[TRtmp0]\n" +
                "Rsource[v3] :- Rt1[v],var v0 = TRt2{.column1 = v.column1},var v3 = v0.\n" +
                "Runion[v4] :- Rsource[v3],var v0 = TRtmp0{.column1 = Some{.x = v3.column1}},var v4 = v0.\n" +
                "Runion[v4] :- Rt4[v1],var v2 = TRtmp0{.column1 = v1.column1},var v4 = v2.\n" +
                "Rv0[v5] :- Runion[v4],var v5 = v4.";
        this.testTranslation(query, program);
    }

    @Test
    public void intersectTest() {
        String query = "create view v0 as SELECT DISTINCT column1 FROM t1 INTERSECT SELECT DISTINCT column1 FROM t2";
        String program = this.header(false) +
                this.relations(false) +
                "relation Rintersect[TRt2]\n" +
                "relation Rintersect0[TRt2]\n" +
                "output relation Rv0[TRt2]\n" +
                "Rintersect[v3] :- Rt1[v],var v0 = TRt2{.column1 = v.column1},var v3 = v0.\n" +
                "Rintersect0[v5] :- Rt2[v1],var v2 = TRt2{.column1 = v1.column1},var v5 = v2.\n" +
                "Rv0[v6] :- Rintersect[v4],Rintersect0[v4],var v6 = v4.";
        this.testTranslation(query, program);
    }

    @Test
    public void intersectMixTest() {
        String query = "create view v0 as SELECT DISTINCT column1 FROM t1 INTERSECT SELECT DISTINCT column1 FROM t4";
        String program = this.header(false) +
                "typedef TRtmp0 = TRtmp0{column1:Option<signed<64>>}\n" +
                this.relations(false) +
                "relation Rsource[TRt2]\n" +
                "relation Rintersect[TRtmp0]\n" +
                "relation Rintersect0[TRtmp0]\n" +
                "output relation Rv0[TRtmp0]\n" +
                "Rsource[v3] :- Rt1[v],var v0 = TRt2{.column1 = v.column1},var v3 = v0.\n" +
                "Rintersect[v4] :- Rsource[v3],var v0 = TRtmp0{.column1 = Some{.x = v3.column1}},var v4 = v0.\n" +
                "Rintersect0[v6] :- Rt4[v1],var v2 = TRtmp0{.column1 = v1.column1},var v6 = v2.\n" +
                "Rv0[v7] :- Rintersect[v5],Rintersect0[v5],var v7 = v5.";
        this.testTranslation(query, program);
    }

    @Test
    public void exceptTest() {
        String query = "create view v0 as SELECT DISTINCT column1 FROM t1 EXCEPT SELECT DISTINCT column1 FROM t2";
        String program = this.header(false) +
                this.relations(false) +
                "relation Rexcept[TRt2]\n" +
                "output relation Rv0[TRt2]\n" +
                "Rexcept[v3] :- Rt2[v1],var v2 = TRt2{.column1 = v1.column1},var v3 = v2.\n" +
                "Rv0[v4] :- Rt1[v],var v0 = TRt2{.column1 = v.column1},var v3 = v0,not Rexcept[v3],var v4 = v0.";
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
                "relation Runion[TRt2]\n" +
                "output relation Rv0[TRt2]\n" +
                "Runion[v3] :- Rt1[v],var v0 = TRt2{.column1 = v.column1},var v3 = v0.\n" +
                "Runion[v3] :- Rt2[v1],var v2 = TRt2{.column1 = v1.column1},var v3 = v2.\n" +
                "Rv0[v4] :- Runion[v3],var v4 = v3.";
        this.testTranslation(query, program, true);
    }

    @Test
    public void exceptMixTest() {
        String query = "create view v0 as SELECT DISTINCT column1 FROM t1 EXCEPT SELECT DISTINCT column1 FROM t4";
        String program = this.header(false) +
                "typedef TRtmp0 = TRtmp0{column1:Option<signed<64>>}\n" +
                this.relations(false) +
                "relation Rsource[TRt2]\n" +
                "relation Rexcept[TRtmp0]\n" +
                "output relation Rv0[TRtmp0]\n" +
                "Rsource[v3] :- Rt1[v],var v0 = TRt2{.column1 = v.column1},var v3 = v0.\n" +
                "Rexcept[v4] :- Rt4[v1],var v2 = TRtmp0{.column1 = v1.column1},var v4 = v2.\n" +
                "Rv0[v5] :- Rsource[v3],var v0 = TRtmp0{.column1 = Some{.x = v3.column1}},var v4 = v0,not Rexcept[v4],var v5 = v0.";
        this.testTranslation(query, program, false);
    }

    @Test
    public void exceptMixTest2() {
        String query = "create view v0 as SELECT DISTINCT column1, column2 FROM t1 EXCEPT SELECT DISTINCT column1, column2 FROM t4";
        String program = this.header(false) +
                "typedef TRtmp = TRtmp{column1:signed<64>, column2:string}\n" +
                this.relations(false) +
                "relation Rsource[TRtmp]\n" +
                "relation Rexcept[TRt4]\n" +
                "output relation Rv0[TRt4]\n" +
                "Rsource[v3] :- Rt1[v],var v0 = TRtmp{.column1 = v.column1,.column2 = v.column2},var v3 = v0.\n" +
                "Rexcept[v4] :- Rt4[v1],var v2 = TRt4{.column1 = v1.column1,.column2 = v1.column2},var v4 = v2.\n" +
                "Rv0[v5] :- Rsource[v3],var v0 = TRt4{.column1 = Some{.x = v3.column1},.column2 = Some{.x = v3.column2}}," +
                "var v4 = v0,not Rexcept[v4],var v5 = v0.";
        this.testTranslation(query, program, false);
    }
}
