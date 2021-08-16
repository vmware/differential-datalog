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

public class NegativeTest extends BaseQueriesTest {
    @Test(expected = TranslationException.class)
    public void testMixAggregate() {
        String query = "create view v0 as select max(column1), column2 from t1";
        this.testTranslation(query, "");
    }

    @Test(expected = TranslationException.class)
    public void testMixAggregate1() {
        String query = "create view v0 as select min(column1) + column2 from t1";
        this.testTranslation(query, "");
    }

    @Test(expected = TranslationException.class)
    public void testMixAggregate2() {
        String query = "create view v0 as select min(min(column1)) from t1";
        this.testTranslation(query, "");
    }

    @Test(expected = TranslationException.class)
    public void testMixAggregate3() {
        String query = "create view v0 as select *, min(column1) from t1";
        this.testTranslation(query, "");
    }

    @Test(expected = TranslationException.class)
    public void testSelectStartGroupBy() {
        String query = "create view v0 as select *, min(column1) from t1 group by column2";
        this.testTranslation(query, "");
    }

    @Test(expected = TranslationException.class)
    public void windowGroupByTest() {
        String query = "create view v1 as\n" +
                "SELECT column2, SUM(column1), SUM(SUM(column1)) OVER (PARTITION by column3) FROM t1 GROUP BY column2";
        this.testTranslation(query, "");
    }

    @Test(expected = TranslationException.class)
    public void noColumnTest() {
        // no column2 in t2
        String query = "create view v0 as SELECT DISTINCT t1.column1, X.c FROM t1 CROSS JOIN (SELECT DISTINCT column2 AS c FROM t2 AS X)";
        this.testTranslation(query, "");
    }

    @Test(expected = TranslationException.class)
    public void unionTest() {
        // no column2 in t2
        String query = "create view v0 as SELECT DISTINCT t1.column1 FROM t1 UNION ALL SELECT DISTINCT t2.column1 FROM t1";
        this.testTranslation(query, "");
    }
}