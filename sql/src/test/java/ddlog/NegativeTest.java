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
    void expectException(String query, String exception) {
        try {
            this.testTranslation(query, "");
        } catch (TranslationException ex) {
            String message = ex.getMessage();
            //System.out.println(message);
            if (message.startsWith(exception))
                return;
            throw new RuntimeException("Exception message is different: " + message + " instead of " + exception);
        }
        throw new RuntimeException("Test should have failed");
    }
    
    @Test
    public void mixAggregateTest() {
        String query = "create view v0 as select max(column1), column2 from t1";
        this.expectException(query, "SELECT with a mix of aggregates and non-aggregates.");
    }

    @Test
    public void mixAggregate1Test() {
        String query = "create view v0 as select min(column1) + column2 from t1";
        this.expectException(query, "Operation between aggregated and non-aggregated values");
    }

    @Test
    public void mixAggregate2Test() {
        String query = "create view v0 as select min(min(column1)) from t1";
        this.expectException(query, "Nested aggregation");
    }

    @Test
    public void testAmbiguousJoin() {
        String query = "create view v0 as SELECT COUNT(t1.column2) as ct FROM t1 JOIN t2 ON column1 = t2.column1";
        this.expectException(query, "Column name 'column1' is ambiguous; could be in either joined table ");
    }

    @Test
    public void testNoSuchColumn() {
        String query = "create view v0 as SELECT DISTINCT t1.column2, t2.column1, t3.column1 AS x FROM t1 JOIN " +
                "(t2 JOIN t3 ON t2.column1 = t3.column1) ON t1.column1 = t2.column1";
        this.expectException(query, "Column 'column1' not present in 't3'");
    }

    @Test
    public void testNoSuchTable() {
        String query = "create view v0 as SELECT DISTINCT t1.column2, t2.column1, t3.column1 AS x FROM t1 JOIN " +
                "(t2 JOIN t4 ON t2.column1 = t3.column1) ON t1.column1 = t2.column1";
        this.expectException(query, "Column 't3.column1' not in one of the joined relation");
    }

    @Test
    public void testMixAggregate3() {
        String query = "create view v0 as select *, min(column1) from t1";
        this.expectException(query, "SELECT with a mix of aggregates and non-aggregates.");
    }

    @Test
    public void testSelectStartGroupBy() {
        String query = "create view v0 as select *, min(column1) from t1 group by column2";
        this.expectException(query, "SELECT with a mix of aggregates and non-aggregates.");
    }

    @Test
    public void windowGroupByTest() {
        String query = "create view v1 as\n" +
                "SELECT column2, SUM(column1), SUM(SUM(column1)) OVER (PARTITION by column3) FROM t1 GROUP BY column2";
        this.expectException(query, "SELECT with a mix of aggregates and non-aggregates.");
    }

    @Test
    public void noColumnTest() {
        // no column2 in t2
        String query = "create view v0 as SELECT DISTINCT t1.column1, X.c FROM t1 CROSS JOIN (SELECT DISTINCT column2 AS c FROM t2 AS X)";
        this.expectException(query, "Could not resolve identifier");
    }

    @Test
    public void nestedAliasTest() {
        String query = "CREATE VIEW v3 AS (SELECT DISTINCT X.column1 FROM (t2 AS X) AS Y)";
        // AS binds to the right, so this query is parsed as
        // CREATE VIEW v3 AS (SELECT DISTINCT X.column1 FROM ((t2 AS X) AS Y))
        this.expectException(query, "Could not resolve relation 'X'");
    }

    @Test
    public void unionTest() {
        String query = "create view v0 as SELECT DISTINCT t1.column1 FROM t1 UNION ALL SELECT DISTINCT t2.column1 FROM t1";
        this.expectException(query, "UNION ALL not supported");
    }

    @Test
    public void duplicateColumnTest() {
        String query = "create view v0 as select DISTINCT column1, column2 as column1 from t1";
        this.expectException(query, "Field name column1:istring is duplicated");
    }

    @Test
    public void testRemovedColumn1() {
        String query = "create view v0 as SELECT DISTINCT t1.column1 AS x, t2.column2 as y FROM t1 AS t2";
        this.expectException(query, "Could not resolve relation");
    }

    @Test
    public void testIndexException() {
        String query = "create index idx_name on non_existent_table (column1)";
        try {
            this.testIndexTranslation(query, "junk program", true);
        } catch (Exception e) {
            assert e.getMessage().equals("Cannot find base table that index refers to");
        }
    }

    @Test
    public void testCastDateToBool() {
        String query = "create view v0 as SELECT DISTINCT CAST(t3.d AS BOOLEAN) AS b FROM t3";
        this.expectException(query, "Illegal cast");
    }
}