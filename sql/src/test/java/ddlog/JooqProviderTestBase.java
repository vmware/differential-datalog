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

import com.vmware.ddlog.DDlogJooqProvider;
import com.vmware.ddlog.ir.DDlogProgram;
import com.vmware.ddlog.translator.Translator;
import com.vmware.ddlog.util.sql.SqlStatement;
import com.vmware.ddlog.util.sql.ToPrestoTranslator;
import ddlogapi.DDlogAPI;
import ddlogapi.DDlogException;
import org.jooq.DSLContext;
import org.jooq.Field;
import org.jooq.Record;
import org.jooq.Result;
import org.jooq.exception.DataAccessException;
import org.junit.*;
import org.junit.rules.TestRule;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;

import javax.annotation.Nullable;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.List;

import static junit.framework.TestCase.*;
import static org.jooq.impl.DSL.field;
import static org.jooq.impl.DSL.table;
import static org.junit.Assert.assertNull;

/*
 * This is the base class for all JooqProviderTest* test classes. Each new dialect of SQL that DDlog can accept can be
 * tested by extending this class and writing a new setup function. The new setup function will use SQL DDL statements
 * written in that dialect and call ddlog-sql classes with the proper dialect enum. See JooqProviderTestCalcite for
 * an example.
 */
public abstract class JooqProviderTestBase {

    @Nullable
    protected static DDlogAPI ddlogAPI;
    protected static DSLContext create;
    protected static DDlogJooqProvider provider;
    private final Field<String> field1 = field("id", String.class);
    private final Field<Integer> field2 = field("capacity", Integer.class);
    private final Field<Boolean> field3 = field("up", Boolean.class);
    private final Record test1 = create.newRecord(field1, field2, field3);
    private final Record test2 = create.newRecord(field1, field2, field3);
    private final Record test3 = create.newRecord(field1, field2, field3);

    public JooqProviderTestBase() {
        test1.setValue(field1, "n1");
        test1.setValue(field2, 10);
        test1.setValue(field3, true);

        test2.setValue(field1, "n54");
        test2.setValue(field2, 18);
        test2.setValue(field3, false);

        test3.setValue(field1, "n9");
        test3.setValue(field2, 2);
        test3.setValue(field3, true);
    }

    @Before
    public void cleanup() {
        assert(create != null);
        final Result<Record> records = create.fetch("select * from hostsv");
        records.forEach(
            r -> create.execute(String.format("delete from hosts where id = '%s'", r.get(0)))
        );
        assertEquals(0, create.fetch("select * from hostsv").size());
    }

    @AfterClass
    public static void teardown() throws DDlogException{
        ddlogAPI.stop();
    }

    /**
     * Skip text execution in the Base class, as this Base class just serves as an aggregation of all test cases.
     */
    private void skipIfTestBase() {
        Assume.assumeTrue(this.getClass() != JooqProviderTestBase.class);
    }

    // This traces the test being executed for debugging
    // @Rule
    public TestRule watcher = new TestWatcher() {
        protected void starting(Description description) {
            System.out.println("Starting test: " + description.getMethodName());
        }
    };

    /*
     * Test with SQL statements that are raw strings and do not use bindings
     */
    @Test
    public void testSqlOpsNoBindings() {
        skipIfTestBase();
        // Insert statements.
        assert(create != null);
        create.execute("insert into \nhosts values ('n1', 10, true)");
        create.batch("insert into hosts values ('n54', 18, false)",
                     "insert into hosts values ('n9', 2, true)").execute();

        // Make sure selects read out the same content inserted above
        final Result<Record> hostsvResults = create.fetch("select * from hostsv");
        assertTrue(hostsvResults.contains(test1));
        assertTrue(hostsvResults.contains(test2));
        assertTrue(hostsvResults.contains(test3));

        final Result<Record> goodHostsResults = create.fetch("select * from good_hosts");
        assertFalse(goodHostsResults.contains(test1));
        assertFalse(goodHostsResults.contains(test2));
        assertTrue(goodHostsResults.contains(test3));

        // Make sure deletes work
        create.execute("delete from hosts where id = 'n9'");

        final Result<Record> hostsvResultsAfterDelete = create.fetch("select * from hostsv");
        assertTrue(hostsvResultsAfterDelete.contains(test1));
        assertTrue(hostsvResultsAfterDelete.contains(test2));
        assertFalse(hostsvResultsAfterDelete.contains(test3));

        final Result<Record> goodHostsResultsAfterDelete = create.fetch("select * from good_hosts");
        assertFalse(goodHostsResultsAfterDelete.contains(test1));
        assertFalse(goodHostsResultsAfterDelete.contains(test2));
        assertFalse(goodHostsResultsAfterDelete.contains(test3));
    }

    @Test
    public void testInsertNull() {
        skipIfTestBase();
        // Issue 1036
        assert(create != null);
        create.insertInto(table("hosts"))
                .values("n1", 10, null)
                .execute();
        // Make sure selects read out the same content inserted above
        Record t1 = create.newRecord(field1, field2, field3);
        t1.setValue(field1, "n1");
        t1.setValue(field2, 10);
        t1.setValue(field3, null);

        final Result<Record> hostsvResults = create.fetch("select * from hostsv");
        assertTrue(hostsvResults.contains(t1));
    }

    @Test
    public void testDeleteNull() {
        skipIfTestBase();
        assert(create != null);
        create.insertInto(table("hosts"))
                .values("n1", 10, null)
                .execute();
        create.deleteFrom(table("hosts")).where(field("id").eq("n1"))
                .execute();
        final Result<Record> hostsvResults = create.fetch("select * from hostsv");
        assertEquals(0, hostsvResults.size());
    }

    @Test
    public void testWhereNull() {
        skipIfTestBase();
        assert(create != null);
        create.insertInto(table("hosts"))
                .values("n1", 10, null)
                .execute();
        Record t1 = create.newRecord(field1, field2, field3);
        t1.setValue(field1, "n1");
        t1.setValue(field2, 10);
        t1.setValue(field3, null);
        final Result<Record> hostsvResults = create.fetch("select * from hostsv");
        assertTrue(hostsvResults.contains(t1));
    }

    @Test
    public void testUpdateBool() {
        skipIfTestBase();
        assert(create != null);
        create.insertInto(table("hosts"))
                .values("n1", 10, true)
                .execute();
        create.execute("update hosts set up = false where id = 'n1'");
        final Result<Record> hostsvResults = create.fetch("select * from hostsv");
        Record t1 = create.newRecord(field1, field2, field3);
        t1.setValue(field1, "n1");
        t1.setValue(field2, 10);
        t1.setValue(field3, false);
        assertTrue(hostsvResults.contains(t1));
    }

    @Test
    public void testUpdateNull1() {
        skipIfTestBase();
        assert(create != null);
        create.execute("insert into hosts values ('n1', 10, null)");
        final Result<Record> results = create.selectFrom(table("hostsv")).fetch();
        assertNull(results.get(0).get(2));
    }

    @Test
    public void testUpdateNull2() {
        skipIfTestBase();
        assert(create != null);
        create.execute("insert into hosts values ('n1', 10, null)");
        create.update(table("hosts")).set(field3, true).where(field1.eq("n1")).execute();
        final Result<Record> hostsvResults = create.selectFrom(table("hostsv")).fetch();
        Record t1 = create.newRecord(field1, field2, field3);
        t1.setValue(field1, "n1");
        t1.setValue(field2, 10);
        t1.setValue(field3, true);
        assertTrue(hostsvResults.contains(t1));
    }

    @Test
    public void testUpdateUnknownColumn() {
        skipIfTestBase();
        try {
            assert(create != null);
            create.insertInto(table("hosts"))
                    .values("n1", 10, null)
                    .execute();
            create.execute("update hosts set upp = 0 where id = 'n1'");
            fail();
        } catch  (final DataAccessException ex) {
            assertTrue(ex.getMessage().contains("Unknown column"));
        }
    }

    @Test
    public void testUpdateWrongType() {
        skipIfTestBase();
        try {
            assert(create != null);
            create.insertInto(table("hosts"))
                    .values("n1", 10, null)
                    .execute();
            create.execute("update hosts set capacity = true where id = 'n1'");
            fail();
        } catch (final DataAccessException ex) {
            assertTrue(ex.getMessage().contains("not an int"));
        }
    }

    /*
     * Test with SQL statements that supply parameters using bindings
     */
    @Test
    public void testSqlOpsWithBindings() {
        skipIfTestBase();
        // Insert statements.
        assert(create != null);
        create.insertInto(table("hosts"))
              .values("n1", 10, true)
              .execute();
        create.batch(create.insertInto(table("hosts")).values("n54", 18, false),
                     create.insertInto(table("hosts")).values("n9", 2, true))
              .execute();

        // Make sure selects read out the same content inserted above
        final Result<Record> hostsvResults = create.selectFrom(table("hostsv")).fetch();
        assertTrue(hostsvResults.contains(test1));
        assertTrue(hostsvResults.contains(test2));
        assertTrue(hostsvResults.contains(test3));

        final Result<Record> goodHostsResults = create.selectFrom(table("good_hosts")).fetch();
        assertFalse(goodHostsResults.contains(test1));
        assertFalse(goodHostsResults.contains(test2));
        assertTrue(goodHostsResults.contains(test3));

        // Make sure deletes work
        create.deleteFrom(table("hosts")).where(field("id").eq("n9")).execute();

        final Result<Record> hostsvResultsAfterDelete = create.selectFrom(table("hostsv")).fetch();
        assertTrue(hostsvResultsAfterDelete.contains(test1));
        assertTrue(hostsvResultsAfterDelete.contains(test2));
        assertFalse(hostsvResultsAfterDelete.contains(test3));

        final Result<Record> goodHostsResultsAfterDelete = create.selectFrom(table("good_hosts")).fetch();
        assertFalse(goodHostsResultsAfterDelete.contains(test1));
        assertFalse(goodHostsResultsAfterDelete.contains(test2));
        assertFalse(goodHostsResultsAfterDelete.contains(test3));
    }

    /*
     * Test batches with a mix of plain insert and delete statements
     */
    @Test
    public void testDeletesAndInsertsInTheSameBatchNoBindings() {
        skipIfTestBase();
        assert(create != null);
        create.execute("insert into hosts values ('n1', 10, true)");
        create.batch("delete from hosts where id = 'n1'",
                     "insert into hosts values ('n2', 15, false)").execute();
        final Result<Record> results = create.selectFrom(table("hostsv")).fetch();
        assertEquals(1, results.size());
        assertEquals("n2", results.get(0).get(0, String.class));
        assertEquals(15, (int) results.get(0).get(1, Integer.class));
        assertFalse(results.get(0).get(2, Boolean.class));
    }

    /*
     * Test batches with a mix of insert and delete statements with bindings
     */
    @Test
    public void testDeletesAndInsertsInTheSameBatchWithBindings() {
        skipIfTestBase();
        assert(create != null);
        create.execute("insert into hosts values ('n1', 10, true)");
        create.batch(create.deleteFrom(table("hosts")).where(field("id").eq("n1")),
                     create.insertInto(table("hosts")).values("n2", 15, false))
              .execute();
        final Result<Record> results = create.selectFrom(table("hostsv")).fetch();
        assertEquals(1, results.size());
        assertEquals("n2", results.get(0).get(0, String.class));
        assertEquals(15, (int) results.get(0).get(1, Integer.class));
        assertFalse(results.get(0).get(2, Boolean.class));
    }

    /*
     * Test multi-row inserts
     */
    @Test
    public void testMultiRowInsertsNoBindings() {
        skipIfTestBase();
        assert(create != null);
        create.execute("insert into hosts values ('n1', 10, true), ('n54', 18, false), ('n9', 2, true)");
        final Result<Record> results = create.selectFrom(table("hostsv")).fetch();
        assertEquals(3, results.size());
        assertTrue(results.contains(test1));
        assertTrue(results.contains(test2));
        assertTrue(results.contains(test3));
    }

    /*
     * Test multi-row inserts with bindings
     */
    @Test
    public void testMultiRowInsertsWithBindings() {
        skipIfTestBase();
        assert(create != null);
        create.insertInto(table("hosts"))
              .values("n1", 10, true)
              .values("n54", 18, false)
              .values("n9", 2, true)
              .execute();
        final Result<Record> results = create.selectFrom(table("hostsv")).fetch();
        assertEquals(3, results.size());
        assertTrue(results.contains(test1));
        assertTrue(results.contains(test2));
        assertTrue(results.contains(test3));
    }

    /*
     * Test inserts with a subset of fields specified. This is currently unsupported and should throw an exception
     */
    @Test
    public void testPartialInserts() {
        skipIfTestBase();
        try {
            assert(create != null);
            create.insertInto(table("hosts"), field1, field2)
                    .values("n1", 10)
                    .execute();
            fail();
        } catch (final RuntimeException ignored) {
        }
    }

    /*
     * Test updates
     */
    @Test
    public void testUpdates() {
        skipIfTestBase();
        assert(create != null);
        create.execute("insert into hosts values ('n1', 10, true)");
        create.execute("update hosts set capacity = 11 where id = 'n1'");
        final Result<Record> results = create.selectFrom(table("hostsv")).fetch();
        assertEquals(results.get(0).get(1), 11);
    }

    /*
     * Test updates
     */
    @Test
    public void testUpdatesWithBindings() {
        skipIfTestBase();
        assert(create != null);
        create.execute("insert into hosts values ('n1', 10, true)");
        create.update(table("hosts")).set(field2, 11).where(field1.eq("n1")).execute();
        final Result<Record> results = create.selectFrom(table("hostsv")).fetch();
        assertEquals(results.get(0).get(1), 11);
    }

    /*
     * Test a select query to a view that does not exist
     */
    @Test
    public void testNonExistentViewsSelect() {
        skipIfTestBase();
        try {
            assert(create != null);
            create.selectFrom(table("s1")).fetch();
            fail();
        } catch (final DataAccessException e) {
            assertTrue(e.getMessage().contains("Table S1 does not exist"));
        }
    }

    /*
     * Test an insert to a base table that does not exist
     */
    @Test
    public void testNonExistentViewsInsert() {
        skipIfTestBase();
        try {
            assert(create != null);
            create.execute("insert into s1 values ('n1', 10, true)");
            fail();
        } catch (final DataAccessException e) {
            assertTrue(e.getMessage().contains("Table S1 does not exist"));
        }
    }

    /*
     * Test a delete to a base table that does not exist
     */
    @Test
    public void testNonExistentViewsDelete() {
        skipIfTestBase();
        try {
            assert(create != null);
            create.execute("delete from S1 where id = '5'");
            fail();
        } catch (final DataAccessException e) {
            assertTrue(e.getMessage().contains("Table S1 does not exist"));
        }
    }
    /*
     * Test an update to a base table that does not exist
     */
    @Test
    public void testNonExistentViewsUpdate() {
        skipIfTestBase();
        try {
            assert(create != null);
            create.execute("update S1 set capacity = 11 where id = 'n1'");
            fail();
        } catch (final DataAccessException e) {
            assertTrue(e.getMessage().contains("Table S1 does not exist"));
        }
    }


    /*
     * Test an insert with a wrong value type: column 1 is of type varchar, but we insert an int instead
     */
    @Test
    public void testWrongTypeInsert() {
        skipIfTestBase();
        try {
            assert(create != null);
            create.execute("insert into hosts values (5, 10, true)");
            fail();
        } catch (final DataAccessException e) {
            assertTrue(e.getMessage().contains("not a string"));
        }
    }

    /*
     * Test we can insert into columns with `not null` annotations.
     */
    @Test
    public void testNotNullColumns() {
        skipIfTestBase();
        // Without bindings
        Assert.assertNotNull(create);
        create.execute("insert into not_null values (5, 'test_string')");
        // With bindings
        create.insertInto(table("not_null"))
                .values(1, "herp")
                .execute();
    }

    /*
     * Inserting null into not-null column fails at runtime.
     */
    @Test
    public void testInsertNullFails() {
        skipIfTestBase();
        // Without bindings
        try {
            Assert.assertNotNull(create);
            create.execute("insert into not_null values (5, null)");
            fail();
        } catch (Exception ex) {
            Assert.assertTrue(ex.getMessage().contains("NULL value for non-null column"));
        }
    }

    @Test
    public void testInsertNullFails1() {
        skipIfTestBase();
        try {
            // With bindings
            Assert.assertNotNull(create);
            create.insertInto(table("not_null"))
                    .values(null, "herp")
                    .execute();
            fail();
        } catch (Exception ex) {
            Assert.assertTrue(ex.getMessage().contains("NULL value for non-null column"));
        }
    }

    public static <R extends SqlStatement> DDlogAPI compileAndLoad(final List<R> ddl, ToPrestoTranslator<R> translator) throws IOException, DDlogException {
        final Translator t = new Translator(null);
        ddl.forEach(x -> t.translateSqlStatement(translator.toPresto(x)));
        final DDlogProgram dDlogProgram = t.getDDlogProgram();
        final String fileName = "/tmp/program.dl";
        File tmp = new File(fileName);
        BufferedWriter bw = new BufferedWriter(new FileWriter(tmp));
        bw.write(dDlogProgram.toString());
        bw.close();
        DDlogAPI.CompilationResult result = new DDlogAPI.CompilationResult(true);
        DDlogAPI.compileDDlogProgram(fileName, result, "../lib", "./lib");
        if (!result.isSuccess())
            throw new RuntimeException("Failed to compile ddlog program");
        return DDlogAPI.loadDDlog();
    }
}
