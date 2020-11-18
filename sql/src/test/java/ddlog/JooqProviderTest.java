/*
 * Copyright 2018-2020 VMware, Inc. All Rights Reserved.
 * SPDX-License-Identifier: BSD-2
 */
package ddlog;

import com.vmware.ddlog.DDlogJooqProvider;
import com.vmware.ddlog.ir.DDlogProgram;
import com.vmware.ddlog.translator.Translator;
import ddlogapi.DDlogAPI;
import ddlogapi.DDlogException;
import org.jooq.DSLContext;
import org.jooq.Field;
import org.jooq.Record;
import org.jooq.Result;
import org.jooq.impl.DSL;
import org.jooq.tools.jdbc.MockConnection;
import org.jooq.tools.jdbc.MockDataProvider;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import javax.annotation.Nullable;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static junit.framework.Assert.fail;
import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.assertFalse;
import static junit.framework.TestCase.assertTrue;
import static org.jooq.impl.DSL.field;
import static org.jooq.impl.DSL.table;

public class JooqProviderTest {

    @Nullable
    private static DSLContext create;
    private final Field<String> field1 = field("id", String.class);
    private final Field<Integer> field2 = field("capacity", Integer.class);
    private final Field<Boolean> field3 = field("up", Boolean.class);
    private final Record test1 = create.newRecord(field1, field2, field3);
    private final Record test2 = create.newRecord(field1, field2, field3);
    private final Record test3 = create.newRecord(field1, field2, field3);

    public JooqProviderTest() {
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


    @BeforeClass
    public static void setup() throws IOException, DDlogException {
        String s1 = "create table hosts (id varchar(36) with (primary_key = true), capacity integer, up boolean)";
        String v2 = "create view hostsv as select distinct * from hosts";
        String v1 = "create view good_hosts as select distinct * from hosts where capacity < 10";
        List<String> ddl = new ArrayList<>();
        ddl.add(s1);
        ddl.add(v2);
        ddl.add(v1);
        compileAndLoad(ddl);
        final DDlogAPI dDlogAPI = new DDlogAPI(1, null, false);

        // Initialise the data provider
        MockDataProvider provider = new DDlogJooqProvider(dDlogAPI, ddl);
        MockConnection connection = new MockConnection(provider);

        // Pass the mock connection to a jOOQ DSLContext
        create = DSL.using(connection);
    }

    @Before
    public void cleanup() {
        final Result<Record> records = create.fetch("select * from hostsv");
        records.forEach(
            r -> create.execute(String.format("delete from hosts where id = '%s'", r.get(0)))
        );
        assertEquals(0, create.fetch("select * from hostsv").size());
    }

    /*
     * Test with SQL statements that are raw strings and do not use bindings
     */
    @Test
    public void testSqlOpsNoBindings() {
        // Insert statements.
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

    /*
     * Test with SQL statements that supply parameters using bindings
     */
    @Test
    public void testSqlOpsWithBindings() {
        // Insert statements.
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
        try {
            create.insertInto(table("hosts"), field1, field2)
                    .values("n1", 10)
                    .execute();
            fail();
        } catch (final RuntimeException ignored) {
        }
    }

    public static void compileAndLoad(final List<String> ddl) throws IOException, DDlogException {
        final Translator t = new Translator(null);
        ddl.forEach(t::translateSqlStatement);
        final DDlogProgram dDlogProgram = t.getDDlogProgram();
        final String fileName = "/tmp/program.dl";
        File tmp = new File(fileName);
        BufferedWriter bw = new BufferedWriter(new FileWriter(tmp));
        bw.write(dDlogProgram.toString());
        bw.close();
        if (!DDlogAPI.compileDDlogProgram(fileName, true, "../lib", "./lib")) {
            throw new RuntimeException("Failed to compile ddlog program");
        }
        DDlogAPI.loadDDlog();
    }
}