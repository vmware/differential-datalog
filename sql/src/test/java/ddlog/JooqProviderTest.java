/*
 * Copyright 2018-2020 VMware, Inc. All Rights Reserved.
 * SPDX-License-Identifier: BSD-2
 */
package ddlog;

import com.vmware.ddlog.DDlogJooqProvider;
import com.vmware.ddlog.ir.DDlogProgram;
import com.vmware.ddlog.translator.Translator;
import ddlogapi.DDlogAPI;
import ddlogapi.DDlogCommand;
import ddlogapi.DDlogException;
import ddlogapi.DDlogRecCommand;
import ddlogapi.DDlogRecord;
import org.jooq.DSLContext;
import org.jooq.Record;
import org.jooq.Result;
import org.jooq.impl.DSL;
import org.jooq.tools.jdbc.MockConnection;
import org.jooq.tools.jdbc.MockDataProvider;
import org.junit.Test;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

public class JooqProviderTest {

    @Test
    public void testddlog() throws IOException, DDlogException {
        final Translator t = new Translator(null);
        final String s1 = "create table hosts (id integer, capacity integer)";
        final String v2 = "create view hostsv as select distinct * from hosts";
        final String v1 = "create view good_hosts as select distinct * from hosts where capacity < 50";
        t.translateSqlStatement(s1);
        t.translateSqlStatement(v2);
        t.translateSqlStatement(v1);
        final DDlogProgram dDlogProgram = t.getDDlogProgram();
        writeProgramToFile(dDlogProgram.toString());
        DDlogAPI.compileDDlogProgram("/tmp/program.dl", true, "../lib", "./lib");
        DDlogAPI.loadDDlog();

        final DDlogAPI dDlogAPI = new DDlogAPI(1, null, true);
        final int numInserts = 5;
        for (int i = 0; i < numInserts; i++) {
            final DDlogRecord rec = new DDlogRecord(i);
            final DDlogRecord cap = new DDlogRecord(20);
            final DDlogRecord struct = DDlogRecord.makeStruct("Thosts", rec, cap);
            final int id = dDlogAPI.getTableId("Rhosts");
            final DDlogRecCommand command = new DDlogRecCommand(DDlogCommand.Kind.Insert, id, struct);
            dDlogAPI.transactionStart();
            dDlogAPI.applyUpdates(new DDlogRecCommand[]{command});
            dDlogAPI.transactionCommit();
        }

        final List<String> ddl = new ArrayList<>();
        ddl.add(s1);
        ddl.add(v2);
        ddl.add(v1);

        // Initialise the data provider
        MockDataProvider provider = new DDlogJooqProvider(dDlogAPI, ddl);
        MockConnection connection = new MockConnection(provider);

        // Pass the mock connection to a jOOQ DSLContext:
        DSLContext create = DSL.using(connection);
        final Result<Record> fetch = create.fetch("select * from hostsv");
        assertEquals(numInserts, fetch.size());
        assertArrayEquals(new int[]{0, 1, 2, 3, 4},
                          fetch.stream().mapToInt(r -> r.get(0, Integer.class)).toArray());
    }

    public File writeProgramToFile(String programBody) throws IOException {
        File tmp = new File("/tmp/program.dl");
        BufferedWriter bw = new BufferedWriter(new FileWriter(tmp));
        bw.write(programBody);
        bw.close();
        return tmp;
    }
}
