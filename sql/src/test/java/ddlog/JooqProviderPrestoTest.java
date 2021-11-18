/*
 * Copyright (c) 2018-2021 VMware, Inc.
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
 */

package ddlog;

import com.vmware.ddlog.DDlogJooqProvider;
import com.vmware.ddlog.util.sql.*;
import ddlogapi.DDlogException;
import org.jooq.impl.DSL;
import org.jooq.tools.jdbc.MockConnection;
import org.junit.BeforeClass;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class JooqProviderPrestoTest extends JooqProviderTestBase {
    @BeforeClass
    public static void setup() throws IOException, DDlogException {
        if (JooqProviderTestBase.skip)
            throw new RuntimeException("Skipping");

        String s1 = "create table hosts (id varchar(36) with (primary_key = true), capacity integer, up boolean)";
        String v2 = "create view hostsv as select distinct * from hosts";
        String v1 = "create view good_hosts as select distinct * from hosts where capacity < 10";
        String checkArrayParse = "create table junk (testCol integer array)";
        String checkNotNullColumns = "create table not_null (test_col1 integer not null, test_col2 varchar(36) not null)";

        String arrayTable = "create table base_array_table (id varchar(36), capacity integer, col3 integer)";
        String checkArrayType = "create view check_array_type as select distinct col3, " +
                "ARRAY_AGG(capacity) over (partition by col3) as agg " +
                "from base_array_table";

        String identityViewName = DDlogJooqProvider.toIdentityViewName("hosts");
        String hostIdentityView = String.format("create view %s as select distinct * from hosts", identityViewName);
        String notNullIdentityView =
                String.format("create view %s as select distinct * from not_null",
                        DDlogJooqProvider.toIdentityViewName("not_null"));

        String createIndexNotNull = "create index not_null_idx on not_null (test_col1)";
        String createIndexHosts = "create index hosts_id_up on hosts (id, up)";

        List<String> ddl = new ArrayList<>();
        ddl.add(s1);
        ddl.add(v2);
        ddl.add(v1);
        ddl.add(checkArrayParse);
        ddl.add(checkNotNullColumns);
        ddl.add(arrayTable);
        ddl.add(checkArrayType);
        ddl.add(hostIdentityView);
        ddl.add(notNullIdentityView);

        List<String> indexStatements = new ArrayList<>();
        indexStatements.add(createIndexNotNull);
        indexStatements.add(createIndexHosts);

        ddlogAPI = compileAndLoad(
                ddl.stream().map(PrestoSqlStatement::new).collect(Collectors.toList()),
                sql -> sql,
                indexStatements);

        ToH2Translator<PrestoSqlStatement> translator = new PrestoToH2Translator();
        // Initialise the data provider
        provider = new DDlogJooqProvider(ddlogAPI,
                Stream.concat(ddl.stream().map(x -> translator.toH2(new PrestoSqlStatement(x))),
                                indexStatements.stream().map(H2SqlStatement::new))
                        .collect(Collectors.toList()));
        MockConnection connection = new MockConnection(provider);

        // Pass the mock connection to a jOOQ DSLContext
        create = DSL.using(connection);
    }
}
