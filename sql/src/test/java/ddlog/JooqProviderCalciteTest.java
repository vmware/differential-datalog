/*
 * Copyright 2018-2020 VMware, Inc. All Rights Reserved.
 * SPDX-License-Identifier: BSD-2
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

public class JooqProviderCalciteTest extends JooqProviderTestBase {

    @BeforeClass
    public static void setup() throws IOException, DDlogException {
        // SQL statements written in the Calcite dialect.
        String s1 = "create table hosts (id varchar(36), capacity integer, up boolean, primary key (id))";
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
        String testIndexParsing = "creATe  index      hosts_id_up_junk on hosts   (id, up)           \t";

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
        indexStatements.add(testIndexParsing);

        ddlogAPI = compileAndLoad(
                ddl.stream().map(CalciteSqlStatement::new).collect(Collectors.toList()),
                new CalciteToPrestoTranslator(),
                indexStatements);

        ToH2Translator<CalciteSqlStatement> translator = new CalciteToH2Translator();
        // Initialise the data provider
        provider = new DDlogJooqProvider(ddlogAPI,
                Stream.concat(ddl.stream().map(x -> translator.toH2(new CalciteSqlStatement(x))),
                                indexStatements.stream().map(H2SqlStatement::new))
                        .collect(Collectors.toList()));
        MockConnection connection = new MockConnection(provider);

        // Pass the mock connection to a jOOQ DSLContext
        create = DSL.using(connection);
    }
}