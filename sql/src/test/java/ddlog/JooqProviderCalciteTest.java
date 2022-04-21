/*
 * Copyright 2018-2020 VMware, Inc. All Rights Reserved.
 * SPDX-License-Identifier: BSD-2
 */
package ddlog;

import com.vmware.ddlog.DDlogHandle;
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
        if (JooqProviderTestBase.skip)
            throw new RuntimeException("Skipping");

        // SQL statements written in the Calcite dialect.
        String s1 = "create table hosts (id varchar(36), capacity integer, up boolean, primary key (id))";
        String v2 = "create view hostsv as select distinct * from hosts";
        String v1 = "create view good_hosts as select distinct * from hosts where capacity < 10";
        String checkArrayParse = "create table junk (testcol array integer)";
        String checkNotNullColumns = "create table not_null (test_col1 integer not null, test_col2 varchar(36) not null)";

        String arrayTable = "create table base_array_table (id varchar(36), capacity integer, col3 integer, primary key (id))";
        String checkArrayTypeOverIntegerArrayAgg = "create view check_array_type_integer as select distinct col3, " +
                "ARRAY_AGG(capacity) over (partition by col3) as agg " +
                "from base_array_table";
        String checkArrayTypeOverStringArrayAgg = "create view check_array_type_string as select distinct col3, " +
                "ARRAY_AGG(id) over (partition by col3) as agg " +
                "from base_array_table";
        String checkArrayContainsStringForNonOptionValue =
                "create view check_contains_non_option as select distinct col3 " +
                "from check_array_type_string " +
                "where array_contains(agg, 'n10')";

        String checkSetTypeOverStringSetAgg = "create view check_set_type_string as select distinct col3, " +
                "SET_AGG(id) over (partition by col3) as agg " +
                "from base_array_table";
        String checkSetContainsStringForNonOptionValue =
                "create view check_contains_non_option as select distinct col3 " +
                        "from check_set_type_string " +
                        "where set_contains(agg, 'n10')";
        String bigIntTable = "create table big_int_table (id bigint)";
        String checkJoin = "create view jv as select distinct hosts.id, hosts.capacity, good_hosts.up from " +
                "hosts left join good_hosts on hosts.id = good_hosts.id";

        String bigIntTableView = generateCreateViewStatement("big_int_table");
        String hostIdentityView = generateCreateViewStatement("hosts");
        String notNullIdentityView = generateCreateViewStatement("not_null");
        String baseArrayTableView = generateCreateViewStatement("base_array_table");

        String createIndexNotNull = "create index not_null_idx on not_null (test_col1)";
        String createIndexHosts = "create index hosts_id_up on hosts (id, up)";
        String testIndexParsing = "creATe  index      hosts_id_up_junk on hosts   (id, up)           \t";

        List<String> ddl = new ArrayList<>();
        ddl.add(s1);
        ddl.add(v2);
        ddl.add(v1);
        ddl.add(checkArrayParse);
        ddl.add(checkJoin);
        ddl.add(checkNotNullColumns);
        ddl.add(arrayTable);
        ddl.add(checkArrayTypeOverIntegerArrayAgg);
        ddl.add(checkArrayTypeOverStringArrayAgg);
        ddl.add(checkArrayContainsStringForNonOptionValue);
        // TODO: Jooq does not support these statements
        //ddl.add(checkSetTypeOverStringSetAgg);
        //ddl.add(checkSetContainsStringForNonOptionValue);
        ddl.add(hostIdentityView);
        ddl.add(notNullIdentityView);
        ddl.add(bigIntTable);
        ddl.add(bigIntTableView);
        ddl.add(baseArrayTableView);

        List<String> indexStatements = new ArrayList<>();
        indexStatements.add(createIndexNotNull);
        indexStatements.add(createIndexHosts);
        indexStatements.add(testIndexParsing);

        ddhandle = new DDlogHandle(
                ddl.stream().map(CalciteSqlStatement::new).collect(Collectors.toList()),
                new CalciteToPrestoTranslator(),
                indexStatements);

        ToH2Translator<CalciteSqlStatement> translator = new CalciteToH2Translator();
        // Initialize the data provider
        provider = new DDlogJooqProvider(ddhandle,
                Stream.concat(ddl.stream().map(x -> translator.toH2(new CalciteSqlStatement(x))),
                                indexStatements.stream().map(H2SqlStatement::new))
                        .collect(Collectors.toList()));
        MockConnection connection = new MockConnection(provider);

        // Pass the mock connection to a jOOQ DSLContext
        create = DSL.using(connection);
    }
}
