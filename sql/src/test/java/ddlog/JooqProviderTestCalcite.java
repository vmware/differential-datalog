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

public class JooqProviderTestCalcite extends JooqProviderTestBase {

    @BeforeClass
    public static void setup() throws IOException, DDlogException {
        // SQL statements written in the Calcite dialect.
        String s1 = "create table hosts (id varchar(36), capacity integer, up boolean, primary key (id))";
        String v2 = "create view hostsv as select distinct * from hosts";
        String v1 = "create view good_hosts as select distinct * from hosts where capacity < 10";
        String checkArrayParse = "create table junk (testCol integer array)";
        String checkNotNullColumns = "create table not_null (test_col1 integer not null, test_col2 varchar(36) not null)";
        String checkArrayType = "create view check_array_type as select distinct id, ARRAY_AGG(capacity) over (partition by id) as agg " +
                "from hosts";
        List<String> ddl = new ArrayList<>();
        ddl.add(s1);
        ddl.add(v2);
        ddl.add(v1);
        ddl.add(checkArrayParse);
        ddl.add(checkNotNullColumns);
        ddl.add(checkArrayType);

        ddlogAPI = compileAndLoad(
                ddl.stream().map(CalciteSqlStatement::new).collect(Collectors.toList()),
                new CalciteToPrestoTranslator());

        ToH2Translator<CalciteSqlStatement> translator = new CalciteToH2Translator();
        // Initialise the data provider
        provider = new DDlogJooqProvider(ddlogAPI,
                ddl.stream().map(x -> translator.toH2(new CalciteSqlStatement(x))).collect(Collectors.toList()));
        MockConnection connection = new MockConnection(provider);

        // Pass the mock connection to a jOOQ DSLContext
        create = DSL.using(connection);
    }
}