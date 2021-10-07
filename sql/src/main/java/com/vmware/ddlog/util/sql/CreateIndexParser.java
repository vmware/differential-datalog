package com.vmware.ddlog.util.sql;

import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class CreateIndexParser {

    public static ParsedCreateIndex parse(String createIndexStatement) {
        final Scanner s = new Scanner(createIndexStatement);
        if (!s.next().equals("create")) {
            throw new RuntimeException("Cannot parse create index statement, expected 'CREATE'");
        }
        if (!s.next().equals("index")) {
            throw new RuntimeException("Cannot parse create index statement, expected 'INDEX'");
        }
        final String indexName = s.next();
        if (!s.next().equals("on")) {
            throw new RuntimeException("Cannot parse create index statement, expected 'ON'");
        }
        final String tableName = s.next();
        // The remainder of the scanner
        final Pattern p = Pattern.compile("\\((.+)\\)");
        final Matcher m = p.matcher(createIndexStatement);
        if (!m.find()) {
            throw new RuntimeException("Cannot parse create index statement, cannot read index columns");
        }

        List<String> list = new ArrayList<>();
        for (String s1 : m.group(1).trim().split(",")) {
            list.add(s1.trim());
        }
        String[] columns = list.toArray(new String[0]);

        return new ParsedCreateIndex(indexName, tableName, columns);
    }
}
