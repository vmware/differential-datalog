/*
 * Copyright (c) 2019 VMware Inc. All Rights Reserved.
 * SPDX-License-Identifier: MIT
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the "Software"), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice (including the next paragraph) shall be included in all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 */

package com.vmware.ddlog.translator;

import com.facebook.presto.sql.parser.ParsingOptions;
import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.tree.*;
import com.vmware.ddlog.ir.DDlogIRNode;
import com.vmware.ddlog.ir.DDlogProgram;
import org.jooq.DSLContext;
import org.jooq.Field;

import javax.annotation.Nullable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * A translator consumes SQL, converts it to an IR using the Presto compiler, and then
 * translates the IR to a DDlog IR tree.
 */
public class Translator {
    private final SqlParser parser;
    /**
     * The DSL context can be used to lookup dynamically various SQL persistent objects.
     */
    @Nullable
    private final DSLContext dynamicContext;
    private final TranslationContext translationContext;
    private final TranslationVisitor visitor;
    private final ParsingOptions options = new ParsingOptions();
    private final boolean debug = true;

    public Translator(@Nullable final DSLContext dynamicContext) {
        this.parser = new SqlParser();
        this.dynamicContext = dynamicContext;
        this.translationContext = new TranslationContext();
        this.visitor = new TranslationVisitor();
    }

    public final DDlogProgram getDDlogProgram() {
        return this.translationContext.getProgram();
    }

    /**
     * Translate one SQL statement; add the result to the DDlogProgram.
     * @param sql  Statement to translate.
     */
    public DDlogIRNode translateSqlStatement(final String sql) {
        Statement statement = this.parser.createStatement(sql, this.options);
        if (this.debug)
            System.out.println("Translating: " + statement.toString());
        return this.visitor.process(statement, this.translationContext);
    }

    public DDlogIRNode translateExpression(final String sql) {
        Expression expr = this.parser.createExpression(sql, this.options);
        return this.translationContext.translateExpression(expr);
    }

    private Map<org.jooq.Table<?>, List<Field<?>>> getTablesAndFields(final DSLContext conn) {
        final List<org.jooq.Table<?>> tables = conn.meta().getTables();
        final Map<org.jooq.Table<?>, List<Field<?>>> tablesToFields = new HashMap<>();
        tables.forEach(
                t -> tablesToFields.put(t, t.fieldStream().collect(Collectors.toList()))
        );
        return tablesToFields;
    }

    private Map<org.jooq.Table<?>, List<Class<?>>> getRecordTypesByTable(final DSLContext conn) {
        final Map<org.jooq.Table<?>, List<Field<?>>> tablesToFields = getTablesAndFields(conn);
        return tablesToFields.entrySet().stream()
                .collect(Collectors.toMap(Map.Entry::getKey,
                        entry -> entry.getValue()
                                .stream()
                                .map(Field::getType)
                                .collect(Collectors.toList())));
    }
}
