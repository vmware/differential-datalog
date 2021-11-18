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

package com.vmware.ddlog.translator;

import com.facebook.presto.sql.parser.ParsingOptions;
import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.Statement;
import com.vmware.ddlog.ir.*;
import com.vmware.ddlog.util.sql.CreateIndexParser;
import com.vmware.ddlog.util.sql.ParsedCreateIndex;
import com.vmware.ddlog.util.sql.PrestoSqlStatement;
import org.jooq.DSLContext;
import org.jooq.Field;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;


/**
 * A translator consumes SQL, converts it to an IR using the Presto compiler, and then
 * translates the IR to a DDlog IR tree.
 */
public class Translator {
    private final SqlParser parser;
    /**
     * The DSL context can be used to lookup dynamically various SQL persistent objects.
     */
    private final TranslationContext translationContext;
    private final TranslationVisitor visitor;
    private final ParsingOptions options = ParsingOptions.builder().build();

    public Translator() {
        this.parser = new SqlParser();
        this.translationContext = new TranslationContext();
        this.visitor = new TranslationVisitor();
    }

    public final DDlogProgram getDDlogProgram() {
        return this.translationContext.getProgram();
    }

    /**
     * Translate one SQL statement in the Presto dialect; add the result to the DDlogProgram.
     * @param sql  Statement to translate.
     */
    public DDlogIRNode translateSqlStatement(final PrestoSqlStatement sql) {
        this.translationContext.beginTranslation();
        Statement statement = this.parser.createStatement(sql.getStatement(), this.options);
        //System.out.println("Translating: " + statement.toString());
        DDlogIRNode result = this.visitor.process(statement, this.translationContext);
        this.translationContext.endTranslation();
        return result;
    }

    /**
     * Translate a single `create index` statement; add the result to the DDlogProgram. If the relation over which
     * the index is built does not exist, then this function will error.
     *
     * Because neither Calcite nor Presto SQL parsers support `create index` statements, we implement a
     * poor-man's parser that expects statements in the following form:
     *
     * CREATE INDEX <idx_name> ON <tbl_name> COLUMNLIST
     *
     * COLUMNLIST: (<col1>, <col2>, ... <coln>)
     *
     * @param sql create index statement to translate
     */
    public DDlogIRNode translateCreateIndexStatement(final String sql) {
        ParsedCreateIndex parsedIndex = CreateIndexParser.parse(sql);

        // Find the typedef of the base relation, so we can populate the typedef of the index
        RelationName rn = new RelationName(RelationName.makeRelationName(parsedIndex.getTableName()), null, null);
        final DDlogRelationDeclaration baseRelation =
                this.translationContext.getRelation(rn);
        if (baseRelation == null) {
            throw new RuntimeException("Cannot find base table that index refers to");
        }
        final DDlogType tableType = baseRelation.getType();
        final List<DDlogTypeDef> typedefs = this.translationContext.getProgram().typedefs;
        DDlogTypeDef baseTableTypeDef = null;
        // Look for the type (columns) of the base table in the typedefs
        for (DDlogTypeDef td : typedefs) {
            if (td.getName().equals(tableType.toString())) {
                baseTableTypeDef = td;
            }
        }
        if (baseTableTypeDef == null) {
            throw new RuntimeException("Cannot find base table that index refers to");
        }
        // We need the fields of the base relation both for the typedef of the index and to populate the index rule
        List<DDlogField> baseRelationFields = Objects.requireNonNull(baseTableTypeDef.getType())
                .to(DDlogTStruct.class).getFields();
        // Holds the field typing information of the index
        List<DDlogField> indexFields = new ArrayList<>();
        // Used to populate the index rule, based on DDlog index creation syntax
        List<String> baseTableMatchingTypeString =
                Stream.generate(() -> "_").limit(baseRelationFields.size()).collect(Collectors.toList());

        for (String col : parsedIndex.getColumns()) {
            col = col.trim();
            // Now find the column name in the typedef of the table
            for (int i = 0; i < baseRelationFields.size(); i++) {
                DDlogField f = baseRelationFields.get(i);
                if (f.getName().equals(col)) {
                    indexFields.add(f);
                    baseTableMatchingTypeString.set(i, f.getName());
                }
            }
        }

        RelationName dIndexName = DDlogIndexDeclaration.indexName(parsedIndex.getIndexName());
        this.translationContext.reserveGlobalName(dIndexName.name);

        // Create the index and add to the current DDlog program
        DDlogIndexDeclaration ddlogIndex = new DDlogIndexDeclaration(null, dIndexName, indexFields, baseRelation,
                String.join(",", baseTableMatchingTypeString));
        this.translationContext.add(ddlogIndex);
        return ddlogIndex;
    }

    public DDlogIRNode translateExpression(final String sql) {
        Expression expr = this.parser.createExpression(sql, this.options);
        return this.translationContext.translateExpression(expr);
    }

    @SuppressWarnings("unused")
    public Map<org.jooq.Table<?>, List<Field<?>>> getTablesAndFields(final DSLContext conn) {
        final List<org.jooq.Table<?>> tables = conn.meta().getTables();
        final Map<org.jooq.Table<?>, List<Field<?>>> tablesToFields = new HashMap<>();
        tables.forEach(
                t -> tablesToFields.put(t, t.fieldStream().collect(Collectors.toList()))
        );
        return tablesToFields;
    }

    public DDlogProgram generateSqlLibrary() {
        return SqlSemantics.semantics.generateLibrary();
    }
}
