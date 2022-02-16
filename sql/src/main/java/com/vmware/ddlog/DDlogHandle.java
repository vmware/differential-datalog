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

package com.vmware.ddlog;

import com.vmware.ddlog.ir.DDlogProgram;
import com.vmware.ddlog.ir.DDlogRelationDeclaration;
import com.vmware.ddlog.ir.DDlogTUser;
import com.vmware.ddlog.ir.DDlogType;
import com.vmware.ddlog.translator.RelationName;
import com.vmware.ddlog.translator.Translator;
import com.vmware.ddlog.util.sql.SqlStatement;
import com.vmware.ddlog.util.sql.ToPrestoTranslator;
import ddlogapi.*;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Consumer;

public class DDlogHandle {
    DDlogProgram program;
    DDlogAPI api;

    // Unfortunately, `create index` statements have to be passed separately, because neither Calcite nor Presto
    // supports them, so we must pass them as SQL strings.
    public <R extends SqlStatement> DDlogHandle(final List<R> ddl, final ToPrestoTranslator<R> translator,
                                         final List<String> createIndexStatements, boolean compile)
            throws IOException, DDlogException {
        final Translator t = new Translator();
        ddl.forEach(x -> t.translateSqlStatement(translator.toPresto(x)));
        createIndexStatements.forEach(t::translateCreateIndexStatement);

        this.program = t.getDDlogProgram();
        // System.out.println(this.program.toString());
        if (compile) {
            final String fileName = "/tmp/program0.dl";
            File tmp = new File(fileName);
            BufferedWriter bw = new BufferedWriter(new FileWriter(tmp));
            bw.write(this.program.toString());
            // System.out.println(this.program.toString());
            bw.close();
            DDlogAPI.CompilationResult result = new DDlogAPI.CompilationResult(true);
            String ddlogSqlHome = System.getenv("DDLOG_HOME") + "/sql/";
            DDlogAPI.compileDDlogProgram(fileName, result, ddlogSqlHome + "../lib", ddlogSqlHome + "./lib");
            if (!result.isSuccess())
                throw new RuntimeException("Failed to compile ddlog program");
        }
        this.api = DDlogAPI.loadDDlog();
    }

    public <R extends SqlStatement> DDlogHandle(final List<R> ddl, final ToPrestoTranslator<R> translator,
                                                final List<String> createIndexStatements)
            throws IOException, DDlogException {
        this(ddl, translator, createIndexStatements, true);
    }

    public void stop() throws DDlogException {
        this.api.stop();
    }

    public void transactionStart() throws DDlogException {
        this.api.transactionStart();
    }

    public void transactionCommitDumpChanges(Consumer<DDlogCommand<DDlogRecord>> onChange) throws DDlogException {
        this.api.transactionCommitDumpChanges(onChange);
    }

    public void transactionRollback() throws DDlogException {
        this.api.transactionRollback();
    }

    public String getTableName(int relationId) throws DDlogException {
        return this.api.getTableName(relationId);
    }

    public int getTableId(String relationName) throws DDlogException {
        return this.api.getTableId(relationName);
    }

    public void applyUpdates(DDlogRecCommand[] commands) throws DDlogException {
        this.api.applyUpdates(commands);
    }

    public String ddlogTableTypeName(final String tableName) {
        DDlogRelationDeclaration rel = this.program.getRelationFromTable(tableName);
        return Objects.requireNonNull(rel).getType().to(DDlogTUser.class).name;
    }

    public String ddlogRelationName(final String tableName) {
        DDlogRelationDeclaration rel = this.program.getRelationFromTable(tableName);
        return Objects.requireNonNull(rel).getName().name;
    }

    /*
     * This corresponds to the naming convention followed by the SQL -> DDlog compiler
     */
    public String relationNameToTableName(final String relationName) {
        return this.program.relationNameToTableName(relationName);
    }

    public void queryIndex(String index, DDlogRecord key, Consumer<DDlogRecord> consumer) throws DDlogException {
        this.api.queryIndex(index, key, consumer);
    }
}
