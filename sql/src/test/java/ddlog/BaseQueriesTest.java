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

package ddlog;

import com.google.common.base.Splitter;
import com.vmware.ddlog.ir.DDlogIRNode;
import com.vmware.ddlog.ir.DDlogProgram;
import com.vmware.ddlog.translator.Translator;
import com.vmware.ddlog.util.sql.PrestoSqlStatement;
import ddlogapi.DDlogAPI;
import ddlogapi.DDlogException;
import org.h2.store.fs.FileUtils;
import org.junit.Assert;
import org.junit.BeforeClass;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.stream.Collectors;

public class BaseQueriesTest {
    protected static boolean runSlowTests = false;

    // TODO: this should only be done once, but it is not clear how this can be achieved.
    @BeforeClass
    public static void createLibrary() throws FileNotFoundException {
        Translator t = new Translator();
        DDlogProgram lib = t.generateSqlLibrary();
        // System.out.println("Current directory " + System.getProperty("user.dir"));
        lib.toFile("lib/sqlop.dl");
    }

    // These strings are part of almost all expected outputs
    protected final String imports = "import fp\n" +
            "import time\n" +
            "import sql\n" +
            "import sqlop\n";
    protected final String tables =
            "typedef TRt1 = TRt1{column1:signed<64>, column2:istring, column3:bool, column4:double}\n" +
            "typedef TRt2 = TRt2{column1:signed<64>}\n" +
            "typedef TRt3 = TRt3{d:Date, t:Time, dt:DateTime}\n" +
            "typedef TRt4 = TRt4{column1:Option<signed<64>>, column2:Option<istring>}\n";
    protected final String tablesWNull =
            "typedef TRt1 = TRt1{column1:Option<signed<64>>, column2:Option<istring>, column3:Option<bool>, column4:Option<double>}\n" +
            "typedef TRt2 = TRt2{column1:Option<signed<64>>}\n" +
            "typedef TRt3 = TRt3{d:Option<Date>, t:Option<Time>, dt:Option<DateTime>}\n" +
            "typedef TRt4 = TRt4{column1:Option<signed<64>>, column2:Option<istring>}\n";

    /**
     * The expected string the generated program starts with.
     * @param withNull  True if the tables can contain nulls.
     */
    protected String header(boolean withNull) {
        if (withNull)
            return this.imports + "\n" + this.tablesWNull;
        return this.imports + "\n" + this.tables;
    }

    /**
     * The expected string for the declared relations
     */
    @SuppressWarnings("unused")
    protected String relations(boolean withNull) {
        return "\n" +
            "input relation Rt1[TRt1]\n" +
            "input relation Rt2[TRt2]\n" +
            "input relation Rt3[TRt3]\n" +
            "input relation Rt4[TRt4]\n";
    }

    protected Translator createInputTables(boolean withNulls) {
        String nulls = withNulls ? "" : " not null";
        String createStatement = "create table t1(column1 integer " + nulls + ",\n" +
                " column2 varchar(36) " + nulls + ",\n" +
                " column3 boolean " + nulls + ",\n" +
                " column4 real " + nulls + ")";
        Translator t = new Translator();
        DDlogIRNode create = t.translateSqlStatement(new PrestoSqlStatement(createStatement));
        Assert.assertNotNull(create);
        String s = create.toString();
        Assert.assertNotNull(s);
        Assert.assertEquals("input relation Rt1[TRt1]", s);

        createStatement = "create table t2(column1 integer " + nulls + ")";
        create = t.translateSqlStatement(new PrestoSqlStatement(createStatement));
        Assert.assertNotNull(create);
        s = create.toString();
        Assert.assertNotNull(s);
        Assert.assertEquals("input relation Rt2[TRt2]", s);

        createStatement = "create table t3(d date " + nulls + ",\n" +
                " t time " + nulls + ",\n" +
                " dt datetime " + nulls + ")";
        create = t.translateSqlStatement(new PrestoSqlStatement(createStatement));
        Assert.assertNotNull(create);
        s = create.toString();
        Assert.assertNotNull(s);
        Assert.assertEquals("input relation Rt3[TRt3]", s);

        // Table t4 always has nullable columns
        createStatement = "create table t4(column1 integer, column2 varchar(36))";
        create = t.translateSqlStatement(new PrestoSqlStatement(createStatement));
        Assert.assertNotNull(create);
        s = create.toString();
        Assert.assertNotNull(s);
        Assert.assertEquals("input relation Rt4[TRt4]", s);

        return t;
    }

    /**
     * Compile the DDlog program given as a string.
     * @param programBody  Program to compile.
     */
    protected void compiledDDlog(String programBody) {
        String basename = null;
        try {
            File tmp = this.writeProgramToFile(programBody);
            basename = tmp.getName();
            //Compiling all the way takes too long
            DDlogAPI.CompilationResult result = new DDlogAPI.CompilationResult(true);
            //DDlogAPI.compileDDlogProgram(tmp.getName(), result, "../lib", "./lib");
            DDlogAPI.compileDDlogProgramToRust(tmp.getName(), result,"../lib", "./lib");
            if (!result.isSuccess()) {
                String[] lines = programBody.split("\n");
                for (int i = 0; i < lines.length; i++) {
                    System.out.printf("%3s ", i+1);
                    System.out.println(lines[i]);
                }
                throw new RuntimeException("Error compiling datalog program");
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            if (basename != null) {
                basename = basename.substring(0, basename.lastIndexOf('.'));
                String tempDir = System.getProperty("java.io.tmpdir");
                String dir = tempDir + "/" + basename + "_ddlog";
                FileUtils.deleteRecursive(dir, false);
            }
        }
    }

    protected void testTranslation(String query, String program, boolean withNulls) {
        Translator t = this.createInputTables(withNulls);
        DDlogIRNode view = t.translateSqlStatement(new PrestoSqlStatement(query));
        Assert.assertNotNull(view);
        String s = view.toString();
        Assert.assertNotNull(s);
        DDlogProgram ddprogram = t.getDDlogProgram(true);
        Assert.assertNotNull(ddprogram);
        s = ddprogram.toString();
        //if (!program.equals(s))
        //    System.out.println(s);
        Assert.assertEquals(program, s);
        this.compiledDDlog(s);
    }

    @SuppressWarnings("SameParameterValue")
    protected void testIndexTranslation(String indexQuery, String program, boolean withNulls) {
        Translator t = this.createInputTables(withNulls);
        DDlogIRNode index = t.translateCreateIndexStatement(indexQuery);
        Assert.assertNotNull(index);
        String s = index.toString();
        Assert.assertNotNull(s);
        DDlogProgram ddprogram = t.getDDlogProgram(true);
        Assert.assertNotNull(ddprogram);
        s = ddprogram.toString();
        Assert.assertEquals(program, s);
        this.compiledDDlog(s);
    }

    protected void testTranslation(List<String> queries, String program, boolean withNulls) {
        Translator t = this.createInputTables(withNulls);
        for (String query: queries) {
            DDlogIRNode view = t.translateSqlStatement(new PrestoSqlStatement(query));
            Assert.assertNotNull(view);
            String s = view.toString();
            Assert.assertNotNull(s);
        }
        DDlogProgram ddprogram = t.getDDlogProgram(true);
        Assert.assertNotNull(ddprogram);
        String s = ddprogram.toString();
        if (!program.equals(s))
            System.out.println(s);
        Assert.assertEquals(program, s);
        this.compiledDDlog(s);
    }

    protected void testTranslation(String query, String program) {
        this.testTranslation(query, program, false);
    }

    public File writeProgramToFile(String programBody) throws IOException {
        File tmp = new File("program.dl");
        BufferedWriter bw = new BufferedWriter(new FileWriter(tmp));
        bw.write(programBody);
        bw.close();
        return tmp;
    }

    /**
     * Compile the specified file from the resources folder.
     */
    public void testFileCompilation(String file) {
        if (!runSlowTests) return;
        final InputStream resourceAsStream = DynamicTest.class.getResourceAsStream(file);
        try (final BufferedReader tables = new BufferedReader(new InputStreamReader(resourceAsStream,
                StandardCharsets.UTF_8))) {
            final Translator t = new Translator();
            final String schemaAsString = tables.lines()
                    .filter(line -> !line.startsWith("--")) // remove SQL comments
                    .collect(Collectors.joining("\n"));
            final List<String> semiColonSeparated = Splitter.on(";")
                    .trimResults()
                    .omitEmptyStrings()
                    .splitToList(schemaAsString);
            semiColonSeparated // remove SQL comments
                    .forEach(x -> t.translateSqlStatement(new PrestoSqlStatement(x)));
            final DDlogProgram dDlogProgram = t.getDDlogProgram(true);
            final String ddlogProgramAsString = dDlogProgram.toString();
            File tmp = this.writeProgramToFile(ddlogProgramAsString);
            DDlogAPI.CompilationResult result = new DDlogAPI.CompilationResult(false);
            DDlogAPI.compileDDlogProgram(tmp.toString(), result,"..", "lib");
            Assert.assertTrue(result.isSuccess());
        } catch (IOException | DDlogException e) {
            throw new RuntimeException(e);
        }
    }
}
