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
import com.vmware.ddlog.ir.*;
// If these are missing you have not run the sql/install-ddlog-jar.sh script
import ddlogapi.DDlogAPI;
import ddlogapi.DDlogException;
import org.jooq.DSLContext;
import org.jooq.Field;

import javax.annotation.Nullable;
import java.io.BufferedReader;
import java.io.File;
import java.io.InputStreamReader;
import java.util.*;
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
    private final ParsingOptions options = ParsingOptions.builder().build();


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
        this.translationContext.beginTranslation();
        Statement statement = this.parser.createStatement(sql, this.options);
        //System.out.println("Translating: " + statement.toString());
        DDlogIRNode result = this.visitor.process(statement, this.translationContext);
        this.translationContext.endTranslation();
        return result;
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

    public DDlogProgram generateLibrary() {
        return this.translationContext.generateLibrary();
    }


    /**
     * Run an external process by executing the specified command.
     * @param commands        Command and arguments.
     * @param workdirectory   If not null the working directory.
     * @return                The exit code of the process.  On error prints
     *                        the process stderr on stderr.
     */
    static int runProcess(List<String> commands, @Nullable String workdirectory) {
        try {
            ProcessBuilder pb = new ProcessBuilder(commands);
            pb.redirectErrorStream(true);
            if (workdirectory != null) {
                pb.directory(new File(workdirectory));
            }
            Process process = pb.start();

            StringBuilder out = new StringBuilder();
            BufferedReader br = new BufferedReader(new InputStreamReader(process.getErrorStream()));
            String line;
            while ((line = br.readLine()) != null) {
                out.append(line).append('\n');
            }

            int exitCode = process.waitFor();
            if (exitCode != 0) {
                System.err.println("Error running " + String.join(" ", commands));
                System.err.println(out.toString());
            }
            return exitCode;
        } catch (Exception ex) {
            System.err.println("Error running " + String.join(" ", commands));
            System.err.println(ex.getMessage());
            return 1;
        }
    }

    /**
     * Compile and load a ddlog program stored in a file.
     * @param ddlogFile  Pathname to the ddlog program.
     * @param ddlogInstallationPath  Path to DDLog installation.
     * @param ddlogLibraryPath  Additional list of paths for needed ddlog libraries.
     * @return           A DDlogAPI which can be used to access the program.
     *                   On error returns null and prints an error on stderr.
     */
    @Nullable
    public static DDlogAPI compileAndLoad(
            String ddlogFile,
            String ddlogInstallationPath,
            String... ddlogLibraryPath) throws DDlogException, NoSuchFieldException, IllegalAccessException {
        List<String> command = new ArrayList<String>();
        // Run DDlog compiler
        command.add("ddlog");
        command.add("-i");
        command.add(ddlogFile);
        for (String s: ddlogLibraryPath) {
            command.add("-L");
            command.add(s);
        }
        command.add("-L");
        command.add(ddlogInstallationPath + "/lib");
        int exitCode = runProcess(command, null);
        if (exitCode != 0)
            return null;

        // Run Rust compiler
        command.clear();
        command.add("cargo");
        command.add("build");
        command.add("--release");
        int dot = ddlogFile.indexOf('.');
        String rustDir = ddlogFile;
        if (dot >= 0)
            rustDir = ddlogFile.substring(0, dot);
        rustDir += "_ddlog";
        exitCode = runProcess(command, rustDir);
        if (exitCode != 0)
            return null;

        // Run C compiler
        command.clear();
        command.add("cc");
        command.add("-shared");
        command.add("-fPIC");
        String javaHome = System.getenv("JAVA_HOME");
        String os = System.getProperty("os.name").toLowerCase();
        String shlibext = "so";
        if (os.equals("darwin"))
            shlibext = "dynlib";
        command.add("-I" + javaHome + "/include");
        command.add("-I" + javaHome + "/include/" + os);
        command.add("-I" + rustDir);
        command.add("-I" + ddlogInstallationPath + "/lib");
        command.add(ddlogInstallationPath + "/java/ddlogapi.c");
        command.add("-L" + rustDir + "/target/release/");
        command.add("-l" + rustDir);
        command.add("-o");
        command.add("libddlogapi." + shlibext);
        exitCode = runProcess(command, null);
        if (exitCode != 0)
            return null;

        // Enable the loader to find the new library created
        // http://fahdshariff.blogspot.com/2011/08/changing-java-library-path-at-runtime.html
        java.lang.reflect.Field usrPathsField = ClassLoader.class.getDeclaredField("usr_paths");
        usrPathsField.setAccessible(true);
        String[] paths = (String[])usrPathsField.get(null);
        String[] newPaths = Arrays.copyOf(paths, paths.length + 1);
        newPaths[newPaths.length - 1] = ".";
        usrPathsField.set(null, newPaths);

        // This will load the dynamic library libddlogapi.
        return new DDlogAPI(1, null, false);
    }
}
