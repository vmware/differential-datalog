package ddlog;

import java.io.*;
import java.util.stream.Collectors;

import com.vmware.ddlog.ir.DDlogIRNode;
import com.vmware.ddlog.ir.DDlogProgram;
import com.vmware.ddlog.translator.Translator;

import org.h2.store.fs.FileUtils;
import org.junit.Assert;
import org.junit.BeforeClass;

public class BaseQueriesTest {
    // TODO: this should only be done once, but it is not clear how this can be achieved.
    @BeforeClass
    public static void createLibrary() throws FileNotFoundException {
        Translator t = new Translator(null);
        DDlogProgram lib = t.generateLibrary();
        System.out.println("Current directory " + System.getProperty("user.dir"));
        lib.toFile("lib/sqlop.dl");
    }
    
    // These strings are part of almost all expected outputs
    protected final String imports = "import sql\nimport sqlop\n";
    protected final String tables =
            "typedef Tt1 = Tt1{column1:signed<64>, column2:string, column3:bool, column4:double}\n" +
            "typedef Tt2 = Tt2{column1:signed<64>}\n" +
            "typedef Tt3 = Tt3{d:sqlDate}\n";
    protected final String tablesWNull =
            "typedef Tt1 = Tt1{column1:Option<signed<64>>, column2:Option<string>, column3:Option<bool>, column4:Option<double>}\n" +
            "typedef Tt2 = Tt2{column1:Option<signed<64>>}\n" +
            "typedef Tt3 = Tt3{d:Option<sqlDate>}\n";

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
            "input relation Rt1[Tt1]\n" +
            "input relation Rt2[Tt2]\n" +
            "input relation Rt3[Tt3]\n";
    }

    protected Translator createInputTables(boolean withNulls) {
        String nulls = withNulls ? "" : " not null";
        String createStatement = "create table t1(column1 integer " + nulls + ",\n" +
                " column2 varchar(36) " + nulls + ",\n" +
                " column3 boolean " + nulls + ",\n" +
                " column4 real " + nulls + ")";
        Translator t = new Translator(null);
        DDlogIRNode create = t.translateSqlStatement(createStatement);
        Assert.assertNotNull(create);
        String s = create.toString();
        Assert.assertNotNull(s);
        Assert.assertEquals("input relation Rt1[Tt1]", s);

        createStatement = "create table t2(column1 integer " + nulls + ")";
        create = t.translateSqlStatement(createStatement);
        Assert.assertNotNull(create);
        s = create.toString();
        Assert.assertNotNull(s);
        Assert.assertEquals("input relation Rt2[Tt2]", s);

        createStatement = "create table t3(d date " + nulls + ")";
        create = t.translateSqlStatement(createStatement);
        Assert.assertNotNull(create);
        s = create.toString();
        Assert.assertNotNull(s);
        Assert.assertEquals("input relation Rt3[Tt3]", s);

        return t;
    }

    protected void compiledDDlog(String program) {
        try {
            File tmp = File.createTempFile("program", ".dl");
            tmp.deleteOnExit();
            BufferedWriter bw = new BufferedWriter(new FileWriter(tmp));
            bw.write(program);
            bw.close();
            Process process = Runtime.getRuntime().exec("ddlog -i " + tmp.toString() + " -L../lib -L./lib");
            int exitCode = process.waitFor();
            if (exitCode != 0) {
                InputStream errorStream = process.getErrorStream();
                String result = new BufferedReader(new InputStreamReader(errorStream))
                        .lines().collect(Collectors.joining("\n"));
                System.out.println(result);
                String[] lines = program.split("\n");
                for (int i = 0; i < lines.length; i++) {
                    System.out.print(String.format("%3s ", i+1));
                    System.out.println(lines[i]);
                }
            }
            Assert.assertEquals(0, exitCode);
            String basename = tmp.getName();
            basename = basename.substring(0, basename.lastIndexOf('.'));
            String tempDir = System.getProperty("java.io.tmpdir");
            String dir = tempDir + "/" + basename + "_ddlog";
            FileUtils.deleteRecursive(dir, false);
        } catch (IOException | InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    protected void testTranslation(String query, String program, boolean withNulls) {
        Translator t = this.createInputTables(withNulls);
        DDlogIRNode view = t.translateSqlStatement(query);
        Assert.assertNotNull(view);
        String s = view.toString();
        Assert.assertNotNull(s);
        DDlogProgram ddprogram = t.getDDlogProgram();
        Assert.assertNotNull(ddprogram);
        s = ddprogram.toString();
        //System.out.println(s);
        Assert.assertEquals(program, s);
        this.compiledDDlog(s);
    }

    protected void testTranslation(String query, String program) {
        this.testTranslation(query, program, false);
    }
}