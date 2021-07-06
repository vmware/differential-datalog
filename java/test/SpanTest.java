import java.io.IOException;
import java.io.File;
import java.io.PrintStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.regex.Pattern;
import java.util.regex.Matcher;
import java.util.*;
import java.time.Instant;
import java.time.Duration;
import java.lang.RuntimeException;
import java.math.BigInteger;

import ddlogapi.DDlogAPI;
import ddlogapi.DDlogConfig;
import ddlogapi.DDlogCommand;
import ddlogapi.DDlogRecord;  // only needed if not using the reflection-based APIs
import ddlogapi.DDlogRecCommand;
import ddlogapi.DDlogException;

public class SpanTest {
    public static abstract class ParentChild {
        BigInteger parent;
        BigInteger child;

        protected ParentChild(final BigInteger parent, final BigInteger child) {
            this.parent = parent;
            this.child = child;
        }
    }
    public static final class Source extends ParentChild {
        public Source(final BigInteger parent, final BigInteger child) { super(parent, child); }
    }
    public static final class Dependency extends ParentChild {
        public Dependency(final BigInteger parent, final BigInteger child) { super(parent, child); }
    }

    // A class that can be used to represent most Span relations
    // including Span, Binding, RuleSpan, ContainerSpan
    public abstract static class SpanBase {
        BigInteger entity;
        BigInteger tn;

        protected SpanBase() {}

        protected SpanBase(BigInteger entity, BigInteger tn) {
            this.entity = entity;
            this.tn = tn;
        }

        protected SpanBase(DDlogRecord r) {
            DDlogRecord entity = r.getStructFieldUnchecked(0);
            DDlogRecord tn = r.getStructFieldUnchecked(1);
            this.entity = entity.getInt();
            this.tn = tn.getInt();
        }

        @Override
        public String toString() {
            return this.getClass().getSimpleName() + "{.entity = " +
                    this.entity.toString() + ", .tn = " + this.tn.toString() + "}";
        }
    }
    static class SpanComparator implements Comparator<SpanBase> {
        @Override
        public int compare(SpanBase left, SpanBase right) {
            int cl = left.entity.compareTo(right.entity);
            if (cl != 0)
                return cl;
            return left.tn.compareTo(right.tn);
        }
    }
    public static final class RuleSpan extends SpanBase {
        // We need an empty constructor for reflection to work.
        public RuleSpan() {}
        public RuleSpan(final BigInteger entity, final BigInteger tn) { super(entity, tn); }
        public RuleSpan(DDlogRecord r) { super(r); }
    }
    public static final class ContainerSpan extends SpanBase {
        public ContainerSpan() {}
        public ContainerSpan(final BigInteger entity, final BigInteger tn) { super(entity, tn); }
        public ContainerSpan(DDlogRecord r) { super(r); }
    }
    public static final class Binding extends SpanBase {
        public Binding(final BigInteger entity, final BigInteger tn) { super(entity, tn); }
    }

    // Classes with just 1 field
    public static class Container {
        final BigInteger id;
        public Container(final BigInteger id) { this.id = id; }
    }
    public static class FWRule {
        final BigInteger id;
        public FWRule(final BigInteger id) { this.id = id; }
    }

    public static class SpanParser {
        static Pattern commandPattern = Pattern.compile("^(\\S+)(.*)([;,])$");
        static Pattern argsPattern = Pattern.compile("^\\s*(\\S+)\\(([^)]*)\\)$");

        /// Command that is being executed.
        String command;
        /// Terminator for current command.
        String terminator;
        /// List of commands to execute
        List<DDlogRecCommand> commands;
        /// `true` when command recording is enabled
        boolean recording;

        private final DDlogAPI api;
        private static final boolean debug = true;
        private static Set<RuleSpan> ruleSpan;
        private static Set<ContainerSpan> containerSpan;
        private int ruleSpanTableId;
        private int containerSpanTableId;
        private final PrintStream logStream;

        /* LOGGING: Module id's for logging purposes.  Must match declarations in
         * `span_uuid.dl`
         */
        static int MOD_SPAN_UUID1 = 100;
        static int MOD_SPAN_UUID2 = 200;

        SpanParser(PrintStream logStream) throws DDlogException {
            /* LOGGING: Log level definitions should be imported from log4j. We use magic
             * numbers instead to avoid extra dependencies. */
            this.logStream = logStream;
            DDlogAPI.logSetDefaultCallback(
                    (msg, level) -> logStream.println("Log msg (" + level + "): " + msg),
                    100/*log4j.FATAL*/);
            DDlogAPI.logSetCallback(
                    MOD_SPAN_UUID1,
                    (msg, level) -> logStream.println("Log msg from module1 (" + level + "): " + msg),
                    2147483647/*log4j.ALL*/);
            DDlogAPI.logSetCallback(
                    MOD_SPAN_UUID2,
                    (msg, level) -> logStream.println("Log msg from module2 (" + level + "): " + msg),
                    100/*log4j.FATAL*/);
            DDlogAPI.logSetDefaultCallback(
                    (msg, level) -> logStream.println("Log msg (" + level + "): " + msg),
                    2147483647/*log4j.ALL*/);
            DDlogConfig config = new DDlogConfig(1);
            config.setProfilingConfig(
                    DDlogConfig.timelyProfiling(
                        DDlogConfig.logToDisk("timely_trace"),
                        DDlogConfig.logDisabled(),
                        DDlogConfig.logToDisk("timely_trace"))
                    );
            this.api = new DDlogAPI(config, true);
            this.command = null;
            this.terminator = "";
            this.commands = new ArrayList<DDlogRecCommand>();
        }

        // Note that this method is synchronized, since it can be invoked concurrently
        // on multiple background threads.  Alternatively, we could use concurrent
        // collections for ruleSpan and containerSpan.
        synchronized void onCommit(DDlogRecCommand command) {
            try {
                if (command.relid() == this.ruleSpanTableId) {
                    RuleSpan span = command.getValue(RuleSpan.class);
                    if (command.kind() == DDlogCommand.Kind.Insert)
                        ruleSpan.add(span);
                    else if (command.kind() == DDlogCommand.Kind.DeleteVal)
                        ruleSpan.remove(span);
                    else
                        throw new RuntimeException("Unexpected command " + this.command);
                } else if (command.relid() == this.containerSpanTableId) {
                    ContainerSpan span = command.getValue(ContainerSpan.class);
                    if (command.kind() == DDlogCommand.Kind.Insert)
                        containerSpan.add(span);
                    else if (command.kind() == DDlogCommand.Kind.DeleteVal)
                        containerSpan.remove(span);
                    else
                        throw new RuntimeException("Unexpected command " + this.command);
                }
            } catch (Exception ex) {
                ex.printStackTrace();
                throw new RuntimeException(ex);
            }
        }

        // Alternative implementation of onCommit which does not use reflection.
        void onCommitDirect(DDlogCommand<DDlogRecord> command) {
            try {
                DDlogRecord record = command.value();
                if (command.relid() == this.ruleSpanTableId) {
                    DDlogRecord entity = record.getStructField(0);
                    DDlogRecord tn = record.getStructField(1);
                    RuleSpan span = new RuleSpan(entity.getInt(), tn.getInt());
                    if (command.kind() == DDlogCommand.Kind.Insert)
                        ruleSpan.add(span);
                    else if (command.kind() == DDlogCommand.Kind.DeleteVal)
                        ruleSpan.remove(span);
                    else
                        throw new RuntimeException("Unexpected command " + this.command);
                } else if (command.relid() == this.containerSpanTableId) {
                    DDlogRecord entity = record.getStructField(0);
                    DDlogRecord tn = record.getStructField(1);
                    ContainerSpan span = new ContainerSpan(entity.getInt(), tn.getInt());
                    if (command.kind() == DDlogCommand.Kind.Insert)
                        containerSpan.add(span);
                    else if (command.kind() == DDlogCommand.Kind.DeleteVal)
                        containerSpan.remove(span);
                    else
                        throw new RuntimeException("Unexpected command " + this.command);
                }
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        }

        void checkSemicolon() {
            if (!this.terminator.equals(";"))
                throw new RuntimeException("Expected semicolon after " + this.command + " found " + this.terminator);
        }

        private static BigInteger[] cleanAndSplit(String s) {
            String[] tmp = s.split(",");
            BigInteger[] result = new BigInteger[tmp.length];
            for (int i = 0; i < tmp.length; i++) {
                String t = tmp[i].trim();
                if (t.startsWith("0x"))
                    t = t.substring(2);
                result[i] = new BigInteger(t, 16);
            }
            return result;
        }

        private static void checkSize(BigInteger[] array, int size) {
            if (array.length != size)
                throw new RuntimeException("Expected " + size + " arguments, got " + array.length);
        }

        private DDlogRecCommand createCommand(String command, String arguments)
                throws IllegalAccessException, DDlogException {
            Matcher m = argsPattern.matcher(arguments);
            if (!m.find())
                throw new RuntimeException("Cannot parse arguments for " + command);
            String relation = m.group(1);
            int id = this.api.getTableId(relation);
            String a = m.group(2);
            BigInteger[] args = cleanAndSplit(a);
            Object o;
            switch (relation) {
                case "Container":
                    checkSize(args, 1);
                    o = new Container(args[0]);
                    break;
                case "FWRule":
                    checkSize(args, 1);
                    o = new FWRule(args[0]);
                    break;
                case "Dependency":
                    checkSize(args, 2);
                    o = new Dependency(args[0], args[1]);
                    break;
                case "Source":
                    checkSize(args, 2);
                    o = new Source(args[0], args[1]);
                    break;
                case "Binding":
                    checkSize(args, 2);
                    o = new Binding(args[0], args[1]);
                    break;
                default:
                    throw new RuntimeException("Unexpected class: " + relation);
            }
            DDlogCommand.Kind kind = command.equals("insert") ? DDlogCommand.Kind.Insert : DDlogCommand.Kind.DeleteVal;
            return new DDlogRecCommand(kind, id, o);
        }

        // Alternative implementation of createCommand which does not
        // use reflection and is more efficient.
        private DDlogRecCommand createCommandDirect(String command, String arguments) throws DDlogException {
            Matcher m = argsPattern.matcher(arguments);
            if (!m.find())
                throw new RuntimeException("Cannot parse arguments for " + command);
            String relation = m.group(1);
            int id = this.api.getTableId(relation);
            String a = m.group(2);
            BigInteger[] args = cleanAndSplit(a);
            DDlogRecord o;
            if (relation.equals("Container") || relation.equals("FWRule")) {
                checkSize(args, 1);
                DDlogRecord s = new DDlogRecord(args[0]);
                DDlogRecord[] sa = { s };
                o = DDlogRecord.makeStruct(relation, sa);
            } else if (relation.equals("Dependency") || relation.equals("Source") || relation.equals("Binding")) {
                checkSize(args, 2);
                DDlogRecord s0 = new DDlogRecord(args[0]);
                DDlogRecord s1 = new DDlogRecord(args[1]);
                DDlogRecord[] sa = { s0, s1 };
                o = DDlogRecord.makeStruct(relation, sa);
            } else {
                throw new RuntimeException("Unexpected class: " + relation);
            }
            DDlogCommand.Kind kind = command.equals("insert") ? DDlogCommand.Kind.Insert : DDlogCommand.Kind.DeleteVal;
            return new DDlogRecCommand(kind, id, o);
        }

        void parseLine(String line)
                throws DDlogException {
            Matcher m = commandPattern.matcher(line);
            if (!m.find())
                throw new RuntimeException("Could not isolate command");
            this.command = m.group(1);
            String rest = m.group(2);
            this.terminator = m.group(3);
            if (debug && !command.equals("insert") && !command.equals("delete"))
                System.err.println(line);

            switch (command) {
                case "echo":
                    System.out.println(rest.trim());
                    this.checkSemicolon();
                    break;
                case "start":
                    this.api.transactionStart();
                    this.checkSemicolon();

                    try {
                        this.api.transactionStart();
                        assert false: "transactionStart inside a transaction should throw an exception";
                    } catch (DDlogException ignored) {}

                    break;
                case "commit":
                    //this.exitCode = this.api.commit_dump_changes(r -> System.err.println(r.toString()));
                    this.api.transactionCommit();
                    this.checkSemicolon();

                    try {
                        this.api.transactionCommit();
                        assert false: "transactionCommit outside of transaction should throw an exception";
                    } catch (DDlogException ignored) {}

                    /* LOGGING: Change logging settings after the first commit.
                     * Disable logging for the first module completely; raise
                     * logging level to DEBUG for the second module */
                    DDlogAPI.logSetCallback(MOD_SPAN_UUID1, null, 0/*log4j.OFF*/);
                    DDlogAPI.logSetCallback(
                            MOD_SPAN_UUID2,
                            (msg, level) -> this.logStream.println("Log msg from module2 (" + level + "): " + msg),
                            500/*log4j.DEBUG*/);

                    // Start recording after the first commit. Dump current
                    // database snapshot to the replay file first
                    if (!this.recording) {
                        try {
                            Files.write(Paths.get("./replay.dat"), "start;\n".getBytes());
                            this.api.dumpInputSnapshot("replay.dat", true);
                            Files.write(Paths.get("./replay.dat"), "commit;\n".getBytes(), StandardOpenOption.APPEND);
                            this.api.recordCommands("replay.dat", true);
                            this.recording = true;
                        }  catch (Exception ex) {
                            ex.printStackTrace();
                            throw new RuntimeException(ex);
                        }
                    }
                    break;
                case "insert":
                case "delete":
                    DDlogRecCommand c = this.createCommandDirect(command, rest);
                    this.commands.add(c);
                    if (this.terminator.equals(";")) {
                        DDlogRecCommand[] ca = this.commands.toArray(new DDlogRecCommand[0]);
                        if (debug)
                            System.err.println("Executing " + ca.length + " commands");
                        this.api.applyUpdates(ca);
                        this.commands.clear();
                    }
                    break;
                case "profile":
                    switch (rest) {
                        case " cpu on":
                            this.api.enableCpuProfiling(true);
                            break;
                        case " cpu off":
                            this.api.enableCpuProfiling(false);
                            break;
                        case "":
                            String profile = this.api.profile();
                            System.out.println("Profile:");
                            System.out.println(profile);
                            this.checkSemicolon();
                            break;
                        default:
                            throw new RuntimeException("Unexpected command " + line);
                    }
                    break;
                case "dump":
                    // Hardwired output relation name
                    System.out.println("ContainerSpan:");
                    this.api.dumpTable("ContainerSpan",
                           (r, w) -> {
                               assert (w == 1): "non-unit weight in ContainerSpan";
                               System.out.println(new ContainerSpan(r));
                           });
                    System.out.println();
                    System.out.println("RuleSpan:");
                    this.api.dumpTable("RuleSpan",
                            (r, w) -> {
                                assert (w == 1): "non-unit weight in RuleSpan";
                                System.out.println(new RuleSpan(r));
                            });
                    System.out.println();
                    this.checkSemicolon();
                    break;
                case "exit":
                    this.checkSemicolon();
                    System.out.println();
                    break;
                default:
                    throw new RuntimeException("Unexpected command " + command);
            }
        }

        void run(String file) throws IOException, DDlogException {
            Files.lines(Paths.get(file)).forEach(l -> {
                    try {
                        parseLine(l);
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                });
            this.api.stop();
        }
    }

    public static void main(String[] args) throws IOException, DDlogException {
        if (args.length != 1) {
            System.err.println("Usage: java -jar span.jar <dat_file_name>");
            System.exit(-1);
        }
        Instant start = Instant.now();
        File logFile = new File("span_uuid.log");
        PrintStream logStream = new PrintStream(logFile);
        SpanParser parser = new SpanParser(logStream);
        parser.run(args[0]);
        Instant end = Instant.now();
        if (true)
            System.err.println("Elapsed time " + Duration.between(start, end));
        logStream.close();
    }
}
