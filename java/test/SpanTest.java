import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.regex.Pattern;
import java.util.regex.Matcher;
import java.util.*;
import java.time.Instant;
import java.time.Duration;
import java.lang.RuntimeException;

import ddlogapi.DDLogAPI;
import ddlogapi.DDLogCommand;

public class SpanTest {
    public static abstract class ParentChild {
        String parent;
        String child;

        protected ParentChild(final String parent, final String child) {
            this.parent = parent;
            this.child = child;
        }
    }
    public static final class Source extends ParentChild {
        public Source(final String parent, final String child) { super(parent, child); }
    }
    public static final class Dependency extends ParentChild {
        public Dependency(final String parent, final String child) { super(parent, child); }
    }

    // A class that can be used to represent most Span relations
    // including Span, Binding, RuleSpan, ContainerSpan
    public abstract static class SpanBase {
        String entity;
        String tn;

        protected SpanBase() {}

        protected SpanBase(String entity, String tn) {
            this.entity = entity;
            this.tn = tn;
        }

        @Override
        public String toString() {
            return this.getClass().getSimpleName() + "{\"" + this.entity + "\",\"" + this.tn + "\"}";
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
        public RuleSpan(final String entity, final String tn) { super(entity, tn); }
    }
    public static final class Binding extends SpanBase {
        public Binding(final String entity, final String tn) { super(entity, tn); }
    }

    // Classes with just 1 field
    public static class Container {
        final String id;
        public Container(final String id) { this.id = id; }
    }
    public static class FWRule {
        final String id;
        public FWRule(final String id) { this.id = id; }
    }

    public static class SpanParser {
        static Pattern commandPattern = Pattern.compile("^(\\S+)(.*)([;,])$");
        static Pattern argsPattern = Pattern.compile("^\\s*(\\S+)\\(([^)]*)\\)$");

        /// Command that is being executed.
        String command;
        /// Exit code of last command
        int exitCode;
        /// Terminator for current command.
        String terminator;
        /// List of commands to execute
        List<DDLogCommand> commands;

        private final DDLogAPI api;
        private static boolean debug = true;
        private static Set<RuleSpan> ruleSpan;
        private int ruleSpanTableId;
        private boolean localTables = true;

        SpanParser() {
            if (localTables) {
                this.api = new DDLogAPI(1, r -> this.onCommit(r));
                this.ruleSpanTableId = this.api.getTableId("RuleSpan");
                this.ruleSpan = new TreeSet<RuleSpan>(new SpanComparator());
            } else {
                this.api = new DDLogAPI(1, null);
            }
            this.command = null;
            this.exitCode = -1;
            this.terminator = "";
            this.commands = new ArrayList<DDLogCommand>();
        }

        void onCommit(DDLogCommand command) {
            if (command.table != this.ruleSpanTableId)
                return;
            try {
                RuleSpan span = command.getValue(RuleSpan.class);
                if (command.kind == DDLogCommand.Kind.Insert)
                    this.ruleSpan.add(span);
                else if (command.kind == DDLogCommand.Kind.DeleteVal)
                    this.ruleSpan.remove(span);
                else
                    throw new RuntimeException("Unexpected command " + this.command);
            } catch (Exception ex) {
                ex.printStackTrace();
                throw new RuntimeException(ex);
            }
        }

        void checkExitCode() {
            if (this.exitCode < 0)
                throw new RuntimeException("Error executing " + this.command);
        }

        void checkSemicolon() {
            if (!this.terminator.equals(";"))
                throw new RuntimeException("Expected semicolon after " + this.command + " found " + this.terminator);
        }

        private static String clean(String s) {
            return s.trim().replace("\"", "");
        }

        private static String[] cleanAndSplit(String s) {
            String[] result = s.split(",");
            for (int i = 0; i < result.length; i++)
                result[i] = clean(result[i]);
            return result;
        }

        private static void checkSize(String[] array, int size) {
            if (array.length != size)
                throw new RuntimeException("Expected " + size + " arguments, got " + array.length);
        }

        void parseLine(String line) throws IllegalAccessException, InstantiationException {
            Matcher m = commandPattern.matcher(line);
            if (!m.find())
                throw new RuntimeException("Could not isolate command");
            this.command = m.group(1);
            String rest = m.group(2);
            this.terminator = m.group(3);
            if (debug && !command.equals("insert"))
                System.err.println(line);

            switch (command) {
                case "echo":
                    System.out.println(rest.trim());
                    this.checkSemicolon();
                    break;
                case "start":
                    this.exitCode = this.api.start();
                    this.checkExitCode();
                    this.checkSemicolon();
                    break;
                case "commit":
                    this.exitCode = this.api.commit();
                    this.checkExitCode();
                    this.checkSemicolon();
                    break;
                case "insert":
                    m = argsPattern.matcher(rest);
                    if (!m.find())
                        throw new RuntimeException("Cannot parse arguments for " + command);
                    String relation = m.group(1);
                    String a = m.group(2);
                    String[] args = cleanAndSplit(a);
                    Object o;
                    if (relation.equals("Container")) {
                        checkSize(args, 1);
                        o = new Container(args[0]);
                    } else  if (relation.equals("FWRule")) {
                        checkSize(args, 1);
                        o = new FWRule(args[0]);
                    } else if (relation.equals("Dependency")) {
                        checkSize(args, 2);
                        o = new Dependency(args[0], args[1]);
                    } else if (relation.equals("Source")) {
                        checkSize(args, 2);
                        o = new Source(args[0], args[1]);
                    } else if (relation.equals("Binding")) {
                        checkSize(args, 2);
                        o = new Binding(args[0], args[1]);
                    } else {
                        throw new RuntimeException("Unexpected class: " + relation);
                    }

                    int id = this.api.getTableId(relation);
                    DDLogCommand c = new DDLogCommand(DDLogCommand.Kind.Insert, id, o);
                    this.commands.add(c);
                    if (this.terminator.equals(";")) {
                        DDLogCommand[] ca = this.commands.toArray(new DDLogCommand[0]);
                        if (debug)
                            System.err.println("Executing " + ca.length + " commands");
                        this.exitCode = this.api.applyUpdates(ca);
                        this.checkExitCode();
                        this.commands.clear();
                    }
                    break;
                case "profile":
                    this.exitCode = this.api.profile();
                    this.checkExitCode();
                    this.checkSemicolon();
                    break;
                case "dump":
                    // Hardwired output relation name
                    if (this.localTables) {
                        System.out.println("RuleSpan:");
                        for (RuleSpan s: this.ruleSpan)
                            System.out.println(s);
                    } else {
                        this.exitCode = this.api.dump("RuleSpan");
                        this.checkExitCode();
                        this.checkSemicolon();
                    }
                    break;
                case "exit":
                    this.checkSemicolon();
                    System.out.println();
                    break;
                default:
                    throw new RuntimeException("Unexpected command " + command);
            }
        }

        void run(String file) throws IOException {
            Files.lines(Paths.get(file)).forEach(l -> {
                    try {
                        parseLine(l);
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                });
            this.api.stop();
            this.ruleSpan.clear();
        }
    }

    public static void main(String[] args) throws IOException {
        if (args.length != 1) {
            System.err.println("Usage: java -jar span.jar <dat_file_name>");
            System.exit(-1);
        };
        Instant start = Instant.now();
        SpanParser parser = new SpanParser();
        parser.run(args[0]);
        Instant end = Instant.now();
        if (true)
            System.err.println("Elapsed time " + Duration.between(start, end));
    }
}
