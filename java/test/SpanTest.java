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
import ddlogapi.DDLogRecord;

public class SpanTest {
    public static class Dependency {
        String parent;
        String child;
    }

    public static class Binding {
        String entity;
        String tn;
    }

    static class SpanComparator implements Comparator<Span> {
        @Override
        public int compare(Span left, Span right) {
            int cl = left.entity.compareTo(right.entity);
            if (cl != 0)
                return cl;
            return left.tn.compareTo(right.tn);
        }
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
        private static Set<Span> span;
        private int spanTableId;
        private boolean localTables = true;

        SpanParser() {
            if (localTables) {
                this.api = new DDLogAPI(1, r -> this.onCommit(r));
                this.spanTableId = this.api.getTableId("Span");
                this.span = new TreeSet<Span>(new SpanComparator());
            } else {
                this.api = new DDLogAPI(1, null);
            }
            this.command = null;
            this.exitCode = -1;
            this.terminator = "";
            this.commands = new ArrayList<DDLogCommand>();
        }

        void onCommit(DDLogCommand command) {
            if (command.table != this.spanTableId)
                return;
            try {
                Span span = (Span)command.value.toObject();
                if (command.kind == DDLogCommand.Kind.Insert)
                    this.span.add(span);
                else if (command.kind == DDLogCommand.Kind.DeleteVal)
                    this.span.remove(span);
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

        void parseLine(String line) throws IllegalAccessException {
            Matcher m = commandPattern.matcher(line);
            if (!m.find())
                throw new RuntimeException("Could not isolate command");
            this.command = m.group(1);
            String rest = m.group(2);
            this.terminator = m.group(3);
            if (debug && !command.equals("insert"))
                System.err.println(command);

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
                    String[] args = a.split(",");
                    if (args.length != 2)
                        throw new RuntimeException("Expected 2 arguments, got " + args.length + ":" + a);
                    /*
                      This is an alternative way to create a DDLogRecord; it requires one
                      to know the internal APIs of DDLogRecord.

                    DDLogRecord[] array = new DDLogRecord[2];
                    array[0] = new DDLogRecord(clean(args[0]));
                    array[1] = new DDLogRecord(clean(args[1]));
                    DDLogRecord strct = DDLogRecord.makeStruct(relation, array);
                    */

                    Object o;
                    if (relation.equals("Dependency")) {
                        Dependency d = new Dependency();
                        d.parent = clean(args[0]);
                        d.child = clean(args[1]);
                        o = d;
                    } else if (relation.equals("Binding")) {
                        Binding b = new Binding();
                        b.entity = clean(args[0]);
                        b.tn = clean(args[1]);
                        o = b;
                    } else {
                        throw new RuntimeException("Unexpected class: " + relation);
                    }

                    DDLogRecord strct = DDLogRecord.convertObject(o);
                    if (debug && false)
                        System.err.println(strct.toString());
                    int id = this.api.getTableId(relation);
                    DDLogCommand c = new DDLogCommand(DDLogCommand.Kind.Insert, id, strct);
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
                        System.out.println("Span:");
                        for (Span s: this.span)
                            System.out.println(s);
                    } else {
                        this.exitCode = this.api.dump("Span");
                        this.checkExitCode();
                        this.checkSemicolon();
                    }
                    break;
                case "exit":
                    this.exitCode = this.api.stop();
                    this.checkExitCode();
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
                    } catch (IllegalAccessException e) {
                        throw new RuntimeException(e);
                    }
                });
            api.stop();
            this.span.clear();
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
