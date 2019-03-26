import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.regex.Pattern;
import java.util.regex.Matcher;
import java.util.*;
import java.time.Instant;
import java.time.Duration;
import java.lang.RuntimeException;
import java.math.BigInteger;

import ddlogapi.DDlogAPI;
import ddlogapi.DDlogCommand;
import ddlogapi.DDlogRecord;  // only needed if not using the reflection-based APIs

public class RedistTest {
    static class Span {
        final int entity;
        final List<Integer> tns;

        public Span(int entity, List<Integer> tns) {
            this.entity = entity;
            this.tns = tns;
        }

        @Override
        public String toString() {
            StringBuilder result = new StringBuilder();
            result.append("Span{");
            result.append(this.entity);
            result.append(",[");
            boolean first = true;
            for (int s : this.tns) {
                if (!first)
                    result.append(", ");
                first = false;
                result.append(s);
            }
            result.append("]}");
            return result.toString();
        }
    }

    public static class SpanComparator implements Comparator<Span> {
        @Override
        public int compare(Span left, Span right) {
            return Integer.compare(left.entity, right.entity);
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
        List<DDlogCommand> commands;

        private final DDlogAPI api;
        private static boolean debug = true;
        private int spanTableId;
        private boolean localTables = false;
        private Set<Span> span;

        SpanParser() {
            if (localTables) {
                this.api = new DDlogAPI(1, r -> this.onCommit(r));
                this.spanTableId = this.api.getTableId("Span");
                System.err.println("Span table id " + this.spanTableId);
                this.span = new TreeSet<Span>(new SpanComparator());
            } else {
                this.api = new DDlogAPI(1, null);
            }
            this.command = null;
            this.exitCode = -1;
            this.terminator = "";
            this.commands = new ArrayList<DDlogCommand>();
        }

        // Alternative implementation of onCommit which does not use reflection.
        void onCommit(DDlogCommand command) {
            DDlogRecord record = command.value;
            //System.err.println(command);
            if (command.table == this.spanTableId) {
                DDlogRecord entity = record.getStructField(0);
                DDlogRecord tn = record.getStructField(1);
                int elements = tn.getSetSize();
                List<Integer> set = new ArrayList<Integer>();
                for (int i = 0; i < elements; i++) {
                    DDlogRecord f = tn.getSetField(i);
                    set.add((int)f.getLong());
                }
                Span s = new Span((int)entity.getLong(), set);
                /*
                if (s.entity == 1)
                    System.err.println(command);
                */
                if (command.kind == DDlogCommand.Kind.Insert)
                    this.span.add(s);
                else if (command.kind == DDlogCommand.Kind.DeleteVal)
                    this.span.remove(s);
                else
                    throw new RuntimeException("Unexpected command " + this.command);
            }
        }

        void checkExitCode() {
            if (this.exitCode < 0)
                throw new RuntimeException("Error executing " + this.command);
        }

        void checkSemicolon() {
            if (!this.terminator.equals(";"))
                throw new RuntimeException("Expected semicolon after " + this.command + " found " + this.terminator);
            this.runCommands();
        }

        private static void checkSize(String[] array, int size) {
            if (array.length != size)
                throw new RuntimeException("Expected " + size + " arguments, got " + array.length);
        }

        private DDlogCommand createCommandDirect(String commang, String arguments) {
            Matcher m = argsPattern.matcher(arguments);
            if (!m.find())
                throw new RuntimeException("Cannot parse arguments for " + command);
            String relation = m.group(1);
            int tid = this.api.getTableId(relation);
            String a = m.group(2);
            String[] args = a.split(",");
            DDlogRecord o;
            if (relation.equals("DdlogNode")) {
                checkSize(args, 1);
                long id = Long.parseLong(args[0]);
                DDlogRecord s = new DDlogRecord(id);
                DDlogRecord sa[] = { s };
                o = DDlogRecord.makeStruct(relation, sa);
            } else if (relation.equals("DdlogBinding")) {
                checkSize(args, 2);
                long tn = Long.parseLong(args[0]);
                long entity = Long.parseLong(args[1]);
                DDlogRecord s0 = new DDlogRecord((short)tn);
                DDlogRecord s1 = new DDlogRecord(entity);
                DDlogRecord sa[] = { s0, s1 };
                o = DDlogRecord.makeStruct(relation, sa);
            }  else if (relation.equals("DdlogDependency")) {
                checkSize(args, 2);
                long parent = Long.parseLong(args[0]);
                long child = Long.parseLong(args[1]);
                DDlogRecord s0 = new DDlogRecord(parent);
                DDlogRecord s1 = new DDlogRecord(child);
                DDlogRecord sa[] = { s0, s1 };
                o = DDlogRecord.makeStruct(relation, sa);
            } else {
                throw new RuntimeException("Unexpected class: " + relation);
            }
            DDlogCommand.Kind kind = command.equals("insert") ? DDlogCommand.Kind.Insert : DDlogCommand.Kind.DeleteVal;
            return new DDlogCommand(kind, tid, o);
        }

        private void runCommands() {
            if (this.commands.size() == 0)
                return;
            DDlogCommand[] ca = this.commands.toArray(new DDlogCommand[0]);
            if (debug)
                System.err.println("Executing " + ca.length + " commands");
            this.exitCode = this.api.applyUpdates(ca);
            this.checkExitCode();
            this.commands.clear();
        }

        void parseLine(String line)
                throws IllegalAccessException, InstantiationException {
            Matcher m = commandPattern.matcher(line);
            if (!m.find())
                throw new RuntimeException("Could not isolate command");
            this.command = m.group(1);
            String rest = m.group(2);
            this.terminator = m.group(3);
            if (debug && !command.equals("insert") && !command.equals("delete"))
                System.err.println(line);

            switch (command) {
                case "timestamp":
                    // TODO
                    this.checkSemicolon();
                    break;
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
                case "delete":
                    DDlogCommand c = this.createCommandDirect(command, rest);
                    this.commands.add(c);
                    if (this.terminator.equals(";"))
                        this.runCommands();
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
                        this.api.dump("Span");
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
            if (this.span != null)
                this.span.clear();
        }
    }

    public static void main(String[] args) throws IOException {
        if (args.length != 1) {
            System.err.println("Usage: java -jar redist.jar <dat_file_name>");
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
