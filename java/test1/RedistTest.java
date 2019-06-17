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
import ddlogapi.DDlogRecord;

import it.unimi.dsi.fastutil.ints.*;

public class RedistTest {
    // If set to false the program is faster
    static final boolean testing = false;

    static abstract class SpanBase {
        public final int entity;

        protected SpanBase(int entity) {
            this.entity = entity;
        }

        public abstract Iterable<Integer> getTNs();
        public abstract int size();

        @Override
        public String toString() {
            StringBuilder result = new StringBuilder();
            result.append("Span{");
            result.append(this.entity);
            result.append(",[");
            boolean first = true;
            for (int s : this.getTNs()) {
                if (!first)
                    result.append(", ");
                first = false;
                result.append(s);
            }
            result.append("]}");
            return result.toString();
        }
    }

    static class Span extends SpanBase {
        private final List<Integer> tns;

        public Span(int entity, List<Integer> tns) {
            super(entity);
            this.tns = tns;
        }

        public Span(DDlogRecord r) {
            super((int)r.getStructField(0).getLong());
            DDlogRecord tn = r.getStructField(1);
            int elements = tn.getSetSize();
            List<Integer> set = new ArrayList<Integer>();
            for (int i = 0; i < elements; i++) {
                DDlogRecord f = tn.getSetField(i);
                set.add((int)f.getLong());
            }
            this.tns = set;
        }

        @Override
        public Iterable<Integer> getTNs() {
            return this.tns;
        }

        @Override
        public int size() {
            return this.tns.size();
        }
    }

    /**
     * A Span implementation that refers to the data in a DeltaSpan
     */
    static class SpanFromDelta extends SpanBase {
        private final DeltaSpan delta;

        public SpanFromDelta(int entity, DeltaSpan delta) {
            super(entity);
            this.delta = delta;
        }

        @Override
        public Iterable<Integer> getTNs() {
            Int2ShortMap values = this.delta.map.get(this.entity);
            if (values == null)
                return Collections.<Integer>emptyList();
            // Check that all values are 1
            if (testing) {
                for (Integer i : values.keySet())  {
                    short s = values.get(i);
                    if (s != 1)
                        System.err.println("Entity " + this.entity + " tn " + i + " has value " + s);
                }
            }
            return values.keySet();
        }

        @Override
        public int size() {
            Int2ShortMap values = this.delta.map.get(this.entity);
            if (values == null)
                return 0;
            return values.size();
        }
    }

    /**
     * Represents a change to the span.
     */
    static class DeltaSpan {
        public final Int2ObjectMap<Int2ShortMap> map;

        public DeltaSpan() {
            // The RBTree map should be used only for testing
            if (testing)
                this.map = new Int2ObjectRBTreeMap<Int2ShortMap>();
            else
                // The hashmap is faster.
                this.map = new Int2ObjectOpenHashMap<Int2ShortMap>();
        }

        /**
         * Add or subtract a span from this delta
         */
        synchronized public void add(Span span, boolean add) {
            short inc = (short)(add ? 1 : -1);
            Int2ShortMap v = this.map.get(span.entity);
            if (v == null) {
                if (testing)
                    // The RBTree is used for testing, the HashMap is faster
                    v = new Int2ShortRBTreeMap();
                else
                    v = new Int2ShortOpenHashMap();
                this.map.put(span.entity, v);
                for (int tn : span.tns)
                    v.put(tn, inc);
                return;
            }
            for (int tn : span.tns) {
                if (testing) {
                    if (!v.containsKey(tn)) {
                        v.put(tn, inc);
                    } else {
                        short s = (short)(v.get(tn) + inc);
                        if (s == 0)
                            v.remove(tn);
                        else
                            v.put(tn, s);
                    }
                } else {
                    short old = ((Int2ShortOpenHashMap)v).addTo(tn, inc);
                    if (old == -inc)
                        v.remove(tn);
                }
            }
        }

        /**
         * Add another DeltaSpan to this Delta.
         */
        synchronized public void add(DeltaSpan other) {
            for (Integer i : other.map.keySet()) {
                Int2ShortMap ov = other.map.get(i);
                Int2ShortMap v = this.map.get(i);
                if (v == null) {
                    if (testing) {
                        v = new Int2ShortRBTreeMap();
                        this.map.put(i, v);
                        for (int tn : ov.keySet())
                            v.put(tn, ov.get(tn));
                    } else {
                        this.map.put(i, ((Int2ShortOpenHashMap)ov).clone());
                    }
                    continue;
                }
                for (int tn : ov.keySet()) {
                    short so = ov.get(tn);
                    if (testing) {
                        if (!v.containsKey(tn)) {
                            v.put(tn, so);
                        } else {
                            short s = (short)(v.get(tn) + so);
                            if (so == 0)
                                v.remove(tn);
                            else
                                v.put(tn, s);
                        }
                    } else {
                        short old = ((Int2ShortOpenHashMap)v).addTo(tn, so);
                        if (old == -so)
                            v.remove(tn);
                    }
                }
            }
        }

        synchronized public void clear() {
            this.map.clear();
        }

        public SpanBase getSpan(int entity) {
            return new SpanFromDelta(entity, this);
        }

        public int size() {
            return this.map.size();
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
        /// If localTables is true we maintain the Span table
        /// contents in Java; otherwise it is maintained by DDlog.
        private boolean localTables = true;
        // The whole Span relation is also represented as a delta (from an empty table).
        private DeltaSpan span;
        // The delta that is being returned by the transaction that is
        // currently committing.
        private DeltaSpan currentDelta;

        SpanParser() {
            if (localTables) {
                this.api = new DDlogAPI(2, r -> this.onCommit(r), false);
                this.spanTableId = this.api.getTableId("Span");
                if (debug)
                    System.err.println("Span table id " + this.spanTableId);
            } else {
                this.api = new DDlogAPI(2, null, true);
            }
            this.command = null;
            this.exitCode = -1;
            this.terminator = "";
            this.commands = new ArrayList<DDlogCommand>();
            this.span = new DeltaSpan();
            this.currentDelta = new DeltaSpan();
        }

        void onCommit(DDlogCommand command) {
            DDlogRecord record = command.value;
            if (command.table == this.spanTableId) {
                Span s = new Span(record);
                if (command.kind == DDlogCommand.Kind.Insert) {
                    this.currentDelta.add(s, true);
                } else if (command.kind == DDlogCommand.Kind.DeleteVal) {
                    this.currentDelta.add(s, false);
                } else {
                    throw new RuntimeException("Unexpected command " + this.command);
                }
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

        private DDlogCommand createCommand(String commang, String arguments) {
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
                    System.out.println("Timestamp: "  + System.currentTimeMillis());
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
                    // Prepare to commit Start with an empty delta;
                    // the commit will insert changes in the delta.
                    this.currentDelta.clear();
                    this.exitCode = this.api.commit();
                    // Once the commit has returned the delta is completed.
                    // We can add it to the span.
                    this.span.add(this.currentDelta);
                    this.checkExitCode();
                    this.checkSemicolon();
                    if (debug)
                        System.err.println("CurrentDelta:" + this.currentDelta.size() +
                                           " Span:" + this.span.size());
                    break;
                case "insert":
                case "delete":
                    DDlogCommand c = this.createCommand(command, rest);
                    this.commands.add(c);
                    if (this.terminator.equals(";"))
                        this.runCommands();
                    break;
                case "profile":
                    if (rest.equals(" cpu on")) {
                        this.exitCode = this.api.enable_cpu_profiling(true);
                        this.checkExitCode();
                    } else if (rest.equals(" cpu off")) {
                        this.exitCode = this.api.enable_cpu_profiling(false);
                        this.checkExitCode();
                    } else if (rest.equals("")) {
                        String profile = this.api.profile();
                        System.out.println("Profile:");
                        System.out.println(profile);
                        this.checkSemicolon();
                    } else {
                        throw new RuntimeException("Unexpected command " + line);
                    }
                    break;
                case "dump":
                    // Hardwired output relation name
                    if (this.localTables) {
                        System.out.println("Span:");
                        for (int entity: this.span.map.keySet()) {
                            SpanBase s = this.span.getSpan(entity);
                            System.out.println(s);
                        }
                    } else {
                        System.out.println("Span:");
                        this.exitCode = this.api.dump("Span",
                                r -> System.out.println(new Span(r)));
                        System.out.println();
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
