import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.regex.Pattern;
import java.util.regex.Matcher;
import java.util.*;
import java.time.Instant;
import java.time.Duration;
import java.lang.RuntimeException;

import ddlogapi.DDlogAPI;
import ddlogapi.DDlogConfig;
import ddlogapi.DDlogCommand;
import ddlogapi.DDlogRecord;
import ddlogapi.DDlogRecCommand;
import ddlogapi.DDlogException;

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
            result.append("Span{.entity = ");
            result.append(this.entity);
            result.append(", .tns = [");
            List<Integer> list = new ArrayList<Integer>();
            for (int i : this.getTNs())
                list.add(i);
            Collections.sort(list);
            boolean first = true;
            for (int i : list) {
                if (!first)
                    result.append(", ");
                else
                    first = false;
                result.append(i);
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
            super(r.getStructFieldUnchecked(0).getInt().intValueExact());
            DDlogRecord tn = r.getStructFieldUnchecked(1);
            int elements = tn.getSetSize();
            List<Integer> set = new ArrayList<Integer>();
            for (int i = 0; i < elements; i++) {
                DDlogRecord f = tn.getSetField(i);
                set.add(f.getInt().intValue());
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
                for (int i : values.keySet())  {
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
            for (int i : other.map.keySet()) {
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
                if (v.isEmpty())
                    this.map.remove(i);
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
        static Pattern argsSquarePattern = Pattern.compile("^\\s*(\\S+)\\[\\S+\\{([^}]*)\\}\\]$");

        /// Command that is being executed.
        String command;
        /// Terminator for current command.
        String terminator;
        /// List of commands to execute
        List<DDlogRecCommand> commands;

        private final DDlogAPI api;
        private static boolean debug = true;
        private int spanTableId;
        // The whole Span relation is also represented as a delta (from an empty table).
        private DeltaSpan span;
        // The delta that is being returned by the transaction that is
        // currently committing.
        private DeltaSpan currentDelta;

        SpanParser() throws DDlogException {
            DDlogConfig config = new DDlogConfig(2);
            config.setProfilingConfig(DDlogConfig.selfProfiling());
            this.api = new DDlogAPI(config, true);


            this.spanTableId = this.api.getTableId("Span");
            this.command = null;
            this.terminator = "";
            this.commands = new ArrayList<DDlogRecCommand>();
            this.span = new DeltaSpan();
            this.currentDelta = new DeltaSpan();
        }

        void onCommit(DDlogCommand<DDlogRecord> command) {
            DDlogRecord record = command.value();
            if (command.relid() == this.spanTableId) {
                Span s = new Span(record);
                if (command.kind() == DDlogCommand.Kind.Insert) {
                    this.currentDelta.add(s, true);
                } else if (command.kind() == DDlogCommand.Kind.DeleteVal) {
                    this.currentDelta.add(s, false);
                } else {
                    throw new RuntimeException("Unexpected command " + this.command);
                }
            }
        }

        boolean printSpan = false;

        void onCommitChange(DDlogCommand<DDlogRecord> command) {
            if (printSpan) {
                printSpan = false;
                System.out.println("Span:");
            }
            DDlogRecord record = command.value();
            if (command.relid() == this.spanTableId) {
                Span s = new Span(record);
                StringBuilder builder = new StringBuilder();
                builder.append("Span{.entity = ").append(s.entity).append(", .tns = [");
                List<Integer> list = new ArrayList<Integer>();
                for (int i : s.getTNs())
                    list.add(i);
                Collections.sort(list);
                boolean first = true;
                for (int i : list) {
                    if (!first)
                        builder.append(", ");
                    else
                        first = false;
                    builder.append(i);
                }
                builder.append("]}: ");

                if (command.kind() == DDlogCommand.Kind.Insert) {
                    builder.append("+1");
                } else if (command.kind() == DDlogCommand.Kind.DeleteVal) {
                    builder.append("-1");
                } else {
                    throw new RuntimeException("Unexpected command " + this.command);
                }
                System.out.println(builder.toString());
            }
        }

        void checkSemicolon() throws DDlogException {
            if (!this.terminator.equals(";"))
                throw new RuntimeException("Expected semicolon after " + this.command + " found " + this.terminator);
            this.runCommands();
        }

        private static void checkSize(String[] array, int size) {
            if (array.length != size) {
                throw new RuntimeException("Expected " + size + " arguments, got " + array.length + ":" +
                                           String.join(",", array));
            }
        }

        private DDlogRecCommand createCommand(String command, String arguments) throws DDlogException {
            Matcher m = argsPattern.matcher(arguments);
            if (!m.find()) {
                m = argsSquarePattern.matcher(arguments);
                if (!m.find())
                    throw new RuntimeException("Cannot parse arguments for " + command);
            }
            String relation = m.group(1);
            int tid = this.api.getTableId(relation);
            String a = m.group(2);
            String[] args = a.split(",");
            DDlogRecord o;
            switch (relation) {
                case "DdlogNode": {
                    checkSize(args, 1);
                    long id = Long.parseLong(args[0].trim());
                    DDlogRecord s = new DDlogRecord(id);
                    DDlogRecord[] sa = {s};
                    o = DDlogRecord.makeStruct(relation, sa);
                    break;
                }
                case "DdlogBinding": {
                    checkSize(args, 2);
                    long tn = Long.parseLong(args[0]);
                    long entity = Long.parseLong(args[1].trim());
                    DDlogRecord s0 = new DDlogRecord((short) tn);
                    DDlogRecord s1 = new DDlogRecord(entity);
                    DDlogRecord[] sa = {s0, s1};
                    o = DDlogRecord.makeStruct(relation, sa);
                    break;
                }
                case "DdlogDependency": {
                    checkSize(args, 2);
                    long parent = Long.parseLong(args[0].trim());
                    long child = Long.parseLong(args[1].trim());
                    DDlogRecord s0 = new DDlogRecord(parent);
                    DDlogRecord s1 = new DDlogRecord(child);
                    DDlogRecord[] sa = {s0, s1};
                    o = DDlogRecord.makeStruct(relation, sa);
                    break;
                }
                default:
                    throw new RuntimeException("Unexpected class: " + relation);
            }
            DDlogCommand.Kind kind = command.equals("insert") ? DDlogCommand.Kind.Insert : DDlogCommand.Kind.DeleteVal;
            return new DDlogRecCommand(kind, tid, o);
        }

        private void runCommands() throws DDlogException {
            if (this.commands.size() == 0)
                return;
            DDlogRecCommand[] ca = this.commands.toArray(new DDlogRecCommand[0]);
            if (debug)
                System.err.println("Executing " + ca.length + " commands");
            this.api.applyUpdates(ca);
            this.commands.clear();
        }

        void parseLine(String line)
                throws DDlogException {
            if (line.trim().length() == 0) {
                return;
            }
            Matcher m = commandPattern.matcher(line);
            if (!m.find())
                throw new RuntimeException("Could not understand command: " + line);
            this.command = m.group(1);
            String rest = m.group(2);
            this.terminator = m.group(3);
            if (debug && !command.equals("insert") && !command.equals("delete"))
                System.err.println(line);

            switch (command) {
                case "clear":
                    int tid = this.api.getTableId(rest.trim());
                    this.api.clearRelation(tid);
                    this.checkSemicolon();
                    break;
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
                    this.api.transactionStart();
                    this.checkSemicolon();
                    break;
                case "commit":
                    // Prepare to commit with an empty delta;
                    // the commit will insert changes in the delta.
                    this.currentDelta.clear();
                    this.checkSemicolon();
                    if (rest.trim().equals("dump_changes")) {
                        printSpan = true;
                        this.api.transactionCommitDumpChanges(this::onCommitChange);
                    } else {
                        // Once the commit has returned the delta is completed.
                        // We can add it to the span.
                        this.api.transactionCommit();
                    }
                    this.span.add(this.currentDelta);
                    if (debug)
                        System.err.println("CurrentDelta:" + this.currentDelta.size() +
                                           " Span:" + this.span.size());
                    break;
                case "insert":
                case "delete":
                    DDlogRecCommand c = this.createCommand(command, rest);
                    this.commands.add(c);
                    if (this.terminator.equals(";"))
                        this.runCommands();
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
                            String prof_file = this.api.dumpProfile();
                            System.out.println(prof_file);
                            this.checkSemicolon();
                            break;
                        default:
                            throw new RuntimeException("Unexpected command " + line);
                    }
                    break;
                case "dump":
                    boolean [] printSpan = { true };
                    boolean [] spanNotEmpty = { false };
                    this.api.dumpTable("Span", (r, w) -> {
                        spanNotEmpty[0] = true;
                        if (printSpan[0]) {
                            System.out.println("Span:");
                            printSpan[0] = false;
                        }
                        System.out.println(new Span(r));
                    });
                    if (spanNotEmpty[0]) {
                        System.out.println("");
                    };
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
            if (this.span != null)
                this.span.clear();
        }
    }

    public static void main(String[] args) throws IOException, DDlogException {
        if (args.length != 1) {
            System.err.println("Usage: java -jar redist.jar <dat_file_name>");
            System.exit(-1);
        }
        Instant start = Instant.now();
        SpanParser parser = new SpanParser();
        parser.run(args[0]);
        Instant end = Instant.now();
        if (true)
            System.err.println("Elapsed time " + Duration.between(start, end));
    }
}
