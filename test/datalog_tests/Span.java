import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.regex.Pattern;
import java.util.regex.Matcher;
import java.util.*;
import java.time.Instant;
import java.time.Duration;

import ddlogapi.DDLogAPI;
import ddlogapi.DDLogCommand;
import ddlogapi.DDLogRecord;

public class Span {
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
        private static boolean debug = false;

        SpanParser(DDLogAPI api) {
            this.api = api;
            this.command = null;
            this.exitCode = -1;
            this.terminator = "";
            this.commands = new ArrayList<DDLogCommand>();
        }

        void checkExitCode() {
            if (this.exitCode < 0)
                throw new RuntimeException("Error executing " + this.command);
        }

        void checkSemicolon() {
            if (!this.terminator.equals(";"))
                throw new RuntimeException("Expected semicolon after " + this.command + " found " + this.terminator);
        }

        void parseLine(String line) {
            Matcher m = commandPattern.matcher(line);
            if (!m.find())
                throw new RuntimeException("Could not isolate command");
            this.command = m.group(1);
            String rest = m.group(2);
            this.terminator = m.group(3);

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
                    DDLogRecord[] array = new DDLogRecord[2];
                    array[0] = new DDLogRecord(args[0].trim().replace("\"", ""));
                    array[1] = new DDLogRecord(args[1].trim().replace("\"", ""));
                    DDLogRecord strct = DDLogRecord.makeStruct(relation, array);

                    DDLogCommand c = new DDLogCommand(DDLogCommand.Kind.Insert, relation, strct);
                    this.commands.add(c);
                    if (this.terminator.equals(";")) {
                        DDLogCommand[] ca = this.commands.toArray(new DDLogCommand[0]);
                        if (debug)
                            System.out.println("Executing " + ca.length + " commands");
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
                    this.exitCode = this.api.dump("Span");
                    this.checkExitCode();
                    this.checkSemicolon();
                    break;
                case "exit":
                    this.exitCode = this.api.stop();
                    this.checkExitCode();
                    this.checkSemicolon();
                    break;
                default:
                    throw new RuntimeException("Unexpected command " + command);
            }
        }

        void run(String file) throws IOException {
            Files.lines(Paths.get(file)).forEach(l -> parseLine(l));
        }
    }

    public static void main(String[] args) throws IOException {
        DDLogAPI api = new DDLogAPI(1);
        Instant start = Instant.now();
        SpanParser parser = new SpanParser(api);
        parser.run("span.dat");
        Instant end = Instant.now();
        if (false)
            System.out.println("Elapsed time " + Duration.between(start, end));
    }
}
