import java.util.*;
import ddlog.x.*;
import ddlogapi.*;

public class XTest {
    private final DDlogAPI api;

    XTest() throws DDlogException {
        this.api = new DDlogAPI(1, null, false);
    }

    void onCommit(DDlogCommand command) {
        System.out.println(command.toString());
    }

    @FunctionalInterface
    public interface CheckedRunnable {
        void run() throws DDlogException;
    }

    void expectFail(String expected, CheckedRunnable r) {
        String message = null;
        try {
            r.run();
        } catch (Exception ex) {
            message = ex.getMessage();
            System.err.println("Message=" + message);
        }
        if (message == null)
            throw new RuntimeException("No exception receieved!");
        if (!message.contains(expected))
            throw new RuntimeException("Error does not look like `" + expected + "'\n" +
                                       "error is `" + message + "'");
    }

    void run() throws DDlogException {
        {
            xUpdateBuilder builder = new xUpdateBuilder();
            // Updates without a transaction
            this.expectFail("no transaction in progress", () -> builder.applyUpdates(this.api) );
        }
        {
            // Transaction start without a transaction commit
            this.api.transactionStart();
            this.expectFail("transaction already in progress", () -> this.api.transactionStart() );
        }
        {
            // Transaction commit without a transaction start
            this.api.transactionCommitDumpChanges(this::onCommit);
            this.expectFail("no transaction in progress", () -> this.api.transactionCommitDumpChanges(this::onCommit) );
        }
        {
            // Apply the same updates twice
            xUpdateBuilder builder = new xUpdateBuilder();
            this.api.transactionStart();
            builder.applyUpdates(this.api);
            this.expectFail("can only be invoked once", () -> builder.applyUpdates(this.api) );
            this.api.transactionCommitDumpChanges(this::onCommit);
        }
        {
            // Insert the same key twice
            xUpdateBuilder builder = new xUpdateBuilder();
            builder.insert_R0(10, 12);
            builder.insert_R0(10, 13);
            this.api.transactionStart();
            this.expectFail("duplicate key", () -> builder.applyUpdates(this.api) );
            this.api.transactionCommitDumpChanges(this::onCommit);
        }
        {
            // Insert the same key twice in separate transactions
            // This does not work because of issue #372
            xUpdateBuilder builder = new xUpdateBuilder();
            builder.insert_R0(10, 12);
            this.api.transactionStart();
            builder.applyUpdates(this.api);
            this.api.transactionCommitDumpChanges(this::onCommit);

            xUpdateBuilder builder1 = new xUpdateBuilder();
            builder1.insert_R0(10, 12);
            this.api.transactionStart();
            this.expectFail("duplicate key", () -> builder1.applyUpdates(this.api) );
            this.api.transactionCommitDumpChanges(this::onCommit);
        }
        {
            // Remove missing key
            xUpdateBuilder builder = new xUpdateBuilder();
            builder.delete_R0(0, 12);
            this.api.transactionStart();
            this.expectFail("key not found", () -> builder.applyUpdates(this.api) );
            this.api.transactionCommitDumpChanges(this::onCommit);
        }
        {
            // Buggy DDlogRecCommand
            DDlogRecord record = new DDlogRecord(true);
            DDlogRecCommand command = new DDlogRecCommand(DDlogCommand.Kind.Insert, 0, record);
            DDlogRecCommand[] commands = new DDlogRecCommand[1];
            commands[0] = command;
            this.api.transactionStart();
            this.expectFail("not a struct", () -> this.api.applyUpdates(commands));
            this.api.transactionCommitDumpChanges(this::onCommit);
        }
        {
            // Buggy table id
            DDlogRecord record = new DDlogRecord(true);
            DDlogRecCommand command = new DDlogRecCommand(DDlogCommand.Kind.Insert, 10, record);
            DDlogRecCommand[] commands = new DDlogRecCommand[1];
            commands[0] = command;
            this.api.transactionStart();
            this.expectFail("Unknown relation", () -> this.api.applyUpdates(commands));
            this.api.transactionCommitDumpChanges(this::onCommit);
        }

        // Two stops in a row
        this.api.stop();
        this.expectFail("stop", () -> this.api.stop() );
    }

    public static void main(String[] args) throws DDlogException {
        XTest test = new XTest();
        test.run();
    }
}
