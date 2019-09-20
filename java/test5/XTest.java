import java.util.*;
import ddlogapi.DDlogAPI;
import ddlogapi.DDlogCommand;
import ddlog.x.*;
import ddlogapi.DDlogException;

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
            throw new RuntimeException("No message receieved with exception");
        if (!message.contains(expected))
            throw new RuntimeException("Error does not look like `" + expected + "'\n" +
                                       "error is `" + message + "'");
    }

    void run() throws DDlogException {
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
            xUpdateBuilder builder = new xUpdateBuilder();
            builder.insert_R0(10, 12);
            this.api.transactionStart();
            builder.applyUpdates(this.api);
            this.api.transactionCommitDumpChanges(this::onCommit);
        }
        this.api.stop();
    }

    public static void main(String[] args) throws DDlogException {
        XTest test = new XTest();
        test.run();
    }
}
