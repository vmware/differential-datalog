import ddlogapi.DDlogAPI;
import ddlogapi.DDlogCommand;
import ddlogapi.DDlogException;
import ddlog.span_string.*;

public class SpanTest {
    private final DDlogAPI api;

    SpanTest() throws DDlogException {
        this.api = new DDlogAPI(1, false);
    }

    void onCommit(DDlogCommand command) {
        System.out.println(command.toString());
    }

    void run() throws DDlogException {
        span_stringUpdateBuilder builder = new span_stringUpdateBuilder();
        builder.insert_Binding("1", "2");
        builder.insert_FWRule("1");
        this.api.transactionStart();
        builder.applyUpdates(this.api);
        this.api.transactionCommitDumpChanges(this::onCommit);
        this.api.stop();
    }

    public static void main(String[] args) throws DDlogException {
        SpanTest test = new SpanTest();
        test.run();
    }
}
