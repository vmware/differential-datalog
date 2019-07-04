import java.io.IOException;
import java.util.*;
import java.lang.RuntimeException;

import ddlogapi.DDlogAPI;
import ddlogapi.DDlogCommand;
import ddlogapi.DDlogRecord;

public class SpanTest {
    private final DDlogAPI api;

    SpanTest() {
        this.api = new DDlogAPI(1, null, false);
    }

    void onCommit(DDlogCommand command) {
        System.out.println(command.toString());
    }

    void run() {
        Span_string.Binding binding = new Span_string.Binding("1", "2");
        DDlogCommand command0 = binding.createCommand(true);
        Span_string.FWRule rule = new Span_string.FWRule("1");
        DDlogCommand command1 = rule.createCommand(true);
        DDlogCommand[] commands = { command0, command1 };
        this.api.start();
        this.api.applyUpdates(commands);
        this.api.commit_dump_changes(r -> this.onCommit(r));
        this.api.stop();
    }

    public static void main(String[] args) throws IOException {
        SpanTest test = new SpanTest();
        test.run();
    }
}
