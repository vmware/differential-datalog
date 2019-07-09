import java.util.*;
import ddlogapi.DDlogAPI;
import ddlogapi.DDlogCommand;
import ddlogapi.DDlogRecord;  // only needed if not using the reflection-based APIs

public class XTest {
    private final DDlogAPI api;

    XTest() {
        this.api = new DDlogAPI(1, null, false);
    }

    void onCommit(DDlogCommand command) {
        System.out.println(command.toString());
    }

    void run() {
        X.R0 r0 = new X.R0(true);
        DDlogCommand command0 = r0.createCommand(true);
        List<Boolean> l = Arrays.asList(true, true, false);

        X.R1 r1 = new X.R1(0, l);
        DDlogCommand command1 = r1.createCommand(true);

        X.UUID u0 = new X.UUID(0, 1);
        X.UUID u1 = new X.UUID(1, 2);
        X.R2 r2 = new X.R2(u0, u1);
        DDlogCommand command2 = r2.createCommand(true);

        X.N n0 = new X.N(true, true);
        X.N n1 = new X.N(false, false);
        X.M m = new X.M(n0, n1);
        X.R3 r3 = new X.R3(m);
        DDlogCommand command3 = r3.createCommand(true);

        DDlogCommand[] commands = { command0, command1, command2, command3 };
        this.api.start();
        this.api.applyUpdates(commands);
        this.api.commit_dump_changes(r -> this.onCommit(r));
        this.api.stop();
    }

    public static void main(String[] args) {
        XTest test = new XTest();
        test.run();
    }
}
