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

    void run() throws DDlogException {
        xUpdateBuilder builder = new xUpdateBuilder();
        builder.insert_R0(true);
        List<Boolean> l = Arrays.asList(true, true, false);
        builder.insert_R1(0, l);

        UUIDWriter u0 = builder.create_UUID(0, 1);
        UUIDWriter u1 = builder.create_UUID(1, 2);
        builder.insert_R2(u0, u1);

        NWriter n0 = builder.create_N(true, true);
        NWriter n1 = builder.create_N(false, false);
        MWriter m = builder.create_M(n0, n1);
        builder.insert_R3(m);

        this.api.transactionStart();
        builder.applyUpdates(this.api);
        this.api.transactionCommitDumpChanges(this::onCommit);
        this.api.stop();
    }

    public static void main(String[] args) throws DDlogException {
        XTest test = new XTest();
        test.run();
    }
}
