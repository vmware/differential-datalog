import java.util.*;
import ddlogapi.*;

public class XTest {
    private final DDlogAPI api;

    XTest(DDlogAPI api) throws DDlogException {
        this.api = api;
    }

    void onCommit(DDlogCommand command) {
        System.out.println(command.toString());
    }

    void run() throws DDlogException {
        DDlogRecord field = new DDlogRecord(10);
        DDlogRecord[] fields = { field };
        DDlogRecord record = DDlogRecord.makeStruct("R", fields);
        int id = this.api.getTableId("R");
        DDlogRecCommand command = new DDlogRecCommand(
            DDlogCommand.Kind.Insert, id, record);
        DDlogRecCommand[] ca = new DDlogRecCommand[1];
        ca[0] = command;

        System.err.println("Executing " + command.toString());
        this.api.transactionStart();
        this.api.applyUpdates(ca);
        this.api.transactionCommitDumpChanges(this::onCommit);
    }

    public static void main(String[] args) throws DDlogException {
        DDlogAPI api = DDlogAPI.compileAndLoad("x.dl", "../..");
        if (api == null) {
            System.err.println("Could not load DDlogAPI");
            System.exit(1);
        }
        XTest test = new XTest(api);
        test.run();
    }
}
