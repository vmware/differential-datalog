import ddlogapi.*;

public class XTest {
    private final DDlogAPI api;

    XTest() throws DDlogException {
        this.api = new DDlogAPI(1, false);
    }

    synchronized void onCommit(DDlogCommand command) {
        System.out.println(command.toString());
    }

    public void run() throws DDlogException {
        DDlogRecCommand[] commands = new DDlogRecCommand[1];
        DDlogRecord[] rs = new DDlogRecord[3];
        // Insert a record
        this.api.transactionStart();
        rs[0] = new DDlogRecord(0);
        rs[1] = new DDlogRecord(1);
        rs[2] = DDlogRecord.some(new DDlogRecord(2));
        DDlogRecord o = DDlogRecord.makeStruct("AI", rs);
        int id = this.api.getTableId("AI");
        commands[0] = new DDlogRecCommand(DDlogCommand.Kind.Insert, id, o);
        this.api.applyUpdates(commands);
        this.api.transactionCommitDumpChanges(this::onCommit);

        // Modify value 'b' corresponding to key '0'
        this.api.transactionStart();
        DDlogRecord key = new DDlogRecord(0);
        String[] names = new String[1];
        names[0] = "b";
        DDlogRecord value = DDlogRecord.makeNamedStruct("", names, new DDlogRecord(2));
        commands[0] = new DDlogRecCommand(DDlogCommand.Kind.Modify, id, key, value);
        this.api.applyUpdates(commands);
        this.api.transactionCommitDumpChanges(this::onCommit);

        // Modify value 'c' corresponding to key '0'
        this.api.transactionStart();
        key = new DDlogRecord(0);
        names[0] = "c";
        value = DDlogRecord.makeNamedStruct("", names, DDlogRecord.some(new DDlogRecord(6)));
        commands[0] = new DDlogRecCommand(DDlogCommand.Kind.Modify, id, key, value);
        this.api.applyUpdates(commands);
        this.api.transactionCommitDumpChanges(this::onCommit);

        this.api.stop();
    }

    public static void main(String[] args) throws DDlogException {
        if (args.length != 0) {
            System.err.println("Usage: java -jar xtest.jar");
            System.exit(-1);
        }
        XTest test = new XTest();
        test.run();
    }
}
