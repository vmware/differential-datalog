import ddlogapi.*;

public class TwoTest {
    private final DDlogAPI api1;
    private final DDlogAPI api2;

    TwoTest() throws DDlogException {
        this.api1 = new DDlogAPI(1, false);
        this.api2 = new DDlogAPI(1, false);
    }

    private DDlogRecCommand[] createCommand(DDlogCommand.Kind kind, String table, int argument) throws DDlogException {
        DDlogRecord r = new DDlogRecord(argument);
        DDlogRecord[] rs = new DDlogRecord[1];
        rs[0] = r;
        DDlogRecord o = DDlogRecord.makeStruct(table, rs);
        int id = this.api1.getTableId(table);
        DDlogRecCommand command = new DDlogRecCommand(kind, id, o);
        DDlogRecCommand[] result = new DDlogRecCommand[1];
        result[0] = command;
        return result;
    }

    public void run() throws DDlogException {
        this.api1.transactionStart();
        this.api2.transactionStart();
        DDlogRecCommand[] c1 = this.createCommand(DDlogCommand.Kind.Insert, "A1", 0);
        DDlogRecCommand[] c2 = this.createCommand(DDlogCommand.Kind.Insert, "A2", 1);
        this.api1.applyUpdates(c1);
        this.api2.applyUpdates(c2);
        this.api1.transactionCommit();
        this.api2.transactionCommit();
        this.api1.stop();
        this.api2.stop();
    }

    public static void main(String[] args) throws DDlogException {
        if (args.length != 0) {
            System.err.println("Usage: java -jar twotest.jar");
            System.exit(-1);
        }
        TwoTest twoTest = new TwoTest();
        twoTest.run();
    }
}
