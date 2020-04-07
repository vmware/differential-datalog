import ddlogapi.*;

public class A {
    private final DDlogAPI api;

    A() throws DDlogException {
        this.api= new DDlogAPI(1, null, false);
    }

    private DDlogRecCommand[] createCommand(DDlogCommand.Kind kind, String table, int argument) throws DDlogException {
        DDlogRecord r = new DDlogRecord(argument);
        DDlogRecord[] rs = new DDlogRecord[1];
        rs[0] = r;
        DDlogRecord o = DDlogRecord.makeStruct(table, rs);
        int id = this.api.getTableId(table);
        DDlogRecCommand command = new DDlogRecCommand(kind, id, o);
        DDlogRecCommand[] result = new DDlogRecCommand[1];
        result[0] = command;
        return result;
    }

    public void run() throws DDlogException {
        this.api.transactionStart();
        DDlogRecCommand[] c1 = this.createCommand(DDlogCommand.Kind.Insert, "AI", 0);
        this.api.applyUpdates(c1);
        DDlogAPI.DDlogCommandVector result = this.api.transactionBatchCommit();
        assert result.size() == 1;
        DDlogRecCommand c = result.get(0);
        assert c.kind() == DDlogCommand.Kind.Insert;
        DDlogRecord rec = c.value();
        System.out.println(rec.toString());
        result.dispose();
        this.api.stop();
    }

    public static void main(String[] args) throws DDlogException {
        if (args.length != 0) {
            System.exit(-1);
        }
        A a = new A();
        a.run();
    }
}
