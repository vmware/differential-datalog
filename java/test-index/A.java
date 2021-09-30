import ddlogapi.*;

public class A {
    private final DDlogAPI api;

    A() throws DDlogException {
        this.api= new DDlogAPI("mylib", 1, false);
    }

    private DDlogRecCommand[] createCommand(DDlogCommand.Kind kind, String table, int argument0, int argument1) throws DDlogException {
        DDlogRecord r0 = new DDlogRecord(argument0);
        DDlogRecord r1 = new DDlogRecord(argument1);
        DDlogRecord[] rs = new DDlogRecord[2];
        rs[0] = r0;
        rs[1] = r1;
        DDlogRecord o = DDlogRecord.makeStruct(table, rs);
        int id = this.api.getTableId(table);
        DDlogRecCommand command = new DDlogRecCommand(kind, id, o);
        DDlogRecCommand[] result = new DDlogRecCommand[1];
        result[0] = command;
        return result;
    }

    public void run() throws DDlogException {
        // Insert something in the input table
        this.api.transactionStart();
        DDlogRecCommand[] c1 = this.createCommand(DDlogCommand.Kind.Insert, "AI", 0, 1);
        this.api.applyUpdates(c1);
        // Apply the changes and get the result
        DDlogAPI.DDlogCommandVector result = this.api.transactionBatchCommit();
        assert result.size() == 1;
        DDlogRecCommand c = result.get(0);
        assert c.kind() == DDlogCommand.Kind.Insert;
        DDlogRecord rec = c.value();
        System.out.println(rec.toString());
        // Deallocate the result
        result.dispose();

        // Dump the entire index.
        this.api.dumpIndex("AII", i -> System.out.println("dump: " + i));
        rec = new DDlogRecord(0);
        // Reuse the same key to query the AIII index.
        this.api.queryIndex("AIII", rec, i -> System.out.println("query 0: " + i));
        // The key is no longer useful, we need to deallocate it explicitly.
        rec.dispose();

        // Create a new key for the AIII index
        rec = new DDlogRecord(1);
        this.api.queryIndex("AIII", rec, i -> System.out.println("query 1: " + i));
        // Deallocate the key
        rec.dispose();

        // Create a tuple key for the AIIII index
        DDlogRecord r0 = new DDlogRecord(0);
        DDlogRecord r1 = new DDlogRecord(1);
        DDlogRecord tuple = DDlogRecord.makeTuple(r0, r1);
        this.api.queryIndex("AIIII", tuple, i -> System.out.println("query (0,1): " + i));
        // Deallocate the key
        tuple.dispose();

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
