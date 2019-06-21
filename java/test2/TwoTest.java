import java.io.IOException;
import java.util.*;
import java.lang.RuntimeException;

import ddlogapi.DDlogAPI;
import ddlogapi.DDlogCommand;
import ddlogapi.DDlogRecord;

public class TwoTest {
    private final DDlogAPI api1;
    private final DDlogAPI api2;

    TwoTest() {
        this.api1 = new DDlogAPI(1, r -> this.onCommit(r), false);
        this.api2 = new DDlogAPI(1, r -> this.onCommit(r), false);
    }

    synchronized void onCommit(DDlogCommand command) {
        System.out.println(command.toString());
    }

    private DDlogCommand[] createCommand(DDlogCommand.Kind kind, String table, int argument) {
        DDlogRecord r = new DDlogRecord(argument);
        DDlogRecord[] rs = new DDlogRecord[1];
        rs[0] = r;
        DDlogRecord o = DDlogRecord.makeStruct(table, rs);
        int id = this.api1.getTableId(table);
        DDlogCommand command = new DDlogCommand(kind, id, o);
        DDlogCommand[] result = new DDlogCommand[1];
        result[0] = command;
        return result;
    }

    public void run() {
        this.api1.start();
        this.api2.start();
        DDlogCommand[] c1 = this.createCommand(DDlogCommand.Kind.Insert, "A1", 0);
        DDlogCommand[] c2 = this.createCommand(DDlogCommand.Kind.Insert, "A2", 1);
        this.api1.applyUpdates(c1);
        this.api2.applyUpdates(c2);
        this.api1.commit();
        this.api2.commit();
    }

    public static void main(String[] args) throws IOException {
        if (args.length != 0) {
            System.err.println("Usage: java -jar twotest.jar");
            System.exit(-1);
        };
        TwoTest twoTest = new TwoTest();
        twoTest.run();
    }
}
