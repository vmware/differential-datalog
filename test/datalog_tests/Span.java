import ddlogapi.DDLogAPI;
import ddlogapi.DDLogCommand;
import ddlogapi.DDLogRecord;

public class Span {
    public static void main(String[] args) {
        DDLogAPI api = new DDLogAPI(1);
        System.out.println("Hi");
        int e = api.start();
        System.out.println("Start " + e);
        int e = api.commit();
        System.out.println("Commit " + e);
        e = api.stop();
        System.out.println("Stop " + e);
    }
}
