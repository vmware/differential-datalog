package ddlogapi;

public class DDlogException extends Exception {
    String msg;

    public DDlogException(String msg) {
        this.msg = msg;
    }

    public String toString() {
        return "DDlogException: " + msg;
    }
}
