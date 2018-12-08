package external;

/**
 * Declares external interfaces
 */
public class External {
    static {
        System.loadLibrary("external");
    }

    public External() {}
    public native void hello();
    public native String concat(String first, String second);
}
