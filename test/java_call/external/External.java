package external;

/**
 * Declares external interfaces
 */
public class External {
     static {
         System.loadLibrary("external");
     }

    public External() {}
    public native void print();
}
