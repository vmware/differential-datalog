package ddlogapi;

/**
 * Java wrapper around Differential Datalog C API that manipulates
 * DDlog programs.
 */
public class DDLogAPI {
    static {
        System.loadLibrary("ddlogapi");
    }

    /**
     * The C ddlog API
     */
    static native long datalog_example_run(int workers);
    static native int  datalog_example_stop(long hprog);
    static native int datalog_example_transaction_start(long hprog);
    static native int datalog_example_transaction_commit(long hprog);
    static native int datalog_example_transaction_rollback(long hprog);
    static public native int datalog_example_apply_updates(long hprog, long[] upds);

    // All the following methods return in fact handles
    static public native void ddlog_free(long handle);
    // Constructors
    static public native long ddlog_bool(boolean b);
    static public native long ddlog_u64(long v);
    static public native long ddlog_string(String s);
    static public native long ddlog_tuple(long[] handles);
    static public native long ddlog_vector(long[] handles);
    static public native long ddlog_set(long[] handles);
    static public native long ddlog_map(long[] handles);
    static public native long ddlog_pair(long handle1, long handle2);
    static public native long ddlog_struct(String constructor, long[] handles);
    // Getters
    static public native boolean ddlog_is_bool(long handle);
    static public native boolean ddlog_get_bool(long handle);
    static public native boolean ddlog_is_int(long handle);
    static public native long ddlog_get_u64(long handle);
    static public native boolean ddlog_is_string(long handle);
    static public native String ddlog_get_str(long handle);
    static public native boolean ddlog_is_tuple(long handle);
    static public native int ddlog_get_tuple_size(long handle);
    static public native long ddlog_get_tuple_field(long tup, int i);
    static public native boolean ddlog_is_vector(long handle);
    static public native int ddlog_get_vector_size(long handle);
    static public native long ddlog_get_vector_elem(long vec, int idx);
    static public native boolean ddlog_is_set(long handle);
    static public native int ddlog_get_set_size(long handle);
    static public native long ddlog_get_set_elem(long set, int i);
    static public native boolean ddlog_is_map(long handle);
    static public native int ddlog_get_map_size(long handle);
    static public native long ddlog_get_map_key(long map, int i);
    static public native long ddlog_get_map_val(long map, int i);
    static public native boolean ddlog_is_pos_struct(long handle);
    static public native String ddlog_get_constructor(long handle);
    static public native long ddlog_get_struct_field(long handle, int i);

    static public native long ddlog_insert_cmd(String table, long recordHandle);
    static public native long ddlog_delete_val_cmd(String table, long recordHandle);
    static public native long ddlog_delete_key_cmd(String table, long recordHandle);

    // This is a handle to the program; it wraps a void*.
    private long hprog;

    public static final int success = 0;
    public static final int error = -1;

    public DDLogAPI(int workers) {
        this.hprog = datalog_example_run(workers);
    }

    public DDLogAPI() {
        this(1);
    }

    public int stop() {
        return DDLogAPI.datalog_example_stop(this.hprog);
    }

    /**
     *  Starts a transaction
     */
    public int start() {
        return DDLogAPI.datalog_example_transaction_start(this.hprog);
    }
    public int commit() {
        return DDLogAPI.datalog_example_transaction_commit(this.hprog);
    }
    public int rollback() {
        return DDLogAPI.datalog_example_transaction_rollback(this.hprog);
    }
    public int applyUpdates(DDLogCommand[] commands) {
        long[] handles = new long[commands.length];
        for (int i=0; i < commands.length; i++)
            handles[i] = commands[i].allocate();
        return datalog_example_apply_updates(this.hprog, handles);
    }
}
