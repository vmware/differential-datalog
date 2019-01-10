package ddlogapi;

import java.util.*;
import java.util.function.*;

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
    native long ddlog_run(int workers, String callbackName);
    native int dump_table(long hprog, int table, String callbackMethod);
    static native int  ddlog_stop(long hprog);
    static native int ddlog_transaction_start(long hprog);
    static native int ddlog_transaction_commit(long hprog);
    static native int ddlog_transaction_rollback(long hprog);
    static native int ddlog_apply_updates(long hprog, long[] upds);
    static native String ddlog_profile(long hprog);

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
    static public native int ddlog_get_table_id(String table);
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
    static public native long ddlog_get_map_key(long handle, int i);
    static public native long ddlog_get_map_val(long handle, int i);
    static public native boolean ddlog_is_struct(long handle);
    static public native String ddlog_get_constructor(long handle);
    static public native long ddlog_get_struct_field(long handle, int i);

    static public native long ddlog_insert_cmd(int table, long recordHandle);
    static public native long ddlog_delete_val_cmd(int table, long recordHandle);
    static public native long ddlog_delete_key_cmd(int table, long recordHandle);

    // This is a handle to the program; it wraps a void*.
    private final long hprog;

    public static final int success = 0;
    public static final int error = -1;
    // Maps table names to table IDs
    private final Map<String, Integer> tableId;
    // Callback to invoke for each record on commit.
    // The command supplied to a callback can only have an Insert or DeleteValue 'kind'.
    // This callback can be invoked simultaneously from multiple threads.
    private final Consumer<DDLogCommand> commitCallback;

    public DDLogAPI(int workers, Consumer<DDLogCommand> callback) {
        this.tableId = new HashMap<String, Integer>();
        String onCommit = callback == null ? null : "onCommit";
        this.commitCallback = callback;
        this.hprog = this.ddlog_run(workers, onCommit);
    }

    /// Callback invoked from commit.
    boolean onCommit(int tableid, long handle, boolean polarity) {
        if (this.commitCallback != null) {
            DDLogCommand.Kind kind = polarity ? DDLogCommand.Kind.Insert : DDLogCommand.Kind.DeleteVal;
            DDLogRecord record = DDLogRecord.fromSharedHandle(handle);
            DDLogCommand command = new DDLogCommand(kind, tableid, record);
            this.commitCallback.accept(command);
        }
        return true;
    }

    public int getTableId(String table) {
        if (!this.tableId.containsKey(table)) {
            int id = ddlog_get_table_id(table);
            this.tableId.put(table, id);
            return id;
        }
        return this.tableId.get(table);
    }

    public int stop() {
        return DDLogAPI.ddlog_stop(this.hprog);
    }

    /**
     *  Starts a transaction
     */
    public int start() {
        return DDLogAPI.ddlog_transaction_start(this.hprog);
    }

    public int commit() {
        return DDLogAPI.ddlog_transaction_commit(this.hprog);
    }

    public int rollback() {
        return DDLogAPI.ddlog_transaction_rollback(this.hprog);
    }

    public int applyUpdates(DDLogCommand[] commands) {
        long[] handles = new long[commands.length];
        for (int i=0; i < commands.length; i++)
            handles[i] = commands[i].allocate();
        return ddlog_apply_updates(this.hprog, handles);
    }

    /// Callback invoked from dump.
    boolean dumpCallback(long handle) {
        DDLogRecord record = DDLogRecord.fromSharedHandle(handle);
        System.out.println(record.toString());
        return true;
    }

    public int dump(String table) {
        System.out.println(table + ":");
        int id = this.getTableId(table);
        if (id == -1)
            throw new RuntimeException("Unknown table " + table);
        return this.dump_table(this.hprog, id, "dumpCallback");
    }

    public int profile() {
        String s = DDLogAPI.ddlog_profile(this.hprog);
        System.out.println("Profile:");
        System.out.println(s);
        return 0;
    }
}
