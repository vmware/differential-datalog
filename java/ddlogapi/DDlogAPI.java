package ddlogapi;

import java.util.*;
import java.util.function.*;

/**
 * Java wrapper around Differential Datalog C API that manipulates
 * DDlog programs.
 */
public class DDlogAPI {
    static {
        System.loadLibrary("ddlogapi");
    }

    /**
     * The C ddlog API
     */
    native long ddlog_run(boolean storeData, int workers, String callbackName);
    static native int ddlog_record_commands(long hprog, String filename);
    static native int ddlog_stop_recording(long hprog, int fd);
    native int dump_table(long hprog, int table, String callbackMethod);
    static native int  ddlog_stop(long hprog);
    static native int ddlog_transaction_start(long hprog);
    static native int ddlog_transaction_commit(long hprog);
    native int ddlog_transaction_commit_dump_changes(long hprog, String callbackName);
    static native int ddlog_transaction_rollback(long hprog);
    static native int ddlog_apply_updates(long hprog, long[] upds);
    static native String ddlog_profile(long hprog);

    // All the following methods return in fact handles
    static public native void ddlog_free(long handle);
    // Constructors
    static public native long ddlog_bool(boolean b);
    static public native long ddlog_u64(long v);
    static public native long ddlog_u128(long hi, long lo);
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
    static public native boolean ddlog_get_u128(long handle, long[] output);  // true for success
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

    // File descriptor used to record DDlog command log
    private int record_fd = -1;

    public static final int success = 0;
    public static final int error = -1;
    // Maps table names to table IDs
    private final Map<String, Integer> tableId;
    // Callback to invoke for each modified record on commit.
    // The command supplied to the callback can only have an Insert or DeleteValue 'kind'.
    // This callback can be invoked simultaneously from multiple threads.
    private final Consumer<DDlogCommand> commitCallback;

    // Callback to invoke for each modified record on commit_dump_changes.
    // The command supplied to a callback can only have an Insert or DeleteValue 'kind'.
    // In the current implementation this callback is invoked sequentially
    // by the same DDlog worker thread.
    private Consumer<DDlogCommand> deltaCallback;

    // Callback to invoke for each record on `DDlogAPI.dump()`.
    // The callback is invoked sequentially by the same DDlog worker thread.
    private Consumer<DDlogRecord> dumpCallback;

    /**
     * Create an API to access the DDlog program.
     * @param workers   number of threads the DDlog program can use
     * @param storeData If true the DDlog background program will store a copy
     *                  of all tables, which can be obtained with "dump".
     * @param callback  A method that is invoked for every tuple added or deleted to
     *                  an output table.  The command argument indicates the table,
     *                  whether it is deletion or insertion, and the actual value
     *                  that is being inserted or deleted.  This callback is invoked
     *                  many times, on potentially different threads, when the "commit"
     *                  API function is called.
     */
    public DDlogAPI(int workers, Consumer<DDlogCommand> callback, boolean storeData) {
        this.tableId = new HashMap<String, Integer>();
        String onCommit = callback == null ? null : "onCommit";
        this.commitCallback = callback;
        this.hprog = this.ddlog_run(storeData, workers, onCommit);
    }

    /// Callback invoked from commit.
    void onCommit(int tableid, long handle, boolean polarity) {
        if (this.commitCallback != null) {
            DDlogCommand.Kind kind = polarity ? DDlogCommand.Kind.Insert : DDlogCommand.Kind.DeleteVal;
            DDlogRecord record = DDlogRecord.fromSharedHandle(handle);
            DDlogCommand command = new DDlogCommand(kind, tableid, record);
            this.commitCallback.accept(command);
        }
    }

    public int getTableId(String table) {
        if (!this.tableId.containsKey(table)) {
            int id = ddlog_get_table_id(table);
            this.tableId.put(table, id);
            return id;
        }
        return this.tableId.get(table);
    }

    // Record DDlog commands to file.
    // Set `filename` to `null` to stop recording.
    public int record_commands(String filename) {
        if (this.record_fd != -1) {
            DDlogAPI.ddlog_stop_recording(this.hprog, this.record_fd);
            this.record_fd = -1;
        }
        if (filename == null) {
            return 0;
        }

        int fd = DDlogAPI.ddlog_record_commands(this.hprog, filename);
        if (fd < 0) {
            return fd;
        } else {
            this.record_fd = fd;
            return 0;
        }
    }

    public int stop() {
        /* Close the file handle. */
        if (this.record_fd != -1) {
            DDlogAPI.ddlog_stop_recording(this.hprog, this.record_fd);
            this.record_fd = -1;
        }
        return DDlogAPI.ddlog_stop(this.hprog);
    }

    /**
     *  Starts a transaction
     */
    public int start() {
        return DDlogAPI.ddlog_transaction_start(this.hprog);
    }

    public int commit() {
        return DDlogAPI.ddlog_transaction_commit(this.hprog);
    }

    public int commit_dump_changes(Consumer<DDlogCommand> callback) {
        String onDelta = callback == null ? null : "onDelta";
        this.deltaCallback = callback;
        return this.ddlog_transaction_commit_dump_changes(this.hprog, onDelta);
    }

    /// Callback invoked from commit_dump_changes.
    void onDelta(int tableid, long handle, boolean polarity) {
        if (this.deltaCallback != null) {
            DDlogCommand.Kind kind = polarity ? DDlogCommand.Kind.Insert : DDlogCommand.Kind.DeleteVal;
            DDlogRecord record = DDlogRecord.fromSharedHandle(handle);
            DDlogCommand command = new DDlogCommand(kind, tableid, record);
            this.deltaCallback.accept(command);
        }
    }

    public int rollback() {
        return DDlogAPI.ddlog_transaction_rollback(this.hprog);
    }

    public int applyUpdates(DDlogCommand[] commands) {
        long[] handles = new long[commands.length];
        for (int i=0; i < commands.length; i++)
            handles[i] = commands[i].allocate();
        return ddlog_apply_updates(this.hprog, handles);
    }

    /// Callback invoked from dump.
    boolean dumpCallback(long handle) {
        if (this.dumpCallback != null) {
            DDlogRecord record = DDlogRecord.fromSharedHandle(handle);
            this.dumpCallback.accept(record);
        }
        return true;
    }

    /**
     * Dump the data in the specified table.
     * For this to work the DDlogAPI must have been created with a
     * storeData parameter set to true.
     */
    public int dump(String table, Consumer<DDlogRecord> callback) {
        int id = this.getTableId(table);
        if (id == -1)
            throw new RuntimeException("Unknown table " + table);
        String onDump = callback == null ? null : "dumpCallback";
        this.dumpCallback = callback;
        return this.dump_table(this.hprog, id, onDump);
    }

    public String profile() {
        return DDlogAPI.ddlog_profile(this.hprog);
    }
}
