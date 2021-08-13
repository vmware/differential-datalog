package ddlogapi;

import java.util.*;
import java.util.function.*;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.io.*;

/**
 * Java wrapper around Differential Datalog C API that manipulates
 * DDlog programs.
 */
public class DDlogAPI {
    static boolean nativeLibraryLoaded = false;

    /**
     * The C ddlog API
     */
    native long ddlog_run(boolean storeData, int workers) throws DDlogException;
    native long ddlog_run_with_config(
            boolean storeData,
            int nworkers,
            boolean enableDebugRegions,
            long differentialIdleMergeEffort,
            int profilingMode,
            int timelyLogMode,
            String timelyLogAddr,
            int timelyProgressLogMode,
            String timelyProgressLogAddr,
            int differentialLogMode,
            String differentialLogAddr) throws DDlogException;

    static native int ddlog_get_table_id(long hprog, String table);
    static native String ddlog_get_table_name(long hprog,int id) throws DDlogException;
    static native String ddlog_get_table_original_name(long hprog, String table) throws DDlogException;
    static native int ddlog_get_index_id(long hprog, String index);
    static native String ddlog_get_index_name(long hprog, int id) throws DDlogException;
    static native int ddlog_record_commands(long hprog, String filename, boolean append) throws DDlogException, IOException;
    static native void ddlog_stop_recording(long hprog, int fd) throws DDlogException;
    static native void ddlog_dump_input_snapshot(long hprog, String filename, boolean append) throws DDlogException, IOException;
    native void dump_table(long hprog, int table, String callbackMethod) throws DDlogException;
    native void dump_index(long hprog, int index, String callbackMethod) throws DDlogException;
    static native void ddlog_stop(long hprog, long callbackHandle) throws DDlogException;
    static native void ddlog_transaction_start(long hprog) throws DDlogException;
    static native void ddlog_transaction_commit(long hprog) throws DDlogException;
    native void ddlog_transaction_commit_dump_changes(long hprog, String callbackName) throws DDlogException;
    static native void ddlog_transaction_commit_dump_changes_to_flatbuf(long hprog, FlatBufDescr fb) throws DDlogException;
    static native void ddlog_flatbuf_free(ByteBuffer buf, long size, long offset);
    static native void ddlog_transaction_rollback(long hprog) throws DDlogException;
    static native void ddlog_apply_updates(long hprog, long[] upds) throws DDlogException;
    static native void ddlog_apply_updates_from_flatbuf(long hprog, byte[] bytes, int position) throws DDlogException;
    static native void ddlog_query_index_from_flatbuf(long hprog, byte[] bytes, int position, FlatBufDescr fb) throws DDlogException;
    static native void ddlog_dump_index_to_flatbuf(long hprog, long idxid, FlatBufDescr fb) throws DDlogException;
    static native int ddlog_clear_relation(long hprog, int relid);
    static native String ddlog_profile(long hprog);
    static native void ddlog_enable_cpu_profiling(long hprog, boolean enable) throws DDlogException;
    static native long ddlog_log_replace_callback(int module, long old_cbinfo, ObjIntConsumer<String> cb, int max_level);
    static native long ddlog_log_replace_default_callback(long old_cbinfo, ObjIntConsumer<String> cb, int max_level);

    static native void ddlog_free(long handle);

    /* All the following methods return in fact `ddlog_record` handles */

    // Constructors
    static native long ddlog_bool(boolean b);
    static native long ddlog_i64(long v);
    static native long ddlog_int(byte[] v);
    static native long ddlog_float(float f);
    static native long ddlog_double(double d);
    static native long ddlog_string(String s) throws DDlogException;
    static native long ddlog_tuple(long[] handles) throws DDlogException;
    static native long ddlog_vector(long[] handles) throws DDlogException;
    static native long ddlog_set(long[] handles) throws DDlogException;
    static native long ddlog_map(long[] handles) throws DDlogException;
    static native long ddlog_pair(long handle1, long handle2);
    static native long ddlog_struct(String constructor, long[] handles) throws DDlogException;
    static native long ddlog_named_struct(String constructor, String[] names, long[] handles) throws DDlogException;
    // Getters
    static native boolean ddlog_is_bool(long handle);
    static native boolean ddlog_get_bool(long handle);
    static native boolean ddlog_is_int(long handle);
    static native long ddlog_get_int(long handle, byte[] buf);
    static native boolean ddlog_is_float(long handle);
    static native float ddlog_get_float(long handle);
    static native boolean ddlog_is_double(long handle);
    static native double ddlog_get_double(long handle);
    static native boolean ddlog_is_string(long handle);
    static native String ddlog_get_str(long handle);
    static native boolean ddlog_is_tuple(long handle);
    static native int ddlog_get_tuple_size(long handle);
    static native long ddlog_get_tuple_field(long tup, int i);
    static native boolean ddlog_is_vector(long handle);
    static native int ddlog_get_vector_size(long handle);
    static native long ddlog_get_vector_elem(long vec, int idx);
    static native boolean ddlog_is_set(long handle);
    static native int ddlog_get_set_size(long handle);
    static native long ddlog_get_set_elem(long set, int i);
    static native boolean ddlog_is_map(long handle);
    static native int ddlog_get_map_size(long handle);
    static native long ddlog_get_map_key(long handle, int i);
    static native long ddlog_get_map_val(long handle, int i);
    static native boolean ddlog_is_struct(long handle);
    static native boolean ddlog_is_named_struct(long handle);
    static native String ddlog_get_constructor(long handle);
    static native long ddlog_get_struct_field(long handle, int i) throws DDlogException;
    static native String ddlog_get_struct_field_name(long handle, int i) throws DDlogException;

    static native long ddlog_insert_cmd(int table, long recordHandle);
    static native long ddlog_delete_val_cmd(int table, long recordHandle);
    static native long ddlog_delete_key_cmd(int table, long recordHandle);
    static native long ddlog_modify_cmd(int table, long keyHandle, long toModifyHandle);

    // This is a handle to the program; it wraps a void*.
    private long hprog;
    // This stores a C pointer which is deallocated when the program stops.
    // This field is written directly from the native code using reflection.
    public long callbackHandle;

    // File descriptor used to record DDlog command log
    private int record_fd = -1;

    // Maps table names to table IDs
    private final Map<String, Integer> tableId;

    // Callback to invoke for each modified record on commit_dump_changes.
    // The command supplied to a callback can only have an Insert or DeleteValue 'kind'.
    private Consumer<DDlogCommand<DDlogRecord>> deltaCallback;

    // Callback to invoke for each record on `DDlogAPI.dumpTable()`.
    // The callback is invoked sequentially by the same DDlog worker thread.
    private BiConsumer<DDlogRecord, Long> dumpCallback;

    // Callback to invoke for each record on `DDlogAPI.dumpIndex()`.
    // The callback is invoked sequentially by the same DDlog worker thread.
    private Consumer<DDlogRecord> dumpIndexCallback;

    // Stores pointer to `struct CallbackInfo` for each registered logging
    // callback.  This is needed so that we can deallocate the `CallbackInfo*`
    // when removing on changing the callback.
    private static final Map<Integer, Long> logCBInfo = new HashMap<>();
    private static Long defaultLogCBInfo;

    private void checkHandle() throws DDlogException {
        if (this.hprog == 0) {
            throw new DDlogException("DDlogAPI method invoked after the DDlog program has terminated.");
        }
    }

    /**
     * Create an API to access the DDlog program.
     * @param workers   number of threads the DDlog program can use
     * @param storeData If true the DDlog background program will store a copy
     *                  of all tables, which can be obtained with "dump".
     */
    public DDlogAPI(int workers, boolean storeData)
            throws DDlogException {
        ensureDllLoaded();
        this.tableId = new HashMap<String, Integer>();
        this.hprog = this.ddlog_run(storeData, workers);
    }

    /**
     * Create an API to access the DDlog program.
     * @param library   Name of the dynamic library to load.
     * @param workers   number of threads the DDlog program can use
     * @param storeData If true the DDlog background program will store a copy
     *                  of all tables, which can be obtained with "dump".
     */
    public DDlogAPI(String library, int workers, boolean storeData)
            throws DDlogException {
        ensureDllLoaded(library);
        this.tableId = new HashMap<String, Integer>();
        this.hprog = this.ddlog_run(storeData, workers);
    }

    /**
     * Create an API to access the DDlog program.
     * @param config    DDlog program configuration.
     * @param storeData If true the DDlog background program will store a copy
     *                  of all tables, which can be obtained with "dump".
     */
    public DDlogAPI(DDlogConfig config, boolean storeData)
            throws DDlogException {
            this(ddlogLibrary, config, storeData);
    }

    /**
     * Create an API to access the DDlog program.
     * @param library   Name of the dynamic library to load.
     * @param config    DDlog program configuration.
     * @param storeData If true the DDlog background program will store a copy
     *                  of all tables, which can be obtained with "dump".
     */
    public DDlogAPI(String library, DDlogConfig config, boolean storeData)
            throws DDlogException {
        ensureDllLoaded(library);
        this.tableId = new HashMap<String, Integer>();
        this.hprog = this.ddlog_run_with_config(
                storeData,
                config.getNumTimelyWorkers(),
                config.getEnableDebugRegions(),
                config.getDifferentialIdleMergeEffort(),
                config.getProfilingConfig().getMode().ordinal(),
                config.getProfilingConfig().getTimelyDestination().getMode().ordinal(),
                config.getProfilingConfig().getTimelyDestination().getAddressStr(),
                config.getProfilingConfig().getTimelyProgressDestination().getMode().ordinal(),
                config.getProfilingConfig().getTimelyProgressDestination().getAddressStr(),
                config.getProfilingConfig().getDifferentialDestination().getMode().ordinal(),
                config.getProfilingConfig().getDifferentialDestination().getAddressStr());
    }

    static void ensureDllLoaded(String libname) {
        /* Notice that, despite the fact that you can specify the library name,
           you cannot really have more than 1 ddlog library loaded at the same time,
           because there is no way to specify an entry point in the library for ddlog_run.
         */
        if (!nativeLibraryLoaded) {
            System.loadLibrary(libname);
            nativeLibraryLoaded = true;
        }
    }

    static void ensureDllLoaded() {
        ensureDllLoaded(ddlogLibrary);
    }

    /**
     * Get DDlog relation ID from its name.
     * @param table  relation name whose id is sought.
     * @return -1 when the relation is not found,
     *          otherwise the relation id, a positive number
     *
     * See <code>ddlog.h: ddlog_get_table_id()</code>
     */
    public int getTableId(String table) throws DDlogException {
        this.checkHandle();
        if (!this.tableId.containsKey(table)) {
            int id = ddlog_get_table_id(this.hprog, table);
            this.tableId.put(table, id);
            return id;
        }
        return this.tableId.get(table);
    }

    /**
     * Get DDlog relation name from ID.
     *
     * See <code>ddlog.h: ddlog_get_table_name()</code>
     */
    public String getTableName(int id) throws DDlogException {
        this.checkHandle();
        return ddlog_get_table_name(this.hprog, id);
    }

    /**
     * Given a table name, returns the original name (from the 'original' DDlog
     * relation annotation), if present, or the table name itself otherwise.
     *
     * See <code>ddlog.h: ddlog_get_table_original_name()</code>
     */
    public String getTableOriginalName(String table) throws DDlogException {
        this.checkHandle();
        return ddlog_get_table_original_name(this.hprog, table);
    }

    /**
     * Get DDlog index ID from its name.
     * @param table  table name whose id is sought.
     * @return -1 when the index is not found,
     *          otherwise a positive number.
     *
     * See <code>ddlog.h: ddlog_get_index_id()</code>
     */
    public int getIndexId(String table) throws DDlogException {
        this.checkHandle();
        return ddlog_get_index_id(this.hprog, table);
    }

    /**
     * Get DDlog index name from ID.
     *
     * See <code>ddlog.h: ddlog_get_index_name()</code>
     */
    public String getIndexName(int id) throws DDlogException {
        this.checkHandle();
        return ddlog_get_index_name(this.hprog, id);
    }

    /**
     * Record commands issued to DDlog via this API in a file.
     *
     * See <code>ddlog.h: ddlog_record_commands()</code>
     *
     * @param filename File to record command to or <code>null</code> to stop recording.
     * @param append Set to <code>true</code> to open the file in append mode.
     */
    public void recordCommands(String filename, boolean append) throws DDlogException, IOException {
        this.checkHandle();
        if (this.record_fd != -1) {
            DDlogAPI.ddlog_stop_recording(this.hprog, this.record_fd);
            this.record_fd = -1;
        }
        if (filename == null) {
            return;
        }

        this.record_fd = DDlogAPI.ddlog_record_commands(this.hprog, filename, append);
    }

    public void dumpInputSnapshot(String filename, boolean append) throws DDlogException, IOException {
        this.checkHandle();
        DDlogAPI.ddlog_dump_input_snapshot(this.hprog, filename, append);
    }

    /**
     * Stops the DDlog program; deallocate all resources.
     *
     * See <code>ddlog.h: ddlog_stop()</code>
     */
    public void stop() throws DDlogException {
        this.checkHandle();
        /* Close the file handle. */
        if (this.record_fd != -1) {
            DDlogAPI.ddlog_stop_recording(this.hprog, this.record_fd);
            this.record_fd = -1;
        }
        ddlog_stop(this.hprog, this.callbackHandle);
        this.hprog = 0;
    }

    /**
     * Start a transaction.
     *
     * See <code>ddlog.h: ddlog_start()</code>
     */
    public void transactionStart() throws DDlogException {
        this.checkHandle();
        DDlogAPI.ddlog_transaction_start(this.hprog);
    }

    /**
     * Commit a transaction.
     *
     * See <code>ddlog.h: ddlog_transaction_commit()</code>
     */
    public void transactionCommit() throws DDlogException {
        this.checkHandle();
        DDlogAPI.ddlog_transaction_commit(this.hprog);
    }

    /**
     * Commit a transaction; invoke <code>callback</code> for each inserted or deleted record in each output relation.
     *
     * See <code>ddlog.h: ddlog_transaction_commit_dump_changes()</code>
     */
    public void transactionCommitDumpChanges(Consumer<DDlogCommand<DDlogRecord>> callback)
            throws DDlogException {
        this.checkHandle();
        String onDelta = callback == null ? null : "onDelta";
        this.deltaCallback = callback;
        this.ddlog_transaction_commit_dump_changes(this.hprog, onDelta);
    }

    /**
     * This class represents a vector of DDlogCommands.
     * The data must be deallocated by calling dispose; at that point
     * all records are no longer valid.
     */
    public static class DDlogCommandVector {
        native void ddlog_transaction_batch_commit(long hprog) throws DDlogException;
        native static void ddlog_batch_free(long pointer, int size);

        private final long hprog;

        protected DDlogCommandVector(long hprog) {
            this.data = null;
            this.hprog = hprog;
            this.data = new ArrayList<DDlogRecCommand>();
        }

        private List<DDlogRecCommand> data;

        void fill() throws DDlogException {
            this.ddlog_transaction_batch_commit(this.hprog);
        }

        /**
         * This method is called from the native side to add a command.
         */
        private void append(int table, long handle, long w) {
            if (this.data == null)
                return;

            DDlogCommand.Kind kind = w > 0 ? DDlogCommand.Kind.Insert : DDlogCommand.Kind.DeleteVal;
            DDlogRecord record = DDlogRecord.fromSharedHandle(handle);
            DDlogRecCommand command = new DDlogRecCommand(kind, java.lang.Math.abs(w), table, record);
            this.data.add(command);
        }

        /**
         * Number of values in vector.
         */
        public int size() {
            if (this.data == null)
                return 0;
            return this.data.size();
        }

        /**
         * Get the command with the i-th index.
         * @param index  Index of the command in the vector.
         * @return       The i-th command; throws if there are no commands or index is out of bounds.
         */
        public DDlogRecCommand get(int index) throws DDlogException {
            if (this.data == null)
                throw new DDlogException("No data");
            return this.data.get(index);
        }

        /**
         * The native world will save here pointers to the native data structures.
         * These are used by the dispose call.
         */
        private int size;
        private long pointer;

        public void dispose() {
            ddlog_batch_free(this.pointer, this.size);
            this.size = -1;
            this.pointer = 0;
        }
    }

    /**
     * Commit a transaction, return all changes in batch.
     * @return   all changes.
     * See <code>ddlog.h: ddlog_transaction_commit_dump_changes_as_array()</code>
     */
    public DDlogCommandVector transactionBatchCommit()
            throws DDlogException {
        this.checkHandle();
        DDlogCommandVector vector = new DDlogCommandVector(this.hprog);
        vector.fill();
        return vector;
    }

    /**
     * Remove all records from an input relation.
     *
     * See <code>ddlog.h: ddlog_clear_relation()</code>
     */
    public int clearRelation(int relid) throws DDlogException {
        this.checkHandle();
        return ddlog_clear_relation(this.hprog, relid);
    }

    // Callback invoked from commit_dump_changes.
    void onDelta(int tableid, long handle, long weight) {
        if (this.deltaCallback != null) {
            DDlogCommand.Kind kind = weight > 0 ? DDlogCommand.Kind.Insert : DDlogCommand.Kind.DeleteVal;
            DDlogRecord record = DDlogRecord.fromSharedHandle(handle);
            DDlogRecCommand command = new DDlogRecCommand(kind, java.lang.Math.abs(weight), tableid, record);
            this.deltaCallback.accept(command);
        }
    }

    // The FlatBufDescr class and commitDumpChangesToFlatbuf method are not
    // meant to be invoked directly by user code; they are only used by the
    // autogenerated FlatBuffer API code.
    public static class FlatBufDescr {
        public FlatBufDescr() {
            this.buf = null;
            this.size = 0;
            this.offset = 0;
        }
        public void set(ByteBuffer buf, long size, long offset) {
            this.buf = buf;
            this.size = size;
            this.offset = offset;
        }
        public ByteBuffer buf;
        public long size;
        public long offset;
    }

    /**
     * Commit transaction, serializing changes to a FlatBuffer.
     *
     * See <code>ddlog.h: ddlog_transaction_commit_dump_changes_to_flatbuf()</code>
     *
     * This method is for use by the <code>UpdateParser</code> class only and should not
     * be invoked by user code.
     */
    public void transactionCommitDumpChangesToFlatbuf(FlatBufDescr fb) throws DDlogException {
        this.checkHandle();
        DDlogAPI.ddlog_transaction_commit_dump_changes_to_flatbuf(this.hprog, fb);
    }

    /**
     * Deallocate flabuffer returned by <code>transactionCommitDumpChangesToFlatbuf</code>.
     *
     * See <code>ddlog.h: ddlog_flatbuf_free()</code>
     *
     * This method is for use by the <code>UpdateParser</code> class only and should not
     * be invoked by user code.
     */
    public void flatbufFree(FlatBufDescr fb) {
        DDlogAPI.ddlog_flatbuf_free(fb.buf, fb.size, fb.offset);
    }

    /**
     * Discard all buffered updates and abort the current transaction.
     *
     * See <code>ddlog.h: ddlog_transaction_rollback()</code>
     */
    public void transactionRollback() throws DDlogException {
        this.checkHandle();
        DDlogAPI.ddlog_transaction_rollback(this.hprog);
    }

    /**
     * Apply updates to DDlog input relations.
     *
     * See <code>ddlog.h: ddlog_apply_updates()</code>
     */
    public void applyUpdates(DDlogRecCommand[] commands) throws DDlogException {
        this.checkHandle();
        long[] handles = new long[commands.length];
        for (int i=0; i < commands.length; i++)
            handles[i] = commands[i].allocate();
        ddlog_apply_updates(this.hprog, handles);
    }

    /**
     * Apply updates to DDlog input relations, serialized in a FlatBuffer.
     *
     * See <code>ddlog.h: ddlog_apply_updates_from_flatbuf()</code>.
     *
     * This method is for use by the <code>UpdateBuilder</code> class only and should not
     * be invoked by user code.
     */
    public void applyUpdatesFromFlatBuf(ByteBuffer buf) throws DDlogException {
        this.checkHandle();
        ddlog_apply_updates_from_flatbuf(this.hprog, buf.array(), buf.position());
    }

    /**
     * Perform a DDlog index query serialized in a flatbuf; returns result in
     * another flatbuf.
     *
     * See <code>ddlog.h: ddlog_query_index_from_flatbuf()</code>.
     *
     * This method is for use by the <code>XXXQuery</code> class only and should not
     * be invoked by user code.
     */
    public void queryIndexFromFlatBuf(ByteBuffer buf, FlatBufDescr resfb) throws DDlogException {
        this.checkHandle();
        ddlog_query_index_from_flatbuf(this.hprog, buf.array(), buf.position(), resfb);
    }

    /**
     * Dump all values in a DDlog index to flatbuf.
     *
     * See <code>ddlog.h: ddlog_dump_index_to_flatbuf()</code>.
     *
     * This method is for use by the <code>XXXQuery</code> class only and should not
     * be invoked by user code.
     */
    public void dumpIndexToFlatBuf(long idxid, FlatBufDescr resfb) throws DDlogException {
        this.checkHandle();
        ddlog_dump_index_to_flatbuf(this.hprog, idxid, resfb);
    }

    /// Callback invoked from dump.
    boolean dumpCallback(long handle, long weight) {
        if (this.dumpCallback != null) {
            DDlogRecord record = DDlogRecord.fromSharedHandle(handle);
            this.dumpCallback.accept(record, weight);
        }
        return true;
    }

    /**
     * Dump the data in the specified *output* table.
     *
     * For this to work the DDlogAPI must have been created with a
     * storeData parameter set to true.
     * @param table DDlog relation name
     * @param callback  Callback invoked with each record inserted or deleted from the table.
     *                  The second argument can be larger than 1 for a multiset relation.
     * Note: this method is not thread-safe: once invoked it should not
     * be invoked again until the previous invocation has returned.
     */
    public void dumpTable(String table, BiConsumer<DDlogRecord, Long> callback) throws DDlogException {
        this.checkHandle();
        int id = this.getTableId(table);
        if (id == -1)
            throw new RuntimeException("Unknown table " + table);
        String onDump = callback == null ? null : "dumpCallback";
        this.dumpCallback = callback;
        this.dump_table(this.hprog, id, onDump);
    }

    /// Callback invoked from dumpIndex.
    void dumpIndexCallback(long handle) {
        if (this.dumpIndexCallback != null) {
            DDlogRecord record = DDlogRecord.fromSharedHandle(handle);
            this.dumpIndexCallback.accept(record);
        }
    }

    /**
     * Dump the data in the specified index.
     * @param index     DDlog index name
     * @param callback  Callback invoked with each record inserted or deleted from the index.
     * Note: this method is not thread-safe: once invoked it should not
     * be invoked again until the previous invocation has returned.
     */
    public void dumpIndex(String index, Consumer<DDlogRecord> callback) throws DDlogException {
        this.checkHandle();
        int id = this.getIndexId(index);
        if (id == -1)
            throw new RuntimeException("Unknown index " + index);
        String onDump = callback == null ? null : "dumpIndexCallback";
        this.dumpIndexCallback = callback;
        this.dump_index(this.hprog, id, onDump);
    }

    /**
     * Returns DDlog program runtime profile as a string.
     *
     * See <code>ddlog.h: ddlog_profile()</code>
     */
    public String profile() throws DDlogException {
        this.checkHandle();
        return DDlogAPI.ddlog_profile(this.hprog);
    }

    /**
     * Controls recording of differential operator runtimes.
     *
     * See <code>ddlog.h: ddlog_enable_cpu_profiling()</code>
     */
    public void enableCpuProfiling(boolean enable) throws DDlogException {
        this.checkHandle();
        DDlogAPI.ddlog_enable_cpu_profiling(this.hprog, enable);
    }

    /**
     * Control DDlog logging behavior
     *
     * See detailed explanation of the logging API in <code>lib/log.dl</code>.
     *
     * @param module    Module id for logging purposes.  Must match module ids
     *                  used in the DDlog program.
     * @param cb        Logging callback that takes log message level and the message
     *                  itself.  Passing <code>null</code> disables logging for the given module.
     * @param max_level Maximal enabled log level.  Log messages with this module
     *                  id and log level above <code>max_level</code> will be dropped by DDlog
     *                  (i.e., the callback will not be invoked for  those messages).
     */
    static public synchronized void logSetCallback(int module, ObjIntConsumer<String> cb, int max_level) {
        ensureDllLoaded();
        Long old_cbinfo = logCBInfo.remove(module);
        long new_cbinfo = ddlog_log_replace_callback(module, old_cbinfo == null ? 0 : old_cbinfo, cb, max_level);
        /* Store pointer to CallbackInfo in internal map */
        if (new_cbinfo != 0) {
            logCBInfo.put(module, new_cbinfo);
        }
    }

    static public synchronized void logSetDefaultCallback(ObjIntConsumer<String> cb, int max_level) {
        ensureDllLoaded();
        Long old_cbinfo = defaultLogCBInfo;
        defaultLogCBInfo = null;
        long new_cbinfo = ddlog_log_replace_default_callback(old_cbinfo == null ? 0 : old_cbinfo, cb, max_level);
        /* Store pointer to CallbackInfo in internal map */
        if (new_cbinfo != 0) {
            defaultLogCBInfo = new_cbinfo;
        }
    }

    /*--------------------------------------------*/

    /**
     * Get the path where DDlog is installed. This queries the DDLOG_HOME
     * environment variable. This throws if the variable is not set.
     */
    public static String ddlogInstallationPath() throws DDlogException {
        String ddlogHome = System.getenv("DDLOG_HOME");
        if (ddlogHome == null)
            throw new DDlogException("No DDLOG_HOME");
        return ddlogHome;
    }

    /**
     * This class is used to collect the standard output and the standard error
     * streams from background processes.
     */
    public static class CompilationResult {
        /**
         * Exit code of last process executed.
         */
        public int exitCode;
        StringBuilder stdout;
        StringBuilder stderr;
        final boolean verbose;  // if true echo messages

        public CompilationResult(boolean verbose) {
            this.exitCode = 0;
            this.verbose = verbose;
            this.stdout = new StringBuilder();
            this.stderr = new StringBuilder();
        }

        synchronized void appendError(String str) {
            if (this.verbose)
                System.err.println(str);
            this.stderr.append(str);
        }

        synchronized void appendOut(String str) {
            if (this.verbose)
                System.out.println(str);
            this.stdout.append(str);
        }

        void setExitCode(int exitCode) {
            this.exitCode = exitCode;
        }

        public boolean isSuccess() {
            return this.exitCode == 0;
        }

        /**
         * Get the standard error data collected.  Should be called
         * when the monitored streams have been closed.
         */
        public String getStderr() {
            return this.stderr.toString();
        }

        /**
         * Get the standard output data collected.  Should be called
         * when the monitored streams have been closed.
         */
        public String getStdout() {
            return this.stdout.toString();
        }

        /**
         * Starts a background thread to collect all the data
         * from the specified stream into stdout.  The thread stops when
         * the stream is closed.
         */
        void consumeOutputStream(InputStream stream) {
            Runnable r = () -> new BufferedReader(new InputStreamReader(stream)).lines().forEach(
                    CompilationResult.this::appendOut);
            new Thread(r).start();
        }

        /**
         * Starts a background thread to collect all the data
         * from the specified stream into stderr.  The thread stops when
         * the stream is closed.
         */
        void consumeErrorStream(InputStream stream) {
            Runnable r = () -> new BufferedReader(new InputStreamReader(stream)).lines().forEach(
                    CompilationResult.this::appendError);
            new Thread(r).start();
        }
    }

    /**
     * Run an external process by executing the specified command.
     * @param commands        Command and arguments.
     * @param workdirectory   If not null the working directory.
     * @param result          Produce here a description of the compilation process.
     */
    public static void runProcess(List<String> commands, String workdirectory, CompilationResult result) {
        try {
            result.appendOut("Running " + String.join(" ", commands) +
                    (workdirectory != null ? " in " + workdirectory : ""));
            ProcessBuilder pb = new ProcessBuilder(commands);
            if (workdirectory != null) {
                pb.directory(new File(workdirectory));
            }
            Process process = pb.start();
            result.consumeOutputStream(process.getInputStream());
            result.consumeErrorStream(process.getErrorStream());
            int exitCode = process.waitFor();
            result.setExitCode(exitCode);
            if (exitCode != 0)
                result.appendError("Error running " + String.join(" ", commands));
        } catch (Exception ex) {
            result.setExitCode(1);
            result.appendOut("Error running " + String.join(" ", commands));
            result.appendOut(ex.getMessage());
        }
    }

    public static final String ddlogLibrary = "ddlogapi";

    public static String libName(String lib) {
        String os = System.getProperty("os.name").toLowerCase();
        if (os.equals("darwin") || os.equals("mac os x"))
            return "lib" + lib + ".dylib";
        else if (os.equals("windows"))
            return lib + ".dll";
        else
            return "lib" + lib + ".so";
    }

    /**
     * Compile a ddlog program stored in a file and generate Rust sources in a directory
     * named <program>_ddlog
     * @param ddlogFile  Pathname to the ddlog program.
     * @param result     Produce here a description of the compilation process.
     * @param ddlogLibraryPath  Additional list of paths for needed ddlog libraries.
     */
    public static void compileDDlogProgramToRust(
            String ddlogFile,
            CompilationResult result,
            String... ddlogLibraryPath) {
        Path path = Paths.get(ddlogFile);
        Path dir = path.getParent();
        Path file = path.getFileName();

        List<String> command = new ArrayList<String>();
        // Run DDlog compiler
        command.add("ddlog");
        command.add("-i");
        String currentDir = System.getProperty("user.dir");
        command.add(file.toString());
        for (String s: ddlogLibraryPath) {
            command.add("-L");
            if (!s.startsWith("/"))
                s = currentDir + "/" + s;
            command.add(s);
        }
        runProcess(command, dir != null ? dir.toString() : null, result);
    }

    /**
     * Compile a ddlog program stored in a file and generate
     * a shared library named *ddlogapi.* (according to host system conventions).
     * @param ddlogFile  Pathname to the ddlog program.
     * @param result     Produce here a description of the compilation process.
     * @param ddlogLibraryPath  Additional list of paths for needed ddlog libraries.
     */
    public static void compileDDlogProgram(
            String ddlogFile,
            CompilationResult result,
            String... ddlogLibraryPath) throws DDlogException {
        compileDDlogProgram(ddlogFile, libName(ddlogLibrary), result, ddlogLibraryPath);
    }

    /**
     * Compile a ddlog program stored in a file and generate
     * a shared library with the specified name.
     * @param ddlogFile  Pathname to the ddlog program.
     * @param outLibName Name of the library that should be generated.
     * @param result    Produce here a description of the compilation process.
     * @param ddlogLibraryPath  Additional list of paths for needed ddlog libraries.
     */
    public static void compileDDlogProgram(
        String ddlogFile,
        String outLibName,
        CompilationResult result,
        String... ddlogLibraryPath) throws DDlogException {
        compileDDlogProgramToRust(ddlogFile, result, ddlogLibraryPath);
        if (!result.isSuccess())
            return;

        String os = System.getProperty("os.name").toLowerCase();
        String ddlogInstallationPath = ddlogInstallationPath();

        List<String> command = new ArrayList<String>();
        command.add("cargo");
        command.add("build");
        command.add("--release");
        int dot = ddlogFile.lastIndexOf('.');
        String rustDir = ddlogFile;
        if (dot >= 0)
            rustDir = ddlogFile.substring(0, dot);
        rustDir += "_ddlog";
        runProcess(command, rustDir, result);
        if (!result.isSuccess())
            return;

        // Run C compiler
        command.clear();
        command.add("cc");
        command.add("-shared");
        command.add("-fPIC");
        String javaHome = System.getenv("JAVA_HOME");

        String ospath = os;
        if (os.equals("mac os x"))
            ospath = "darwin";
        command.add("-I" + javaHome + "/include");
        command.add("-I" + javaHome + "/include/" + ospath);
        command.add("-I" + rustDir);
        command.add("-I" + ddlogInstallationPath + "/lib");
        command.add(ddlogInstallationPath + "/java/ddlogapi.c");
        command.add("-L" + rustDir + "/target/release/");
        String libRoot = Paths.get(rustDir).getFileName().toString();
        command.add("-l" + libRoot);
        command.add("-o");
        command.add(outLibName);
        runProcess(command, null, result);
    }

    static boolean loaded = false;

    /**
     * Load the the ddlogLibrary in the current process.
     * @return The API that can be used to interact with this library.
     */
    public static DDlogAPI loadDDlog() throws DDlogException {
        if (loaded) {
            return new ddlogapi.DDlogAPI(1, false);
        }

        loaded = true;
        final Path libraryPath = Paths.get(libName(ddlogLibrary)).toAbsolutePath();
        System.load(libraryPath.toString());
        return new ddlogapi.DDlogAPI(1, false);
    }
}
