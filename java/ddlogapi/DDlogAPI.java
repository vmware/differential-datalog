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
    native long ddlog_run(boolean storeData, int workers, String callbackName) throws DDlogException;
    static native int ddlog_record_commands(long hprog, String filename, boolean append) throws DDlogException, IOException;
    static native void ddlog_stop_recording(long hprog, int fd) throws DDlogException;
    static native void ddlog_dump_input_snapshot(long hprog, String filename, boolean append) throws DDlogException, IOException;
    native void dump_table(long hprog, int table, String callbackMethod) throws DDlogException;
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
    // Getters
    static native int ddlog_get_table_id(String table);
    static native String ddlog_get_table_name(int id);
    static native boolean ddlog_is_bool(long handle);
    static native boolean ddlog_get_bool(long handle);
    static native boolean ddlog_is_int(long handle);
    static native long ddlog_get_int(long handle, byte[] buf);
    static native long ddlog_get_i64(long handle);
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
    static native String ddlog_get_constructor(long handle);
    static native long ddlog_get_struct_field(long handle, int i);

    static native long ddlog_insert_cmd(int table, long recordHandle);
    static native long ddlog_delete_val_cmd(int table, long recordHandle);
    static native long ddlog_delete_key_cmd(int table, long recordHandle);

    // This is a handle to the program; it wraps a void*.
    private long hprog;
    // This stores a C pointer which is deallocated when the program stops.
    // This field is written directly from the native code using reflection.
    public long callbackHandle;

    // File descriptor used to record DDlog command log
    private int record_fd = -1;

    // Maps table names to table IDs
    private final Map<String, Integer> tableId;
    // Callback to invoke for each modified record on commit.
    // The command supplied to the callback can only have an Insert or DeleteValue 'kind'.
    // This callback can be invoked simultaneously from multiple threads.
    private final Consumer<DDlogCommand<DDlogRecord>> commitCallback;

    // Callback to invoke for each modified record on commit_dump_changes.
    // The command supplied to a callback can only have an Insert or DeleteValue 'kind'.
    private Consumer<DDlogCommand<DDlogRecord>> deltaCallback;

    // Callback to invoke for each record on `DDlogAPI.dumpTable()`.
    // The callback is invoked sequentially by the same DDlog worker thread.
    private Consumer<DDlogRecord> dumpCallback;

    // Stores pointer to `struct CallbackInfo` for each registered logging
    // callback.  This is needed so that we can deallocate the `CallbackInfo*`
    // when removing on changing the callback.
    private static Map<Integer, Long> logCBInfo = new HashMap<>();
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
     * @param callback  A method that is invoked for every tuple added or deleted to
     *                  an output table.  The command argument indicates the table,
     *                  whether it is deletion or insertion, and the actual value
     *                  that is being inserted or deleted.  This callback is invoked
     *                  many times, on potentially different threads, when the "commit"
     *                  API function is called.
     */
    public DDlogAPI(int workers, Consumer<DDlogCommand<DDlogRecord>> callback, boolean storeData)
            throws DDlogException {
        ensureDllLoaded();
        this.tableId = new HashMap<String, Integer>();
        String onCommit = callback == null ? null : "onCommit";
        this.commitCallback = callback;
        this.hprog = this.ddlog_run(storeData, workers, onCommit);
    }

    static void ensureDllLoaded() {
        if (!nativeLibraryLoaded) {
            System.loadLibrary("ddlogapi");
            nativeLibraryLoaded = true;
        }
    }

    /// Callback invoked from commit.
    void onCommit(int tableid, long handle, long w) {
        if (this.commitCallback != null) {
            DDlogCommand.Kind kind = w > 0 ? DDlogCommand.Kind.Insert : DDlogCommand.Kind.DeleteVal;
            for (long i = 0; i < java.lang.Math.abs(w); i++) {
                DDlogRecord record = DDlogRecord.fromSharedHandle(handle);
                DDlogRecCommand command = new DDlogRecCommand(kind, tableid, record);
                this.commitCallback.accept(command);
            }
        }
    }

    /**
     * Get DDlog relation ID from its name.
     * @param table  relation name whose id is sought.
     * @returns -1 when the relation is not found, or the relation id,
     *          a positive number otherwise.
     *
     * See <code>ddlog.h: ddlog_get_table_id()</code>
     */
    public int getTableId(String table) {
        if (!this.tableId.containsKey(table)) {
            int id = ddlog_get_table_id(table);
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
    public String getTableName(int id) {
        return ddlog_get_table_name(id);
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
        checkHandle();
        if (this.record_fd != -1) {
            DDlogAPI.ddlog_stop_recording(this.hprog, this.record_fd);
            this.record_fd = -1;
        }
        if (filename == null) {
            return;
        }

        int fd = DDlogAPI.ddlog_record_commands(this.hprog, filename, append);
        this.record_fd = fd;
    }

    public void dumpInputSnapshot(String filename, boolean append) throws DDlogException, IOException {
        checkHandle();
        DDlogAPI.ddlog_dump_input_snapshot(this.hprog, filename, append);
    }

    /**
     * Stops the DDlog program; deallocate all resources.
     *
     * See <code>ddlog.h: ddlog_stop()</code>
     */
    public void stop() throws DDlogException {
        checkHandle();
        /* Close the file handle. */
        if (this.record_fd != -1) {
            DDlogAPI.ddlog_stop_recording(this.hprog, this.record_fd);
            this.record_fd = -1;
        }
        this.ddlog_stop(this.hprog, this.callbackHandle);
        this.hprog = 0;
    }

    /**
     * Start a transaction.
     *
     * See <code>ddlog.h: ddlog_start()</code>
     */
    public void transactionStart() throws DDlogException {
        checkHandle();
        DDlogAPI.ddlog_transaction_start(this.hprog);
    }

    /**
     * Commit a transaction.
     *
     * See <code>ddlog.h: ddlog_transaction_commit()</code>
     */
    public void transactionCommit() throws DDlogException {
        checkHandle();
        DDlogAPI.ddlog_transaction_commit(this.hprog);
    }

    /**
     * Commit a transaction; invoke <code>callback</code> for each inserted or deleted record in each output relation.
     *
     * See <code>ddlog.h: ddlog_transaction_commit_dump_changes()</code>
     */
    public void transactionCommitDumpChanges(Consumer<DDlogCommand<DDlogRecord>> callback)
            throws DDlogException {
        checkHandle();
        String onDelta = callback == null ? null : "onDelta";
        this.deltaCallback = callback;
        this.ddlog_transaction_commit_dump_changes(this.hprog, onDelta);
    }

    /**
     * Remove all records from an input relation.
     *
     * See <code>ddlog.h: ddlog_clear_relation()</code>
     */
    public int clearRelation(int relid) throws DDlogException {
        checkHandle();
        return ddlog_clear_relation(this.hprog, relid);
    }

    // Callback invoked from commit_dump_changes.
    void onDelta(int tableid, long handle, boolean polarity) {
        if (this.deltaCallback != null) {
            DDlogCommand.Kind kind = polarity ? DDlogCommand.Kind.Insert : DDlogCommand.Kind.DeleteVal;
            DDlogRecord record = DDlogRecord.fromSharedHandle(handle);
            DDlogRecCommand command = new DDlogRecCommand(kind, tableid, record);
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
        checkHandle();
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
        checkHandle();
        DDlogAPI.ddlog_transaction_rollback(this.hprog);
    }

    /**
     * Apply updates to DDlog input relations.
     *
     * See <code>ddlog.h: ddlog_apply_updates()</code>
     */
    public void applyUpdates(DDlogRecCommand[] commands) throws DDlogException {
        checkHandle();
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
        checkHandle();
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
        checkHandle();
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
        checkHandle();
        ddlog_dump_index_to_flatbuf(this.hprog, idxid, resfb);
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
     *
     * For this to work the DDlogAPI must have been created with a
     * storeData parameter set to true.
     */
    public void dumpTable(String table, Consumer<DDlogRecord> callback) throws DDlogException {
        checkHandle();
        int id = this.getTableId(table);
        if (id == -1)
            throw new RuntimeException("Unknown table " + table);
        String onDump = callback == null ? null : "dumpCallback";
        this.dumpCallback = callback;
        this.dump_table(this.hprog, id, onDump);
    }

    /**
     * Returns DDlog program runtime profile as a string.
     *
     * See <code>ddlog.h: ddlog_profile()</code>
     */
    public String profile() throws DDlogException {
        checkHandle();
        return DDlogAPI.ddlog_profile(this.hprog);
    }

    /**
     * Controls recording of differential operator runtimes.
     *
     * See <code>ddlog.h: ddlog_enable_cpu_profiling()</code>
     */
    public void enableCpuProfiling(boolean enable) throws DDlogException {
        checkHandle();
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
            defaultLogCBInfo = new Long(new_cbinfo);
        }
    }

    /******************************************/

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
     * Run an external process by executing the specified command.
     * @param commands        Command and arguments.
     * @param workdirectory   If not null the working directory.
     * @param verbose         If true echo output and stderr of subprocess.
     * @return                The exit code of the process.
     */
    public static int runProcess(List<String> commands, String workdirectory, boolean verbose) {
        try {
            System.out.println("Running " + String.join(" ", commands) +
                (workdirectory != null ? " in " + workdirectory : ""));
            ProcessBuilder pb = new ProcessBuilder(commands);
            if (verbose)
                pb.inheritIO();
            if (workdirectory != null) {
                pb.directory(new File(workdirectory));
            }
            Process process = pb.start();
            int exitCode = process.waitFor();
            if (exitCode != 0)
                System.err.println("Error running " + String.join(" ", commands));
            return exitCode;
        } catch (Exception ex) {
            System.err.println("Error running " + String.join(" ", commands));
            System.err.println(ex.getMessage());
            return 1;
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
     * @param verbose    If true show stdout and stderr of processes invoked.
     * @param ddlogLibraryPath  Additional list of paths for needed ddlog libraries.
     * @return  true on success.
     */
    public static boolean compileDDlogProgramToRust(
            String ddlogFile,
            boolean verbose,
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
        int exitCode = runProcess(command, dir != null ? dir.toString() : null, verbose);
        if (exitCode != 0)
            return false;
        return true;
    }

    /**
     * Compile a ddlog program stored in a file and generate
     * a shared library named *ddlogapi.*.
     * @param ddlogFile  Pathname to the ddlog program.
     * @param verbose    If true show stdout and stderr of processes invoked.
     * @param ddlogLibraryPath  Additional list of paths for needed ddlog libraries.
     * @return  true on success.
     */
    public static boolean compileDDlogProgram(
        String ddlogFile,
        boolean verbose,
        String... ddlogLibraryPath) throws DDlogException, NoSuchFieldException, IllegalAccessException {
        boolean success = compileDDlogProgramToRust(ddlogFile, verbose, ddlogLibraryPath);
        if (!success)
            return false;

        String os = System.getProperty("os.name").toLowerCase();
        String ddlogInstallationPath = ddlogInstallationPath();

        List<String> command = new ArrayList<String>();
        command.clear();
        command.add("cargo");
        command.add("build");
        command.add("--release");
        int dot = ddlogFile.lastIndexOf('.');
        String rustDir = ddlogFile;
        if (dot >= 0)
            rustDir = ddlogFile.substring(0, dot);
        rustDir += "_ddlog";
        int exitCode = runProcess(command, rustDir, verbose);
        if (exitCode != 0)
            return false;

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
        /*
          Flags for the linker.
          command.add("-z");
          command.add("noexecstack");
        */
        command.add("-o");
        command.add(libName(ddlogLibrary));
        exitCode = runProcess(command, null, verbose);
        if (exitCode != 0)
            return false;

        return true;
    }

    public static DDlogAPI loadDDlog() throws DDlogException {
        final Path libraryPath = Paths.get(libName(ddlogLibrary)).toAbsolutePath();
        System.load(libraryPath.toString());
        return new ddlogapi.DDlogAPI(1, null, false);
    }
}
