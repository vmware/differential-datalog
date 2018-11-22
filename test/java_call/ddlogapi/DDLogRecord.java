package ddlogapi;

/**
 * Java wrapper around Differential Datalog C API that manipulates
 * DDlog data structures.
 */
public class DDLogRecord {
    // This wraps a void* that points to a C ddlog_record object.
    private long handle;
    // If true the handle is shared with other objects and should not be
    // used in some operations.
    private boolean shared;

    protected DDLogRecord() {
        // A zero handle indicates an invalid object.
        this.handle = 0;
        this.shared = false;
    }

    /**
     * Invalidates this container and returns the contained handle.
     */
    public long getHandleAndInvalidate() {
        this.checkHandle();
        if (this.shared)
            throw new RuntimeException("Attempting to get shared handle");
        long result = this.handle;
        this.handle = 0;
        return result;
    }

    /**
     * Deallocate the data from this DDLogRecord.
     */
    public void release() {
        if (this.handle == 0)
            return;
        if (!this.shared)
            DDLogAPI.ddlog_free(this.handle);
        this.handle = 0;
        this.shared = false;
    }

    private void checkHandle() {
        if (this.handle == 0)
            throw new RuntimeException("Accessing invalid handle.");
    }

    private static DDLogRecord fromHandle(long handle) {
        DDLogRecord result = new DDLogRecord();
        if (handle == DDLogAPI.error)
            throw new RuntimeException("Received invalid handle.");
        result.handle = handle;
        result.shared = false;
        return result;
    }

    /**
     * Creates an object where the handle is shared with other
     * objects.
     */
    private static DDLogRecord fromSharedHandle(long handle) {
        DDLogRecord result = fromHandle(handle);
        result.shared = true;
        return result;
    }

    public DDLogRecord(boolean b) {
        this.handle = DDLogAPI.ddlog_bool(b);
        this.shared = false;
    }

    public DDLogRecord(long v) {
        this.handle = DDLogAPI.ddlog_u64(v);
        this.shared = false;
    }

    public DDLogRecord(String s) {
        this.handle = DDLogAPI.ddlog_string(s);
        this.shared = false;
    }

    /**
     * Creates a pair with the specified fields.
     */
    public DDLogRecord(DDLogRecord first, DDLogRecord second) {
        this.handle = DDLogAPI.ddlog_pair(
                first.getHandleAndInvalidate(),
                second.getHandleAndInvalidate());
        this.shared = false;
    }

    private static long[] getHandlesAndInvalidate(DDLogRecord[] fields) {
        long[] handles = new long[fields.length];
        for (int i = 0; i < fields.length; i++) {
            handles[i] = fields[i].getHandleAndInvalidate();
        }
        return handles;
    }

    public static DDLogRecord makeTuple(DDLogRecord[] fields) {
        long[] handles = getHandlesAndInvalidate(fields);
        return fromHandle(DDLogAPI.ddlog_tuple(handles));
    }

    public static DDLogRecord makeVector(DDLogRecord[] fields) {
        long[] handles = getHandlesAndInvalidate(fields);
        return fromHandle(DDLogAPI.ddlog_vector(handles));
    }

    public static DDLogRecord makeSet(DDLogRecord[] fields) {
        long[] handles = getHandlesAndInvalidate(fields);
        return fromHandle(DDLogAPI.ddlog_set(handles));
    }

    public static DDLogRecord makeStruct(String name, DDLogRecord[] fields) {
        long[] handles = getHandlesAndInvalidate(fields);
        return fromHandle(DDLogAPI.ddlog_struct(name, handles));
    }

    /**
     * Creates a map from a vector of pairs.
     */
    public static DDLogRecord makeMap(DDLogRecord[] fields) {
        long[] handles = getHandlesAndInvalidate(fields);
        return fromHandle(DDLogAPI.ddlog_map(handles));
    }

    public boolean getBoolean() {
        this.checkHandle();
        if (!DDLogAPI.ddlog_is_bool(this.handle))
            throw new RuntimeException("Value is not boolean");
        return DDLogAPI.ddlog_get_bool(this.handle);
    }

    public long getLong() {
        this.checkHandle();
        if (!DDLogAPI.ddlog_is_int(this.handle))
            throw new RuntimeException("Value is not long");
        return DDLogAPI.ddlog_get_u64(this.handle);
    }

    public int getTupleSize() {
        this.checkHandle();
        if (!DDLogAPI.ddlog_is_tuple(this.handle))
            throw new RuntimeException("Value is not a tuple");
        return DDLogAPI.ddlog_get_tuple_size(this.handle);
    }

    public DDLogRecord getTupleField(int index) {
        this.checkHandle();
        if (!DDLogAPI.ddlog_is_tuple(this.handle))
            throw new RuntimeException("Value is not a tuple");
        return fromSharedHandle(DDLogAPI.ddlog_get_tuple_field(this.handle, index));
    }

    public int getVectorSize() {
        this.checkHandle();
        if (!DDLogAPI.ddlog_is_vector(this.handle))
            throw new RuntimeException("Value is not a vector");
        return DDLogAPI.ddlog_get_vector_size(this.handle);
    }

    public DDLogRecord getVectorField(int index) {
        this.checkHandle();
        if (!DDLogAPI.ddlog_is_vector(this.handle))
            throw new RuntimeException("Value is not a vector");
        return fromSharedHandle(DDLogAPI.ddlog_get_vector_elem(this.handle, index));
    }

    public int getSetSize() {
        this.checkHandle();
        if (!DDLogAPI.ddlog_is_set(this.handle))
            throw new RuntimeException("Value is not a set");
        return DDLogAPI.ddlog_get_set_size(this.handle);
    }

    public DDLogRecord getSetField(int index) {
        this.checkHandle();
        if (!DDLogAPI.ddlog_is_set(this.handle))
            throw new RuntimeException("Value is not a set");
        return fromSharedHandle(DDLogAPI.ddlog_get_set_elem(this.handle, index));
    }

    public int getMapSize() {
        this.checkHandle();
        if (!DDLogAPI.ddlog_is_map(this.handle))
            throw new RuntimeException("Value is not a map");
        return DDLogAPI.ddlog_get_map_size(this.handle);
    }

    public DDLogRecord getMapKey(int index) {
        this.checkHandle();
        if (!DDLogAPI.ddlog_is_map(this.handle))
            throw new RuntimeException("Value is not a map");
        return fromSharedHandle(DDLogAPI.ddlog_get_map_key(this.handle, index));
    }

    public DDLogRecord getMapValue(int index) {
        this.checkHandle();
        if (!DDLogAPI.ddlog_is_map(this.handle))
            throw new RuntimeException("Value is not a map");
        return fromSharedHandle(DDLogAPI.ddlog_get_map_val(this.handle, index));
    }

    public String getString() {
        this.checkHandle();
        if (!DDLogAPI.ddlog_is_string(this.handle))
            throw new RuntimeException("Value is not a string");
        return DDLogAPI.ddlog_get_str(this.handle);
    }

    public boolean isStruct() {
        this.checkHandle();
        return DDLogAPI.ddlog_is_pos_struct(this.handle);
    }

    public String getStructName() {
        if (!this.isStruct())
            throw new RuntimeException("Value is not a struct");
        return DDLogAPI.ddlog_get_constructor(this.handle);
    }

    public DDLogRecord getStructField(int index) {
        if (!this.isStruct())
            throw new RuntimeException("Value is not a struct");
        return fromSharedHandle(DDLogAPI.ddlog_get_struct_field(this.handle, index));
    }
}
