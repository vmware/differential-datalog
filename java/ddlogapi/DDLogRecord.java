package ddlogapi;

import java.lang.reflect.*;

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
    public static DDLogRecord fromSharedHandle(long handle) {
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
     * Convert an object o into a DDLogRecord that represents a struct.
     * The name of the struct is the class name of o.
     */
    public static DDLogRecord convertObject(Object o) throws IllegalAccessException {
        String name = o.getClass().getSimpleName();
        Field[] fields = o.getClass().getDeclaredFields();
        DDLogRecord[] fra = new DDLogRecord[fields.length];

        int index = 0;
        for (Field f: fields) {
            DDLogRecord fr = createField(o, f);
            fra[index++] = fr;
        }
        return DDLogRecord.makeStruct(name, fra);
    }

    private static DDLogRecord createField(Object o, Field field) throws IllegalAccessException {
        field.setAccessible(true);
        Object value = field.get(o);
        if (value == null)
            throw new RuntimeException("Null field " + field.getName());
        Class<?> type = field.getType();
        if (String.class.equals(type))
            return new DDLogRecord((String)value);
        if (!type.isPrimitive())
            throw new RuntimeException("Field " + field.getName() + " of type " + type + " not supported");
        if (long.class.equals(type))
            return new DDLogRecord((long)value);
        else if (int.class.equals(type))
            return new DDLogRecord((long)(int)value);
        else if (short.class.equals(type))
            return new DDLogRecord((long)(short)value);
        else if (byte.class.equals(type))
            return new DDLogRecord((long)(byte)value);
        else if (boolean.class.equals(type))
            return new DDLogRecord((boolean)value);
        throw new RuntimeException("Field " + field.getName() + " of type " + type + " not supported");
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

    /**
     * Converts a DDLogRecord which is a struct to a Java Object.
     * The class name and class fields must match the struct name and fields.
     */
    public Object toObject() throws ClassNotFoundException, InstantiationException, IllegalAccessException {
        if (!DDLogAPI.ddlog_is_struct(this.handle))
            throw new RuntimeException("This is not a struct");

        long h = this.handle;
        String constructor = DDLogAPI.ddlog_get_constructor(h);
        Class c = Class.forName(constructor);
        Object instance = c.newInstance();

        // Get the first field and check to see whether it is a struct with the same constructor
        long f0 = DDLogAPI.ddlog_get_struct_field(h, 0);
        if (f0 != 0) {
            if (DDLogAPI.ddlog_is_struct(f0)) {
                String f0type = DDLogAPI.ddlog_get_constructor(f0);
                if (f0type.equals(constructor))
                    h = f0;  // Scan the fields of f0
            }

            Field[] fields = c.getDeclaredFields();
            for (int i = 0; ; i++) {
                long fh = DDLogAPI.ddlog_get_struct_field(h, i);
                if (fh == 0)
                    break;
                Field f = fields[i];
                f.setAccessible(true);

                DDLogRecord field = DDLogRecord.fromSharedHandle(fh);
                field.checkHandle();

                Class<?> type = f.getType();
                if (String.class.equals(type)) {
                    String s = field.getString();
                    f.set(instance, s);
                } else if (long.class.equals(type)) {
                    long l = field.getLong();
                    f.set(instance, l);
                } else if (int.class.equals(type)) {
                    long l = field.getLong();
                    f.set(instance, (int)l);
                } else if (short.class.equals(type)) {
                    long l = field.getLong();
                    f.set(instance, (short)l);
                } else if (byte.class.equals(type)) {
                    long l = field.getLong();
                    f.set(instance, (byte)l);
                } else if (boolean.class.equals(type)) {
                    boolean b = field.getBoolean();
                    f.set(instance, b);
                } else {
                    throw new RuntimeException("Unsupported field type " + f.getType());
                }
            }
        }
        return instance;
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
        return DDLogAPI.ddlog_is_struct(this.handle);
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

    @Override
    public String toString() {
        this.checkHandle();
        if (DDLogAPI.ddlog_is_bool(this.handle)) {
            boolean b = DDLogAPI.ddlog_get_bool(this.handle);
            return Boolean.toString(b);
        }

        if (DDLogAPI.ddlog_is_int(this.handle)) {
            long l = DDLogAPI.ddlog_get_u64(this.handle);
            return Long.toString(l);
        }

        if (DDLogAPI.ddlog_is_string(this.handle)) {
            String s = DDLogAPI.ddlog_get_str(this.handle);
            // TODO: this should escape some characters...
            return "\"" + s + "\"";
        }

        StringBuilder builder = new StringBuilder();
        if (DDLogAPI.ddlog_is_tuple(this.handle)) {
            int fields = DDLogAPI.ddlog_get_tuple_size(this.handle);
            builder.append("(");
            for (int i = 0; i < fields; i++) {
                if (i > 0)
                    builder.append(", ");
                long handle = DDLogAPI.ddlog_get_tuple_field(this.handle, i);
                DDLogRecord field = DDLogRecord.fromSharedHandle(handle);
                builder.append(field.toString());
            }
            builder.append(")");
            return builder.toString();
        }

        if (DDLogAPI.ddlog_is_vector(this.handle)) {
            int fields = DDLogAPI.ddlog_get_vector_size(this.handle);
            builder.append("[");
            for (int i = 0; i < fields; i++) {
                if (i > 0)
                    builder.append(", ");
                long handle = DDLogAPI.ddlog_get_vector_elem(this.handle, i);
                DDLogRecord field = DDLogRecord.fromSharedHandle(handle);
                builder.append(field.toString());
            }
            builder.append("]");
            return builder.toString();
        }

        if (DDLogAPI.ddlog_is_set(this.handle)) {
            int fields = DDLogAPI.ddlog_get_set_size(this.handle);
            builder.append("{");
            for (int i = 0; i < fields; i++) {
                if (i > 0)
                    builder.append(", ");
                long handle = DDLogAPI.ddlog_get_set_elem(this.handle, i);
                DDLogRecord field = DDLogRecord.fromSharedHandle(handle);
                builder.append(field.toString());
            }
            builder.append("}");
            return builder.toString();
        }

        if (DDLogAPI.ddlog_is_map(this.handle)) {
            int fields = DDLogAPI.ddlog_get_map_size(this.handle);
            builder.append("{");
            for (int i = 0; i < fields; i++) {
                if (i > 0)
                    builder.append(", ");
                long handle = DDLogAPI.ddlog_get_map_key(this.handle, i);
                DDLogRecord field = DDLogRecord.fromSharedHandle(handle);
                builder.append(field.toString());
                builder.append("=>");
                handle = DDLogAPI.ddlog_get_map_val(this.handle, i);
                field = DDLogRecord.fromSharedHandle(handle);
                builder.append(field.toString());
            }
            builder.append("}");
            return builder.toString();
        }

        if (DDLogAPI.ddlog_is_struct(this.handle)) {
            long h = this.handle;
            String type = DDLogAPI.ddlog_get_constructor(h);
            builder.append(type + "{");

            // Get the first field and check to see whether it is a struct with the same constructor
            long f0 = DDLogAPI.ddlog_get_struct_field(h, 0);
            if (f0 != 0) {
                if (DDLogAPI.ddlog_is_struct(f0)) {
                    String f0type = DDLogAPI.ddlog_get_constructor(f0);
                    if (f0type.equals(type))
                        h = f0;  // Scan the fields of f0
                }

                for (int i = 0; ; i++) {
                    long fh = DDLogAPI.ddlog_get_struct_field(h, i);
                    if (fh == 0)
                        break;
                    if (i > 0)
                        builder.append(",");
                    DDLogRecord field = DDLogRecord.fromSharedHandle(fh);
                    builder.append(field.toString());
                }
            }
            builder.append("}");
            return builder.toString();
        }

        throw new RuntimeException("Unhandled record type");
    }
}
