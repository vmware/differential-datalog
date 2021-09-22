package ddlogapi;

import java.util.*;
import java.lang.reflect.*;
import java.math.BigInteger;

/**
 * Java wrapper around Differential Datalog C API that manipulates
 * DDlog data structures.
 */
public class DDlogRecord {
    // This wraps a void* that points to a C ddlog_record object.
    private long handle;
    // If true the handle is shared with other objects and should not be
    // used in some operations.
    private boolean shared;

    protected DDlogRecord() {
        // A zero handle indicates an invalid object.
        this.handle = 0;
        this.shared = false;
    }

    private static <T> T checkNull(T value) {
        if (value == null)
            throw new NullPointerException();
        return value;
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
     * Deallocate the data from this DDlogRecord.
     */
    public void release() {
        if (this.handle == 0)
            return;
        if (!this.shared)
            DDlogAPI.ddlog_free(this.handle);
        this.handle = 0;
        this.shared = false;
    }

    private long checkHandle() {
        if (this.handle == 0)
            throw new RuntimeException("Accessing invalid handle.");
        return this.handle;
    }

    private static DDlogRecord fromHandle(long handle) {
        DDlogRecord result = new DDlogRecord();
        if (handle == 0)
            throw new RuntimeException("Received invalid handle.");
        result.handle = handle;
        result.shared = false;
        return result;
    }

    /**
     * Creates an object where the handle is shared with other
     * objects.
     */
    static DDlogRecord fromSharedHandle(long handle) {
        DDlogRecord result = fromHandle(handle);
        result.shared = true;
        return result;
    }

    public DDlogRecord(boolean b) {
        this.handle = DDlogAPI.ddlog_bool(b);
        this.shared = false;
    }

    public DDlogRecord(long v) {
        this.handle = DDlogAPI.ddlog_i64(v);
        this.shared = false;
    }

    public DDlogRecord(BigInteger v) {
        this.handle = DDlogAPI.ddlog_int(v.toByteArray());
        this.shared = false;
    }

    public DDlogRecord(String s) throws DDlogException {
        this.handle = DDlogAPI.ddlog_string(checkNull(s));
        this.shared = false;
    }

    public DDlogRecord(double d) {
        this.handle = DDlogAPI.ddlog_double(d);
        this.shared = false;
    }

    public DDlogRecord(float f) {
        this.handle = DDlogAPI.ddlog_float(f);
        this.shared = false;
    }

    private static void getAllFields(Class<?> clazz, List<Field> result) {
        while (clazz != null) {
            Field[] fields = clazz.getDeclaredFields();
            result.addAll(Arrays.asList(fields));
            clazz = clazz.getSuperclass();
        }
    }

    private static List<Field> getAllFields(Class<?> clazz) {
        ArrayList<Field> result = new ArrayList<Field>();
        getAllFields(clazz, result);
        return result;
    }

    /**
     * Convert an object o into a DDlogRecord that represents a struct.
     * The name of the struct is the class name of o.
     */
    public static DDlogRecord convertObject(Object o)
            throws IllegalAccessException, DDlogException {
        if (o == null)
            return null;
        String name = o.getClass().getSimpleName();
        List<Field> fields = getAllFields(o.getClass());
        DDlogRecord[] fra = new DDlogRecord[fields.size()];

        int index = 0;
        for (Field f: fields) {
            DDlogRecord fr = createField(o, f);
            fra[index++] = fr;
        }
        return DDlogRecord.makeStruct(name, fra);
    }

    private static DDlogRecord createField(Object o, Field field)
            throws IllegalAccessException, DDlogException {
        field.setAccessible(true);
        Object value = field.get(o);
        if (value == null)
            throw new RuntimeException("Null field " + field.getName());
        Class<?> type = field.getType();
        if (String.class.equals(type))
            return new DDlogRecord((String)value);
        else if (BigInteger.class.equals(type))
            return new DDlogRecord((BigInteger)value);

        if (!type.isPrimitive())
            throw new RuntimeException("Field " + field.getName() + " of type " + type + " not supported");
        if (long.class.equals(type))
            return new DDlogRecord(BigInteger.valueOf((long)value));
        else if (int.class.equals(type))
            return new DDlogRecord(BigInteger.valueOf((int)value));
        else if (short.class.equals(type))
            return new DDlogRecord(BigInteger.valueOf((short)value));
        else if (byte.class.equals(type))
            return new DDlogRecord(BigInteger.valueOf((byte)value));
        else if (boolean.class.equals(type))
            return new DDlogRecord((boolean)value);
        throw new RuntimeException("Field " + field.getName() + " of type " + type + " not supported");
    }

    /**
     * Creates a pair with the specified fields.
     */
    public DDlogRecord(DDlogRecord first, DDlogRecord second) {
        this.handle = DDlogAPI.ddlog_pair(
                first.getHandleAndInvalidate(),
                second.getHandleAndInvalidate());
        this.shared = false;
    }

    private static long[] getHandlesAndInvalidate(DDlogRecord[] fields) {
        long[] handles = new long[fields.length];
        for (int i = 0; i < fields.length; i++) {
            handles[i] = fields[i].getHandleAndInvalidate();
        }
        return handles;
    }

    public <T> T toTypedObject(Class<T> classOfT)
            throws InstantiationException, IllegalAccessException, NoSuchMethodException, InvocationTargetException, DDlogException {
        Object instance = classOfT.getDeclaredConstructor().newInstance();
        long h = this.checkHandle();

        // Get the first field and check to see whether it is a struct with the same constructor
        long f0 = DDlogAPI.ddlog_get_struct_field(h, 0);
        if (f0 != 0) {
            if (DDlogAPI.ddlog_is_struct(f0)) {
                String f0type = DDlogAPI.ddlog_get_constructor(f0);
                if (f0type.equals(classOfT.getSimpleName()))
                    h = f0;  // Scan the fields of f0
            }

            List<Field> fields = getAllFields(classOfT);
            for (int i = 0; ; i++) {
                long fh = DDlogAPI.ddlog_get_struct_field(h, i);
                if (fh == 0)
                    break;
                Field f = fields.get(i);
                f.setAccessible(true);

                DDlogRecord field = DDlogRecord.fromSharedHandle(fh);
                field.checkHandle();

                Class<?> type = f.getType();
                if (String.class.equals(type)) {
                    String s = field.getString();
                    f.set(instance, s);
                } else if (BigInteger.class.equals(type)) {
                    BigInteger bi = field.getInt();
                    f.set(instance, bi);
                } else if (long.class.equals(type)) {
                    long l = field.getInt().longValueExact();
                    f.set(instance, l);
                } else if (int.class.equals(type)) {
                    int x = field.getInt().intValueExact();
                    f.set(instance, x);
                } else if (short.class.equals(type)) {
                    short s = field.getInt().shortValueExact();
                    f.set(instance, s);
                } else if (byte.class.equals(type)) {
                    byte b = field.getInt().byteValueExact();
                    f.set(instance, b);
                } else if (boolean.class.equals(type)) {
                    boolean b = field.getBoolean();
                    f.set(instance, b);
                } else {
                    throw new RuntimeException("Unsupported field type " + f.getType());
                }
            }
        }
        return classOfT.cast(instance);
    }

    /**
     * Converts a DDlogRecord which is a struct to a Java Object.
     * The class name and class fields must match the struct name and fields.
     */
    public Object toObject()
            throws ClassNotFoundException, InstantiationException, IllegalAccessException, NoSuchMethodException, InvocationTargetException, DDlogException {
        if (!DDlogAPI.ddlog_is_struct(this.handle))
            throw new RuntimeException("This is not a struct");

        long h = this.handle;
        String constructor = DDlogAPI.ddlog_get_constructor(h);
        Class<?> c = Class.forName(constructor);
        return toTypedObject(c);
    }

    public static DDlogRecord makeTuple(DDlogRecord... fields) throws DDlogException {
        long[] handles = getHandlesAndInvalidate(fields);
        return fromHandle(DDlogAPI.ddlog_tuple(handles));
    }

    public static DDlogRecord makeVector(DDlogRecord... fields) throws DDlogException {
        long[] handles = getHandlesAndInvalidate(fields);
        return fromHandle(DDlogAPI.ddlog_vector(handles));
    }

    public static DDlogRecord makeSet(DDlogRecord... fields) throws DDlogException{
        long[] handles = getHandlesAndInvalidate(fields);
        return fromHandle(DDlogAPI.ddlog_set(handles));
    }

    public static DDlogRecord makeStruct(String name, DDlogRecord... fields) throws DDlogException {
        long[] handles = getHandlesAndInvalidate(fields);
        return fromHandle(DDlogAPI.ddlog_struct(checkNull(name), handles));
    }

    public static DDlogRecord makeNamedStruct(String name, String[] names, DDlogRecord... fields) throws DDlogException {
        long[] handles = getHandlesAndInvalidate(fields);
        return fromHandle(DDlogAPI.ddlog_named_struct(name, names, handles));
    }

    public boolean isNamedStruct() {
        return DDlogAPI.ddlog_is_named_struct(this.checkHandle());
    }

    public String getFieldName(int index) throws DDlogException {
        if (!this.isNamedStruct())
            throw new DDlogException("Cannot get field name from a record which is not a named struct");
        return DDlogAPI.ddlog_get_struct_field_name(this.checkHandle(), index);
    }

    /**
     * Creates a map from a vector of pairs.
     */
    public static DDlogRecord makeMap(DDlogRecord... fields) throws DDlogException {
        long[] handles = getHandlesAndInvalidate(fields);
        return fromHandle(DDlogAPI.ddlog_map(handles));
    }

    public boolean getBoolean() {
        if (!DDlogAPI.ddlog_is_bool(this.checkHandle()))
            throw new RuntimeException("Value is not boolean");
        return DDlogAPI.ddlog_get_bool(this.handle);
    }

    public BigInteger getInt() {
        if (!DDlogAPI.ddlog_is_int(this.checkHandle()))
            throw new RuntimeException("Value is not an integer type (bigint, bit<>, or signed<>)");
        long sz = DDlogAPI.ddlog_get_int(this.handle, null);
        byte [] buf = new byte[(int)sz];
        sz = DDlogAPI.ddlog_get_int(this.handle, buf);
        if (sz < 0)
            throw new RuntimeException("Unexpected error in DDlogAPI.ddlog_get_int()");
        return new BigInteger(buf);
    }

    public int getTupleSize() {
        if (!DDlogAPI.ddlog_is_tuple(this.checkHandle()))
            throw new RuntimeException("Value is not a tuple");
        return DDlogAPI.ddlog_get_tuple_size(this.handle);
    }

    public DDlogRecord getTupleField(int index) {
        if (!DDlogAPI.ddlog_is_tuple(this.checkHandle()))
            throw new RuntimeException("Value is not a tuple");
        return fromSharedHandle(DDlogAPI.ddlog_get_tuple_field(this.handle, index));
    }

    public int getVectorSize() {
        if (!DDlogAPI.ddlog_is_vector(this.checkHandle()))
            throw new RuntimeException("Value is not a vector");
        return DDlogAPI.ddlog_get_vector_size(this.handle);
    }

    public DDlogRecord getVectorField(int index) {
        if (!DDlogAPI.ddlog_is_vector(this.checkHandle()))
            throw new RuntimeException("Value is not a vector");
        return fromSharedHandle(DDlogAPI.ddlog_get_vector_elem(this.handle, index));
    }

    public int getSetSize() {
        if (!DDlogAPI.ddlog_is_set(this.checkHandle()))
            throw new RuntimeException("Value is not a set");
        return DDlogAPI.ddlog_get_set_size(this.handle);
    }

    public DDlogRecord getSetField(int index) {
        if (!DDlogAPI.ddlog_is_set(this.checkHandle()))
            throw new RuntimeException("Value is not a set");
        return fromSharedHandle(DDlogAPI.ddlog_get_set_elem(this.handle, index));
    }

    public int getMapSize() {
        if (!DDlogAPI.ddlog_is_map(this.checkHandle()))
            throw new RuntimeException("Value is not a map");
        return DDlogAPI.ddlog_get_map_size(this.handle);
    }

    public DDlogRecord getMapKey(int index) {
        if (!DDlogAPI.ddlog_is_map(this.checkHandle()))
            throw new RuntimeException("Value is not a map");
        return fromSharedHandle(DDlogAPI.ddlog_get_map_key(this.handle, index));
    }

    public DDlogRecord getMapValue(int index) {
        if (!DDlogAPI.ddlog_is_map(this.checkHandle()))
            throw new RuntimeException("Value is not a map");
        return fromSharedHandle(DDlogAPI.ddlog_get_map_val(this.handle, index));
    }

    public String getString() {
        if (!DDlogAPI.ddlog_is_string(this.checkHandle()))
            throw new RuntimeException("Value is not a string");
        return DDlogAPI.ddlog_get_str(this.handle);
    }

    public float getFloat() {
        if (!DDlogAPI.ddlog_is_float(this.checkHandle()))
            throw new RuntimeException("Value is not a float");
        return DDlogAPI.ddlog_get_float(this.handle);
    }

    public double getDouble() {
        if (!DDlogAPI.ddlog_is_double(this.checkHandle()))
            throw new RuntimeException("Value is not a double");
        return DDlogAPI.ddlog_get_double(this.handle);
    }

    public boolean isVector() {
        return DDlogAPI.ddlog_is_vector(this.checkHandle());
    }
    
    public boolean isBool() {
        return DDlogAPI.ddlog_is_bool(this.checkHandle());
    }

    public boolean isInt() {
        return DDlogAPI.ddlog_is_int(this.checkHandle());
    }

    public boolean isFloat() {
        return DDlogAPI.ddlog_is_float(this.checkHandle());
    }

    public boolean isDouble() {
        return DDlogAPI.ddlog_is_double(this.checkHandle());
    }

    public boolean isString() {
        return DDlogAPI.ddlog_is_string(this.checkHandle());
    }

    public boolean isStruct() {
        return DDlogAPI.ddlog_is_struct(this.checkHandle());
    }

    public String getStructName() {
        if (!this.isStruct())
            throw new RuntimeException("Value is not a struct");
        return DDlogAPI.ddlog_get_constructor(this.handle);
    }

    public DDlogRecord getStructField(int index) throws DDlogException {
        if (!this.isStruct())
            throw new RuntimeException("Value is not a struct");
        return fromSharedHandle(DDlogAPI.ddlog_get_struct_field(this.handle, index));
    }

    public DDlogRecord getStructFieldUnchecked(int index) {
        try {
            return this.getStructField(index);
        } catch (DDlogException ex) {
            throw new RuntimeException(ex);
        }
    }

    /**
     * A DDlogRecord representing the value None{}, can be used as a value of the
     * standard library type Option.
     */
    public static DDlogRecord none() throws DDlogException {
        return DDlogRecord.makeStruct("ddlog_std::None");
    }

    /**
     * A DDlogRecord representing the value Some{V}, can be used as a value of the
     * standard library type Option.
     */
    public static DDlogRecord some(DDlogRecord value) throws DDlogException {
        return DDlogRecord.makeStruct("ddlog_std::Some", value);
    }

    @Override
    public String toString() {
        this.checkHandle();
        if (DDlogAPI.ddlog_is_bool(this.handle)) {
            boolean b = DDlogAPI.ddlog_get_bool(this.handle);
            return Boolean.toString(b);
        }

        if (DDlogAPI.ddlog_is_float(this.handle)) {
            float f = DDlogAPI.ddlog_get_float(this.handle);
            return Float.toString(f);
        }

        if (DDlogAPI.ddlog_is_double(this.handle)) {
            double d = DDlogAPI.ddlog_get_double(this.handle);
            return Double.toString(d);
        }

        if (DDlogAPI.ddlog_is_int(this.handle)) {
            BigInteger i = this.getInt();
            return i.toString(10);
        }

        if (DDlogAPI.ddlog_is_string(this.handle)) {
            String s = DDlogAPI.ddlog_get_str(this.handle);
            // TODO: this should escape some characters...
            return "\"" + s + "\"";
        }

        StringBuilder builder = new StringBuilder();
        if (DDlogAPI.ddlog_is_tuple(this.handle)) {
            int fields = DDlogAPI.ddlog_get_tuple_size(this.handle);
            builder.append("(");
            for (int i = 0; i < fields; i++) {
                if (i > 0)
                    builder.append(", ");
                long handle = DDlogAPI.ddlog_get_tuple_field(this.handle, i);
                DDlogRecord field = DDlogRecord.fromSharedHandle(handle);
                builder.append(field.toString());
            }
            builder.append(")");
            return builder.toString();
        }

        if (DDlogAPI.ddlog_is_vector(this.handle)) {
            int fields = DDlogAPI.ddlog_get_vector_size(this.handle);
            builder.append("[");
            for (int i = 0; i < fields; i++) {
                if (i > 0)
                    builder.append(", ");
                long handle = DDlogAPI.ddlog_get_vector_elem(this.handle, i);
                DDlogRecord field = DDlogRecord.fromSharedHandle(handle);
                builder.append(field.toString());
            }
            builder.append("]");
            return builder.toString();
        }

        if (DDlogAPI.ddlog_is_set(this.handle)) {
            int fields = DDlogAPI.ddlog_get_set_size(this.handle);
            builder.append("[");
            for (int i = 0; i < fields; i++) {
                if (i > 0)
                    builder.append(", ");
                long handle = DDlogAPI.ddlog_get_set_elem(this.handle, i);
                DDlogRecord field = DDlogRecord.fromSharedHandle(handle);
                builder.append(field.toString());
            }
            builder.append("]");
            return builder.toString();
        }

        if (DDlogAPI.ddlog_is_map(this.handle)) {
            int fields = DDlogAPI.ddlog_get_map_size(this.handle);
            builder.append("{");
            for (int i = 0; i < fields; i++) {
                if (i > 0)
                    builder.append(", ");
                long handle = DDlogAPI.ddlog_get_map_key(this.handle, i);
                DDlogRecord field = DDlogRecord.fromSharedHandle(handle);
                builder.append(field.toString());
                builder.append("=>");
                handle = DDlogAPI.ddlog_get_map_val(this.handle, i);
                field = DDlogRecord.fromSharedHandle(handle);
                builder.append(field.toString());
            }
            builder.append("}");
            return builder.toString();
        }

        if (DDlogAPI.ddlog_is_struct(this.handle)) {
            long h = this.handle;
            String type = DDlogAPI.ddlog_get_constructor(h);
            builder.append(type).append("{");

            // Get the first field and check to see whether it is a struct with the same constructor
            long f0;
            try {
                f0 = DDlogAPI.ddlog_get_struct_field(h, 0);
            } catch (DDlogException e) {
                // Should never happen
                throw new RuntimeException(e);
            }
            if (f0 != 0) {
                if (DDlogAPI.ddlog_is_struct(f0)) {
                    String f0type = DDlogAPI.ddlog_get_constructor(f0);
                    if (f0type.equals(type))
                        h = f0;  // Scan the fields of f0
                }

                for (int i = 0; ; i++) {
                    long fh;
                    try {
                        fh = DDlogAPI.ddlog_get_struct_field(h, i);
                    } catch (DDlogException e) {
                        // Should never happen
                        throw new RuntimeException(e);
                    }
                    if (fh == 0)
                        break;
                    if (i > 0)
                        builder.append(",");
                    DDlogRecord field = DDlogRecord.fromSharedHandle(fh);
                    builder.append(field.toString());
                }
            }
            builder.append("}");
            return builder.toString();
        }

        throw new RuntimeException("Unhandled record type");
    }
}
