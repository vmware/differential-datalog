package ddlogapi;

import java.lang.reflect.*;

/**
 * Implementation of DDlogCommand that uses generic (type-erased)
 * representation of DDlog records.
 */
public class DDlogRecCommand implements DDlogCommand<DDlogRecord> {
    public final Kind kind() {
        return this._kind;
    }

    public final int relid() {
        return this._relid;
    }

    public final long weight() {
        return this._weight;
    }

    public final DDlogRecord value() {
        return this._value;
    }

    private Kind _kind;
    private int _relid;
    private long _weight;
    private DDlogRecord _value;

    public DDlogRecCommand(final Kind kind, final long weight, final int relid, final DDlogRecord value) {
        if (value == null)
            throw new NullPointerException("DDlogRecord is null");
        this._kind = kind;
        this._weight = weight;
        this._relid = relid;
        this._value = value;
    }

    public DDlogRecCommand(final Kind kind, final long weight, final int relid, final Object value)
            throws IllegalAccessException, DDlogException {
        if (value == null)
            throw new NullPointerException("DDlogRecord is null");
        this._kind = kind;
        this._weight = weight;
        this._relid = relid;
        this._value = DDlogRecord.convertObject(value);
    }

    public DDlogRecCommand(final Kind kind, final int relid, final DDlogRecord value) {
        this(kind, 1, relid, value);
    }

    public DDlogRecCommand(final Kind kind, final int relid, final Object value)
            throws IllegalAccessException, DDlogException {
            this(kind, 1, relid, value);
    }

    /**
     * Allocates the underlying C data structure representing the command.
     * At this time the command takes ownership of the value.
     * Returns a handle to the underlying data structure.
     */
    public long allocate() {
        switch (this._kind) {
            case DeleteKey:
                return DDlogAPI.ddlog_delete_key_cmd(
                        this._relid, this._value.getHandleAndInvalidate());
            case DeleteVal:
                return DDlogAPI.ddlog_delete_val_cmd(
                        this._relid, this._value.getHandleAndInvalidate());
            case Insert:
                return DDlogAPI.ddlog_insert_cmd(
                        this._relid, this._value.getHandleAndInvalidate());
            default:
                throw new RuntimeException("Unexpected command " + this._kind);
        }
    }

    public <T> T getValue(Class<T> classOfT)
        throws InstantiationException, IllegalAccessException, NoSuchMethodException, InvocationTargetException {
        return (T) this._value.toTypedObject(classOfT);
    }

    @Override
    public String toString() {
        return "From " + this._relid + " " + this._kind + " " + this._value.toString();
    }
}
