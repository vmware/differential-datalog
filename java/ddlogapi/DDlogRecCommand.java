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

    public final DDlogRecord toModify() { return this._toModify; }

    private final Kind _kind;
    private final int _relid;
    private final long _weight;
    private final DDlogRecord _value; // also used to represent the key for Modify commands.
    private final DDlogRecord _toModify;

    public DDlogRecCommand(final Kind kind, final long weight, final int relid, final DDlogRecord value, final DDlogRecord toModify) {
        if (value == null)
            throw new NullPointerException("value DDlogRecord is null");
        this._kind = kind;
        this._weight = weight;
        this._relid = relid;
        this._value = value;
        this._toModify = toModify;
        if (kind == Kind.Modify) {
            if (toModify == null)
                throw new NullPointerException("toModify DDlogRecord is null");
        } else {
            if (toModify != null)
                throw new RuntimeException("toModify must be null for commands of kind " + kind);
        }
    }

    public DDlogRecCommand(final Kind kind, final long weight, final int relid, final DDlogRecord value) {
        this(kind, weight, relid, value, null);
    }

    public DDlogRecCommand(final Kind kind, final long weight, final int relid, final Object value, final Object toModify)
            throws IllegalAccessException, DDlogException {
        if (value == null)
            throw new NullPointerException("DDlogRecord is null");
        this._kind = kind;
        this._weight = weight;
        this._relid = relid;
        this._value = DDlogRecord.convertObject(value);
        this._toModify = DDlogRecord.convertObject(toModify);
    }

    public DDlogRecCommand(final Kind kind, final long weight, final int relid, final Object value)
            throws IllegalAccessException, DDlogException {
        this(kind, weight, relid, value, null);
    }

    public DDlogRecCommand(final Kind kind, final int relid, final DDlogRecord value, final DDlogRecord toModify) {
        this(kind, 1, relid, value, toModify);
    }

    public DDlogRecCommand(final Kind kind, final int relid, final DDlogRecord value) {
        this(kind, 1, relid, value);
    }

    public DDlogRecCommand(final Kind kind, final int relid, final Object value, final Object toModify)
            throws IllegalAccessException, DDlogException {
            this(kind, 1, relid, value, toModify);
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
            case Modify:
                return DDlogAPI.ddlog_modify_cmd(
                        this._relid, this._value.getHandleAndInvalidate(), this._toModify.getHandleAndInvalidate());
            default:
                throw new RuntimeException("Unexpected command " + this._kind);
        }
    }

    public <T> T getValue(Class<T> classOfT)
        throws InstantiationException, IllegalAccessException, NoSuchMethodException, InvocationTargetException, DDlogException {
        return (T) this._value.toTypedObject(classOfT);
    }

    @Override
    public String toString() {
        return "From " + this._relid + " " + this._kind + " " + this._value.toString();
    }
}
