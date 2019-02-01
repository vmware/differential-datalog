package ddlogapi;

import java.lang.reflect.*;

public class DDlogCommand {
    public enum Kind {
        DeleteVal,
        DeleteKey,
        Insert
    };

    public final Kind kind;
    public final int table;
    public final DDlogRecord value;

    public DDlogCommand(final Kind kind, final int table, final DDlogRecord value) {
        this.kind = kind;
        this.table = table;
        this.value = value;
    }

    public DDlogCommand(final Kind kind, final int table, final Object value)
            throws IllegalAccessException, InstantiationException, IllegalAccessException {
        this.kind = kind;
        this.table = table;
        this.value = DDlogRecord.convertObject(value);
    }

    /**
     * Allocates the underlying C data structure representing the command.
     * At this time the command takes ownership of the value.
     * Returns a handle to the underlying data structure.
     */
    public long allocate() {
        switch (this.kind) {
            case DeleteKey:
                return DDlogAPI.ddlog_delete_key_cmd(
                        this.table, this.value.getHandleAndInvalidate());
            case DeleteVal:
                return DDlogAPI.ddlog_delete_val_cmd(
                        this.table, this.value.getHandleAndInvalidate());
            case Insert:
                return DDlogAPI.ddlog_insert_cmd(
                        this.table, this.value.getHandleAndInvalidate());
            default:
                throw new RuntimeException("Unexpected command " + this.kind);
        }
    }

    public <T> T getValue(Class<T> classOfT)
        throws InstantiationException, IllegalAccessException, NoSuchMethodException, InvocationTargetException {
        return (T) this.value.toTypedObject(classOfT);
    }

    @Override
    public String toString() {
        return "From " + this.table + " " + this.kind + " " + this.value.toString();
    }
}
