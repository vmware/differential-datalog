package ddlogapi;

public class DDLogCommand {
    public enum Kind {
        DeleteVal,
        DeleteKey,
        Insert
    };

    public final Kind kind;
    public final int table;
    public final DDLogRecord value;

    public DDLogCommand(final Kind kind, final int table, final DDLogRecord value) {
        this.kind = kind;
        this.table = table;
        this.value = value;
    }

    /**
     * Allocates the underlying C data structure representing the command.
     * At this time the command takes ownership of the value.
     * Returns a handle to the underlying data structure.
     */
    public long allocate() {
        switch (this.kind) {
            case DeleteKey:
                return DDLogAPI.ddlog_delete_key_cmd(
                        this.table, this.value.getHandleAndInvalidate());
            case DeleteVal:
                return DDLogAPI.ddlog_delete_val_cmd(
                        this.table, this.value.getHandleAndInvalidate());
            case Insert:
                return DDLogAPI.ddlog_insert_cmd(
                        this.table, this.value.getHandleAndInvalidate());
            default:
                throw new RuntimeException("Unexpected command " + this.kind);
        }
    }
}
