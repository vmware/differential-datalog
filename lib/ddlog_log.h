/*
 * C prototypes for the logging API (see `log.dl`, `log.rs`)
 */

/*
 * Set logging callback and log level for a given module.
 *
 * `module`    - Opaque module identifier.  Can represent module, subsystem, log
 *               stream, etc.
 * `cb`        - Callback to be invoked for each log message with the given module
 *               id and log level `<= max_level`.  Passing `NULL` disables
 *               logging for the given module.
 * `max_level` - Don't invoke the callback for messages whoe log level is
 *               `> max_level`.
 */
extern void ddlog_log_set_callback(
        int module,
        void (*cb)(uintptr_t arg, int level, const char* msg),
        uintptr_t cb_arg,
        int max_level);
