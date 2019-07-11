#ifndef __DDLOG_H__
#define __DDLOG_H__

#include <stdint.h>
#include <stdbool.h>

/*
 * *Note:* all functions in this library, with the exception of
 * `ddlog_record_commands()` and `ddlog_stop()` are thread-safe. E.g.,
 * it is legal to call `ddlog_transaction_start()` from thread 1,
 * `ddlog_apply_ovsdb_updates()` from thread 2, and
 * `ddlog_transaction_commit()` from thread 3.  Multiple concurrent
 * updates from different threads are also valid.
 *
 * However, DDlog currently does not support concurrent or nested
 * transactions.  An attempt to start a transaction while another
 * transaction is in progress (in the same or different thread) will
 * return an error.
 */

/*
 * Opaque handle to an instance of DDlog program.
 */
typedef void * ddlog_prog;

/*
 * Insert or delete command to be passed to the `ddlog_apply_updates()`
 */
typedef void ddlog_cmd;

/*
 * Unique DDlog table identifier
 */
typedef size_t table_id;

/*
 * The record type represents DDlog values that can be written to and
 * read from the DDlog database.
 *
 * DDlog supports the following value types:
 * - booleans
 * - arbitrary-width integers (but the API currently only supports
 *   integers up to 64 bits)
 * - strings
 * - tuples
 * - vectors
 * - sets
 * - maps
 * - variant types (aka structs)
 */
typedef void ddlog_record;


/*
 * Get DDlog table id by name.
 *
 * On error, returns -1.
 */
extern table_id ddlog_get_table_id(const char* tname);

/*
 * Create an instance of DDlog program.
 *
 * `workers` is the number of DDlog worker threads that will be
 * allocated to run the program.  While any positive integer value is
 * valid, values larger than the number of cores in the system are
 * likely to hurt the performance.
 *
 * `do_store` - set to true to store the copy of output tables inside DDlog.
 * When set, the client can use the following APIs to retrieve the contents of
 * tables:
 *	- `ddlog_dump_ovsdb_delta()`
 *	- `ddlog_dump_table()`
 * This has a cost in terms of memory and CPU.  In addition, the current implementation
 * serializes all writes to its internal copies of tables, introducing contention
 * when `workers > 1`.  Therefore, this flag should be set to `false` if the
 * client prefers to use DDlog in streaming mode, via the callback mechanism
 * (see below).
 *
 * `cb` - callback to be invoked for every record added to or removed from an
 * output relation.  DDlog guarantees that the callback can only be invoked in
 * two situations: (1) from the `ddlog_run()` function, as the DDlog program
 * is initialized with static records (if any), (2) from
 * `ddlog_transaction_commit()`, as DDlog applies updates performed by the
 * transaction.
 *   The `cb` function takes the following arguments:
 *	- `arg`	     - opaque used-defined value
 *	- `table`    - table being modified
 *	- `rec`	     - record that has been inserted or deleted
 *	- `polarity` - `true` - record has been added
 *		       `false` - record has been deleted
 *
 * IMPORTANT: Thread safety: DDlog invokes the callback from its worker threads
 * without any serialization to avoid contention. Hence, if the `workers` argument
 * is greater than 1, then `cb` must be prepared to handle concurrent invocations
 * from multiple threads.
 *
 * IMPORTANT: DDlog does not guarantee that callback is invoked at most once for
 * each record and for each transaction.  Depending on your specific dataflow,
 * it is possible that DDlog will, e.g., create and then delete a record within a
 * transaction, and invoke `cb` both times.
 *
 * Setting `cb` to NULL disables notifications.
 *
 * `print_err_msg` - callback to redirect diagnostic messages to.  Before
 * returning an error, functions in this API invoke this callback to print
 * error explanation.
 *
 * Setting `print_err_msg` to NULL causes ddlog to print to `stderr`.
 *
 * Returns a program handle to be used in subsequent calls to
 * `ddlog_transaction_start()`,
 * `ddlog_transaction_commit()`, etc., or NULL in case of error.
 */
extern ddlog_prog ddlog_run(unsigned int workers,
			    bool do_store,
                            void (*cb)(void *arg,
                                       table_id table,
                                       const ddlog_record *rec,
                                       bool polarity),
			    uintptr_t cb_arg,
			    void (*print_err_msg)(const char *msg));

/*
 * Record commands issued to DDlog via this API in a file.
 *
 * This is a debugging feature used to record DDlog commands issued through
 * functions in this file in a DDlog command file that can later be replayed
 * through the CLI interface.
 *
 * `fd` - file descriptor.  Passing -1 in this argument instructs DDlog to stop
 * recording. The caller is responsible for opening and closing the file.  They
 * can inject additional commands, e.g., `echo` to the file.
 *
 * IMPORTANT: this function is _not_ thread-safe and must not be invoked
 * concurrently with other functions in this API.
 */
extern int ddlog_record_commands(ddlog_prog hprog, int fd);

/*
 * Dump current snapshot of input tables to a file in a format suitable
 * for replay debugging.
 *
 * This function is intended to be used in conjunction with
 * `ddlog_record_commands`.  It is useful if one wants to start recording
 * after the program has been running for some time, or if the current log is
 * full and needs to be rotated out.  Simply calling `ddlog_record_commands`
 * with a new file descriptor at this point will generate an incomplete log that
 * will not reflect the state of input tables at the time recording is starting.
 * Such a log cannot be replayed in a meaningful way.
 *
 * Instead, we would like to start replay from the current input state (which
 * is in essence a compressed representation of the entire previous execution
 * history).  This can be achieved by first calling this function to store a
 * snapshot of input tables, followed by `ddlog_record_commands()` to continue
 * recording subsequent commands to the same file.
 *
 * This function generates input snapshot in the following format:
 *
 * ```
 * insert Table1[val1],
 * insert Table1[val2],
 * insert Table2[val3],
 * insert Table2[val4],
 * ...
 * ```
 *
 * NOTE: it does not wrap its output in a transaction.  The caller is
 * responsible for injecting `start;` and `commit;` commands, if necessary.
 *
 * `fd` - valid writable file descriptor.  The caller is responsible for opening
 * and closing the file.
 */
extern int ddlog_dump_input_snapshot(ddlog_prog hprog, int fd);

/*
 * Stops the program; deallocates all resources, invalidates the handle.
 *
 * All concurrent calls using the handle must complete before calling this
 * function.
 *
 * On success, returns `0`; on error, returns `-1` and prints error message
 * (see `print_err_msg` parameter to `ddlog_run()`).
 *
 * IMPORTANT: this function is _not_ thread-safe and must not be invoked
 * concurrently with other functions in this API.
 */
extern int ddlog_stop(ddlog_prog hprog);

/*
 * Start a transaction.
 *
 * On success, returns `0`; on error, returns `-1` and prints error message
 * (see `print_err_msg` parameter to `ddlog_run()`).
 *
 * This function will fail if another transaction is in progress.
 *
 * Within a transaction, updates to input relations are buffered until
 * `ddlog_transaction_commit()` is called.
 */
extern int ddlog_transaction_start(ddlog_prog hprog);

/*
 * Commit a transaction; propagate all buffered changes through all
 * rules in the program and update all output relations.
 *
 * On success, returns `0`; on error, returns `-1` and prints error message
 * (see `print_err_msg` parameter to `ddlog_run()`).
 *
 * This function will fail if there is no transaction in progress.
 */
extern int ddlog_transaction_commit(ddlog_prog hprog);

/*
 * Commit a transaction; propagate all buffered changes through all
 * rules in the program and update all output relations.  Once all
 * updates are finished, invokes `cb` for each new or deleted record
 * in each output relation.
 *
 * Note: unlike the `cb` parameter to `ddlog_run`, this function will
 * invoke the callback exactly once for every modified record.
 *
 * On success, returns `0`; on error, returns `-1` and prints error message
 * (see `print_err_msg` parameter to `ddlog_run()`).
 *
 * This function will fail if there is no transaction in progress.
 */
extern int
ddlog_transaction_commit_dump_changes(ddlog_prog hprog,
                                      void (*cb)(void *arg,
                                                 table_id table,
                                                 const ddlog_record *rec,
                                                 bool polarity),
                                      uintptr_t cb_arg);

/*
 * Discard all buffered updates and abort the current transaction.
 *
 * On success, returns `0`; on error, returns `-1` and prints error message
 * (see `print_err_msg` parameter to `ddlog_run()`).
 *
 * This function will fail if there is no transaction in progress.
 */
extern int ddlog_transaction_rollback(ddlog_prog hprog);

/*
 * Parse OVSDB JSON <table-updates> value into DDlog commands; apply
 * commands to a DDlog program.
 *
 * Must be called in the context of a transaction.
 *
 * `prefix` contains is the prefix to be added to JSON table names, e.g,
 * `OVN_Southbound.` or `OVN_Northbound.` for OVN southbound and
 * northbound database updates.
 *
 * `updates` is the JSON string, e.g.,
 * ```
 * {"Logical_Switch":{"ffe8d84e-b4a0-419e-b865-19f151eed878":{"new":{"acls":["set",[]],"dns_records":["set",[]],"external_ids":["map",[]],"load_balancer":["set",[]],"name":"lsw0","other_config":["map",[]],"ports":["set",[]],"qos_rules":["set",[]]}}}}
 * ```
 */
extern int ddlog_apply_ovsdb_updates(ddlog_prog hprog, const char *prefix,
                                     const char *updates);

/*
 * Dump Delta-Plus, Delta-Minus, and Delta-Update tables for OVSDB table
 * `table` declared in DDlog module `module`, as a sequence of OVSDB insert,
 * delete, and update commands in * JSON format.
 *
 * `module` must be a fully qualified name of a module.
 *
 * Requires that `hprog` was created by calling `ddlog_run()` with
 * `do_store` flag set to `true`.  Fails otherwise.
 *
 * On success, returns `0` and stores a pointer to JSON string in
 * `json`.  This pointer must be later deallocated by calling
 * `ddlog_free_json()`
 *
 * On error, returns a negative number and writes error message
 * (see `print_err_msg` parameter to `ddlog_run()`).
 */
extern int ddlog_dump_ovsdb_delta(ddlog_prog hprog, const char *module,
				  const char *table, char **json);

/*
 * Deallocates strings returned by other functions in this API.
 */
extern void ddlog_free_json(char *json);


/*
 * Apply updates to DDlog tables.  See the ddlog_cmd API below.
 *
 * On success, returns `0`. On error, returns a negative value and
 * writes error message (see `print_err_msg` parameter to `ddlog_run()`).
 *
 * Whether the function succeeds or fails, it consumes all commands in
 * the `upds` array (but not the array itself), so they can no longer be
 * accessed by the caller after the function returns.
 */
extern int ddlog_apply_updates(ddlog_prog prog, ddlog_cmd **upds, size_t n);

/*
 * Remove all records from an input relation.
 *
 * Fails if there is no transaction in progress.
 *
 * On success, returns `0`. On error, returns a negative value and
 * writes error message (see `print_err_msg` parameter to `ddlog_run()`).
 */
extern int ddlog_clear_relation(ddlog_prog prog, table_id table);

/*
 * Dump the content of an output table by invoking `cb` for each value
 * in the table.
 *
 * `cb` returns `true` to allow enumeration to continue or `false` to
 * abort the dump.
 *
 * `cb_arg` is an opaque argument passed to each invocation.
 *
 * Requires that `hprog` was created by calling `ddlog_run()` with
 * `do_store` flag set to `true`.  Fails otherwise.
 *
 * The `rec` argument of the callback function is a borrowed reference
 * that is only valid for the duration of the callback.
 *
 * The content of the table returned by this function represents
 * database state after the last committed transaction.
 */
extern int ddlog_dump_table(ddlog_prog prog, table_id table,
                            bool (*cb)(uintptr_t arg, const ddlog_record *rec),
                            uintptr_t cb_arg);

/***********************************************************************
 * Profiling
 ***********************************************************************/

/*
 * Controls recording of differential operator runtimes.  When enabled,
 * DDlog records each activation of every operator and prints the
 * per-operator CPU usage summary in the profile.  When disabled, the
 * recording stops, but the previously accumulated profile is preserved.
 *
 * Recording CPU events can be expensive in large dataflows and is
 * therefore disabled by default.
 */
extern int ddlog_enable_cpu_profiling(ddlog_prog prog, bool enable);

/*
 * Returns DDlog program runtime profile as a C string.
 *
 * The returned string must be deallocated using ddlog_string_free().
 */
extern char* ddlog_profile(ddlog_prog prog);

/***********************************************************************
 * Record API
 ***********************************************************************/

/*
 * The record API is intended solely for passing values to and from
 * DDlog.  It allows constructing and reading values, but does not
 * provide methods to otherwise manipulate them, e.g., to lookup an
 * element in a set or map.
 *
 * This API supports the following ownership policy (see additional
 * details in individual function description):
 *
 * - The client obtains ownership of a record by creating it using
 *   `ddlog_bool()`, `ddlog_string()`, `ddlog_tuple()`, etc.  This is
 *   currently the only way to obtain ownership.
 *
 * - The client yields ownership by either passing the record to DDlog
 *   as part of an update command (see `ddlog_apply_updates()`) or by
 *   attaching it to another owned object (e.g., appending it to a
 *   vector using `ddlog_vector_push()`).
 *
 * - Each owned record encapsulates some dynamically allocated memory.
 *   To avoid memory leaks, the client must transfer the ownership of
 *   every record they own or deallocate the record using
 *   `ddlog_free()`.
 *
 * - There is a limited API for modifying _owned_ records, e.g., by
 *   appending elements to an array.
 *
 * - In addition to owned records, the client can also obtain pointers to
 *   *borrowed* records in one of two ways:
 *
 *    1. By invoking the `ddlog_dump_table()` function to enumerate the
 *       content of an output table.  This function takes a user callback
 *       and invokes it once for each record in the table.  The record,
 *       passed as argument to the callback is owned by DDlog and is only
 *       valid for the duration of the callback.
 *       (TODO: the only reason for this is convenience, so the client
 *       does not need to worry about deallocating the record later.
 *       The API could be changed to return owned records.)
 *
 *    2. By querying another record of type tuple, vector, set or map.
 *       For instance, `ddlog_get_vector_elem()` returns a borrowed
 *       reference to an element of the vector.
 *
 * - The client may inspect a borrowed record, but not modify it, attach
 *   to another records or pass to DDlog as part of an update.
 *
 * - Owned records are represented in the API by mutable pointers
 *   (`ddlog_record*`), whereas borrowed records are represented by
 *   immutable pointers (`const ddlog_record*`).
 *
 * Type checking:
 *
 * The Record API does not perform any type checking, e.g., one can
 * create a vector with elements of different types or a struct whose
 * fields don't match declarations in the DDlog program. Type checking
 * is performed by DDlog before inserting records to the database, e.g.,
 * in the `ddlog_apply_updates()` function.  The function will fail if
 * it detects type mismatch between relation type and the record
 * supplied by the client.
 *
 * The client can rely on records read from the database to have types
 * that matches the corresponding DDlog output table declaration.
 */

/*
 * Dump record into a string for debug printing.
 *
 * Returns `NULL` on error.
 *
 * The returned string must be deallocated using `ddlog_string_free()`.
 */
extern char* ddlog_dump_record(const ddlog_record *rec);

/*
 * Deallocate an owned record.
 */
extern void ddlog_free(ddlog_record *rec);

/*
 * Deallocate a string returned by DDlog
 * (currently only applicable to the string returned by `ddlog_profile()` and
 * `ddlog_dump_record()`).
 */
extern void ddlog_string_free(char *s);

/*
 * Create a Boolean value
 */
extern ddlog_record* ddlog_bool(bool b);

/*
 * Returns `true` if `rec` is a Boolean and `false` otherwise
 */
extern bool ddlog_is_bool(const ddlog_record *rec);

/*
 * Retrieves the value of a Boolean.
 *
 * Returns `false` if `rec` is not a Boolean.
 */
extern bool ddlog_get_bool(const ddlog_record *rec);

/*
 * Returns `true` if `rec` is an integer and `false` otherwise.
 */
extern bool ddlog_is_int(const ddlog_record *rec);

/*
 * Returns the fewest bits necessary to express the integer value,
 * not including the sign.
 *
 * Returns `0` if the `rec` is not an integer record.
 */
extern size_t ddlog_int_bits(const ddlog_record *rec);

/*
 * Create an integer value.  Can be used to populate any ddlog field
 * of type `bit<N>`, `N<=64`
 */
extern ddlog_record* ddlog_u64(uint64_t v);

/*
 * Retrieves the value of an integer.
 *
 * Returns `0` if `rec` is not an integer or if its value does not
 * fit into 64 bits.
 */
extern uint64_t ddlog_get_u64(const ddlog_record *rec);

/*
 * Create an integer value.  Can be used to populate any ddlog field
 * of type `bit<N>`, `N<=128`
 */
extern ddlog_record* ddlog_u128(__uint128_t v);

/*
 * Retrieves the value of an integer.
 *
 * Returns `0` if `rec` is not an integer or if its value does not
 * fit into 128 bits.
 */
extern __uint128_t ddlog_get_u128(const ddlog_record *rec);

/*
 * Create a string value.  This function copies `s` to an internal
 * buffer, so the caller is responsible for deallocating `s` if it was
 * dynamically allocated.
 *
 * Returns `NULL` if `s` is not a valid null-terminated UTF8 string.
 */
extern ddlog_record* ddlog_string(const char *s);

/*
 * Returns `true` if `rec` is a string and `false` otherwise
 */
extern bool ddlog_is_string(const ddlog_record *rec);

/*
 * Retrieves the length of a string.
 *
 * Returns `0` if `rec` is not a string.
 */
extern size_t ddlog_get_strlen(const ddlog_record *rec);

/*
 * Returns the content of a DDlog string.
 *
 * WARNING: DDlog strings are _not_ null-terminated; use `ddlog_get_strlen()`
 * to determine the length of the string.
 *
 * The pointer returned by this function points to an internal DDlog
 * buffer. The caller must not modify the content of the string or
 * deallocate this pointer.  The lifetime of the pointer coincides with
 * the lifetime of the record it was obtained from, e.g., the pointer is
 * invalidated when the value is written to the database.
 */
extern const char * ddlog_get_str_non_nul(const ddlog_record *rec);

/*
 * Create a tuple with specified fields.
 *
 * `len` is the length of the `fields` array.
 * If `len` is greater than `0`, then `fields` must not be NULL.
 *
 * The function takes ownership of all records, invalidating all
 * `ddlog_record` pointers in `fields`.  However it does not take
 * ownership of the `fields` array itself.  The caller is responsible
 * for deallocating the array if needed.
 */
extern ddlog_record* ddlog_tuple(ddlog_record ** fields, size_t len);

/*
 * Returns `true` if `rec` is a tuple and false otherwise
 */
extern bool ddlog_is_tuple(const ddlog_record *rec);

/*
 * Retrieves the number of fields in a tuple.
 *
 * Returns `0` if `rec` is not a tuple.
 */
extern size_t ddlog_get_tuple_size(const ddlog_record *rec);

/*
 * Retrieves `i`th field of the tuple.
 *
 * Returns NULL if `tup` is not a tuple or if the tuple has fewer than `i`
 * fields.
 *
 * The pointer returned by this function is owned by DDlog. The caller
 * may inspect the returned record, but must not modify it, attach to
 * other records (e.g., using `ddlog_tuple_push()`) or write to the
 * database.  The lifetime of the pointer coincides with the lifetime of
 * the record it was obtained from, e.g., the pointer is invalidated
 * when the value is written to the database.
 */
extern const ddlog_record* ddlog_get_tuple_field(const ddlog_record* tup,
                                                 size_t i);

/*
 * Convenience method to create a 2-tuple.  Such tuples are useful,
 * e.g., in constructing maps out of key-value pairs.
 *
 * The function takes ownership of `v1` and `v2`.
 */
extern ddlog_record* ddlog_pair(ddlog_record *v1, ddlog_record *v2);

/*
 * An alternative way to construct tuples by adding fields one-by-one.
 *
 * To use this function, start with creating a tuple using, e.g.,
 *
 * ```
 * ddlog_tuple(NULL, 0);
 * ```
 *
 * and then call `ddlog_tuple_push()` once for each field, in the order
 * fields appear in the tuple.
 *
 * This function takes ownership of `rec`, which should not be used
 * after the call.
 */
extern void ddlog_tuple_push(ddlog_record *tup, ddlog_record *rec);

/*
 * Create a vector with specified elements.
 *
 * `len` is the length of the `recs` array.
 * If `len` is greater than `0`, then `recs` must not be NULL.
 *
 * The function takes ownership of all records, invalidating all
 * `ddlog_record` pointers in `recs`.  However it does not take ownership of
 * the `recs` array itself.  The caller is responsible for deallocating the
 * array if needed.
 */
extern ddlog_record* ddlog_vector(ddlog_record **recs, size_t len);

/*
 * Returns `true` if `rec` is a vector and false otherwise
 */
extern bool ddlog_is_vector(const ddlog_record *rec);

/*
 * Retrieves the number of elements in a vector.
 *
 * Returns `0` if `rec` is not a vector.
 */
extern size_t ddlog_get_vector_size(const ddlog_record *rec);

/*
 * Retrieves `i`th element of the vector.
 *
 * Returns NULL if `vec` is not a vector or if the vector is shorter than `i`.
 *
 * The pointer returned by this function is owned by DDlog. The caller
 * may inspect the returned record, but must not modify it, attach to
 * other records (e.g., using `ddlog_vector_push()`) or write to the
 * database.  The lifetime of the pointer coincides with the lifetime of
 * the record it was obtained from, e.g., the pointer is invalidated
 * when the value is written to the database.
 */
extern const ddlog_record* ddlog_get_vector_elem(const ddlog_record *vec,
                                                 size_t idx);

/*
 * Append a value at the end of the vector.
 *
 * This function takes ownership of `rec`, which should not be used after the
 * call.
 */
extern void ddlog_vector_push(ddlog_record *vec, ddlog_record *rec);

/*
 * Create a set with specified elements.
 *
 * `len` is the length of the `recs` array.  If `len` is greater than
 * `0`, then `recs` must not be NULL.
 *
 * The function takes ownership of all records, invalidating all
 * `ddlog_record` pointers in `recs`.  However it does not take
 * ownership of the `recs` array itself.  The caller is responsible for
 * deallocating the array if needed.
 */
extern ddlog_record* ddlog_set(ddlog_record **recs, size_t len);

/*
 * Returns `true` if `rec` is a set and false otherwise
 */
extern bool ddlog_is_set(const ddlog_record *rec);

/*
 * Retrieves the number of values in a set.
 *
 * Returns `0` if `rec` is not a set.
 */
extern size_t ddlog_get_set_size(const ddlog_record *rec);

/*
 * Retrieves `i`th element of the set.  The `ddlog_record` type
 * internally represents sets as vectors of elements.  `idx` indexes
 * into this vector.
 *
 * Returns NULL if `set` is not a set or if the set has fewer than `i`
 * elements.
 *
 * The pointer returned by this function is owned by DDlog. The caller
 * may inspect the returned record, but must not modify it, attach to
 * other records (e.g., using `ddlog_vector_push()`) or write to the
 * database.  The lifetime of the pointer coincides with the lifetime of
 * the record it was obtained from, e.g., the pointer is invalidated
 * when the value is written to the database.
 */
extern const ddlog_record* ddlog_get_set_elem(const ddlog_record* set,
                                              size_t i);

/*
 * Append a value to a set.  The `ddlog_record` type internally
 * represents sets as vectors of elements.  These vectors only get
 * converted to an actual set representation inside DDlog.  In
 * particular, duplicate values are not merged inside a record.
 *
 * This function takes ownership of `rec`, which should not be used
 * after the call.
 */
extern void ddlog_set_push(ddlog_record *set, ddlog_record *rec);

/*
 * Create a map with specified elements.  Each element in `recs` must be
 * a 2-tuple representing a key-value pair.
 *
 * `len` is the length of the `recs` array.  If `len` is greater than
 * `0`, then `recs` must not be NULL.
 *
 * The function takes ownership of all records, invalidating all
 * `ddlog_record` pointers in `recs`.  However it does not take
 * ownership of the `recs` array itself.  The caller is responsible for
 * deallocating the array if needed.
 */
extern ddlog_record* ddlog_map(ddlog_record **recs, size_t len);

/*
 * Returns `true` if `rec` is a map and false otherwise
 */
extern bool ddlog_is_map(const ddlog_record *rec);

/*
 * Retrieves the number of elements in a map.
 *
 * Returns `0` if `rec` is not a map.
 */
extern size_t ddlog_get_map_size(const ddlog_record *rec);

/*
 * Retrieves the key of the `i`th element of the map.  The `ddlog_record`
 * type internally represents maps as vectors of elements.  `i` indexes
 * into this vector.
 *
 * Returns NULL if `map` is not a map or if the map has fewer than `i`
 * elements.
 *
 * The pointer returned by this function is owned by DDlog. The caller
 * may inspect the returned record, but must not modify it, attach to
 * other records (e.g., using `ddlog_vector_push()`) or write to the
 * database.  The lifetime of the pointer coincides with the lifetime of
 * the record it was obtained from, e.g., the pointer is invalidated
 * when the value is written to the database.
 */
extern const ddlog_record* ddlog_get_map_key(const ddlog_record *map,
                                             size_t i);

/*
 * Retrieves the value of the `i`th element of the map.  The
 * `ddlog_record` type internally represents maps as vectors of
 * elements.  `i` indexes into this vector.
 *
 * Returns NULL if `map` is not a map or if the map has fewer than `i`
 * elements.
 *
 * The pointer returned by this function is owned by DDlog. The caller
 * may inspect the returned record, but must not modify it, attach to
 * other records (e.g., using `ddlog_vector_push()`) or write to the
 * database.  The lifetime of the pointer coincides with the lifetime of
 * the record it was obtained from, e.g., the pointer is invalidated
 * when the value is written to the database.
 */
extern const ddlog_record* ddlog_get_map_val(const ddlog_record *rec,
                                             size_t i);

/*
 * Append a key-value pair to a map.  The `ddlog_record` type internally
 * represents maps as vectors of elements.  These vectors only get
 * converted to an actual map representation inside DDlog.  In
 * particular, duplicate keys are not merged inside a record.
 *
 * This function takes ownership of `key` and `val`, which should not be
 * used after the call.
 */
extern void ddlog_map_push(ddlog_record *map,
                           ddlog_record *key, ddlog_record *val);

/*
 * Create a struct with specified constructor and arguments.  This creates a
 * "positional record" where arguments are identified by their order.
 *
 * The number and types of field should match the corresponding DDlog
 * constructor declaration.
 *
 * `constructor` can point to statically, dynamically or stack-allocated
 * strings.  The function copies constructor name to an internal buffer,
 * so the caller is responsible for deallocating it if necessary.
 *
 * `len` is the length of the `args` array.  If `len` is greater than
 * `0`, then `args` must not be NULL.
 *
 * The function takes ownership of all records, invalidating all
 * `ddlog_record` pointers in `args`.  However it does not take
 * ownership of the `recs` array itself.  The caller is responsible for
 * deallocating the array if needed.
 */
extern ddlog_record* ddlog_struct(const char* constructor,
                                  ddlog_record ** args, size_t len);

/*
 * Same as ddlog_struct(), but assumes that `constructor` is a statically
 * allocated string and stores the pointer internally instead of copying it to
 * another buffer.
 */
extern ddlog_record* ddlog_struct_static_cons(const char *constructor,
                                              ddlog_record **args, size_t len);

/*
 * Returns `true` if `rec` is a struct.
 */
extern bool ddlog_is_struct(const ddlog_record *rec);

/*
 * Retrieves constructor name as a non-null-terminated string.  Returns
 * string length in `len`.
 *
 * Returns NULL if `rec` is not a struct.
 */
extern const char * ddlog_get_constructor_non_null(const ddlog_record *rec,
                                                   size_t *len);


/*
 * Retrieves `i`th argument of a struct.
 *
 * Returns NULL if `rec` is not a struct or if the struct has fewer than
 * `i` arguments.
 *
 * The pointer returned by this function is owned by DDlog. The caller
 * may inspect the returned record, but must not modify it, attach to
 * other records (e.g., using `ddlog_vector_push()`) or write to the
 * database.  The lifetime of the pointer coincides with the lifetime of
 * the record it was obtained from, e.g., the pointer is invalidated
 * when the value is written to the database.
 */
extern const ddlog_record* ddlog_get_struct_field(const ddlog_record* rec,
                                                  size_t i);

/***********************************************************************
 * Command API
 ***********************************************************************/

/*
 * Create an insert command.
 *
 * `table` is the table to insert to.  `rec` is the record to insert.
 * The function takes ownership of this record.
 *
 * Returns pointer to a new command, which can be sent to DDlog by calling
 * `ddlog_apply_updates()`.
 */
extern ddlog_cmd* ddlog_insert_cmd(table_id table, ddlog_record *rec);

/*
 * Create delete-by-value command.
 *
 * `table` is the table to delete from. `rec` is the record to delete.
 * The function takes ownership of this record.
 *
 * Returns pointer to a new command, which can be sent to DDlog by calling
 * `ddlog_apply_updates()`.
 */
extern ddlog_cmd* ddlog_delete_val_cmd(table_id table, ddlog_record *rec);

/*
 * Create delete-by-key command.
 *
 * `table` is the table to delete from.  `rec` is the key to delete.  The
 * table must have a primary key and `rec` type must match the type of the
 * key.  The function takes ownership of `rec`.
 *
 * Returns pointer to a new command, which can be sent to DDlog by calling
 * `ddlog_apply_updates()`.
 *
 */
extern ddlog_cmd* ddlog_delete_key_cmd(table_id table, ddlog_record *rec);

#endif
