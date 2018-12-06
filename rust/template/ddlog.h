#ifndef __DDLOG_H__
#define __DDLOG_H__

#include <stdint.h>
#include <stdbool.h>

/*
 * *Note:* all functions in this library are thread-safe. E.g., it is legal to
 * call `ddlog_transaction_start()` from thread 1,
 * `ddlog_apply_ovsdb_updates()` from thread 2, and
 * `ddlog_transaction_commit()` from thread 3.  Multiple concurrent
 * updates from different threads are also valid.
 *
 * However, DDlog currently does not support concurrent or nested transactions.
 * An attempt to start a transaction while another transaction is in progress (in
 * the same or different thread) will return an error.
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
 * The record type represents DDlog values that can be written to and read from
 * the DDlog database.
 *
 * DDlog supports the following value types:
 * - booleans
 * - arbitrary-width integers (but the API currently only supports integers up
 *   to 64 bits)
 * - strings
 * - tuples
 * - vectors
 * - sets
 * - maps
 * - variant types (aka structs)
 */
typedef void ddlog_record;


/*
 * Create an instance of DDlog program.
 *
 * `workers` is the number of DDlog worker threads that will be allocated to run the program.
 * While any positive integer value is valid, values larger than the number of cores in
 * the system are likely to hurt the performance.
 *
 * Returns a program handle to be used in subsequent calls to
 * `ddlog_transaction_start()`,
 * `ddlog_transaction_commit()`, etc., or NULL in case of error.
 */
extern ddlog_prog ddlog_run(unsigned int workers);

/*
 * Stops the program; deallocates all resources, invalidates the handle.
 *
 * All concurrent calls using the handle must complete before calling this
 * function.
 *
 * On success, returns `0`; on error, returns `-1` and prints error message
 * to `stderr`.
 */
extern int ddlog_stop(ddlog_prog hprog);

/*
 * Start a transaction.
 *
 * On success, returns `0`; on error, returns `-1` and prints error message
 * to `stderr`.
 *
 * This function will fail if another transaction is in progress.
 *
 * Within a transaction, updates to input relations are buffered until
 * `ddlog_transaction_commit()` is called.
 */
extern int ddlog_transaction_start(ddlog_prog hprog);

/*
 * Commit a transaction; propagate all buffered changes through all rules in the
 * program and update all computed relations.
 *
 * On success, returns `0`; on error, returns `-1` and prints error message
 * to `stderr`.
 *
 * This function will fail if there is no transaction in progress.
 */
extern int ddlog_transaction_commit(ddlog_prog hprog);

/*
 * Discard all buffered updates and abort the current transaction.
 *
 * On success, returns `0`; on error, returns `-1` and prints error message
 * to `stderr`.
 *
 * This function will fail if there is no transaction in progress.
 */
extern int ddlog_transaction_rollback(ddlog_prog hprog);

/*
 * Parse OVSDB JSON <table-updates> value into DDlog commands; apply commands to a DDlog program.
 *
 * Must be called in the context of a transaction.
 *
 * `prefix` contains is the prefix to be added to JSON table names, e.g, `OVN_Southbound_` or
 * `OVN_Northbound_` for OVN southbound and northbound database updates.
 *
 * `updates` is the JSON string, e.g.,
 * ```
 * {"Logical_Switch":{"ffe8d84e-b4a0-419e-b865-19f151eed878":{"new":{"acls":["set",[]],"dns_records":["set",[]],"external_ids":["map",[]],"load_balancer":["set",[]],"name":"lsw0","other_config":["map",[]],"ports":["set",[]],"qos_rules":["set",[]]}}}}
 * ```
 */
extern int ddlog_apply_ovsdb_updates(
	ddlog_prog hprog,
	const char *prefix,
	const char *updates);

/*
 * Dump OVSDB Delta-Plus table as a sequence of OVSDB Insert commands in JSON format.
 *
 * On success, returns `0` and stores a pointer to JSON string in `json`.  This pointer must be
 * later deallocated by calling `ddlog_free_json()`
 *
 * On error, returns a negative number and writes error message to stderr.
 */
extern int ddlog_dump_ovsdb_deltaplus_table(
	ddlog_prog hprog,
	const char *table,
	char **json);

/*
 * Dump OVSDB Delta-Minus table as a sequence of OVSDB Delete commands in JSON format.
 *
 * On success, returns `0` and stores a pointer to JSON string in `json`.  This pointer must be
 * later deallocated by calling `ddlog_free_json()`
 *
 * On error, returns a negative number and write error message to stderr.
 */
extern int ddlog_dump_ovsdb_deltaminus_table(
	ddlog_prog hprog,
	const char *table,
	char **json);

/*
 * Dump OVSDB Delta-Update table as a sequence of OVSDB Update commands in JSON format.
 *
 * On success, returns `0` and stores a pointer to JSON string in `json`.  This pointer must be
 * later deallocated by calling `ddlog_free_json()`
 *
 * On error, returns a negative number and writes error message to stderr.
 */
extern int ddlog_dump_ovsdb_deltaupdate_table(
	ddlog_prog hprog,
	const char *table,
	char **json);

/*
 * Deallocates strings returned by other functions in this API.
 */
extern void ddlog_free_json(char *str);


/*
 * Apply updates to DDlog tables.  See the ddlog_cmd API below.
 *
 * Takes ownership of all commands in the `upds` array, but not the array
 * itself.
 *
 * On success, returns `0`. On error, returns a negative value and writes error
 * message to stderr.
 */
extern int ddlog_apply_updates(ddlog_prog prog,
					 ddlog_cmd **upds,
					 size_t n);

/*
 * Remove all records from an input relation.
 *
 * Fails if there is no transaction in progress.
 *
 * On success, returns `0`. On error, returns a negative value and writes error
 * message to stderr.
 */
extern int ddlog_clear_relation(ddlog_prog prog,
					  const char *table);

/*
 * Dump the content of an output table by invoking `cb` for each value in the table.
 *
 * `table` is a null-terminated string that specifies the name of the table.
 * `cb_arg` is an opaque argument passed to each invocation.
 *
 * The `rec` argument of the callback function is a borrowed reference that is
 * only valid for the duration of the callback.
 *
 * The content of the table returned by this function represents database state
 * after the last committed transaction.
 */
extern int ddlog_dump_table(ddlog_prog prog,
				      const char *table,
				      bool (*cb)(void *arg, const ddlog_record *rec),
				      void *cb_arg);

/***********************************************************************
 * Record API
 ***********************************************************************/

/*
 * The record API is intended solely for passing values to and from DDlog.  It
 * allows constructing and reading values, but does not provide methods to
 * otherwise manipulate them, e.g., to lookup an element in a set or map.
 *
 * This API supports the following ownership policy (see additional details in
 * individual function description):
 *
 * - The client obtains ownership of a record by creating it using
 *   `ddlog_bool()`, `ddlog_string()`, `ddlog_tuple()`, etc.  This is currently
 *   the only way to obtain ownership.
 *
 * - The client yields ownership by either passing the record to DDlog as part
 *   of an update command (see `ddlog_apply_updates()`) or by
 *   attaching it to another owned object (e.g., appending it to a vector
 *   using `ddlog_vector_push()`).
 *
 * - Each owned record encapsulates some dynamically allocated memort.  To avoid
 *   memory leaks, the client must transfer the ownership of every record they
 *   own or deallocate the record using `ddlog_free()`.
 *
 * - There is a limited API for modifying _owned_ records, e.g., by
 *   appending elements to an array.
 *
 * - In addition to owned records, the client can also obtain pointers to
 *   *borrowed* records in one of two ways:
 *
 *	1. By invoking the `ddlog_dump_table()` function to enumerate the
 *   content of an output table.  This function takes a user callback and invokes it once
 *   for each record in the table.  The record, passed as argument to the
 *   callback is owned by DDlog and is only valid for the duration of the callback.
 *   (TODO: the only reason for this is convenience, so the client does not need to worry
 *   about deallocating the record later.  The API could be changed to return
 *   owned records.)
 *
 *	2. By querying another record of type tuple, vector, set or map.  For
 *   instance, `ddlog_get_vector_elem()` returns a borrowed reference to an
 *   element of the vector.
 *
 * - The client may inspect a borrowed record, but not modify it, attach to another
 *   records or pass to DDlog as part of an update.
 *
 * - Owned records are represented in the API by mutable pointers (`ddlog_record*`),
 *   whereas borrowed records are represented by immutable pointers
 *   (`const ddlog_record*`).
 *
 * Type checking:
 *
 * The Record API does not perform any type checking, e.g., one can create a
 * vector with elements of different types or a struct whose fields don't match
 * declarations in the DDlog program. Type checking is performed by DDlog
 * before inserting records to the database, e.g., in the `ddlog_apply_updates()`
 * function.  The function will fail if it detects type mismatch between relation type
 * and the record supplied by the client.
 *
 * The client can rely on records read from the database to have types that
 * matches the corresponsing DDlog output table declaration.
 */

/*
 * Deallocate an owned record.
 */
extern void ddlog_free(ddlog_record *rec);

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
 * Create an integer value.
 */
extern ddlog_record* ddlog_u64(uint64_t v);

/*
 * Returns `true` if `rec` is an integer and `false` otherwise
 */
extern bool ddlog_is_int(const ddlog_record *rec);

/*
 * Retrieves the value of an integer.
 *
 * Returns `0` if `rec` is not an integer.
 */
extern uint64_t ddlog_get_u64(const ddlog_record *rec);

/*
 * Create a string value.  This function copies `s` to an internal buffer,
 * so the caller is responsible for deallocating `s` if it was
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
 * WARNING: DDlog strings are _not_ null-terminated; use `ddlog_get_strlen()` to
 * determine the length of the string.
 *
 * The pointer returned by this function points to an internal DDlog buffer. The
 * caller must not modify the content of the string or deallocate this pointer.
 * The lifetime of the pointer coincides with the lifetime of the record it was
 * obtained from, e.g., the pointer is invalidated when the value is written to
 * the database.
 */
extern const char * ddlog_get_str_non_nul(const ddlog_record *rec);

/*
 * Create a tuple with specified fields.
 *
 * `len` is the length of the `fields` array.
 * If `len` is greater than `0`, then `fields` must not be NULL.
 *
 * The function takes ownership of all records, invalidating all
 * `ddlog_record` pointers in `fields`.  However it does not take ownership of
 * the `fields` array itself.  The caller is responsible for deallocating the
 * array if needed.
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
 * The pointer returned by this function is owned by DDlog. The caller may
 * inspect the returned record, but must not modify it, attach to other records
 * (e.g., using `ddlog_tuple_push()`) or write to the database.
 * The lifetime of the pointer coincides with the lifetime of the record it was
 * obtained from, e.g., the pointer is invalidated when the value is written to
 * the database.
 */
extern const ddlog_record* ddlog_get_tuple_field(const ddlog_record* tup,
						 size_t i);

/*
 * Convenience method to create a 2-tuple.  Such tuples are useful, e.g., in
 * constructing maps out of key-value pairs.
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
 * and then call `ddlog_tuple_push()` once for each field, in the order fields
 * appear in the tuple.
 *
 * This function takes ownership of `rec`, which should not be used after the
 * call.
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
 * The pointer returned by this function is owned by DDlog. The caller may
 * inspect the returned record, but must not modify it, attach to other records
 * (e.g., using `ddlog_vector_push()`) or write to the database.
 * The lifetime of the pointer coincides with the lifetime of the record it was
 * obtained from, e.g., the pointer is invalidated when the value is written to
 * the database.
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
 * `len` is the length of the `recs` array.
 * If `len` is greater than `0`, then `recs` must not be NULL.
 *
 * The function takes ownership of all records, invalidating all
 * `ddlog_record` pointers in `recs`.  However it does not take ownership of
 * the `recs` array itself.  The caller is responsible for deallocating the
 * array if needed.
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
 * Retrieves `i`th element of the set.  The `ddlog_record` type internally
 * represents sets as vectors of elements.  `idx` indexes into this vector.
 *
 * Returns NULL if `set` is not a set or if the set has fewer than `i` elements.
 *
 * The pointer returned by this function is owned by DDlog. The caller may
 * inspect the returned record, but must not modify it, attach to other records
 * (e.g., using `ddlog_vector_push()`) or write to the database.
 * The lifetime of the pointer coincides with the lifetime of the record it was
 * obtained from, e.g., the pointer is invalidated when the value is written to
 * the database.
 */
extern const ddlog_record* ddlog_get_set_elem(const ddlog_record* set,
					      size_t i);

/*
 * Append a value to a set.  The `ddlog_record` type internally
 * represents sets as vectors of elements.  These vectors only get converted to
 * an actual set representation inside DDlog.  In particular, duplicate values
 * are not merged inside a record.
 *
 * This function takes ownership of `rec`, which should not be used after the
 * call.
 */
extern void ddlog_set_push(ddlog_record *set, ddlog_record *rec);

/*
 * Create a map with specified elements.  Each element in `recs` must be a
 * 2-tuple representing a key-value pair.
 *
 * `len` is the length of the `recs` array.
 * If `len` is greater than `0`, then `recs` must not be NULL.
 *
 * The function takes ownership of all records, invalidating all
 * `ddlog_record` pointers in `recs`.  However it does not take ownership of
 * the `recs` array itself.  The caller is responsible for deallocating the
 * array if needed.
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
extern size_t ddlog_get_map_size(ddlog_record *rec);

/*
 * Retrieves the key of the `i`th element of the map.  The `ddlog_record` type
 * internally represents maps as vectors of elements.  `i` indexes into this vector.
 *
 * Returns NULL if `map` is not a map or if the map has fewer than `i` elements.
 *
 * The pointer returned by this function is owned by DDlog. The caller may
 * inspect the returned record, but must not modify it, attach to other records
 * (e.g., using `ddlog_vector_push()`) or write to the database.
 * The lifetime of the pointer coincides with the lifetime of the record it was
 * obtained from, e.g., the pointer is invalidated when the value is written to
 * the database.
 */
extern const ddlog_record* ddlog_get_map_key(const ddlog_record *map,
					     size_t i);

/*
 * Retrieves the value of the `i`th element of the map.  The `ddlog_record` type
 * internally represents maps as vectors of elements.  `i` indexes into this vector.
 *
 * Returns NULL if `map` is not a map or if the map has fewer than `i` elements.
 *
 * The pointer returned by this function is owned by DDlog. The caller may
 * inspect the returned record, but must not modify it, attach to other records
 * (e.g., using `ddlog_vector_push()`) or write to the database.
 * The lifetime of the pointer coincides with the lifetime of the record it was
 * obtained from, e.g., the pointer is invalidated when the value is written to
 * the database.
 */
extern const ddlog_record* ddlog_get_map_val(const ddlog_record *rec,
					     size_t i);

/*
 * Append a key-value pair to a map.  The `ddlog_record` type internally
 * represents maps as vectors of elements.  These vectors only get converted to
 * an actual map representation inside DDlog.  In particular, duplicate keys
 * are not merged inside a record.
 *
 * This function takes ownership of `key` and `val`, which should not be used
 * after the call.
 */
extern void ddlog_map_push(ddlog_record *map,
			   ddlog_record *key,
			   ddlog_record *val);

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
 * `len` is the length of the `args` array.
 * If `len` is greater than `0`, then `args` must not be NULL.
 *
 * The function takes ownership of all records, invalidating all
 * `ddlog_record` pointers in `args`.  However it does not take ownership of
 * the `recs` array itself.  The caller is responsible for deallocating the
 * array if needed.
 */
extern ddlog_record* ddlog_struct(const char* constructor,
				  ddlog_record ** args,
				  size_t len);

/*
 * Same as ddlog_struct(), but assumes that `constructor` is a statically
 * allocated string and stores the pointer internally instead of copying it to
 * another buffer.
 */
extern ddlog_record* ddlog_struct_static_cons(const char *constructor,
					      ddlog_record **args,
					      size_t len);

/*
 * Returns `true` if `rec` is a positional struct.
 */
extern bool ddlog_is_struct(const ddlog_record *rec);

/*
 * Retrieves constructor name as a non-null-terminated string.  Returns string
 * length in `len`.
 *
 * Returns NULL if `rec` is not a struct.
 */
extern const char * ddlog_get_constructor_non_null(const ddlog_record *rec,
						   size_t *len);


/*
 * Retrieves `i`th argument of a struct.
 *
 * Returns NULL if `rec` is not a struct or if the struct has fewer than `i`
 * arguments.
 *
 * The pointer returned by this function is owned by DDlog. The caller may
 * inspect the returned record, but must not modify it, attach to other records
 * (e.g., using `ddlog_vector_push()`) or write to the database.
 * The lifetime of the pointer coincides with the lifetime of the record it was
 * obtained from, e.g., the pointer is invalidated when the value is written to
 * the database.
 */
extern const ddlog_record* ddlog_get_struct_field(const ddlog_record* rec,
						  size_t i);

/***********************************************************************
 * Command API
 ***********************************************************************/

/*
 * Create an insert command.
 *
 * `table` is the null-terminated name of the table to insert to.
 * `rec` is the record to insert.  The function takes ownership of this record.
 *
 * Returns pointer to a new command, which can be sent to DDlog by calling
 * `ddlog_apply_updates()`.
 */
extern ddlog_cmd* ddlog_insert_cmd(const char *table,
				   ddlog_record *rec);

/*
 * Create delete-by-value command.
 *
 * `table` is the null-terminated name of the table to delete from.
 * `rec` is the record to delete.  The function takes ownership of this record.
 *
 * Returns pointer to a new command, which can be sent to DDlog by calling
 * `ddlog_apply_updates()`.
 */
extern ddlog_cmd* ddlog_delete_val_cmd(const char *table,
				       ddlog_record *rec);

/*
 * Create delete-by-key command.
 *
 * `table` is the null-terminated name of the table to delete from.
 * `rec` is the key to delete.  The table must have a primary key and `rec` type
 * must match the type of the key.  The function takes ownership of `rec`.
 *
 * Returns pointer to a new command, which can be sent to DDlog by calling
 * `ddlog_apply_updates()`.
 *
 */
extern ddlog_cmd* ddlog_delete_key_cmd(const char *table,
				       ddlog_record *rec);

#endif
