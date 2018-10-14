#ifndef __DDLOG_H__
#define __DDLOG_H__

/*
 * *Note:* all functions in this library are thread-safe. E.g., it is legal to
 * call `datalog_example_transaction_start()` from thread 1,
 * `datalog_example_apply_ovsdb_updates()` from thread 2, and
 * `datalog_example_transaction_commit()` from thread 3.  Multiple concurrent
 * updates from different threads are also valid.
 *
 * However, DDlog currently does not support concurrent or nested transactions.
 * An attempt to start a transaction while another transaction is in progress (in
 * the same or different thread) will return an error.
 */

/*
 * Opaque handle to an instance of DDlog program.
 */
typedef void * datalog_example_ddlog_prog;

/*
 * Create an instance of DDlog program.
 *
 * `workers` is the number of DDlog worker threads that will be allocated to run the program.
 * While any positive integer value is valid, values larger than the number of cores in
 * the system are likely to hurt the performance.
 *
 * Returns a program handle to be used in subsequent calls to
 * `datalog_example_transaction_start()`,
 * `datalog_example_transaction_commit()`, etc., or NULL in case of error.
 */
extern datalog_example_ddlog_prog datalog_example_run(unsigned int workers);

/*
 * Stops the program; deallocates all resources, invalidates the handle.
 *
 * All concurrent calls using the handle must complete before calling this
 * function.
 *
 * On success, returns `0`; on error, returns `-1` and prints error message
 * to `stderr`.
 */
extern int datalog_example_stop(datalog_example_ddlog_prog hprog);

/*
 * Start a transaction.
 *
 * On success, returns `0`; on error, returns `-1` and prints error message
 * to `stderr`.
 *
 * This function will fail if another transaction is in progress.
 *
 * Within a transaction, updates to input relations are buffered until
 * `datalog_example_transaction_commit()` is called.
 */
extern int datalog_example_transaction_start(datalog_example_ddlog_prog hprog);

/*
 * Commit a transaction; propagate all buffered changes through all rules in the
 * program and update all computed relations.
 *
 * On success, returns `0`; on error, returns `-1` and prints error message
 * to `stderr`.
 *
 * This function will fail if there is no transaction in progress.
 */
extern int datalog_example_transaction_commit(datalog_example_ddlog_prog hprog);

/*
 * Discard all buffered updates and abort the current transaction.
 *
 * On success, returns `0`; on error, returns `-1` and prints error message
 * to `stderr`.
 *
 * This function will fail if there is no transaction in progress.
 */
extern int datalog_example_transaction_rollback(datalog_example_ddlog_prog hprog);

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
extern int datalog_example_apply_ovsdb_updates(
	ddlog_program hprog,
	const char *prefix,
	const char *updates);

#endif
