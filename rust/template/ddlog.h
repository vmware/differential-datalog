#ifndef __DDLOG_H__
#define __DDLOG_H__

typedef void *datalog_example_ddlog_prog;


extern datalog_example_ddlog_prog* datalog_example_run(unsigned int workers);
extern int datalog_example_stop(datalog_example_ddlog_prog *prog);

extern int datalog_example_transaction_start(datalog_example_ddlog_prog *prog);
extern int datalog_example_transaction_commit(datalog_example_ddlog_prog * prog);
extern int datalog_example_transaction_rollback(datalog_example_ddlog_prog *prog);

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
extern int ddlog_example_apply_ovsdb_updates(
	ddlog_program *prog,
	const char *prefix,
	const char *updates);

#endif
