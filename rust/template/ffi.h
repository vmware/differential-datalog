#include <stdbool.h>
#include <stdint.h>
#include <stddef.h>

struct Int;
struct Uint;

struct Uint* uint_from_u64(uint64_t v);
struct Uint* uint_from_str(const char *s, uint32_t radix);
void uint_free(struct Uint*);

struct Int* int_from_i64(int64_t v);
struct Int* int_from_u64(uint64_t v);
struct Int* int_from_str(const char *s, uint32_t radix);
void int_free(struct Int*);

struct Value;
typedef void* RunningProgram;

typedef void (*UpdateCallback)(size_t relid, const struct Value* val, bool pol);

RunningProgram* datalog_example_run(UpdateCallback * upd_cb);
int datalog_example_stop(RunningProgram *prog);
int datalog_example_transaction_start(RunningProgram *prog);
int datalog_example_transaction_commit(RunningProgram *prog);
int datalog_example_transaction_rollback(RunningProgram *prog);
int datalog_example_insert(RunningProgram *prog,
			   size_t relid,
			   const struct Value *v);
int datalog_example_delete(RunningProgram *prog,
			   size_t relid,
			   const struct Value *v);
struct Update {
    bool pol;
    struct Value *v;
};

int datalog_example_apply_updates(RunningProgram *prog,
				  struct Update *updates,
				  size_t nupdates);
