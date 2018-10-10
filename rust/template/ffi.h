#include <stdbool.h>
#include <stdint.h>
#include <stddef.h>

struct Int;
struct Uint;

typedef size_t RelId;

struct Uint* uint_from_u64(uint64_t v);
struct Uint* uint_from_str(const char *s, uint32_t radix);
void uint_free(struct Uint*);

struct Int* int_from_i64(int64_t v);
struct Int* int_from_u64(uint64_t v);
struct Int* int_from_str(const char *s, uint32_t radix);
void int_free(struct Int*);

struct Uint128_le_t {
    uint64_t word0;
    uint64_t word1;
};

struct Value;
typedef void* RunningProgram;

typedef void (*UpdateCallback)(uintptr_t ctx,
			       RelId relid,
			       struct Value* val,
			       bool pol);

RunningProgram* datalog_example_run(UpdateCallback upd_cb, uintptr_t ctx);
int datalog_example_stop(RunningProgram *prog);
int datalog_example_transaction_start(RunningProgram *prog);
int datalog_example_transaction_commit(RunningProgram *prog);
int datalog_example_transaction_rollback(RunningProgram *prog);
int datalog_example_insert(RunningProgram *prog,
			   RelId relid,
			   const struct Value *v);
int datalog_example_delete(RunningProgram *prog,
			   RelId relid,
			   const struct Value *v);
enum UpdateOp {
    UpdateInsert,
    UpdateDelete,
    UpdateDeleteKey
}

struct Update {
    enum UpdateOp op;
    struct Value *v;
};

int datalog_example_apply_updates(RunningProgram *prog,
				  struct Update *updates,
				  size_t nupdates);

typedef void* ValMap;

ValMap val_map_new();
void val_map_print(ValMap vmap);
void val_map_print_rel(ValMap vmap, RelId relid);
void val_map_update(ValMap vmap, RelId relid, struct Value* val, bool pol);
