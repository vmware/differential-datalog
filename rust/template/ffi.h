#include <stdbool.h>
#include <stdint.h>

struct Int;
struct Uint;

struct Uint* uint_from_u64(uint64_t v);
struct Uint* uint_from_str(const char *s, uint32_t radix);
void uint_free(struct Uint*);

struct Int* int_from_i64(int64_t v);
struct Int* int_from_u64(uint64_t v);
struct Int* int_from_str(const char* s, uint32_t radix);
void int_free(struct Int*);

