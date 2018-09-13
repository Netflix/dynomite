#include "dyn_hashkit.h"

#include "../dyn_string.h"

#define DEFINE_ACTION(_hash, _name) string(#_name),
struct string hash_strings[] = {HASH_CODEC(DEFINE_ACTION) null_string};
#undef DEFINE_ACTION

// Defines all the hashing functions
#define DEFINE_ACTION(_hash, _name)                               \
  rstatus_t hash_##_name(const unsigned char *key, size_t length, \
                         struct dyn_token *token);
HASH_CODEC(DEFINE_ACTION)

#undef DEFINE_ACTION

// Creates an array of hash functions
#define DEFINE_ACTION(_hash, _name) hash_##_name,
static hash_func_t hash_algos[] = {HASH_CODEC(DEFINE_ACTION) NULL};
#undef DEFINE_ACTION

hash_func_t get_hash_func(hash_type_t hash_type) {
  if ((hash_type >= 0) && (hash_type < HASH_INVALID))
    return hash_algos[hash_type];
  return NULL;
}

hash_type_t get_hash_type(struct string *hash_name) {
  struct string *hash_iter;
  for (hash_iter = hash_strings; hash_iter->len != 0; hash_iter++) {
    if (string_compare(hash_name, hash_iter) != 0) {
      continue;
    }

    return hash_iter - hash_strings;
  }
  return HASH_INVALID;
}
