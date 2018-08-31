/*
 * Dynomite - A thin, distributed replication layer for multi non-distributed
 * storages. Copyright (C) 2014 Netflix, Inc.
 */

/*
 * twemproxy - A fast and lightweight proxy for memcached protocol.
 * Copyright (C) 2011 Twitter, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include <dyn_core.h>
#include <dyn_token.h>

static uint64_t FNV_64_INIT = UINT64_C(0xcbf29ce484222325);
static uint64_t FNV_64_PRIME = UINT64_C(0x100000001b3);
static uint32_t FNV_32_INIT = 2166136261UL;
static uint32_t FNV_32_PRIME = 16777619;

rstatus_t hash_fnv1_64(const unsigned char *key, size_t key_length,
                       struct dyn_token *token) {
  uint64_t hash = FNV_64_INIT;
  size_t x;

  for (x = 0; x < key_length; x++) {
    hash *= FNV_64_PRIME;
    hash ^= (uint64_t)key[x];
  }

  // note: original version simply downcast the uint64_t to uint32_t
  uint32_t val = (uint32_t)hash;
  size_dyn_token(token, 1);
  set_int_dyn_token(token, val);

  return DN_OK;
}

rstatus_t hash_fnv1a_64(const unsigned char *key, size_t key_length,
                        struct dyn_token *token) {
  uint32_t hash = (uint32_t)FNV_64_INIT;
  size_t x;

  for (x = 0; x < key_length; x++) {
    uint32_t val = (uint32_t)key[x];
    hash ^= val;
    hash *= (uint32_t)FNV_64_PRIME;
  }

  size_dyn_token(token, 1);
  set_int_dyn_token(token, hash);

  return DN_OK;
}

rstatus_t hash_fnv1_32(const unsigned char *key, size_t key_length,
                       struct dyn_token *token) {
  uint32_t hash = FNV_32_INIT;
  size_t x;

  for (x = 0; x < key_length; x++) {
    uint32_t val = (uint32_t)key[x];
    hash *= FNV_32_PRIME;
    hash ^= val;
  }

  size_dyn_token(token, 1);
  set_int_dyn_token(token, hash);

  return DN_OK;
}

rstatus_t hash_fnv1a_32(const unsigned char *key, size_t key_length,
                        struct dyn_token *token) {
  uint32_t hash = FNV_32_INIT;
  size_t x;

  for (x = 0; x < key_length; x++) {
    uint32_t val = (uint32_t)key[x];
    hash ^= val;
    hash *= FNV_32_PRIME;
  }

  size_dyn_token(token, 1);
  set_int_dyn_token(token, hash);

  return DN_OK;
}
