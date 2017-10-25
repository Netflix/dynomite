/*
 * Dynomite - A thin, distributed replication layer for multi non-distributed storages.
 * Copyright (C) 2014 Netflix, Inc.
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

#include <dyn_token.h>
#include <dyn_core.h>
#include <murmur3.h>

#define MURMUR3_SEED 0xc0a1e5ce

rstatus_t
hash_murmur3(const unsigned char *key, size_t length, struct dyn_token *token)
{
    rstatus_t status = size_dyn_token(token, 4);
    if (status != DN_OK) {
        return status;
    }

//    MurmurHash3_x86_128(key, length, MURMUR3_SEED, token->mag);

    return DN_OK;
}
