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

#ifndef _DYN_TOKEN_H_
#define _DYN_TOKEN_H_

#include <nc_core.h>

struct dyn_token {
    uint32_t signum;
    uint32_t *mag;
    uint32_t len;
};

void init_dyn_token(struct dyn_token *token);
rstatus_t set_dyn_token(uint8_t *start, uint8_t *end, struct dyn_token *token);
uint32_t cmp_dyn_token(struct dyn_token *t1, struct dyn_token *t2);

#endif
