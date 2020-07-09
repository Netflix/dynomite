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

#include "dyn_core.h"

#ifndef _DYN_CLIENT_H_
#define _DYN_CLIENT_H_

void init_client_conn(struct conn *conn);

rstatus_t req_forward_to_peer(struct context *ctx, struct conn *c_conn,
     struct msg *req, struct node *peer, uint8_t* key, uint32_t keylen,
     struct mbuf *orig_mbuf, bool force_copy, bool force_swallow,
     dyn_error_t *dyn_error_code);

#endif
