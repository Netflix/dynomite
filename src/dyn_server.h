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

#ifndef _DYN_SERVER_H_
#define _DYN_SERVER_H_

#include "dyn_core.h"
#include "dyn_token.h"
#include "dyn_dict.h"

/*
 * server_pool is a collection of servers and their continuum. Each
 * server_pool is the owner of a single proxy connection and one or
 * more client connections. server_pool itself is owned by the current
 * context.
 *
 * Each server is the owner of one or more server connections. server
 * itself is owned by the server_pool.
 *
 *  +-------------+
 *  |             |<---------------------+
 *  |             |<------------+        |
 *  |             |     +-------+--+-----+----+--------------+
 *  |   pool 0    |+--->|          |          |              |
 *  |             |     | server 0 | server 1 | ...     ...  |
 *  |             |     |          |          |              |--+
 *  |             |     +----------+----------+--------------+  |
 *  +-------------+                                             //
 *  |             |
 *  |             |
 *  |             |
 *  |   pool 1    |
 *  |             |
 *  |             |
 *  |             |
 *  +-------------+
 *  |             |
 *  |             |
 *  .             .
 *  .    ...      .
 *  .             .
 *  |             |
 *  |             |
 *  +-------------+
 *            |
 *            |
 *            //
 */


msec_t server_timeout(struct conn *conn);
rstatus_t server_init(struct server_pool *sp, struct array *conf_server);
rstatus_t server_connect(struct context *ctx, struct datastore *server, struct conn *conn);

struct conn *get_datastore_conn(struct thread_ctx *ptctx);
rstatus_t server_pool_init(struct server_pool *server_pool, struct conf_pool *conf_pool, struct context *ctx);
void server_pool_deinit(struct server_pool *server_pool);
rstatus_t server_pool_init_my_dc_rack(struct server_pool *sp);
void init_server_conn(struct conn *conn);
void datastore_req_forward(struct conn *c_conn, struct msg *msg, uint8_t *key,
                           uint32_t keylen);
void datastore_forward_stats(struct context *ctx, struct msg *msg);

#endif
