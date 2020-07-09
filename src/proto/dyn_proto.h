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

#ifndef _DN_PROTO_H_
#define _DN_PROTO_H_

#include <stdbool.h>

#include "../dyn_message.h"
#include "../dyn_types.h"

// Forward declarations
struct context;
struct msg;
struct msg_tqh;
struct rack;
struct response_mgr;
struct server_pool;
struct string;

void memcache_parse_req(struct msg *r, struct context *ctx);
void memcache_parse_rsp(struct msg *r, struct context *ctx);
void memcache_pre_coalesce(struct msg *r);
void memcache_post_coalesce(struct msg *r);
bool memcache_is_multikey_request(struct msg *r);
struct msg *memcache_reconcile_responses(struct response_mgr *rspmgr);
rstatus_t memcache_fragment(struct msg *r, struct server_pool *pool,
                            struct rack *rack, struct msg_tqh *frag_msgq);
rstatus_t memcache_verify_request(struct msg *r, struct server_pool *pool,
                                  struct rack *rack);
rstatus_t memcache_rewrite_query(struct msg *orig_msg, struct context *ctx,
                                 bool *did_rewrite, struct msg **new_msg_ptr);
rstatus_t memcache_rewrite_query_with_timestamp_md(struct msg *orig_msg,
    struct context *ctx, bool *did_rewrite, struct msg **new_msg_ptr);
rstatus_t memcache_make_repair_query(struct context *ctx, struct response_mgr *rspmgr,
    struct msg **new_msg_ptr);
rstatus_t memcache_clear_repair_md_for_key(struct context *ctx, struct msg *req,
    struct msg **new_msg_ptr);

void redis_parse_req(struct msg *r, struct context *ctx);
void redis_parse_rsp(struct msg *r, struct context *ctx);
void redis_pre_coalesce(struct msg *r);
void redis_post_coalesce(struct msg *r);
bool redis_is_multikey_request(struct msg *r);
struct msg *redis_reconcile_responses(struct response_mgr *rspmgr);
rstatus_t redis_fragment(struct msg *r, struct server_pool *pool,
                         struct rack *rack, struct msg_tqh *frag_msgq);
rstatus_t redis_verify_request(struct msg *r, struct server_pool *pool,
                               struct rack *rack);
rstatus_t redis_rewrite_query(struct msg *orig_msg, struct context *ctx,
                              bool *did_rewrite, struct msg **new_msg_ptr);
rstatus_t redis_rewrite_query_with_timestamp_md(struct msg *orig_msg,
    struct context *ctx, bool *did_rewrite, struct msg **new_msg_ptr);
rstatus_t redis_make_repair_query(struct context *ctx, struct response_mgr *rspmgr,
    struct msg **new_msg_ptr);
rstatus_t redis_clear_repair_md_for_key(struct context *ctx, struct msg *req,
    struct msg **new_msg_ptr);

#endif
