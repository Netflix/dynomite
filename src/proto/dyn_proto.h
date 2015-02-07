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

#include <dyn_core.h>


#ifndef _DN_PROTO_H_
#define _DN_PROTO_H_




void memcache_parse_req(struct msg *r);
void memcache_parse_rsp(struct msg *r);
void memcache_pre_splitcopy(struct mbuf *mbuf, void *arg);
rstatus_t memcache_post_splitcopy(struct msg *r);
void memcache_pre_coalesce(struct msg *r);
void memcache_post_coalesce(struct msg *r);
bool memcache_broadcast_racks(struct msg *r);

void redis_parse_req(struct msg *r);
void redis_parse_rsp(struct msg *r);
void redis_pre_splitcopy(struct mbuf *mbuf, void *arg);
rstatus_t redis_post_splitcopy(struct msg *r);
void redis_pre_coalesce(struct msg *r);
void redis_post_coalesce(struct msg *r);
bool redis_broadcast_racks(struct msg *r);

#endif
