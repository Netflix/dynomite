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

#include <stdio.h>
#include <stdlib.h>
#include <math.h>

#include <dyn_peer.h>
#include <nc_core.h>
#include <nc_server.h>
#include <nc_hashkit.h>

static int
vnode_item_cmp(const void *t1, const void *t2)
{
    const struct continuum *ct1 = t1, *ct2 = t2;

    return cmp_dyn_token(ct1->token, ct2->token);
}

rstatus_t
vnode_datacenter_verify_continuum(void *elem, void *data)
{
    struct datacenter *dc = elem;
    qsort(dc->continuum, dc->ncontinuum, sizeof(*dc->continuum),
          vnode_item_cmp);

    log_debug(LOG_VERB, "**** printing continuums for dc '%.*s'", dc->name->len, dc->name->data);
    for (uint32_t i = 0; i < dc->ncontinuum; i++) {
        struct continuum *c = &dc->continuum[i];
        log_debug(LOG_VERB, "next c[%d]: idx = %u, token->mag = %u", i, c->index, c->token->mag[0]);
    }
    log_debug(LOG_VERB, "**** end printing continuums for dc '%.*s'", dc->name->len, dc->name->data);

    return NC_OK;
}

rstatus_t
vnode_update(struct server_pool *pool)
{
    ASSERT(array_n(&pool->peers) > 0);

    int64_t now = nc_usec_now();
    if (now < 0) {
        return NC_ERROR;
    }

    for (int i = 0, len = array_n(&pool->peers); i < len; i++) {
        struct peer *peer = array_get(&pool->peers, i);
        struct datacenter *dc = server_get_datacenter(pool, &peer->dc);

        if (dc == NULL) {
            dc = array_push(&pool->datacenter);
            datacenter_init(dc);
            dc->name = &peer->dc;
        }

        uint32_t token_cnt = array_n(&peer->tokens);
        uint32_t orig_cnt = dc->nserver_continuum;
        uint32_t new_cnt = orig_cnt + token_cnt;
        struct continuum *continuum = nc_realloc(dc->continuum, sizeof(*continuum) * new_cnt);
        if (continuum == NULL) {
            return NC_ENOMEM;
        }

        dc->continuum = continuum;
        dc->nserver_continuum = new_cnt;

        for (int j = 0; j < token_cnt; j++) {
            struct continuum *c = &dc->continuum[orig_cnt + j];
            c->index = i;
            c->value = 0;  /* set this to an empty value, only used by ketama */
            c->token = array_get(&peer->tokens, j);
            dc->ncontinuum++;
        }
    }

    rstatus_t status = array_each(&pool->datacenter, vnode_datacenter_verify_continuum, NULL);
    if (status != NC_OK) {
        return status;
    }

    return NC_OK;
}


uint32_t
vnode_dispatch(struct continuum *continuum, uint32_t ncontinuum, struct dyn_token *token)
{
    struct continuum *begin, *end, *left, *right, *middle;

    ASSERT(continuum != NULL);
    ASSERT(ncontinuum != 0);

    begin = left = continuum;
    end = right = continuum + ncontinuum;

    while (left < right) {
        middle = left + (right - left) / 2;
        int32_t cmp = cmp_dyn_token(middle->token, token);
        if (cmp == 0) {
            return right->index;
        } else if (cmp < 0) {
          left = middle + 1;
        } else {
          right = middle;
        }
    }

    if (right == end) {
        right = begin;
    }

    return right->index;
}
