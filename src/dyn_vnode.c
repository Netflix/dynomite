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

#include <math.h>
#include <stdio.h>
#include <stdlib.h>

#include <dyn_core.h>
#include <dyn_dnode_peer.h>
#include <dyn_server.h>
#include <dyn_vnode.h>


// Similar to strcmp() but compares 2 'dyn_token' structs instead.
static int vnode_item_cmp(const void *t1, const void *t2) {
  const struct continuum *ct1 = t1, *ct2 = t2;

  return cmp_dyn_token(ct1->token, ct2->token);
}

// Sorts the continuum for a rack based on their tokens.
static rstatus_t vnode_rack_verify_continuum(void *elem) {
  struct rack *rack = elem;
  qsort(rack->continuums.elem, rack->ncontinuum, sizeof(struct continuum),
        vnode_item_cmp);

  log_debug(LOG_VERB, "**** printing continuums for rack '%.*s'",
            rack->name->len, rack->name->data);
  uint32_t i;
  for (i = 0; i < rack->ncontinuum; i++) {
    struct continuum *c = (struct continuum*) array_get(&rack->continuums, i);
    ASSERT(c != NULL);
    log_debug(LOG_VERB, "next c[%d]: idx = %u, token->mag = %u", i, c->index,
              c->token->mag[0]);
  }
  log_debug(LOG_VERB, "**** end printing continuums for rack '%.*s'",
            rack->name->len, rack->name->data);

  return DN_OK;
}

rstatus_t vnode_update(struct server_pool *sp) {
  ASSERT(array_n(&sp->peers) > 0);

  uint32_t i, len;
  for (i = 0, len = array_n(&sp->peers); i < len; i++) {
    struct node *peer = *(struct node **)array_get(&sp->peers, i);

    log_debug(LOG_VERB, "peer name       : '%.*s'", peer->name.len,
              peer->name.data);
    log_debug(LOG_VERB, "peer rack       : '%.*s'", peer->rack.len,
              peer->rack.data);
    log_debug(LOG_VERB, "peer dc       : '%.*s'", peer->dc.len, peer->dc.data);
    log_debug(LOG_VERB, "peer->processed = %d", peer->processed);

    // update its own state
    if (i == 0) {
      peer->state = sp->ctx->dyn_state;
    }

    if (peer->processed) {
      continue;
    }

    peer->processed = 1;

    struct datacenter *dc = server_get_dc(sp, &peer->dc);
    struct rack *rack = server_get_rack(dc, &peer->rack);

    ASSERT(rack != NULL);

    uint32_t token_cnt = array_n(&peer->tokens);
    uint32_t orig_cnt = rack->nserver_continuum;
    uint32_t new_cnt = orig_cnt + token_cnt;

    struct continuum *continuum = array_push(&rack->continuums);
    if (continuum == NULL) {
      log_error("Could not allocate memory to expand the continuum.");
      return DN_ENOMEM;
    }
    rack->nserver_continuum = new_cnt;

    uint32_t j;
    for (j = 0; j < token_cnt; j++) {
      struct continuum *c = (struct continuum*) array_get(
          &rack->continuums, orig_cnt + j);
      ASSERT(c != NULL);
      c->index = i;
      c->value = 0; /* set this to an empty value, only used by ketama */
      c->token = array_get(&peer->tokens, j);
      rack->ncontinuum++;
    }

    if (array_n(&dc->racks) != 0) {
      rstatus_t status = array_each(&dc->racks, vnode_rack_verify_continuum);
      if (status != DN_OK) {
        return status;
      }
    }
  }

  return DN_OK;
}

uint32_t vnode_dispatch(struct array *continuums, uint32_t ncontinuum,
                        struct dyn_token *token) {
  struct continuum *left, *right, *middle;

  ASSERT(continuums != NULL);
  ASSERT(ncontinuum != 0);

  left = (struct continuum*) array_get(continuums, 0);
  right = (struct continuum*) array_get(continuums, ncontinuum - 1);

  if (cmp_dyn_token(right->token, token) < 0 ||
      cmp_dyn_token(left->token, token) >= 0)
    return left->index;

  while (left < right) {
    middle = left + (right - left) / 2;
    int32_t cmp = cmp_dyn_token(middle->token, token);
    if (cmp == 0) {
      return middle->index;
    } else if (cmp < 0) {
      left = middle + 1;
    } else {
      right = middle;
    }
  }

  return right->index;
}
