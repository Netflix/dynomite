/*
 * dyn_ring_queue.c
 *
 *  Created on: May 31, 2014
 *      Author: mdo
 */

#include "dyn_ring_queue.h"
#include "dyn_array.h"
#include "dyn_core.h"
#include "dyn_gossip.h"
#include "dyn_token.h"

_C2G_InQ C2G_InQ = {};
_C2G_OutQ C2G_OutQ = {};
// should use pooling to store struct ring_message so that we can reuse
struct ring_msg *create_ring_msg(void) {
  struct ring_msg *msg = dn_alloc(sizeof(*msg));

  if (msg == NULL) return NULL;

  ring_msg_init(msg, 1, true);
  msg->data = NULL;

  return msg;
}

struct ring_msg *create_ring_msg_with_data(uint32_t capacity) {
  struct ring_msg *msg = dn_alloc(sizeof(*msg));

  if (msg == NULL) return NULL;

  rstatus_t status = ring_msg_init(msg, 1, true);
  if (status != DN_OK) {
    dn_free(msg);
    return NULL;
  }

  msg->data = dn_zalloc(sizeof(uint8_t) * capacity);
  msg->capacity = capacity;
  msg->len = 0;

  return msg;
}

struct ring_msg *create_ring_msg_with_size(uint32_t size, bool init_node) {
  struct ring_msg *msg = dn_alloc(sizeof(*msg));

  if (msg == NULL) return NULL;

  rstatus_t status = ring_msg_init(msg, size, init_node);
  if (status != DN_OK) {
    dn_free(msg);
    return NULL;
  }

  msg->data = NULL;
  msg->capacity = 0;
  msg->len = 0;

  return msg;
}

rstatus_t ring_msg_init(struct ring_msg *msg, uint32_t n, bool init_node) {
  if (msg == NULL) return DN_ERROR;

  rstatus_t status = array_init(&msg->nodes, n, sizeof(struct gossip_node));
  if (status != DN_OK) return status;

  if (init_node) {
    uint32_t i;
    for (i = 0; i < n; i++) {
      struct gossip_node *node = array_push(&msg->nodes);
      node_init(node);
    }
  }

  return DN_OK;
}

rstatus_t ring_msg_deinit(struct ring_msg *msg) {
  if (msg == NULL) return DN_ERROR;

  uint32_t i;
  for (i = 0; i < array_n(&msg->nodes); i++) {
    struct gossip_node *node = array_get(&msg->nodes, i);
    node_deinit(node);
  }
  array_deinit(&msg->nodes);

  if (msg->data != NULL) {
    dn_free(msg->data);
  }

  dn_free(msg);

  return DN_OK;
}

struct gossip_node *create_node() {
  struct gossip_node *result = dn_alloc(sizeof(*result));
  node_init(result);

  return result;
}

rstatus_t node_init(struct gossip_node *node) {
  if (node == NULL) return DN_ERROR;

  init_dyn_token(&node->token);
  string_init(&node->dc);
  string_init(&node->rack);
  string_init(&node->name);
  string_init(&node->pname);

  node->port = 8101;

  node->is_local = false;
  node->state = INIT;

  return DN_OK;
}

rstatus_t node_deinit(struct gossip_node *node) {
  if (node == NULL) return DN_ERROR;

  // array_deinit(&node->tokens);
  string_deinit(&node->dc);
  string_deinit(&node->rack);
  string_deinit(&node->name);
  string_deinit(&node->pname);
  deinit_dyn_token(&node->token);

  // dn_free(node);

  return DN_OK;
}

rstatus_t node_copy(const struct gossip_node *src, struct gossip_node *dst) {
  if (src == NULL || dst == NULL) return DN_ERROR;

  dst->state = src->state;
  dst->is_local = src->is_local;
  dst->port = src->port;
  dst->is_secure = src->is_secure;

  string_copy(&dst->pname, src->pname.data, src->pname.len);
  string_copy(&dst->name, src->name.data, src->name.len);
  string_copy(&dst->rack, src->rack.data, src->rack.len);
  string_copy(&dst->dc, src->dc.data, src->dc.len);

  copy_dyn_token(&src->token, &dst->token);
  return DN_OK;
}
