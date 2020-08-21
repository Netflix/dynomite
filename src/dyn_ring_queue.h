/*
 * Dynomite - A thin, distributed replication layer for multi non-distributed
 * storages. Copyright (C) 2014 Netflix, Inc.
 */

#include "dyn_gossip.h"

#ifndef _DYN_RING_QUEUE_
#define _DYN_RING_QUEUE_

#define C2G_InQ_SIZE 256
#define C2G_OutQ_SIZE 256

struct gossip_node;

typedef rstatus_t (*callback_t)(void *msg);
typedef void (*data_func_t)(void *);

typedef volatile struct {
  long m_getIdx;
  long m_putIdx;
  void *m_entry[C2G_InQ_SIZE];
} _C2G_InQ;

typedef volatile struct {
  long m_getIdx;
  long m_putIdx;
  void *m_entry[C2G_OutQ_SIZE];
} _C2G_OutQ ;

extern _C2G_InQ C2G_InQ;
extern _C2G_OutQ C2G_OutQ;

struct ring_msg {
  callback_t cb;
  uint8_t *data;     /* place holder for a msg */
  uint32_t capacity; /* max capacity */
  uint32_t len;      /* # of useful bytes in data (len =< capacity) */
  struct array nodes;
  struct server_pool *sp;
};

struct ring_msg *create_ring_msg(void);
struct ring_msg *create_ring_msg_with_data(uint32_t capacity);
struct ring_msg *create_ring_msg_with_size(uint32_t size, bool init_node);
rstatus_t ring_msg_init(struct ring_msg *msg, uint32_t size, bool init_node);
rstatus_t ring_msg_deinit(struct ring_msg *msg);

struct gossip_node *create_node(void);
rstatus_t node_init(struct gossip_node *node);
rstatus_t node_deinit(struct gossip_node *node);
rstatus_t node_copy(const struct gossip_node *src, struct gossip_node *dst);

#endif
