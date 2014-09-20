/*
 * Dynomite - A thin, distributed replication layer for multi non-distributed storages.
 * Copyright (C) 2014 Netflix, Inc.
 */


#include "dyn_gossip.h"


#ifndef _DYN_RING_QUEUE_
#define _DYN_RING_QUEUE_


#define C2G_InQ_SIZE     256
#define C2G_OutQ_SIZE    256

#define C2S_InQ_SIZE     256
#define C2S_OutQ_SIZE    256

struct node;

typedef rstatus_t (*callback_t)(struct server_pool *, struct node *);


volatile struct
{
     long     m_getIdx;
     long     m_putIdx;
     void*    m_entry[C2G_InQ_SIZE];
} C2G_InQ;



volatile struct
{
     long     m_getIdx;
     long     m_putIdx;
     void*    m_entry[C2G_OutQ_SIZE];
} C2G_OutQ;



struct ring_message {
	callback_t         cb;
    struct array       nodes;
	struct server_pool *sp;
};



volatile struct
{
     long     m_getIdx;
     long     m_putIdx;
     void*    m_entry[C2S_InQ_SIZE];
} C2S_InQ;



volatile struct
{
     long     m_getIdx;
     long     m_putIdx;
     void*    m_entry[C2S_OutQ_SIZE];
} C2S_OutQ;


struct stat_message {
	void*         cb;
	void*         post_cb;
	void*         data;
	stats_cmd_t   cmd;
};




struct ring_message * create_ring_message(void);
struct ring_message *create_ring_message_with_size(uint32_t size, bool init_node);
rstatus_t ring_message_init(struct ring_message *msg, uint32_t size, bool init_node);
rstatus_t ring_message_deinit(struct ring_message *msg);

struct node * create_node(void);
rstatus_t node_init(struct node *node);
rstatus_t node_deinit(struct node *node);
rstatus_t node_copy(const struct node *src, struct node *dst);


#endif
