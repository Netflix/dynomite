/*
 * Dynomite - A thin, distributed replication layer for multi non-distributed storages.
 * Copyright (C) 2014 Netflix, Inc.
 */


#ifndef _DYN_RING_QUEUE_
#define _DYN_RING_QUEUE_


#define C2G_InQ_SIZE     256
#define C2G_OutQ_SIZE    256

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



struct node {
    struct array       tokens;        /* array of dyn_tokens */
    struct string      dc;
    //struct gossip_dc   *dc;           /* logical datacenter */

    struct string      pname;         /* name:port */
    struct string      name;          /* name  */
    int                port;          /* port */
    int                family;        /* socket family */
    socklen_t          addrlen;       /* socket length */
    struct sockaddr    *addr;         /* socket address  */

    int64_t            next_retry;    /* next retry time in usec */
    int64_t            last_retry;    /* last retry time in usec */
    uint32_t           failure_count; /* # consecutive failures */

    bool               is_seed;       /* seed? */
    bool               is_local;      /* is this peer the current running node?  */
    uint8_t            status;        /* 0: down, 1: up, 2:unknown */
};


struct ring_message {
	callback_t         cb;
    struct node        *node;
	struct server_pool *sp;
};


struct ring_message * create_ring_message(void);
rstatus_t ring_message_init(struct ring_message *msg);
rstatus_t ring_message_deinit(struct ring_message *msg);

struct node * create_node(void);
rstatus_t node_init(struct node *node);
rstatus_t node_deinit(struct node *node);
rstatus_t node_copy(const struct node *src, struct node *dst);


#endif
