/*
 * Dynomite - A thin, distributed replication layer for multi non-distributed storages.
 * Copyright (C) 2014 Netflix, Inc.
 */ 

#ifndef _DYN_DNODE_MSG_H_
#define _DYN_DNODE_MSG_H_

#include "dyn_core.h"


typedef enum dmsg_version {
    VERSION_10 = 1
} dmsg_version_t;


typedef enum dmsg_type {
    DMSG_DEBUG = 1,
    DMSG_UNKNOWN,
    DMSG_PARSE_ERROR,
    DMSG_REQ_MC_READ,                       /* memcache retrieval requests */
    DMSG_REQ_MC_WRITE,
    DMSG_REQ_MC_DELETE,
    GOSSIP_PING,
    GOSSIP_PING_REPLY,
    GOSSIP_DIGEST_SYN,
    GOSSIP_DIGEST_ACK,
    GOSSIP_DIGEST_ACK2,
    GOSSIP_SHUTDOWN
} dmsg_type_t;


struct dval {
    uint8_t  type;   
    uint32_t len;   /*  length */
    uint8_t  *data; /*  data */
};


struct dmsg {
    TAILQ_ENTRY(dmsg)     m_tqe;           /* link in free q */

    struct msg           *owner;
    struct mhdr          mhdr;            /* message mbuf header */

    uint64_t             id;              /* message id */
    dmsg_type_t          type;            /* message type */
    dmsg_version_t       version;         /* version of the message sender */

    struct sockaddr      *source_address; /* source ip */
    uint32_t mlen;                        /*  length */
    uint8_t  *data;                       /*  data */ 
};


TAILQ_HEAD(dmsg_tqh, dmsg);


void dyn_parse_req(struct msg *r);
void dyn_parse_rsp(struct msg *r);

void dmsg_free(struct dmsg *dmsg);
void dmsg_put(struct dmsg *dmsg);
void dmsg_dump(struct dmsg *dmsg);
void dmsg_init(void);
void dmsg_deinit(void);
bool dmsg_empty(struct dmsg *msg);
struct dmsg *dmsg_get(void);
rstatus_t dmsg_write(struct mbuf *mbuf, uint64_t msg_id, uint8_t type, uint8_t version, struct string *data);
bool dmsg_process(struct context *ctx, struct conn *conn, struct dmsg *dmsg);

#endif
