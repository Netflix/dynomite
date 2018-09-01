/*
 * Dynomite - A thin, distributed replication layer for multi non-distributed storages.
 * Copyright (C) 2014 Netflix, Inc.
 */ 

#ifndef _DYN_DNODE_MSG_H_
#define _DYN_DNODE_MSG_H_

#include <stdbool.h>

#include "dyn_queue.h"
#include "dyn_types.h"

typedef enum dmsg_version {
    VERSION_10 = 1
} dmsg_version_t;

typedef enum {
        DYN_START = 0,
        DYN_MAGIC_STRING = 1000,
        DYN_MSG_ID,
        DYN_TYPE_ID,
        DYN_BIT_FIELD,
        DYN_VERSION,
        DYN_SAME_DC,
        DYN_STAR,
        DYN_DATA_LEN,
        DYN_DATA,
        DYN_SPACES_BEFORE_PAYLOAD_LEN,
        DYN_PAYLOAD_LEN,
        DYN_CRLF_BEFORE_DONE,
        DYN_DONE,
        DYN_POST_DONE,
        DYN_UNKNOWN
} dyn_parse_state_t;

typedef enum dmsg_type {
    DMSG_UNKNOWN = 0,
    DMSG_DEBUG,
    DMSG_PARSE_ERROR,
    DMSG_REQ,
    DMSG_REQ_FORWARD,
    DMSG_RES,
    CRYPTO_HANDSHAKE,
    GOSSIP_SYN,
    GOSSIP_SYN_REPLY,
    GOSSIP_ACK,
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

    uint64_t             id;              /* message id */
    dmsg_type_t          type;            /* message type */
    uint8_t              flags;       /* bits to indicate encryption or decryption. Right most bit indicates encryption.
                                             2nd right most bit indicates compression */
    dmsg_version_t       version;         /* version of the message sender */
    uint8_t              same_dc;         /* indicate it is an inter_dc */

    struct sockaddr      *source_address; /* source ip */
    uint32_t mlen;                        /*  length */
    uint8_t  *data;                       /*  data */ 

    uint32_t plen;                        /* payload length */
    uint8_t  *payload;                    /* pointer to payload */
};


TAILQ_HEAD(dmsg_tqh, dmsg);


void dyn_parse_req(struct msg *r, const struct string *hash_tag);
void dyn_parse_rsp(struct msg *r, const struct string *UNUSED);

void dmsg_free(struct dmsg *dmsg);
void dmsg_put(struct dmsg *dmsg);
void dmsg_dump(struct dmsg *dmsg);
void dmsg_init(void);
void dmsg_deinit(void);
bool dmsg_empty(struct dmsg *msg);
struct dmsg *dmsg_get(void);
rstatus_t dmsg_write(struct mbuf *mbuf, uint64_t msg_id, uint8_t type,
		             struct conn *conn, uint32_t payload_len);

rstatus_t dmsg_write_mbuf(struct mbuf *mbuf, uint64_t msg_id, uint8_t type,
		                  struct conn *conn, uint32_t plen);
bool dmsg_process(struct context *ctx, struct conn *conn, struct dmsg *dmsg);

#endif
