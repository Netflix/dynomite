#ifndef _NC_DYN_MESSAGE_H_
#define _NC_DYN_MESSAGE_H_

//#include <nc_message.h>

typedef enum dmsg_version {
    VERSION_10
} dmsg_version_t;


typedef enum dmsg_type {
    DMSG_UNKNOWN,
    DMSG_REQ_MC_READ,                       /* memcache retrieval requests */
    DMSG_REQ_MC_WRITE,
    DMSG_REQ_MC_DELETE,
    GOSSIP_DIGEST_SYN,
    GOSSIP_DIGEST_ACK,
    GOSSIP_DIGEST_ACK2,
    GOSSIP_SHUTDOWN,
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
    dmsg_version_t       version;         /* version of the message sender */

    struct mhdr          mhdr;            /* message mbuf header */

    uint32_t             mlen;            /* message length */

    int                  state;           /* current parser state */
    uint8_t              *pos;            /* parser position marker */
    uint8_t              *token;          /* token marker */
  
    struct sockaddr      source_address;
    struct dval          arg1; 
};


TAILQ_HEAD(dmsg_tqh, dmsg);


void dyn_parse_req(struct msg *r);


void dmsg_free(struct dmsg *dmsg);
void dmsg_put(struct dmsg *dmsg);
void dmsg_dump(struct dmsg *dmsg);
void dmsg_init(void);
void dmsg_deinit(void);
bool dmsg_empty(struct dmsg *msg);
struct dmsg *dmsg_get(void);
rstatus_t dmsg_write(struct dmsg *dmsg);

#endif
