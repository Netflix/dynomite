
#ifndef DYN_GOSSIP_H_
#define DYN_GOSSIP_H_


#include "dyn_token.h"
#include "dyn_core.h"


#define GOS_NOOPS     1
#define GOS_OK        0
#define GOS_ERROR    -1

#define SIMPLE_PROVIDER           "simple_provider"
#define FLORIDA_PROVIDER          "florida_provider"

typedef uint8_t (*seeds_provider_t)(struct context *, struct string *);

struct node {
    struct dyn_token   token;        /* token for this node */
    struct string      region;
    struct string      dc;

    struct string      pname;         /* name:port */
    struct string      name;          /* name  */

    int                port;          /* port */

    int64_t            next_retry;    /* next retry time in usec */
    int64_t            last_retry;    /* last retry time in usec */
    uint32_t           failure_count; /* # consecutive failures */

    bool               is_seed;       /* seed? */
    bool               is_local;      /* is this peer the current running node?  */
    uint8_t            state;         /* state of a node that this host knows */
    uint64_t           ts;            /* timestamp */
};


struct gossip_dc {
    struct string      name;
    uint32_t           nnodes;           /* # total nodes */
    uint32_t           nlive_nodes;      /* # live nodes */
    struct array       nodes;            /* nodes */
};


struct gossip_node_pool {
	struct string      *name;                /* pool name (ref in conf_pool) */
    uint32_t           idx;                  /* pool index */
    struct context     *ctx;                 /* owner context */
    seeds_provider_t   seeds_provider;       /* seeds provider */
    struct array       datacenters;          /* gossip_dc  */
    uint32_t           nlive_server;         /* # live server */
    int64_t            last_run;             /* last time run in usec */

    int                g_interval;           /* gossip interval */

};


rstatus_t gossip_pool_init(struct context *ctx);
void gossip_pool_deinit(struct context *ctx);
rstatus_t gossip_start(struct server_pool *sp);
rstatus_t gossip_destroy(struct server_pool *sp);


rstatus_t gossip_peer_join(struct server_pool *sp, struct node *node);


#endif /* DYN_GOSSIP_H_ */
