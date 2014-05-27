
#ifndef DYN_GOSSIP_H_
#define DYN_GOSSIP_H_

#include "dyn_core.h"
#include "hashkit/dyn_token.h"

struct socket_conn  {
    struct string      pname;         /* name:port:weight (ref in conf_server) */
    struct string      name;          /* name (ref in conf_server) */
    uint16_t           port;          /* port */
    uint32_t           weight;        /* weight */
    int                family;        /* socket family */
    socklen_t          addrlen;       /* socket length */
    struct sockaddr    *addr;         /* socket address (ref in conf_server) */
};

struct node {
    struct dyn_token   *token;        /* used in vnode/dyn_token situations */
    struct gossip_dc   *dc;           /* logical datacenter */
    struct socket_conn socket_conn;

    int64_t            next_retry;    /* next retry time in usec */
    int64_t            last_retry;    /* last retry time in usec */
    uint32_t           failure_count; /* # consecutive failures */

    bool               is_seed;       /* seed? */
    bool               is_local;      /* is this peer the current running node?  */
};

struct gossip_dc {
    struct string      *name;
    uint32_t           nnodes;        /* # total nodes */
    uint32_t           nlive_nodes;   /* # live nodes */
    struct array       nodes;        /* nodes */
};


struct gossip_node_pool {
	struct string      *name;                 /* pool name (ref in conf_pool) */
    uint32_t           idx;                  /* pool index */
    struct context     *ctx;                 /* owner context */

    struct array       datacenters;          /* gossip_dc  */
    uint32_t           nlive_server;         /* # live server */
    int64_t            last_run;             /* last time run in usec */

    struct string      seed_provider;
    //struct array       seeds;                /*dyn seeds */
    int                g_interval;           /* gossip interval */

};



rstatus_t gossip_pool_init(struct context *ctx);
void gossip_pool_deinit(struct context *ctx);



#endif /* DYN_GOSSIP_H_ */
