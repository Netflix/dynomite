
#ifndef DYN_GOSSIP_H_
#define DYN_GOSSIP_H_

#include "dyn_core.h"
#include "hashkit/dyn_token.h"


#define GOS_NOOPS     1
#define GOS_OK        0
#define GOS_ERROR    -1


typedef uint8_t (*seeds_provider_t)(struct context *, struct string *);


struct socket_conn  {
    struct string      pname;         /* name:port:weight (ref in conf_server) */
    struct string      name;          /* name (ref in conf_server) */
    uint16_t           port;          /* port */
    int                family;        /* socket family */
    socklen_t          addrlen;       /* socket length */
    struct sockaddr    *addr;         /* socket address (ref in conf_server) */
};

struct node {
    struct array       tokens;        /* array of dyn_tokens */
    struct gossip_dc   *dc;           /* logical datacenter */
    struct socket_conn socket_conn;

    int64_t            next_retry;    /* next retry time in usec */
    int64_t            last_retry;    /* last retry time in usec */
    uint32_t           failure_count; /* # consecutive failures */

    bool               is_seed;       /* seed? */
    bool               is_local;      /* is this peer the current running node?  */
    uint8_t            status;        /* 0: down, 1: up, 2:unknown */
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



#endif /* DYN_GOSSIP_H_ */
