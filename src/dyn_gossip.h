#include "hashkit/dyn_token.h"
#include "dyn_core.h"
#include "dyn_dict.h"

#ifndef DYN_GOSSIP_H_
#define DYN_GOSSIP_H_


#define GOS_NOOPS     1
#define GOS_OK        0
#define GOS_ERROR    -1

#define SIMPLE_PROVIDER           "simple_provider"
#define FLORIDA_PROVIDER          "florida_provider"
#define DNS_PROVIDER              "dns_provider"

#define SEED_BUF_SIZE (1024*1024)     //in bytes


typedef uint8_t (*seeds_provider_t)(struct context *, struct mbuf *);
extern struct gossip_node_pool gn_pool;


// In comparison to conf_server in dyn_conf.h, this structure,
// has sockinfo & valid flag missing
// It has is_local, state, and timestamp extra
// Also in conf_server, pname is name:port:weight,
// whereas here it is just name:port
struct gossip_node {
    struct string      pname;            /* name:port */
    struct string      name;             /* name  */
    int                port;             /* port */
    // info is missing
    struct dyn_token   token;            /* token for this node */
    struct string      rack;
    struct string      dc;
    bool               is_secure;        /* is a secured conn */

    bool               is_local;         /* is this peer the current running node?  */
    uint8_t            state;            /* state of a node that this host knows */
    uint64_t           ts;               /* timestamp */

};


struct gossip_rack {
    struct string      name;
    struct string      dc;
    uint32_t           nnodes;           /* # total nodes */
    uint32_t           nlive_nodes;      /* # live nodes */
    struct array       nodes;            /* nodes */
    dict               *dict_token_nodes;
    dict               *dict_name_nodes;
};


struct gossip_dc {
	struct string      name;            /* datacenter name */
	struct array       racks;           /* list of gossip_rack in a datacenter */
	dict               *dict_rack;
};

struct gossip_node_pool {
	struct string      *name;                /* pool name (ref in conf_pool) */
    uint32_t           idx;                  /* pool index */
    struct context     *ctx;                 /* owner context */
    seeds_provider_t   seeds_provider;       /* seeds provider */
    struct array       datacenters;          /* gossip datacenters */
    int64_t            last_run;             /* last time run in usec */
    int                g_interval;           /* gossip interval */
    dict               *dict_dc;

};


rstatus_t gossip_pool_init(struct context *ctx);
void gossip_pool_deinit(struct context *ctx);
rstatus_t gossip_start(struct server_pool *sp);
rstatus_t gossip_destroy(struct server_pool *sp);


rstatus_t gossip_msg_peer_update(void *msg);


#endif /* DYN_GOSSIP_H_ */
