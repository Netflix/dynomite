#ifndef _DYN_RING_H_
#define _DYN_RING_H_

#include <nc_core.h>

struct ring_node {
    struct server_pool *owner;        /* owner pool */

    struct string      pname;         /* name:port:weight (ref in conf_server) */
    struct string      name;          /* name (ref in conf_server) */
    uint16_t           port;          /* port */
    int                family;        /* socket family */
    socklen_t          addrlen;       /* socket length */
    struct sockaddr    *addr;         /* socket address (ref in conf_server) */

    uint32_t           ns_conn_q;     /* #peer connection */
    struct conn_tqh    s_conn_q;      /* peer connection q */

    // this is where, is suspect, we'll keep things like gossip's vclock, 
    // and other, node-specific state
    struct string      dc;            /* logical datacenter */
    struct array       tokens;        /* DHT tokens this peer owns */
    bool               is_local;      /* is this peer the current running node?  */
    unsigned           is_seed:1;     /* seed? */    

};

struct dyn_ring {
    struct array      ring_nodes; /* array of ring_nodes currently known in the cluster  */
};

rstatus_t dyn_ring_init(struct array *conf_seeds, struct server_pool *sp);

#endif 
