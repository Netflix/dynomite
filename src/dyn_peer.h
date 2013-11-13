#ifndef _NC_PEER_H_
#define _NC_PEER_H_

#include <nc_core.h>



struct peer {
    uint32_t           idx;           /* server index */
    struct server_pool *owner;        /* owner pool */

    struct string      pname;         /* name:port:weight (ref in conf_server) */
    struct string      name;          /* name (ref in conf_server) */
    uint16_t           port;          /* port */
    uint32_t           weight;        /* weight */
    int                family;        /* socket family */
    socklen_t          addrlen;       /* socket length */
    struct sockaddr    *addr;         /* socket address (ref in conf_server) */

    uint32_t           ns_conn_q;     /* #peer connection */
    struct conn_tqh    s_conn_q;      /* peer connection q */

    int64_t            next_retry;    /* next retry time in usec */
    uint32_t           failure_count; /* # consecutive failures */
};

          


#endif 
