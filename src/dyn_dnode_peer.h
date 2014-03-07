/*
 * Dynomite - A thin, distributed replication layer for multi non-distributed storages.
 * Copyright (C) 2014 Netflix, Inc.
 */ 

#ifndef _DYN_DNODE_PEER_H_
#define _DYN_DNODE_PEER_H_

#include <dyn_core.h>



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
    unsigned           is_seed:1;     /* seed? */    
    struct string      dc;            /* logical datacenter */
    struct array       tokens;        /* DHT tokens this peer owns */
    bool               is_local;      /* is this peer the current running node?  */
};



void dnode_peer_ref(struct conn *conn, void *owner);
void dnode_peer_unref(struct conn *conn);
int dnode_peer_timeout(struct conn *conn);
bool dnode_peer_active(struct conn *conn);
rstatus_t dnode_peer_init(struct array *conf_seeds, struct server_pool *sp);
void dnode_peer_deinit(struct array *server);
struct conn *dnode_peer_conn(struct peer *server);
rstatus_t dnode_peer_connect(struct context *ctx, struct peer *server, struct conn *conn);
void dnode_peer_close(struct context *ctx, struct conn *conn);
void dnode_peer_connected(struct context *ctx, struct conn *conn);
void dnode_peer_ok(struct context *ctx, struct conn *conn);

struct conn *dnode_peer_pool_conn(struct context *ctx, struct server_pool *pool, struct datacenter *dc, uint8_t *key, uint32_t keylen);
rstatus_t dnode_peer_pool_run(struct server_pool *pool);
rstatus_t dnode_peer_pool_preconnect(struct context *ctx);
void dnode_peer_pool_disconnect(struct context *ctx);
rstatus_t dnode_peer_pool_init(struct array *server_pool, struct array *conf_pool, struct context *ctx);
void dnode_peer_pool_deinit(struct array *server_pool);

          


#endif 
