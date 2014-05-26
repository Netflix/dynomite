/*
 * Dynomite - A thin, distributed replication layer for multi non-distributed storages.
 * Copyright (C) 2014 Netflix, Inc.
 */ 

#ifndef _DYN_DNODE_PEER_H_
#define _DYN_DNODE_PEER_H_

#include "dyn_core.h"
#include "dyn_server.h"


void dnode_peer_ref(struct conn *conn, void *owner);
void dnode_peer_unref(struct conn *conn);
int dnode_peer_timeout(struct conn *conn);
bool dnode_peer_active(struct conn *conn);
rstatus_t dnode_peer_init(struct array *conf_seeds, struct server_pool *sp);
void dnode_peer_deinit(struct array *server);
struct conn *dnode_peer_conn(struct server *server);
rstatus_t dnode_peer_connect(struct context *ctx, struct server *server, struct conn *conn);
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
