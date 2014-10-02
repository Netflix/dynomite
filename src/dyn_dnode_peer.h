/*
 * Dynomite - A thin, distributed replication layer for multi non-distributed storages.
 * Copyright (C) 2014 Netflix, Inc.
 */ 
#include "dyn_core.h"
#include "dyn_server.h"


#ifndef _DYN_DNODE_PEER_H_
#define _DYN_DNODE_PEER_H_


void dnode_peer_ref(struct conn *conn, void *owner);
void dnode_peer_unref(struct conn *conn);
int dnode_peer_timeout(struct conn *conn);
bool dnode_peer_active(struct conn *conn);
rstatus_t dnode_peer_each_pool_init(void *elem, void *context);
rstatus_t dnode_peer_init(struct array *server_pool, struct context *ctx);
void dnode_peer_deinit(struct array *server);
struct conn *dnode_peer_conn(struct server *server);
rstatus_t dnode_peer_connect(struct context *ctx, struct server *server, struct conn *conn);
void dnode_peer_close(struct context *ctx, struct conn *conn);
void dnode_peer_connected(struct context *ctx, struct conn *conn);
void dnode_peer_ok(struct context *ctx, struct conn *conn);

struct conn *dnode_peer_pool_conn(struct context *ctx, struct server_pool *pool, struct rack *rack, uint8_t *key, uint32_t keylen, uint8_t msg_type);
rstatus_t dnode_peer_pool_run(struct server_pool *pool);
rstatus_t dnode_peer_pool_update(struct server_pool *pool);
rstatus_t dnode_peer_pool_preconnect(struct context *ctx);
void dnode_peer_pool_disconnect(struct context *ctx);
//rstatus_t dnode_peer_pool_init(struct array *server_pool, struct array *conf_pool, struct context *ctx);
//void dnode_peer_pool_deinit(struct array *server_pool);

rstatus_t dnode_peer_add(struct server_pool *sp, struct node *node);
rstatus_t dnode_peer_replace(struct server_pool *sp, struct node *node);
rstatus_t dnode_peer_remove(struct server_pool *sp, struct node *node);
rstatus_t dnode_peer_add_rack(struct server_pool *sp, struct node *node);
rstatus_t dnode_peer_handshake_announcing(struct server_pool *sp);

#endif 
