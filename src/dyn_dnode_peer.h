/*
 * Dynomite - A thin, distributed replication layer for multi non-distributed storages.
 * Copyright (C) 2014 Netflix, Inc.
 */ 
#include "dyn_core.h"
#include "dyn_server.h"


#ifndef _DYN_DNODE_PEER_H_
#define _DYN_DNODE_PEER_H_

#define WAIT_BEFORE_RECONNECT_IN_MILLIS      30000
#define WAIT_BEFORE_UPDATE_PEERS_IN_MILLIS   30000

int dnode_peer_timeout(struct msg *msg, struct conn *conn);
rstatus_t dnode_peer_init(struct array *server_pool, struct context *ctx);
void dnode_peer_deinit(struct array *nodes);
void dnode_peer_connected(struct context *ctx, struct conn *conn);

struct server *dnode_peer_pool_server(struct context *ctx, struct server_pool *pool, struct rack *rack, uint8_t *key, uint32_t keylen, uint8_t msg_type);
struct conn *dnode_peer_pool_server_conn(struct context *ctx, struct server *server);
rstatus_t dnode_peer_pool_preconnect(struct context *ctx);
//rstatus_t dnode_peer_pool_init(struct array *server_pool, struct array *conf_pool, struct context *ctx);
//void dnode_peer_pool_deinit(struct array *server_pool);
bool is_same_dc(struct server_pool *sp, struct server *peer_node);


rstatus_t dnode_peer_forward_state(void *rmsg);
rstatus_t dnode_peer_add(void *rmsg);
rstatus_t dnode_peer_replace(void *rmsg);
rstatus_t dnode_peer_handshake_announcing(void *rmsg);

void init_dnode_peer_conn(struct conn *conn);
void preselect_remote_rack_for_replication(struct context *ctx);
#endif 
