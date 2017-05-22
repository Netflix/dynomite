/*
 * Dynomite - A thin, distributed replication layer for multi non-distributed storages.
 * Copyright (C) 2014 Netflix, Inc.
 */ 
#include "dyn_core.h"
#include "dyn_server.h"


#ifndef _DYN_DNODE_PEER_H_
#define _DYN_DNODE_PEER_H_

#define MIN_WAIT_BEFORE_RECONNECT_IN_SECS    1ULL
#define MAX_WAIT_BEFORE_RECONNECT_IN_SECS    10
#define WAIT_BEFORE_UPDATE_PEERS_IN_MILLIS   30000

msec_t dnode_peer_timeout(struct msg *msg, struct conn *conn);
rstatus_t dnode_initialize_peers(struct context *ctx);
void dnode_peer_deinit(struct array *nodes);
void dnode_peer_connected(struct context *ctx, struct conn *conn);

struct node *dnode_peer_pool_server(struct context *ctx, struct server_pool *pool,
                                    struct rack *rack, uint8_t *key, uint32_t keylen,
                                    msg_routing_t msg_routing);
struct conn *dnode_peer_pool_server_conn(struct context *ctx, struct node *server);
rstatus_t dnode_peer_pool_preconnect(struct context *ctx);
void dnode_peer_pool_disconnect(struct context *ctx);
rstatus_t dnode_peer_forward_state(void *rmsg);
rstatus_t dnode_peer_add(void *rmsg);
rstatus_t dnode_peer_replace(void *rmsg);
rstatus_t dnode_peer_handshake_announcing(void *rmsg);

void init_dnode_peer_conn(struct conn *conn);
void preselect_remote_rack_for_replication(struct context *ctx);
#endif 
