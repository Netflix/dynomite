/*
 * Dynomite - A thin, distributed replication layer for multi non-distributed
 * storages. Copyright (C) 2014 Netflix, Inc.
 */

#ifndef _DYN_DNODE_SERVER_H_
#define _DYN_DNODE_SERVER_H_

#include "dyn_types.h"

// Forward declarations
struct conn;
struct context;

rstatus_t dnode_proxy_init(struct context *ctx);
void dnode_proxy_deinit(struct context *ctx);
void init_dnode_proxy_conn(struct conn *conn);

#endif
