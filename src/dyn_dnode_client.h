/*
 * Dynomite - A thin, distributed replication layer for multi non-distributed storages.
 * Copyright (C) 2014 Netflix, Inc.
 */ 

#ifndef _DYN_DNODE_CLIENT_H_
#define _DYN_DNODE_CLIENT_H_

#include "dyn_core.h"

bool dnode_client_active(struct conn *conn);
void dnode_client_ref(struct conn *conn, void *owner);
void dnode_client_unref(struct conn *conn);
void dnode_client_close(struct context *ctx, struct conn *conn);

#endif
