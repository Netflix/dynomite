/*
 * Dynomite - A thin, distributed replication layer for multi non-distributed
 * storages. Copyright (C) 2014 Netflix, Inc.
 */

#ifndef _DYN_DNODE_CLIENT_H_
#define _DYN_DNODE_CLIENT_H_

// Forward declarations
struct conn;

void init_dnode_client_conn(struct conn *conn);

#endif
