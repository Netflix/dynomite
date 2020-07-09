/*
 * Dynomite - A thin, distributed replication layer for multi non-distributed
 * storages. Copyright (C) 2019 Netflix, Inc.
 */

#pragma once
#include <dyn_types.h>

// Initializes (on first call) and updates (on subsequent calls) the per rack continuums
// and makes sure the tokens managed by the continuums are ascending.
rstatus_t vnode_update(struct server_pool *pool);

// Returns the index of the continuum from 'continuums' where 'token' falls.
// If 'token' falls into interval (a,b], we return b.
uint32_t vnode_dispatch(struct array *continuums, uint32_t ncontinuum,
                        struct dyn_token *token);
