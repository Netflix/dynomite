/*
 * Dynomite - A thin, distributed replication layer for multi non-distributed storages.
 * Copyright (C) 2014 Netflix, Inc.
 */ 

#ifndef _DYN_RING_H_
#define _DYN_RING_H_

#include <dyn_core.h>

struct dyn_ring {
    struct array      *ring_nodes; /* array of ring_nodes currently known in the cluster  */
    struct server_pool *owner;        /* owner pool */
};

rstatus_t dyn_ring_init(struct array *conf_seeds, struct server_pool *sp);
rstatus_t dyn_gos_run(struct context *ctx);
#endif 
