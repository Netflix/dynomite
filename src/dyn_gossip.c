/*
 * Dynomite - A thin, distributed replication layer for multi non-distributed storages.
 * Copyright (C) 2014 Netflix, Inc.
 */


#include <stdlib.h>
#include <unistd.h>
#include <dyn_core.h>

#include "dyn_server.h"
#include "dyn_gossip.h"
#include "dyn_dnode_peer.h"
#include "dyn_stats.h"


static void *
gossip_loop(void *arg)
{
	struct server_pool *sp = arg;
	for(;;) {
		//loga("Running gossip...");
		log_debug(LOG_VERB, "Running gossip :::::: '%.*s'",
		                  sp->seed_provider.len, sp->seed_provider.data);

		get_seeds(sp);
	}
    //event_loop_stats(gossip_loop_callback, arg);
    return NULL;
}

/*
static void
gossip_loop_callback(void *arg1, void *arg2)
{
    struct stats *st = arg1;
    int n = *((int *)arg2);


    stats_aggregate(st);

    if (n == 0) {
        return;
    }


    stats_send_rsp(st);
}
*/


static rstatus_t
gossip_start(struct server_pool *sp)
{
    rstatus_t status;
    pthread_t tid;

    status = pthread_create(&tid, NULL, gossip_loop, sp);
    if (status < 0) {
        log_error("gossip service create failed: %s", strerror(status));
        return NC_ERROR;
    }

    return NC_OK;
}

static void
gossip_stop(struct stats *st)
{
   loga("Stop gossip");
}


static rstatus_t
gossip_pool_each_init(void *elem, void *data)
{
    rstatus_t status;
    struct server_pool *sp = elem;

    log_debug(LOG_VERB, "Seeed provider :::::: '%.*s'",
                  sp->seed_provider.len, sp->seed_provider.data);

    struct stats *st;

    status = gossip_start(sp);
    if (status != NC_OK) {
        goto error;
    }


    return st;

error:
    gossip_destroy(st);
    return NULL;

    //loga("seed provider....................... %s", sp->name);
    //if (!sp->preconnect) {
    //    return NC_OK;
    //}

    //status = array_each(&sp->peers, dnode_peer_each_preconnect, NULL);
    //if (status != NC_OK) {
    //    return status;
    //}

    return NC_OK;
}


rstatus_t
gossip_pool_init(struct context *ctx)
{
    rstatus_t status;

    status = array_each(&ctx->pool, gossip_pool_each_init, NULL);
    if (status != NC_OK) {
        return status;
    }

    return NC_OK;
}


void gossip_pool_deinit(struct context *ctx)
{

}


void
gossip_destroy(struct stats *st)
{
    /*
	stats_stop_aggregator(st);
    stats_pool_unmap(&st->sum);
    stats_pool_unmap(&st->shadow);
    stats_pool_unmap(&st->current);
    stats_destroy_buf(st);
    nc_free(st);
    */
}
