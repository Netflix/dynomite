/*
 * Dynomite - A thin, distributed replication layer for multi non-distributed storages.
 * Copyright (C) 2014 Netflix, Inc.
 */ 

/*
 * twemproxy - A fast and lightweight proxy for memcached protocol.
 * Copyright (C) 2011 Twitter, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include <stdlib.h>
#include <unistd.h>

#include "dyn_core.h"
#include "dyn_topology.h"
#include "dyn_thread_ctx.h"
#include "dyn_conf.h"
#include "dyn_server.h"
#include "dyn_proxy.h"
#include "dyn_dnode_proxy.h"
#include "dyn_dnode_peer.h"
#include "dyn_gossip.h"

static void
core_debug(struct context *ctx)
{
    struct server_pool *sp = ctx_get_pool(ctx);
    struct topology *topo = ctx_get_topology(ctx);
	log_debug(LOG_VERB, "=====================Peers info=====================");
    log_debug(LOG_VERB, "Server pool          : '%.*s'", sp->name);
    topo_print(topo);
}

static rstatus_t
core_init_last(struct context *ctx)
{
	core_debug(ctx);
    preselect_remote_rack_for_replication(ctx);
    return DN_OK;
}

static rstatus_t
core_gossip_pool_init(struct context *ctx)
{
	//init ring msg queue
	CBUF_Init(C2G_InQ);
	CBUF_Init(C2G_OutQ);

	//init stats msg queue
	CBUF_Init(C2S_InQ);
	CBUF_Init(C2S_OutQ);

    THROW_STATUS(gossip_pool_init(ctx));
    THROW_STATUS(server_pool_init_my_dc_rack(&ctx->pool));
    THROW_STATUS(core_init_last(ctx));
    return DN_OK;
}

static rstatus_t
core_dnode_peer_pool_preconnect(struct context *ctx)
{
	THROW_STATUS(dnode_peer_pool_preconnect(ctx));
    rstatus_t status = core_gossip_pool_init(ctx);
    //if (status != DN_OK)
      //  gossip_pool_deinit(ctx);
    return status;
}
static rstatus_t
core_topo_init_seeds_peers(struct context *ctx)
{
	/* initialize peers */
	THROW_STATUS(topo_init_seeds_peers(ctx));
	rstatus_t status = core_dnode_peer_pool_preconnect(ctx);
	if (status != DN_OK)
	    dnode_peer_pool_disconnect(ctx);
    return status;
}

static rstatus_t
core_dnode_init(struct context *ctx)
{
	/* initialize dnode listener per server pool */
    THROW_STATUS(dnode_init(ctx));

	ctx->dyn_state = JOINING;  //TODOS: change this to JOINING
    rstatus_t status = core_topo_init_seeds_peers(ctx);
    if (status != DN_OK)
        topo_deinit_seeds_peers(ctx);
    return status;
}

static rstatus_t
core_proxy_init(struct context *ctx)
{
	/* initialize proxy per server pool */
    THROW_STATUS(proxy_init(ctx));
    rstatus_t status = core_dnode_init(ctx);
    if (status != DN_OK)
        dnode_deinit(ctx);
    return status;
}

static rstatus_t
core_datastore_preconnect(struct context *ctx)
{
	if (ctx->pool.preconnect) {
	    THROW_STATUS(datastore_preconnect(ctx->pool.datastore));
    }

     rstatus_t status = core_proxy_init(ctx);
     if (status != DN_OK)
        proxy_deinit(ctx);
    return status;
}

static rstatus_t
core_thread_ctx_init(struct context *ctx)
{
    THROW_STATUS(thread_ctx_init(ctx));
    rstatus_t status = core_datastore_preconnect(ctx);
    if (status != DN_OK)
        datastore_disconnect(ctx->pool.datastore);
    return status;
}

static rstatus_t
core_stats_create(struct context *ctx)
{
    struct instance *nci = ctx->instance;
	ctx->stats = stats_create(nci->stats_port, nci->stats_addr, nci->stats_interval,
			                  nci->hostname, &ctx->pool, ctx);
    if (ctx->stats == NULL) {
		loga("Failed to create stats!!!");
		return DN_ERROR;
	}
    rstatus_t status = core_thread_ctx_init(ctx);
    if (status != DN_OK)
        thread_ctx_deinit(ctx);
    return status;
}

static rstatus_t
core_crypto_init(struct context *ctx)
{
	/* crypto init */
    THROW_STATUS(crypto_init(&ctx->pool));
    rstatus_t status = core_stats_create(ctx);
    if (status != DN_OK)
		stats_destroy(ctx->stats);
    return status;
}

static rstatus_t
core_server_pool_init(struct context *ctx)
{
    THROW_STATUS(server_pool_init(&ctx->pool, &ctx->cf->pool, ctx));
    rstatus_t status = core_crypto_init(ctx);
    if (status != DN_OK)
		crypto_deinit();
    return status;
}

static rstatus_t
ctx_init(struct context *ctx, struct instance *nci)
{
	ctx->max_timeout = nci->stats_interval;
	ctx->timeout = ctx->max_timeout;
	ctx->dyn_state = INIT;
    ctx->admin_opt = 0;
    return DN_OK;

}

static rstatus_t
core_ctx_create(struct instance *nci)
{
	struct context *ctx;

	srand((unsigned) time(NULL));

	ctx = dn_zalloc(sizeof(*ctx));
	if (ctx == NULL) {
		loga("Failed to create context!!!");
		return DN_ERROR;
	}
    THROW_STATUS(ctx_init(ctx, nci));
    nci->ctx = ctx;
    ctx->instance = nci;
	/* parse and create configuration */
    ctx->cf = conf_create(nci->conf_filename);
	if (ctx->cf == NULL) {
		loga("Failed to create conf!!!");
		conf_destroy(ctx->cf);
		dn_free(ctx);
		return DN_ERROR;
	}
	rstatus_t status = core_server_pool_init(ctx);
    if (status != DN_OK) {
		server_pool_deinit(&ctx->pool);
		conf_destroy(ctx->cf);
		dn_free(ctx);
		return DN_ERROR;
    }
    return status;
}

static void
core_ctx_destroy(struct context *ctx)
{
	proxy_deinit(ctx);
    datastore_disconnect(ctx->pool.datastore);
	stats_destroy(ctx->stats);
	server_pool_deinit(&ctx->pool);
	conf_destroy(ctx->cf);
	dn_free(ctx);
}

rstatus_t
core_create(struct instance *nci)
{
	mbuf_init(nci);
	msg_init(nci);
	conn_init();

    rstatus_t status = core_ctx_create(nci);
    if (status != DN_OK) {
        conn_deinit();
        msg_deinit();
        dmsg_deinit();
        mbuf_deinit();
    }

	return status;
}

void
core_destroy(struct context *ctx)
{
	conn_deinit();
	msg_deinit();
	dmsg_deinit();
	mbuf_deinit();
	core_ctx_destroy(ctx);
}

static void
core_timeout(struct context *ctx)
{
	for (;;) {
		struct msg *msg;
		struct conn *conn;
		msec_t now, then;

		msg = msg_tmo_min();
		if (msg == NULL) {
			ctx->timeout = ctx->max_timeout;
			return;
		}

		/* skip over req that are in-error or done */

		if (msg->error || msg->done) {
			msg_tmo_delete(msg);
			continue;
		}

		/*
		 * timeout expired req and all the outstanding req on the timing
		 * out server
		 */

		conn = msg->tmo_rbe.data;
		then = msg->tmo_rbe.key;

		now = dn_msec_now();
		if (now < then) {
			int delta = (int)(then - now);
			ctx->timeout = MIN(delta, ctx->max_timeout);
			return;
		}

        log_warn("req %"PRIu64" on %s %d timedout, timeout was %d", msg->id,
                 conn_get_type_string(conn), conn->sd, msg->tmo_rbe.timeout);

		msg_tmo_delete(msg);

		if (conn->dyn_mode) {
			if (conn->type == CONN_DNODE_PEER_SERVER) { //outgoing peer requests
                if (conn->same_dc)
			        stats_pool_incr(ctx, peer_timedout_requests);
                else
			        stats_pool_incr(ctx, remote_peer_timedout_requests);
			}
		} else {
			if (conn->type == CONN_SERVER) { //storage server requests
			   stats_server_incr(ctx, server_dropped_requests);
			}
		}

		conn->err = ETIMEDOUT;

		conn_close(ctx, conn);
	}
}


static rstatus_t
core_process_messages(void)
{
	log_debug(LOG_VVVERB, "length of C2G_OutQ : %d", CBUF_Len( C2G_OutQ ));

	while (!CBUF_IsEmpty(C2G_OutQ)) {
		struct ring_msg *msg = (struct ring_msg *) CBUF_Pop(C2G_OutQ);
		if (msg != NULL && msg->cb != NULL) {
			msg->cb(msg);
			core_debug(msg->sp->ctx);
			ring_msg_deinit(msg);
		}
	}

	return DN_OK;
}

rstatus_t
core_loop(struct context *ctx)
{
	core_process_messages();
    // Run the thread function once.
    thread_ctx_run_once(array_get(&ctx->thread_ctxs, 0));
	core_timeout(ctx);
	stats_swap(ctx->stats);

	return DN_OK;
}

struct event_base *
core_get_evb_for_connection(struct context *ctx, connection_type_t type)
{
    pthread_ctx ptctx = array_get(&ctx->thread_ctxs, 0);
    return ptctx->evb;
}
