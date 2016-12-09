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
#include "dyn_conf.h"
#include "dyn_server.h"
#include "dyn_proxy.h"
#include "dyn_dnode_proxy.h"
#include "dyn_dnode_peer.h"
#include "dyn_gossip.h"
#include "event/dyn_event.h"

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
core_dnode_peer_init(struct context *ctx)
{
	/* initialize peers */
	THROW_STATUS(dnode_peer_init(ctx));
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
    rstatus_t status = core_dnode_peer_init(ctx);
    if (status != DN_OK)
        dnode_peer_deinit(&ctx->pool.peers);
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
core_server_pool_preconnect(struct context *ctx)
{
	THROW_STATUS(server_pool_preconnect(ctx));

     rstatus_t status = core_proxy_init(ctx);
     if (status != DN_OK)
        proxy_deinit(ctx);
    return status;
}

static rstatus_t
core_event_base_create(struct context *ctx)
{
    /* initialize event handling for client, proxy and server */
    ctx->evb = event_base_create(EVENT_SIZE, &core_core);
    if (ctx->evb == NULL) {
	log_error("Failed to create socket event handling!!!");
	return DN_ERROR;
    }
    rstatus_t status = core_server_pool_preconnect(ctx);
    if (status != DN_OK)
		server_pool_disconnect(ctx);
    return status;
}

/**
 * Initialize anti-entropy.
 * @param[in,out] ctx Context.
 * @return rstatus_t Return status code.
 */
static rstatus_t
core_entropy_init(struct context *ctx)
{
    struct instance *nci = ctx->instance;
    /* initializing anti-entropy */
    ctx->entropy = entropy_init(ctx, nci->entropy_port, nci->entropy_addr);
    if (ctx->entropy == NULL) {
    	log_error("Failed to create entropy!!!");
    }

    rstatus_t status = core_event_base_create(ctx);
    if (status != DN_OK)
       event_base_destroy(ctx->evb);
    
    return status;
}

/**
 * Create the Dynomite server performance statistics and assign it to the
 * context, plus initialize anti-entropy.
 * @param[in,out] ctx Context.
 * @return rstatus_t Return status code.
 */
static rstatus_t
core_stats_create(struct context *ctx)
{
    struct instance *nci = ctx->instance;
    struct server_pool *sp = &ctx->pool;

    ctx->stats = stats_create(sp->stats_endpoint.port, sp->stats_endpoint.pname, sp->stats_interval,
			                  nci->hostname, &ctx->pool, ctx);
    if (ctx->stats == NULL) {
        log_error("Failed to create stats!!!");
        return DN_ERROR;
    }

    rstatus_t status = core_entropy_init(ctx);
    if (status != DN_OK)
    	entropy_conn_destroy(ctx->entropy);
    
    return status;
}

/**
 * Initialize crypto and create the Dynomite server performance statistics.
 * @param[in,out] ctx Dynomite server context.
 * @return rstatus_t Return status code.
 */
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

/**
 * Initialize the server pool.
 * @param[in,out] ctx Context.
 * @return rstatus_t Return status code.
 */
static rstatus_t
core_server_pool_init(struct context *ctx)
{
    THROW_STATUS(server_pool_init(&ctx->pool, &ctx->cf->pool, ctx));
    rstatus_t status = core_crypto_init(ctx);
    if (status != DN_OK)
		crypto_deinit();
    return status;
}

/**
 * Create a context for the dynomite process.
 * @param[in,out] nci Dynomite instance.
 * @return rstatus_t Return status code.
 */
static rstatus_t
core_ctx_create(struct instance *nci)
{
    struct context *ctx;

    srand((unsigned) time(NULL));

    ctx = dn_alloc(sizeof(*ctx));
    if (ctx == NULL) {
        loga("Failed to create context!!!");
        return DN_ERROR;
    }
    nci->ctx = ctx;
    ctx->instance = nci;
    ctx->cf = NULL;
    ctx->stats = NULL;
    ctx->evb = NULL;

    /* parse and create configuration */
    ctx->cf = conf_create(nci->conf_filename);
    if (ctx->cf == NULL) {
        loga("Failed to create conf!!!");
        conf_destroy(ctx->cf);
        dn_free(ctx);
        return DN_ERROR;
    }

    struct conf_pool *cp = &ctx->cf->pool;
    ctx->max_timeout = cp->stats_interval;
    ctx->timeout = ctx->max_timeout;

    /* Gossip initialization */
    ctx->dyn_state = INIT;

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
    server_pool_disconnect(ctx);
    event_base_destroy(ctx->evb);
    stats_destroy(ctx->stats);
    server_pool_deinit(&ctx->pool);
    conf_destroy(ctx->cf);
    dn_free(ctx);
}

/**
 * Initialize memory buffers, message queue, and connections.
 * @param[in] nci Dynomite instance.
 * @return rstatus_t Return status code.
 */
rstatus_t
core_start(struct instance *nci)
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

/**
 * Deinitialize connections, message queue, memory buffers and destroy the
 * context.
 * @param[in] ctx Dynomite process context.
 */
void
core_stop(struct context *ctx)
{
	conn_deinit();
	msg_deinit();
	dmsg_deinit();
	mbuf_deinit();
	core_ctx_destroy(ctx);
}

static rstatus_t
core_recv(struct context *ctx, struct conn *conn)
{
	rstatus_t status;

	status = conn_recv(ctx, conn);
	if (status != DN_OK) {
		log_info("recv on %s %d failed: %s", conn_get_type_string(conn),
				 conn->sd, strerror(errno));
	}

	return status;
}

static rstatus_t
core_send(struct context *ctx, struct conn *conn)
{
	rstatus_t status;

	status = conn_send(ctx, conn);
	if (status != DN_OK) {
		log_info("send on %s %d failed: %s", conn_get_type_string(conn),
				 conn->sd, strerror(errno));
	}

	return status;
}

static void
core_close_log(struct conn *conn)
{
	char *addrstr;

	if ((conn->type == CONN_CLIENT) || (conn->type == CONN_DNODE_PEER_CLIENT)) {
		addrstr = dn_unresolve_peer_desc(conn->sd);
	} else {
		addrstr = dn_unresolve_addr(conn->addr, conn->addrlen);
	}
	log_debug(LOG_NOTICE, "close %s %d '%s' on event %04"PRIX32" eof %d done "
			  "%d rb %zu sb %zu%c %s", conn_get_type_string(conn), conn->sd,
              addrstr, conn->events, conn->eof, conn->done, conn->recv_bytes,
              conn->send_bytes,
              conn->err ? ':' : ' ', conn->err ? strerror(conn->err) : "");

}

static void
core_close(struct context *ctx, struct conn *conn)
{
	rstatus_t status;

	ASSERT(conn->sd > 0);

    core_close_log(conn);

	status = conn_event_del_conn(conn);
	if (status < 0) {
		log_warn("event del conn %d failed, ignored: %s",
		          conn->sd, strerror(errno));
	}

	conn_close(ctx, conn);
}

static void
core_error(struct context *ctx, struct conn *conn)
{
	rstatus_t status;

	status = dn_get_soerror(conn->sd);
	if (status < 0) {
	log_warn("get soerr on %s client %d failed, ignored: %s",
             conn_get_type_string(conn), conn->sd, strerror(errno));
	}
	conn->err = errno;

	core_close(ctx, conn);
}

static void
core_timeout(struct context *ctx)
{
	for (;;) {
		struct msg *req;
		struct conn *conn;
		msec_t now, then;

		req = msg_tmo_min();
		if (req == NULL) {
			ctx->timeout = ctx->max_timeout;
			return;
		}

		/* skip over req that are in-error or done */

		if (req->is_error || req->done) {
			msg_tmo_delete(req);
			continue;
		}

		/*
		 * timeout expired req and all the outstanding req on the timing
		 * out server
		 */

		conn = req->tmo_rbe.data;
		then = req->tmo_rbe.key;

		now = dn_msec_now();
		if (now < then) {
			int delta = (int)(then - now);
			ctx->timeout = MIN(delta, ctx->max_timeout);
			return;
		}

        log_warn("req %"PRIu64" on %s %d timedout, timeout was %d", req->id,
                 conn_get_type_string(conn), conn->sd, req->tmo_rbe.timeout);

		msg_tmo_delete(req);

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

		core_close(ctx, conn);
	}
}



rstatus_t
core_core(void *arg, uint32_t events)
{
	rstatus_t status;
	struct conn *conn = arg;
	struct context *ctx = conn_to_ctx(conn);

    log_debug(LOG_VVVERB, "event %04"PRIX32" on %s %d", events,
              conn_get_type_string(conn), conn->sd);

	conn->events = events;

	/* error takes precedence over read | write */
	if (events & EVENT_ERR) {
		if (conn->err && conn->dyn_mode) {
			loga("conn err on dnode EVENT_ERR: %d", conn->err);
		}
		core_error(ctx, conn);

		return DN_ERROR;
	}

	/* read takes precedence over write */
	if (events & EVENT_READ) {
		status = core_recv(ctx, conn);

		if (status != DN_OK || conn->done || conn->err) {
			if (conn->dyn_mode) {
				if (conn->err) {
					loga("conn err on dnode EVENT_READ: %d", conn->err);
					core_close(ctx, conn);
					return DN_ERROR;
				}
				return DN_OK;
			}

			core_close(ctx, conn);
			return DN_ERROR;
		}
	}

	if (events & EVENT_WRITE) {
		status = core_send(ctx, conn);
		if (status != DN_OK || conn->done || conn->err) {
			if (conn->dyn_mode) {
				if (conn->err) {
					loga("conn err on dnode EVENT_WRITE: %d", conn->err);
					core_close(ctx, conn);
					return DN_ERROR;
				}
				return DN_OK;
			}

			core_close(ctx, conn);
			return DN_ERROR;
		}
	}

	return DN_OK;
}


void
core_debug(struct context *ctx)
{
	log_debug(LOG_VERB, "=====================Peers info=====================");
    struct server_pool *sp = &ctx->pool;
    log_debug(LOG_VERB, "Server pool          : '%.*s'", sp->name);
    uint32_t j, n;
    for (j = 0, n = array_n(&sp->peers); j < n; j++) {
        log_debug(LOG_VERB, "==============================================");
        struct node *peer = (struct node *) array_get(&sp->peers, j);
        log_debug(LOG_VERB, "\tPeer DC            : '%.*s'",peer ->dc);
        log_debug(LOG_VERB, "\tPeer Rack          : '%.*s'", peer->rack);

        log_debug(LOG_VERB, "\tPeer name          : '%.*s'", peer->name);
        log_debug(LOG_VERB, "\tPeer pname         : '%.*s'", peer->endpoint.pname);

        log_debug(LOG_VERB, "\tPeer state         : %"PRIu32"", peer->state);
        log_debug(LOG_VERB, "\tPeer port          : %"PRIu32"", peer->endpoint.port);
        log_debug(LOG_VERB, "\tPeer is_local      : %"PRIu32" ", peer->is_local);
        log_debug(LOG_VERB, "\tPeer failure_count : %"PRIu32" ", peer->failure_count);
        log_debug(LOG_VERB, "\tPeer num tokens    : %d", array_n(&peer->tokens));

        uint32_t k;
        for (k = 0; k < array_n(&peer->tokens); k++) {
            struct dyn_token *token = (struct dyn_token *) array_get(&peer->tokens, k);
            print_dyn_token(token, 12);
        }
    }

    log_debug(LOG_VERB, "Peers Datacenters/racks/nodes .................................................");
    uint32_t dc_index, dc_len;
    for(dc_index = 0, dc_len = array_n(&sp->datacenters); dc_index < dc_len; dc_index++) {
        struct datacenter *dc = array_get(&sp->datacenters, dc_index);
        log_debug(LOG_VERB, "Peer datacenter........'%.*s'", dc->name->len, dc->name->data);
        uint32_t rack_index, rack_len;
        for(rack_index=0, rack_len = array_n(&dc->racks); rack_index < rack_len; rack_index++) {
            struct rack *rack = array_get(&dc->racks, rack_index);
            log_debug(LOG_VERB, "\tPeer rack........'%.*s'", rack->name->len, rack->name->data);
            log_debug(LOG_VERB, "\tPeer rack ncontinuumm    : %d", rack->ncontinuum);
            log_debug(LOG_VERB, "\tPeer rack nserver_continuum    : %d", rack->nserver_continuum);
        }
    }
	log_debug(LOG_VERB, "...............................................................................");
}

/**
 * Process elements in the circular buffer.
 * @return rstatus_t Return status code.
 */
static rstatus_t
core_process_messages(void)
{
	log_debug(LOG_VVVERB, "length of C2G_OutQ : %d", CBUF_Len( C2G_OutQ ));

	// Continue to process messages while the circular buffer is not empty
	while (!CBUF_IsEmpty(C2G_OutQ)) {
		// Get an element from the beginning of the circular buffer
		struct ring_msg *ring_msg = (struct ring_msg *) CBUF_Pop(C2G_OutQ);
		if (ring_msg != NULL && ring_msg->cb != NULL) {
			// CBUF_Push
			// ./src/dyn_dnode_msg.c
			// ./src/dyn_gossip.c
			ring_msg->cb(ring_msg);
			core_debug(ring_msg->sp->ctx);
			ring_msg_deinit(ring_msg);
		}
	}

	return DN_OK;
}

/**
 * Primary loop for the Dynomite server process.
 * @param[in] ctx Dynomite process context.
 * @return rstatus_t Return status code.
 */
rstatus_t
core_loop(struct context *ctx)
{
	int nsd;

	core_process_messages();

	nsd = event_wait(ctx->evb, ctx->timeout);
	if (nsd < 0) {
		return nsd;
	}

	core_timeout(ctx);
    // go through all the ready queue and send each of them
    struct server_pool *sp = &ctx->pool;
    struct conn *conn, *nconn;
    TAILQ_FOREACH_SAFE(conn, &sp->ready_conn_q, ready_tqe, nconn) {
		rstatus_t status = core_send(ctx, conn);
        if (status == DN_OK) {
            conn_event_del_out(conn);
        } else {
            TAILQ_REMOVE(&sp->ready_conn_q, conn, ready_tqe);
        }
    }
	stats_swap(ctx->stats);

	return DN_OK;
}
