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
#include "dyn_dnode_server.h"
#include "dyn_dnode_peer.h"
#include "dyn_gossip.h"


static uint32_t ctx_id; /* context generation */
static int64_t last; //storing last time for gossip

static struct context *
core_ctx_create(struct instance *nci)
{
	rstatus_t status;
	struct context *ctx;

	ctx = dn_alloc(sizeof(*ctx));
	if (ctx == NULL) {
		return NULL;
	}
	ctx->id = ++ctx_id;
	ctx->cf = NULL;
	ctx->stats = NULL;
	ctx->evb = NULL;
	array_null(&ctx->pool);
	ctx->max_timeout = nci->stats_interval;
	ctx->timeout = ctx->max_timeout;

	/* parse and create configuration */
	ctx->cf = conf_create(nci->conf_filename);
	if (ctx->cf == NULL) {
		dn_free(ctx);
		return NULL;
	}

	/* initialize server pool from configuration */
	status = server_pool_init(&ctx->pool, &ctx->cf->pool, ctx);
	if (status != DN_OK) {
		conf_destroy(ctx->cf);
		dn_free(ctx);
		return NULL;
	}

	/* create stats per server pool */
	ctx->stats = stats_create(nci->stats_port, nci->stats_addr, nci->stats_interval,
			nci->hostname, &ctx->pool);
	if (ctx->stats == NULL) {
		server_pool_deinit(&ctx->pool);
		conf_destroy(ctx->cf);
		dn_free(ctx);
		return NULL;
	}

	/* initialize event handling for client, proxy and server */
	ctx->evb = event_base_create(EVENT_SIZE, &core_core);
	if (ctx->evb == NULL) {
		stats_destroy(ctx->stats);
		server_pool_deinit(&ctx->pool);
		conf_destroy(ctx->cf);
		dn_free(ctx);
		return NULL;
	}

	/* preconnect? servers in server pool */
	status = server_pool_preconnect(ctx);
	if (status != DN_OK) {
		server_pool_disconnect(ctx);
		event_base_destroy(ctx->evb);
		stats_destroy(ctx->stats);
		server_pool_deinit(&ctx->pool);
		conf_destroy(ctx->cf);
		dn_free(ctx);
		return NULL;
	}

	/* initialize proxy per server pool */
	status = proxy_init(ctx);
	if (status != DN_OK) {
		server_pool_disconnect(ctx);
		event_base_destroy(ctx->evb);
		stats_destroy(ctx->stats);
		server_pool_deinit(&ctx->pool);
		conf_destroy(ctx->cf);
		dn_free(ctx);
		return NULL;
	}

	/* initialize dnode listener per server pool */
	status = dnode_init(ctx);
	if (status != DN_OK) {
		server_pool_disconnect(ctx);
		event_base_destroy(ctx->evb);
		stats_destroy(ctx->stats);
		server_pool_deinit(&ctx->pool);
		conf_destroy(ctx->cf);
		dn_free(ctx);
		return NULL;
	}

	/* initialize peers */
	status = dnode_peer_init(&ctx->pool, ctx);
	if (status != DN_OK) {
		dnode_deinit(ctx);
		server_pool_disconnect(ctx);
		event_base_destroy(ctx->evb);
		stats_destroy(ctx->stats);
		server_pool_deinit(&ctx->pool);
		conf_destroy(ctx->cf);
		dn_free(ctx);
		return status;
	}

	core_debug(ctx);

	/* preconntect peers - probably start gossip here */
	status = dnode_peer_pool_preconnect(ctx);
	if (status != DN_OK) {
		dnode_peer_deinit(ctx);
		dnode_deinit(ctx);
		server_pool_disconnect(ctx);
		event_base_destroy(ctx->evb);
		stats_destroy(ctx->stats);
		server_pool_deinit(&ctx->pool);
		conf_destroy(ctx->cf);
		dn_free(ctx);
		return NULL;
	}

	//init ring queue
	CBUF_Init(C2G_InQ);
	CBUF_Init(C2G_OutQ);

	gossip_pool_init(ctx);

	log_debug(LOG_VVERB, "created ctx %p id %"PRIu32"", ctx, ctx->id);

	return ctx;
}

static void
core_ctx_destroy(struct context *ctx)
{
	log_debug(LOG_VVERB, "destroy ctx %p id %"PRIu32"", ctx, ctx->id);
	proxy_deinit(ctx);
	server_pool_disconnect(ctx);
	event_base_destroy(ctx->evb);
	stats_destroy(ctx->stats);
	server_pool_deinit(&ctx->pool);
	conf_destroy(ctx->cf);
	dn_free(ctx);
}

struct context *
core_start(struct instance *nci)
{
	struct context *ctx;
	last = dn_msec_now();

	mbuf_init(nci);
	msg_init();
	conn_init();

	ctx = core_ctx_create(nci);
	if (ctx != NULL) {
		nci->ctx = ctx;
		return ctx;
	}

	conn_deinit();
	msg_deinit();
	mbuf_deinit();

	return NULL;
}

void
core_stop(struct context *ctx)
{
	conn_deinit();
	msg_deinit();
	mbuf_deinit();
	core_ctx_destroy(ctx);
}

static rstatus_t
core_recv(struct context *ctx, struct conn *conn)
{
	rstatus_t status;

	status = conn->recv(ctx, conn);
	if (status != DN_OK) {
		log_debug(LOG_INFO, "recv on %c %d failed: %s",
				conn->client ? 'c' : (conn->proxy ? 'p' : 's'), conn->sd,
						strerror(errno));
	}

	return status;
}

static rstatus_t
core_send(struct context *ctx, struct conn *conn)
{
	rstatus_t status;

	status = conn->send(ctx, conn);
	if (status != DN_OK) {
		log_debug(LOG_INFO, "send on %c %d failed: %s",
				conn->client ? 'c' : (conn->proxy ? 'p' : 's'), conn->sd,
						strerror(errno));
	}

	return status;
}

static void
core_close(struct context *ctx, struct conn *conn)
{
	rstatus_t status;
	char type, *addrstr;

	ASSERT(conn->sd > 0);

	if (conn->client) {
		type = 'c';
		addrstr = dn_unresolve_peer_desc(conn->sd);
	} else {
		type = conn->proxy ? 'p' : 's';
		addrstr = dn_unresolve_addr(conn->addr, conn->addrlen);
	}
	log_debug(LOG_NOTICE, "close %c %d '%s' on event %04"PRIX32" eof %d done "
			"%d rb %zu sb %zu%c %s", type, conn->sd, addrstr, conn->events,
			conn->eof, conn->done, conn->recv_bytes, conn->send_bytes,
			conn->err ? ':' : ' ', conn->err ? strerror(conn->err) : "");

	status = event_del_conn(ctx->evb, conn);
	if (status < 0) {
		log_warn("event del conn %c %d failed, ignored: %s",
				type, conn->sd, strerror(errno));
	}

	conn->close(ctx, conn);
}

static void
core_error(struct context *ctx, struct conn *conn)
{
	rstatus_t status;
	char type = conn->client ? 'c' : (conn->proxy ? 'p' : 's');

	status = dn_get_soerror(conn->sd);
	if (status < 0) {
		log_warn("get soerr on %c %d failed, ignored: %s", type, conn->sd,
				strerror(errno));
	}
	conn->err = errno;

	core_close(ctx, conn);
}

static void
core_timeout(struct context *ctx)
{
	for (;;) {
		struct msg *msg;
		struct conn *conn;
		int64_t now, then;

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

		log_debug(LOG_INFO, "req %"PRIu64" on s %d timedout", msg->id, conn->sd);

		msg_tmo_delete(msg);
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

	log_debug(LOG_VVERB, "event %04"PRIX32" on %c %d", events,
			conn->client ? 'c' : (conn->proxy ? 'p' : 's'), conn->sd);

	conn->events = events;

	/* error takes precedence over read | write */
	if (events & EVENT_ERR) {
		core_error(ctx, conn);
		return DN_ERROR;
	}

	/* read takes precedence over write */
	if (events & EVENT_READ) {
		status = core_recv(ctx, conn);
		if (status != DN_OK || conn->done || conn->err) {
			core_close(ctx, conn);
			return DN_ERROR;
		}
	}


	if (events & EVENT_WRITE) {
		status = core_send(ctx, conn);
		if (status != DN_OK || conn->done || conn->err) {
			core_close(ctx, conn);
			return DN_ERROR;
		}
	}


	return DN_OK;
}

static bool core_run_gossip(void)
{
	int64_t now = dn_msec_now();

	int delta = (int)(now - last);
	if (delta > 10000) {
		last = now;
		return true;
	}

	return false;
}


void
core_debug(struct context *ctx)
{
	log_debug(LOG_VERB, "Peers info.................................................");
	uint32_t i, nelem;
	for (i = 0, nelem = array_n(&ctx->pool); i < nelem; i++) {
		struct server_pool *sp = (struct server_pool *) array_get(&ctx->pool, i);
		log_debug(LOG_VERB, "Server pool          : %"PRIu32"", sp->idx);
		uint32_t j, n;
		for (j = 0, n = array_n(&sp->peers); j < n; j++) {
			log_debug(LOG_VERB, "==============================================");
			struct server *server = (struct server *) array_get(&sp->peers, j);
			log_debug(LOG_VERB, "\tPeer DC          : '%.*s'", server->dc);
			log_debug(LOG_VERB, "\tPeer name          : '%.*s'", server->name);
			log_debug(LOG_VERB, "\tPeer pname         : '%.*s'", server->pname);
			log_debug(LOG_VERB, "\tPeer port          : %"PRIu32"", server->port);
			log_debug(LOG_VERB, "\tPeer is_local      : %"PRIu32" ", server->is_local);
			log_debug(LOG_VERB, "\tPeer failure_count : %"PRIu32" ", server->failure_count);
			log_debug(LOG_VERB, "\tPeer num tokens    : %d", array_n(&server->tokens));

			uint32_t k;
			for (k = 0; k < array_n(&server->tokens); k++) {
				struct dyn_token *token = (struct dyn_token *) array_get(&server->tokens, k);
				print_dyn_token(token, 12);
			}
		}

		log_debug(LOG_VERB, "Peers DCs.................................................");
		log_debug(LOG_VERB, "Peer DC size    : %d", array_n(&sp->datacenter));
		for (j = 0, n = array_n(&sp->datacenter); j < n; j++) {
			struct datacenter *dc = (struct datacenter *) array_get(&sp->datacenter, j);
			log_debug(LOG_VERB, "\tDC '%.*s'", dc->name->len, dc->name->data);
			log_debug(LOG_VERB, "\tPeer DC ncontinuumm    : %d", dc->ncontinuum);
			log_debug(LOG_VERB, "\tPeer DC nserver_continuum    : %d", dc->nserver_continuum);
		}
	}
	log_debug(LOG_VERB, "..........................................................");
}


static rstatus_t
core_process_messages(void)
{
	//loga("Leng of C2G_OutQ ::: %d", CBUF_Len( C2G_OutQ ));
	while (!CBUF_IsEmpty(C2G_OutQ)) {
		struct ring_message *msg = (struct ring_message *) CBUF_Pop(C2G_OutQ);
		if (msg != NULL && msg->cb != NULL) {
			msg->cb(msg->sp, msg->node);
			ring_message_deinit(msg);
			core_debug(msg->sp->ctx);
		}
	}

	return DN_OK;
}


rstatus_t
core_loop(struct context *ctx)
{
	int nsd;
	loga("timeout = %d", ctx->timeout);
	nsd = event_wait(ctx->evb, ctx->timeout);
	if (nsd < 0) {
		return nsd;
	}

	core_timeout(ctx);
	core_process_messages();

	//core_debug(ctx);
	//if (core_run_gossip())
	//   dyn_gos_run(ctx);

	stats_swap(ctx->stats);

	return DN_OK;
}
