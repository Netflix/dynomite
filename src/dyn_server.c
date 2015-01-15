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
#include "dyn_server.h"
#include "dyn_conf.h"
#include "dyn_token.h"

void
server_ref(struct conn *conn, void *owner)
{
	struct server *server = owner;

	ASSERT(!conn->client && !conn->proxy);
	ASSERT(conn->owner == NULL);

	conn->family = server->family;
	conn->addrlen = server->addrlen;
	conn->addr = server->addr;

	server->ns_conn_q++;
	TAILQ_INSERT_TAIL(&server->s_conn_q, conn, conn_tqe);

	conn->owner = owner;

	log_debug(LOG_VVERB, "ref conn %p owner %p into '%.*s", conn, server,
			server->pname.len, server->pname.data);
}

void
server_unref(struct conn *conn)
{
	struct server *server;

	ASSERT(!conn->client && !conn->proxy);
	ASSERT(conn->owner != NULL);

	server = conn->owner;
	conn->owner = NULL;

	ASSERT(server->ns_conn_q != 0);
	server->ns_conn_q--;
	TAILQ_REMOVE(&server->s_conn_q, conn, conn_tqe);

	log_debug(LOG_VVERB, "unref conn %p owner %p from '%.*s'", conn, server,
			server->pname.len, server->pname.data);
}

int
server_timeout(struct conn *conn)
{
	struct server *server;
	struct server_pool *pool;

	ASSERT(!conn->client && !conn->proxy);

	server = conn->owner;
	pool = server->owner;

	return pool->timeout;
}

bool
server_active(struct conn *conn)
{
	ASSERT(!conn->client && !conn->proxy);

	if (!TAILQ_EMPTY(&conn->imsg_q)) {
		log_debug(LOG_VVERB, "s %d is active", conn->sd);
		return true;
	}

	if (!TAILQ_EMPTY(&conn->omsg_q)) {
		log_debug(LOG_VVERB, "s %d is active", conn->sd);
		return true;
	}

	if (conn->rmsg != NULL) {
		log_debug(LOG_VVERB, "s %d is active", conn->sd);
		return true;
	}

	if (conn->smsg != NULL) {
		log_debug(LOG_VVERB, "s %d is active", conn->sd);
		return true;
	}

	log_debug(LOG_VVERB, "s %d is inactive", conn->sd);

	return false;
}

static rstatus_t
server_each_set_owner(void *elem, void *data)
{
	struct server *s = elem;
	struct server_pool *sp = data;

	s->owner = sp;

	return DN_OK;
}

rstatus_t
server_init(struct array *servers, struct array *conf_server,
		struct server_pool *sp)
{
	rstatus_t status;
	uint32_t nserver;

	nserver = array_n(conf_server);
	ASSERT(nserver != 0);
	ASSERT(array_n(servers) == 0);

	status = array_init(servers, nserver, sizeof(struct server));
	if (status != DN_OK) {
		return status;
	}

	/* transform conf server to server */
	status = array_each(conf_server, conf_server_each_transform, servers);
	if (status != DN_OK) {
		server_deinit(servers);
		return status;
	}
	ASSERT(array_n(servers) == nserver);

	/* set server owner */
	status = array_each(servers, server_each_set_owner, sp);
	if (status != DN_OK) {
		server_deinit(servers);
		return status;
	}

	log_debug(LOG_DEBUG, "init %"PRIu32" servers in pool %"PRIu32" '%.*s'",
			nserver, sp->idx, sp->name.len, sp->name.data);

	return DN_OK;
}

void
server_deinit(struct array *server)
{
	uint32_t i, nserver;

	for (i = 0, nserver = array_n(server); i < nserver; i++) {
		struct server *s = array_pop(server);
		ASSERT(TAILQ_EMPTY(&s->s_conn_q) && s->ns_conn_q == 0);
	}
	array_deinit(server);
}

struct conn *
server_conn(struct server *server)
{
	struct server_pool *pool;
	struct conn *conn;

	pool = server->owner;

	/*
	 * FIXME: handle multiple server connections per server and do load
	 * balancing on it. Support multiple algorithms for
	 * 'server_connections:' > 0 key
	 */

	if (server->ns_conn_q < pool->server_connections) {
		return conn_get(server, false, pool->redis);
	}
	ASSERT(server->ns_conn_q == pool->server_connections);

	/*
	 * Pick a server connection from the head of the queue and insert
	 * it back into the tail of queue to maintain the lru order
	 */
	conn = TAILQ_FIRST(&server->s_conn_q);
	ASSERT(!conn->client && !conn->proxy);

	TAILQ_REMOVE(&server->s_conn_q, conn, conn_tqe);
	TAILQ_INSERT_TAIL(&server->s_conn_q, conn, conn_tqe);

	return conn;
}

static rstatus_t
server_each_preconnect(void *elem, void *data)
{
	rstatus_t status;
	struct server *server;
	struct server_pool *pool;
	struct conn *conn;

	server = elem;
	pool = server->owner;

	conn = server_conn(server);
	if (conn == NULL) {
		return DN_ENOMEM;
	}

	status = server_connect(pool->ctx, server, conn);
	if (status != DN_OK) {
		log_warn("connect to server '%.*s' failed, ignored: %s",
				server->pname.len, server->pname.data, strerror(errno));
		server_close(pool->ctx, conn);
	}

	return DN_OK;
}

static rstatus_t
server_each_disconnect(void *elem, void *data)
{
	struct server *server;
	struct server_pool *pool;

	server = elem;
	pool = server->owner;

	while (!TAILQ_EMPTY(&server->s_conn_q)) {
		struct conn *conn;

		ASSERT(server->ns_conn_q > 0);

		conn = TAILQ_FIRST(&server->s_conn_q);
		conn->close(pool->ctx, conn);
	}

	return DN_OK;
}

static void
server_failure(struct context *ctx, struct server *server)
{
	struct server_pool *pool = server->owner;
	int64_t now, next;
	rstatus_t status;

	if (!pool->auto_eject_hosts) {
		return;
	}

	server->failure_count++;

	log_debug(LOG_VERB, "server '%.*s' failure count %"PRIu32" limit %"PRIu32,
			server->pname.len, server->pname.data, server->failure_count,
			pool->server_failure_limit);

	if (server->failure_count < pool->server_failure_limit) {
		return;
	}

	now = dn_usec_now();
	if (now < 0) {
		return;
	}

	stats_server_set_ts(ctx, server, server_ejected_at, now);

	next = now + pool->server_retry_timeout;

	log_debug(LOG_INFO, "update pool %"PRIu32" '%.*s' to delete server '%.*s' "
			"for next %"PRIu32" secs", pool->idx, pool->name.len,
			pool->name.data, server->pname.len, server->pname.data,
			pool->server_retry_timeout / 1000 / 1000);

	stats_pool_incr(ctx, pool, server_ejects);

	server->failure_count = 0;
	server->next_retry = next;

	status = server_pool_run(pool);
	if (status != DN_OK) {
		log_error("updating pool %"PRIu32" '%.*s' failed: %s", pool->idx,
				pool->name.len, pool->name.data, strerror(errno));
	}
}

static void
server_close_stats(struct context *ctx, struct server *server, err_t err,
		unsigned eof, unsigned connected)
{
	if (connected) {
		stats_server_decr(ctx, server, server_connections);
	}

	if (eof) {
		stats_server_incr(ctx, server, server_eof);
		return;
	}

	switch (err) {
	case ETIMEDOUT:
		stats_server_incr(ctx, server, server_timedout);
		break;
	case EPIPE:
	case ECONNRESET:
	case ECONNABORTED:
	case ECONNREFUSED:
	case ENOTCONN:
	case ENETDOWN:
	case ENETUNREACH:
	case EHOSTDOWN:
	case EHOSTUNREACH:
	default:
		stats_server_incr(ctx, server, server_err);
		break;
	}
}

void
server_close(struct context *ctx, struct conn *conn)
{
	rstatus_t status;
	struct msg *msg, *nmsg; /* current and next message */
	struct conn *c_conn;    /* peer client connection */

	ASSERT(!conn->client && !conn->proxy);

	server_close_stats(ctx, conn->owner, conn->err, conn->eof,
			conn->connected);

	if (conn->sd < 0) {
		server_failure(ctx, conn->owner);
		conn->unref(conn);
		conn_put(conn);
		return;
	}

	for (msg = TAILQ_FIRST(&conn->imsg_q); msg != NULL; msg = nmsg) {
		nmsg = TAILQ_NEXT(msg, s_tqe);

		/* dequeue the message (request) from server inq */
				conn->dequeue_inq(ctx, conn, msg);

				/*
				 * Don't send any error response, if
				 * 1. request is tagged as noreply or,
				 * 2. client has already closed its connection
				 */
				if (msg->swallow || msg->noreply) {
					log_debug(LOG_INFO, "close s %d swallow req %"PRIu64" len %"PRIu32
							" type %d", conn->sd, msg->id, msg->mlen, msg->type);
					req_put(msg);
				} else {
					c_conn = msg->owner;
					//ASSERT(c_conn->client && !c_conn->proxy);

					msg->done = 1;
					msg->error = 1;
					msg->err = conn->err;
					msg->dyn_error = STORAGE_CONNECTION_REFUSE;

					if (req_done(c_conn, TAILQ_FIRST(&c_conn->omsg_q))) {
						event_add_out(ctx->evb, msg->owner);
					}

					log_debug(LOG_INFO, "close s %d schedule error for req %"PRIu64" "
							"len %"PRIu32" type %d from c %d%c %s", conn->sd, msg->id,
							msg->mlen, msg->type, c_conn->sd, conn->err ? ':' : ' ',
									conn->err ? strerror(conn->err): " ");
				}
	}
	ASSERT(TAILQ_EMPTY(&conn->imsg_q));

	for (msg = TAILQ_FIRST(&conn->omsg_q); msg != NULL; msg = nmsg) {
		nmsg = TAILQ_NEXT(msg, s_tqe);

		/* dequeue the message (request) from server outq */
		conn->dequeue_outq(ctx, conn, msg);

		if (msg->swallow) {
			log_debug(LOG_INFO, "close s %d swallow req %"PRIu64" len %"PRIu32
					" type %d", conn->sd, msg->id, msg->mlen, msg->type);
			req_put(msg);
		} else {
			c_conn = msg->owner;
			//ASSERT(c_conn->client && !c_conn->proxy);

			msg->done = 1;
			msg->error = 1;
			msg->err = conn->err;

			if (req_done(c_conn, TAILQ_FIRST(&c_conn->omsg_q))) {
				event_add_out(ctx->evb, msg->owner);
			}

			log_debug(LOG_INFO, "close s %d schedule error for req %"PRIu64" "
					"len %"PRIu32" type %d from c %d%c %s", conn->sd, msg->id,
					msg->mlen, msg->type, c_conn->sd, conn->err ? ':' : ' ',
							conn->err ? strerror(conn->err): " ");
		}
	}
	ASSERT(TAILQ_EMPTY(&conn->omsg_q));

	msg = conn->rmsg;
	if (msg != NULL) {
		conn->rmsg = NULL;

		ASSERT(!msg->request);
		ASSERT(msg->peer == NULL);

		rsp_put(msg);

		log_debug(LOG_INFO, "close s %d discarding rsp %"PRIu64" len %"PRIu32" "
				"in error", conn->sd, msg->id, msg->mlen);
	}

	ASSERT(conn->smsg == NULL);

	server_failure(ctx, conn->owner);

	conn->unref(conn);

	status = close(conn->sd);
	if (status < 0) {
		log_error("close s %d failed, ignored: %s", conn->sd, strerror(errno));
	}
	conn->sd = -1;

	conn_put(conn);
}

rstatus_t
server_connect(struct context *ctx, struct server *server, struct conn *conn)
{
	rstatus_t status;

	ASSERT(!conn->client && !conn->proxy);

	if (conn->sd > 0) {
		/* already connected on server connection */
		return DN_OK;
	}

	log_debug(LOG_VVERB, "connect to server '%.*s'", server->pname.len,
			server->pname.data);

	conn->sd = socket(conn->family, SOCK_STREAM, 0);
	if (conn->sd < 0) {
		log_error("socket for server '%.*s' failed: %s", server->pname.len,
				server->pname.data, strerror(errno));
		status = DN_ERROR;
		goto error;
	}

	status = dn_set_nonblocking(conn->sd);
	if (status != DN_OK) {
		log_error("set nonblock on s %d for server '%.*s' failed: %s",
				conn->sd,  server->pname.len, server->pname.data,
				strerror(errno));
		goto error;
	}

	if (server->pname.data[0] != '/') {
		status = dn_set_tcpnodelay(conn->sd);
		if (status != DN_OK) {
			log_warn("set tcpnodelay on s %d for server '%.*s' failed, ignored: %s",
					conn->sd, server->pname.len, server->pname.data,
					strerror(errno));
		}
	}

	status = event_add_conn(ctx->evb, conn);
	if (status != DN_OK) {
		log_error("event add conn s %d for server '%.*s' failed: %s",
				conn->sd, server->pname.len, server->pname.data,
				strerror(errno));
		goto error;
	}

	ASSERT(!conn->connecting && !conn->connected);

	status = connect(conn->sd, conn->addr, conn->addrlen);
	if (status != DN_OK) {
		if (errno == EINPROGRESS) {
			conn->connecting = 1;
			log_debug(LOG_DEBUG, "connecting on s %d to server '%.*s'",
					conn->sd, server->pname.len, server->pname.data);
			return DN_OK;
		}

		log_error("connect on s %d to server '%.*s' failed: %s", conn->sd,
				server->pname.len, server->pname.data, strerror(errno));

		goto error;
	}

	ASSERT(!conn->connecting);
	conn->connected = 1;
	log_debug(LOG_INFO, "connected on s %d to server '%.*s'", conn->sd,
			server->pname.len, server->pname.data);

	return DN_OK;

	error:
	conn->err = errno;
	return status;
}

void
server_connected(struct context *ctx, struct conn *conn)
{
	struct server *server = conn->owner;

	ASSERT(!conn->client && !conn->proxy);
	ASSERT(conn->connecting && !conn->connected);

	stats_server_incr(ctx, server, server_connections);

	conn->connecting = 0;
	conn->connected = 1;

	log_debug(LOG_INFO, "connected on s %d to server '%.*s'", conn->sd,
			server->pname.len, server->pname.data);
}

void
server_ok(struct context *ctx, struct conn *conn)
{
	struct server *server = conn->owner;

	ASSERT(!conn->client && !conn->proxy);
	ASSERT(conn->connected);

	if (server->failure_count != 0) {
		log_debug(LOG_VERB, "reset server '%.*s' failure count from %"PRIu32
				" to 0", server->pname.len, server->pname.data,
				server->failure_count);
		server->failure_count = 0;
		server->next_retry = 0LL;
	}
}

static rstatus_t
server_pool_update(struct server_pool *pool)
{
	rstatus_t status;
	int64_t now;
	uint32_t pnlive_server; /* prev # live server */

	if (!pool->auto_eject_hosts) {
		return DN_OK;
	}

	if (pool->next_rebuild == 0LL) {
		return DN_OK;
	}

	now = dn_usec_now();
	if (now < 0) {
		return DN_ERROR;
	}

	if (now <= pool->next_rebuild) {
		if (pool->nlive_server == 0) {
			errno = ECONNREFUSED;
			return DN_ERROR;
		}
		return DN_OK;
	}

	pnlive_server = pool->nlive_server;

	status = server_pool_run(pool);
	if (status != DN_OK) {
		log_error("updating pool %"PRIu32" with dist %d failed: %s", pool->idx,
				pool->dist_type, strerror(errno));
		return status;
	}

	log_debug(LOG_INFO, "update pool %"PRIu32" '%.*s' to add %"PRIu32" servers",
			pool->idx, pool->name.len, pool->name.data,
			pool->nlive_server - pnlive_server);


	return DN_OK;
}

static struct server *
server_pool_server(struct server_pool *pool, uint8_t *key, uint32_t keylen)
{
	struct server *server;

	ASSERT(array_n(&pool->server) != 0);

	//just return the first (memcache) entry in the array
	server = array_get(&pool->server, 0);

	return server;
}

struct conn *
server_pool_conn(struct context *ctx, struct server_pool *pool, uint8_t *key,
		uint32_t keylen)
{
	rstatus_t status;
	struct server *server;
	struct conn *conn;

	status = server_pool_update(pool);
	if (status != DN_OK) {
		return NULL;
	}

	/* from a given {key, keylen} pick a server from pool */
	server = server_pool_server(pool, key, keylen);
	if (server == NULL) {
		return NULL;
	}

	/* pick a connection to a given server */
	conn = server_conn(server);
	if (conn == NULL) {
		return NULL;
	}

	status = server_connect(ctx, server, conn);
	if (status != DN_OK) {
		server_close(ctx, conn);
		return NULL;
	}

	return conn;
}

static rstatus_t
server_pool_each_preconnect(void *elem, void *data)
{
	rstatus_t status;
	struct server_pool *sp = elem;

	if (!sp->preconnect) {
		return DN_OK;
	}

	status = array_each(&sp->server, server_each_preconnect, NULL);
	if (status != DN_OK) {
		return status;
	}

	return DN_OK;
}

rstatus_t
server_pool_preconnect(struct context *ctx)
{
	rstatus_t status;

	status = array_each(&ctx->pool, server_pool_each_preconnect, NULL);
	if (status != DN_OK) {
		return status;
	}

	return DN_OK;
}

static rstatus_t
server_pool_each_disconnect(void *elem, void *data)
{
	rstatus_t status;
	struct server_pool *sp = elem;

	status = array_each(&sp->server, server_each_disconnect, NULL);
	if (status != DN_OK) {
		return status;
	}

	return DN_OK;
}

void
server_pool_disconnect(struct context *ctx)
{
	array_each(&ctx->pool, server_pool_each_disconnect, NULL);
}

static rstatus_t
server_pool_each_set_owner(void *elem, void *data)
{
	struct server_pool *sp = elem;
	struct context *ctx = data;

	sp->ctx = ctx;

	return DN_OK;
}

rstatus_t
server_pool_run(struct server_pool *pool)
{
	ASSERT(array_n(&pool->server) != 0);

	switch (pool->dist_type) {
	case DIST_KETAMA:
		return ketama_update(pool);

	case DIST_VNODE:
		//return vnode_update(pool);
		break;

	case DIST_MODULA:
		return modula_update(pool);

	case DIST_RANDOM:
		return random_update(pool);

	case DIST_SINGLE:
		return DN_OK;

	default:
		NOT_REACHED();
		return DN_ERROR;
	}

	return DN_OK;
}

static rstatus_t
server_pool_each_run(void *elem, void *data)
{
	return server_pool_run(elem);
}

rstatus_t
server_pool_init(struct array *server_pool, struct array *conf_pool,
		struct context *ctx)
{
	rstatus_t status;
	uint32_t npool;

	npool = array_n(conf_pool);
	ASSERT(npool != 0);
	ASSERT(array_n(server_pool) == 0);

	status = array_init(server_pool, npool, sizeof(struct server_pool));
	if (status != DN_OK) {
		return status;
	}

	/* transform conf pool to server pool */
	status = array_each(conf_pool, conf_pool_each_transform, server_pool);
	if (status != DN_OK) {
		server_pool_deinit(server_pool);
		return status;
	}
	ASSERT(array_n(server_pool) == npool);

	/* set ctx as the server pool owner */
	status = array_each(server_pool, server_pool_each_set_owner, ctx);
	if (status != DN_OK) {
		server_pool_deinit(server_pool);
		return status;
	}

	/* update server pool continuum */
	status = array_each(server_pool, server_pool_each_run, NULL);
	if (status != DN_OK) {
		server_pool_deinit(server_pool);
		return status;
	}

	log_debug(LOG_DEBUG, "init %"PRIu32" pools", npool);

	return DN_OK;
}


void
server_pool_deinit(struct array *server_pool)
{
	uint32_t i, npool;

	for (i = 0, npool = array_n(server_pool); i < npool; i++) {
		struct server_pool *sp;

		sp = array_pop(server_pool);
		ASSERT(sp->p_conn == NULL);
		ASSERT(TAILQ_EMPTY(&sp->c_conn_q) && sp->dn_conn_q == 0);

		server_deinit(&sp->server);

		sp->nlive_server = 0;

		log_debug(LOG_DEBUG, "deinit pool %"PRIu32" '%.*s'", sp->idx,
				sp->name.len, sp->name.data);
	}

	array_deinit(server_pool);

	log_debug(LOG_DEBUG, "deinit %"PRIu32" pools", npool);
}


dictType dc_string_dict_type = {
		dict_string_hash,            /* hash function */
		NULL,                        /* key dup */
		NULL,                        /* val dup */
		dict_string_key_compare,     /* key compare */
		dict_string_destructor,      /* key destructor */
		NULL                         /* val destructor */
};


rstatus_t
rack_init(struct rack *rack)
{
	rack->continuum = dn_alloc(sizeof(struct continuum));
	rack->ncontinuum = 0;
	rack->nserver_continuum = 0;
	rack->name = dn_alloc(sizeof(struct string));
	string_init(rack->name);

	rack->dc = dn_alloc(sizeof(struct string));
	string_init(rack->dc);

	return DN_OK;
}


rstatus_t
rack_deinit(struct rack *rack)
{
	if (rack->continuum != NULL) {
		dn_free(rack->continuum);
	}

	return DN_OK;
}


rstatus_t dc_init(struct datacenter *dc)
{
	rstatus_t status;

	dc->dict_rack = dictCreate(&dc_string_dict_type, NULL);
	dc->name = dn_alloc(sizeof(struct string));
	string_init(dc->name);

	status = array_init(&dc->racks, 3, sizeof(struct rack));

	return status;
}

rstatus_t
dc_deinit(struct datacenter *dc)
{
	array_each(&dc->racks, rack_destroy, NULL);
	string_deinit(dc->name);
	//dictRelease(dc->dict_rack);
	return DN_OK;
}


rstatus_t
rack_destroy(void *elem, void *data)
{
	struct rack *rack = elem;
	return rack_deinit(rack);
}

rstatus_t
datacenter_destroy(void *elem, void *data)
{
	struct datacenter *dc = elem;
	dc_deinit(dc);

	return DN_OK;
}



struct datacenter *
server_get_dc(struct server_pool *pool, struct string *dcname)
{
	struct datacenter *dc;
	uint32_t i, len;

	log_debug(LOG_DEBUG, "server_get_dc pool  '%.*s'",
			                dcname->len, dcname->data);

	for (i = 0, len = array_n(&pool->datacenters); i < len; i++) {
		dc = (struct datacenter *) array_get(&pool->datacenters, i);
		ASSERT(dc != NULL);
		ASSERT(dc->name != NULL);

		if (string_compare(dc->name, dcname) == 0) {
			return dc;
		}
	}

	dc = array_push(&pool->datacenters);
	dc_init(dc);
	string_copy(dc->name, dcname->data, dcname->len);

	log_debug(LOG_DEBUG, "server_get_dc pool about to exit  '%.*s'",
			dc->name->len, dc->name->data);

	return dc;
}


struct rack *
server_get_rack(struct datacenter *dc, struct string *rackname)
{
	ASSERT(dc != NULL);
	ASSERT(dc->dict_rack != NULL);
	ASSERT(dc->name != NULL);

	log_debug(LOG_DEBUG, "server_get_rack   '%.*s'", rackname->len, rackname->data);

	/*
   struct rack *rack = dictFetchValue(dc->dict_rack, rackname);
   if (rack == NULL) {
      rack = array_push(&dc->racks);
      rack_init(rack);
      string_copy(rack->name, rackname->data, rackname->len);
      string_copy(rack->dc, dc->name->data, dc->name->len);
      rack->continuum = dn_alloc(sizeof(struct continuum));

   	dictAdd(dc->dict_rack, rackname, rack);
   }
	 */

	struct rack *rack;
	uint32_t i, len;
	for (i = 0, len = array_n(&dc->racks); i < len; i++) {
		rack = (struct rack *) array_get(&dc->racks, i);

		if (string_compare(rack->name, rackname) == 0) {
			return rack;
		}
	}

	rack = array_push(&dc->racks);
	rack_init(rack);
	string_copy(rack->name, rackname->data, rackname->len);
	string_copy(rack->dc, dc->name->data, dc->name->len);

	log_debug(LOG_DEBUG, "server_get_rack exiting  '%.*s'",
			rack->name->len, rack->name->data);

	return rack;
}


struct rack *
server_get_rack_by_dc_rack(struct server_pool *sp, struct string *rackname, struct string *dcname)
{
	struct datacenter *dc = server_get_dc(sp, dcname);
	return server_get_rack(dc, rackname);
}
