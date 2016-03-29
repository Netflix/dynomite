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
#include "dyn_dnode_peer.h"

static void server_close(struct context *ctx, struct conn *conn);
static void
server_ref(struct conn *conn, void *owner)
{
	struct server *server = owner;

    ASSERT(conn->type == CONN_SERVER);
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

static void
server_unref(struct conn *conn)
{
	struct server *server;

    ASSERT(conn->type == CONN_SERVER);
	ASSERT(conn->owner != NULL);

	server = conn->owner;
	conn->owner = NULL;

	ASSERT(server->ns_conn_q != 0);
	server->ns_conn_q--;
	TAILQ_REMOVE(&server->s_conn_q, conn, conn_tqe);

	log_debug(LOG_VVERB, "unref conn %p owner %p from '%.*s'", conn, server,
			server->pname.len, server->pname.data);
}

msec_t
server_timeout(struct conn *conn)
{
	struct server *server;
	struct server_pool *pool;

    ASSERT(conn->type == CONN_SERVER);

	server = conn->owner;
	pool = server->owner;

	return pool->timeout;
}

static bool
server_active(struct conn *conn)
{
    ASSERT(conn->type == CONN_SERVER);

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

static void
server_deinit(struct array *server)
{
	uint32_t i, nserver;

	for (i = 0, nserver = array_n(server); i < nserver; i++) {
		struct server *s = array_pop(server);
        IGNORE_RET_VAL(s);
		ASSERT(TAILQ_EMPTY(&s->s_conn_q) && s->ns_conn_q == 0);
	}
	array_deinit(server);
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

static struct conn *
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
		return conn_get(server, false, pool->data_store);
	}
	ASSERT(server->ns_conn_q == pool->server_connections);

	/*
	 * Pick a server connection from the head of the queue and insert
	 * it back into the tail of queue to maintain the lru order
	 */
	conn = TAILQ_FIRST(&server->s_conn_q);
	ASSERT(conn->type == CONN_SERVER);

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
		conn_close(pool->ctx, conn);
	}

	return DN_OK;
}

static void
server_failure(struct context *ctx, struct server *server)
{
	struct server_pool *pool = server->owner;
	msec_t now, next;
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

	now = dn_msec_now();
	if (now == 0) {
		return;
	}

	stats_server_set_ts(ctx, server, server_ejected_at, now);

	next = now + pool->server_retry_timeout_ms;

	log_debug(LOG_INFO, "update pool %"PRIu32" '%.*s' to delete server '%.*s' "
			"for next %"PRIu32" secs", pool->idx, pool->name.len,
			pool->name.data, server->pname.len, server->pname.data,
			pool->server_retry_timeout_ms/1000);

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

static void
server_ack_err(struct context *ctx, struct conn *conn, struct msg *req)
{
    // I want to make sure we do not have swallow here.
    //ASSERT_LOG(!req->swallow, "req %d:%d has swallow set??", req->id, req->parent_id);
    if ((req->swallow && req->noreply) ||
        (req->swallow && (req->consistency == DC_ONE)) ||
        (req->swallow && (req->consistency == DC_QUORUM)
                      && (!conn->same_dc))) {
        log_info("close %s %d swallow req %"PRIu64" len %"PRIu32
                 " type %d", conn_get_type_string(conn), conn->sd, req->id,
                 req->mlen, req->type);
        req_put(req);
        return;
    }
    struct conn *c_conn = req->owner;
    // At other connections, these responses would be swallowed.
    ASSERT_LOG((c_conn->type == CONN_CLIENT) ||
               (c_conn->type == CONN_DNODE_PEER_CLIENT), "c_conn type %s",
               conn_get_type_string(c_conn));

    // Create an appropriate response for the request so its propagated up;
    // This response gets dropped in rsp_make_error anyways. But since this is
    // an error path its ok with the overhead.
    struct msg *rsp = msg_get(conn, false, conn->data_store, __FUNCTION__);
    if (rsp == NULL) {
        log_warn("Could not allocate msg.");
        return;
    }
    req->done = 1;
    req->peer = rsp;
    rsp->peer = req;
    rsp->error = req->error = 1;
    rsp->err = req->err = conn->err;
    rsp->dyn_error = req->dyn_error = STORAGE_CONNECTION_REFUSE;
    rsp->dmsg = NULL;
    log_debug(LOG_DEBUG, "%d:%d <-> %d:%d", req->id, req->parent_id, rsp->id, rsp->parent_id);

    log_info("close %s %d req %u:%u "
             "len %"PRIu32" type %d from c %d%c %s", conn_get_type_string(conn),
             conn->sd, req->id, req->parent_id,
             req->mlen, req->type, c_conn->sd, conn->err ? ':' : ' ',
             conn->err ? strerror(conn->err): " ");
    rstatus_t status =
            conn_handle_response(c_conn, req->parent_id ? req->parent_id : req->id,
                                 rsp);
    IGNORE_RET_VAL(status);
    if (req->swallow)
        req_put(req);
}

static void
server_close(struct context *ctx, struct conn *conn)
{
	rstatus_t status;
	struct msg *msg, *nmsg; /* current and next message */

    ASSERT(conn->type == CONN_SERVER);

	server_close_stats(ctx, conn->owner, conn->err, conn->eof,
			conn->connected);

	if (conn->sd < 0) {
		server_failure(ctx, conn->owner);
		conn_unref(conn);
		conn_put(conn);
		return;
	}

    uint32_t out_counter = 0;
	for (msg = TAILQ_FIRST(&conn->omsg_q); msg != NULL; msg = nmsg) {
		nmsg = TAILQ_NEXT(msg, s_tqe);

		/* dequeue the message (request) from server outq */
        conn_dequeue_outq(ctx, conn, msg);
        server_ack_err(ctx, conn, msg);
        out_counter++;
	}
	ASSERT(TAILQ_EMPTY(&conn->omsg_q));

    uint32_t in_counter = 0;
    for (msg = TAILQ_FIRST(&conn->imsg_q); msg != NULL; msg = nmsg) {
		nmsg = TAILQ_NEXT(msg, s_tqe);

		/* dequeue the message (request) from server inq */
		conn_dequeue_inq(ctx, conn, msg);
        // We should also remove the msg from the timeout rbtree.
        msg_tmo_delete(msg);
        server_ack_err(ctx, conn, msg);
        in_counter++;

		stats_server_incr(ctx, conn->owner, server_dropped_requests);
	}
	ASSERT(TAILQ_EMPTY(&conn->imsg_q));

    log_warn("close %s %d Dropped %u outqueue & %u inqueue requests",
             conn_get_type_string(conn), conn->sd, out_counter, in_counter);

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

	conn_unref(conn);

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

    ASSERT(conn->type == CONN_SERVER);

	if (conn->sd > 0) {
		/* already connected on server connection */
		return DN_OK;
	}

        if (log_loggable(LOG_VVERB)) {
	   log_debug(LOG_VVERB, "connect to server '%.*s'", server->pname.len,
		   	server->pname.data);
        }

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

static void
server_connected(struct context *ctx, struct conn *conn)
{
	struct server *server = conn->owner;

    ASSERT(conn->type == CONN_SERVER);
	ASSERT(conn->connecting && !conn->connected);

	stats_server_incr(ctx, server, server_connections);

	conn->connecting = 0;
	conn->connected = 1;

        if (log_loggable(LOG_INFO)) {
	   log_debug(LOG_INFO, "connected on s %d to server '%.*s'", conn->sd,
		   	server->pname.len, server->pname.data);
        }
}

static void
server_ok(struct context *ctx, struct conn *conn)
{
	struct server *server = conn->owner;

    ASSERT(conn->type == CONN_SERVER);
	ASSERT(conn->connected);

	if (server->failure_count != 0) {
           if (log_loggable(LOG_VERB)) {
		   log_debug(LOG_VERB, "reset server '%.*s' failure count from %"PRIu32
				 " to 0", server->pname.len, server->pname.data,
				 server->failure_count);
           }
           server->failure_count = 0;
           server->next_retry = 0ULL;
	}
}

static rstatus_t
server_pool_update(struct server_pool *pool)
{
	rstatus_t status;
	usec_t now;
	uint32_t pnlive_server; /* prev # live server */

	if (!pool->auto_eject_hosts) {
		return DN_OK;
	}

	if (pool->next_rebuild == 0ULL) {
		return DN_OK;
	}

	now = dn_usec_now();
	if (now == 0) {
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


static rstatus_t
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


static rstatus_t
rack_deinit(struct rack *rack)
{
	if (rack->continuum != NULL) {
		dn_free(rack->continuum);
	}

	return DN_OK;
}


static rstatus_t
dc_init(struct datacenter *dc)
{
	rstatus_t status;

	dc->dict_rack = dictCreate(&dc_string_dict_type, NULL);
	dc->name = dn_alloc(sizeof(struct string));
	string_init(dc->name);

	status = array_init(&dc->racks, 3, sizeof(struct rack));

	return status;
}

static rstatus_t
rack_destroy(void *elem, void *data)
{
	struct rack *rack = elem;
	return rack_deinit(rack);
}

static rstatus_t
dc_deinit(struct datacenter *dc)
{
	array_each(&dc->racks, rack_destroy, NULL);
	string_deinit(dc->name);
	//dictRelease(dc->dict_rack);
	return DN_OK;
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

	if (log_loggable(LOG_DEBUG)) {
		log_debug(LOG_DEBUG, "server_get_dc pool  '%.*s'",
				dcname->len, dcname->data);
	}

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

	if (log_loggable(LOG_DEBUG)) {
		log_debug(LOG_DEBUG, "server_get_dc pool about to exit  '%.*s'",
				dc->name->len, dc->name->data);
	}

	return dc;
}


struct rack *
server_get_rack(struct datacenter *dc, struct string *rackname)
{
	ASSERT(dc != NULL);
	ASSERT(dc->dict_rack != NULL);
	ASSERT(dc->name != NULL);

	if (log_loggable(LOG_DEBUG)) {
		log_debug(LOG_DEBUG, "server_get_rack   '%.*s'", rackname->len, rackname->data);
	}
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

	if (log_loggable(LOG_DEBUG)) {
		log_debug(LOG_DEBUG, "server_get_rack exiting  '%.*s'",
				rack->name->len, rack->name->data);
	}

	return rack;
}


struct rack *
server_get_rack_by_dc_rack(struct server_pool *sp, struct string *rackname, struct string *dcname)
{
	struct datacenter *dc = server_get_dc(sp, dcname);
	return server_get_rack(dc, rackname);
}

struct msg *
rsp_recv_next(struct context *ctx, struct conn *conn, bool alloc)
{
    struct msg *msg;

    ASSERT((conn->type == CONN_DNODE_PEER_SERVER) ||
           (conn->type == CONN_SERVER));

    if (conn->eof) {
        msg = conn->rmsg;

        if (conn->dyn_mode) {
            if (conn->non_bytes_recv > MAX_CONN_ALLOWABLE_NON_RECV) {
                conn->err = EPIPE;
                return NULL;
            }
            conn->eof = 0;
            return msg;
        }

        /* server sent eof before sending the entire request */
        if (msg != NULL) {
            conn->rmsg = NULL;

            ASSERT(msg->peer == NULL);
            ASSERT(!msg->request);

            log_error("eof s %d discarding incomplete rsp %"PRIu64" len "
                      "%"PRIu32"", conn->sd, msg->id, msg->mlen);

            rsp_put(msg);
        }

        /*
         * We treat TCP half-close from a server different from how we treat
         * those from a client. On a FIN from a server, we close the connection
         * immediately by sending the second FIN even if there were outstanding
         * or pending requests. This is actually a tricky part in the FA, as
         * we don't expect this to happen unless the server is misbehaving or
         * it crashes
         */
        conn->done = 1;
        log_debug(LOG_DEBUG, "s %d active %d is done", conn->sd, conn_active(conn));

        return NULL;
    }

    msg = conn->rmsg;
    if (msg != NULL) {
        ASSERT(!msg->request);
        return msg;
    }

    if (!alloc) {
        return NULL;
    }

    msg = rsp_get(conn);
    if (msg != NULL) {
        conn->rmsg = msg;
    }

    return msg;
}

static bool
server_rsp_filter(struct context *ctx, struct conn *conn, struct msg *msg)
{
    struct msg *pmsg;

    ASSERT(conn->type == CONN_SERVER);

    if (msg_empty(msg)) {
        ASSERT(conn->rmsg == NULL);
        log_debug(LOG_VERB, "filter empty rsp %"PRIu64" on s %d", msg->id,
                  conn->sd);
        rsp_put(msg);
        return true;
    }

    pmsg = TAILQ_FIRST(&conn->omsg_q);
    if (pmsg == NULL) {
        log_debug(LOG_VERB, "filter stray rsp %"PRIu64" len %"PRIu32" on s %d",
                  msg->id, msg->mlen, conn->sd);
        rsp_put(msg);
        return true;
    }

    if (pmsg->noreply) {
         conn_dequeue_outq(ctx, conn, pmsg);
         req_put(pmsg);
         rsp_put(msg);
         return true;
    }

    ASSERT(pmsg->peer == NULL);
    ASSERT(pmsg->request && !pmsg->done);

    if (pmsg->swallow) {
        conn_dequeue_outq(ctx, conn, pmsg);
        pmsg->done = 1;

        log_debug(LOG_DEBUG, "swallow rsp %"PRIu64" len %"PRIu32" of req "
                  "%"PRIu64" on s %d", msg->id, msg->mlen, pmsg->id,
                  conn->sd);

        rsp_put(msg);
        req_put(pmsg);
        return true;
    }

    return false;
}

static void
server_rsp_forward_stats(struct context *ctx, struct server *server,
                         struct msg *msg)
{
	ASSERT(!msg->request);

	if (msg->is_read) {
		stats_server_incr(ctx, server, read_responses);
		stats_server_incr_by(ctx, server, read_response_bytes, msg->mlen);
	} else {
		stats_server_incr(ctx, server, write_responses);
		stats_server_incr_by(ctx, server, write_response_bytes, msg->mlen);
	}
}

static void
server_rsp_forward(struct context *ctx, struct conn *s_conn, struct msg *rsp)
{
    rstatus_t status;
    struct msg *req;
    struct conn *c_conn;
    ASSERT(s_conn->type == CONN_SERVER);

    /* response from server implies that server is ok and heartbeating */
    server_ok(ctx, s_conn);

    /* dequeue peer message (request) from server */
    req = TAILQ_FIRST(&s_conn->omsg_q);
    ASSERT(req != NULL && req->peer == NULL);
    ASSERT(req->request && !req->done);

    conn_dequeue_outq(ctx, s_conn, req);
    req->done = 1;

    /* establish rsp <-> req (response <-> request) link */
    req->peer = rsp;
    rsp->peer = req;

    rsp->pre_coalesce(rsp);

    c_conn = req->owner;
    log_info("c_conn %p %d:%d <-> %d:%d", c_conn, req->id, req->parent_id,
               rsp->id, rsp->parent_id);
    
    ASSERT((c_conn->type == CONN_CLIENT) ||
           (c_conn->type == CONN_DNODE_PEER_CLIENT));

    server_rsp_forward_stats(ctx, s_conn->owner, rsp);
    // this should really be the message's response handler be doing it
    if (req_done(c_conn, req)) {
        // handler owns the response now
        status = conn_handle_response(c_conn, c_conn->type == CONN_CLIENT ?
                                      req->id : req->parent_id, rsp);
        IGNORE_RET_VAL(status);
     }
}

static void
rsp_recv_done(struct context *ctx, struct conn *conn, struct msg *msg,
                     struct msg *nmsg)
{
    ASSERT(conn->type == CONN_SERVER);
    ASSERT(msg != NULL && conn->rmsg == msg);
    ASSERT(!msg->request);
    ASSERT(msg->owner == conn);
    ASSERT(nmsg == NULL || !nmsg->request);

    /* enqueue next message (response), if any */
    conn->rmsg = nmsg;

    if (server_rsp_filter(ctx, conn, msg)) {
        return;
    }
    server_rsp_forward(ctx, conn, msg);
}

struct msg *
req_send_next(struct context *ctx, struct conn *conn)
{
    rstatus_t status;
    struct msg *msg, *nmsg; /* current and next message */

    ASSERT((conn->type == CONN_SERVER) ||
           (conn->type == CONN_DNODE_PEER_SERVER));

    if (conn->connecting) {
        if (conn->type == CONN_SERVER) {
           server_connected(ctx, conn);
        } else if (conn->type == CONN_DNODE_PEER_SERVER) {
           dnode_peer_connected(ctx, conn);
        }
    }

    nmsg = TAILQ_FIRST(&conn->imsg_q);
    if (nmsg == NULL) {
        /* nothing to send as the server inq is empty */
        status = event_del_out(ctx->evb, conn);
        if (status != DN_OK) {
            conn->err = errno;
        }

        return NULL;
    }

    msg = conn->smsg;
    if (msg != NULL) {
        ASSERT(msg->request && !msg->done);
        nmsg = TAILQ_NEXT(msg, s_tqe);
    }

    conn->smsg = nmsg;

    if (nmsg == NULL) {
        return NULL;
    }

    ASSERT(nmsg->request && !nmsg->done);

    if (log_loggable(LOG_VVERB)) {
       log_debug(LOG_VVERB, "send next req %"PRIu64" len %"PRIu32" type %d on "
                "s %d", nmsg->id, nmsg->mlen, nmsg->type, conn->sd);
    }

    return nmsg;
}

void
req_send_done(struct context *ctx, struct conn *conn, struct msg *msg)
{
    ASSERT((conn->type == CONN_SERVER) ||
           (conn->type == CONN_DNODE_PEER_SERVER));
    ASSERT(msg != NULL && conn->smsg == NULL);
    ASSERT(msg->request && !msg->done);
    //ASSERT(msg->owner == conn);

    if (log_loggable(LOG_VVERB)) {
       log_debug(LOG_VVERB, "send done req %"PRIu64" len %"PRIu32" type %d on "
                "s %d", msg->id, msg->mlen, msg->type, conn->sd);
    }

    /* dequeue the message (request) from server inq */
    conn_dequeue_inq(ctx, conn, msg);

    /*
     * noreply request instructs the server not to send any response. So,
     * enqueue message (request) in server outq, if response is expected.
     * Otherwise, free the noreply request
     */
    if (!msg->noreply || (conn->type == CONN_SERVER))
        conn_enqueue_outq(ctx, conn, msg);
    else
        req_put(msg);
}

static void
req_server_enqueue_imsgq(struct context *ctx, struct conn *conn, struct msg *msg)
{
    ASSERT(msg->request);
    ASSERT(conn->type == CONN_SERVER);

    /*
     * timeout clock starts ticking the instant the message is enqueued into
     * the server in_q; the clock continues to tick until it either expires
     * or the message is dequeued from the server out_q
     *
     * noreply request are free from timeouts because client is not interested
     * in the reponse anyway!
     */
    if (!msg->noreply) {
        msg_tmo_insert(msg, conn);
    }

    TAILQ_INSERT_TAIL(&conn->imsg_q, msg, s_tqe);
    log_debug(LOG_VERB, "conn %p enqueue inq %d:%d", conn, msg->id, msg->parent_id);

    conn->imsg_count++;
    histo_add(&ctx->stats->server_in_queue, conn->imsg_count);
    stats_server_incr(ctx, conn->owner, in_queue);
    stats_server_incr_by(ctx, conn->owner, in_queue_bytes, msg->mlen);
}

static void
req_server_dequeue_imsgq(struct context *ctx, struct conn *conn, struct msg *msg)
{
    ASSERT(msg->request);
    ASSERT(conn->type == CONN_SERVER);

    TAILQ_REMOVE(&conn->imsg_q, msg, s_tqe);
    log_debug(LOG_VERB, "conn %p dequeue inq %d:%d", conn, msg->id, msg->parent_id);

    conn->imsg_count--;
    histo_add(&ctx->stats->server_in_queue, conn->imsg_count);
    stats_server_decr(ctx, conn->owner, in_queue);
    stats_server_decr_by(ctx, conn->owner, in_queue_bytes, msg->mlen);
}

static void
req_server_enqueue_omsgq(struct context *ctx, struct conn *conn, struct msg *msg)
{
    ASSERT(msg->request);
    ASSERT(conn->type == CONN_SERVER);

    TAILQ_INSERT_TAIL(&conn->omsg_q, msg, s_tqe);
    log_debug(LOG_VERB, "conn %p enqueue outq %d:%d", conn, msg->id, msg->parent_id);

    conn->omsg_count++;
    histo_add(&ctx->stats->server_out_queue, conn->omsg_count);
    stats_server_incr(ctx, conn->owner, out_queue);
    stats_server_incr_by(ctx, conn->owner, out_queue_bytes, msg->mlen);
}

static void
req_server_dequeue_omsgq(struct context *ctx, struct conn *conn, struct msg *msg)
{
    ASSERT(msg->request);
    ASSERT(conn->type == CONN_SERVER);

    msg_tmo_delete(msg);

    TAILQ_REMOVE(&conn->omsg_q, msg, s_tqe);
    log_debug(LOG_VERB, "conn %p dequeue outq %d:%d", conn, msg->id, msg->parent_id);

    conn->omsg_count--;
    histo_add(&ctx->stats->server_out_queue, conn->omsg_count);
    stats_server_decr(ctx, conn->owner, out_queue);
    stats_server_decr_by(ctx, conn->owner, out_queue_bytes, msg->mlen);
}

struct conn_ops server_ops = {
    msg_recv,
    rsp_recv_next,
    rsp_recv_done,
    msg_send,
    req_send_next,
    req_send_done,
    server_close,
    server_active,
    server_ref,
    server_unref,
    req_server_enqueue_imsgq,
    req_server_dequeue_imsgq,
    req_server_enqueue_omsgq,
    req_server_dequeue_omsgq,
    conn_cant_handle_response
};

void
init_server_conn(struct conn *conn)
{
    conn->type = CONN_SERVER;
    conn->ops = &server_ops;
}
