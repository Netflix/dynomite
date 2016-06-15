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

#include <sys/un.h>

#include "dyn_core.h"
#include "dyn_server.h"
#include "dyn_proxy.h"
#include "dyn_thread_ctx.h"

static void
proxy_ref(struct conn *conn, void *owner)
{
    struct server_pool *pool = owner;

    ASSERT(conn->p.type == CONN_PROXY);
    ASSERT(conn->owner == NULL);

    conn->family = pool->proxy_endpoint.family;
    conn->addrlen = pool->proxy_endpoint.addrlen;
    conn->addr = pool->proxy_endpoint.addr;
    string_duplicate(&conn->pname, &pool->proxy_endpoint.pname);

    pool->p_conn = conn;

    /* owner of the proxy connection is the server pool */
    conn->owner = owner;
    conn->ptctx = core_get_ptctx_for_conn(pool->ctx, conn);

    log_debug(LOG_VVERB, "ref conn %p owner %p", conn, pool);
}

static void
proxy_unref(struct conn *conn)
{
    struct server_pool *pool;

    ASSERT(conn->p.type == CONN_PROXY);
    ASSERT(conn->owner != NULL);

    pool = conn->owner;
    conn->owner = NULL;

    pool->p_conn = NULL;

    log_debug(LOG_VVERB, "unref conn %p owner %p", conn, pool);
}

static void
proxy_close(struct context *ctx, struct conn *conn)
{
    rstatus_t status;

    ASSERT(conn->p.type == CONN_PROXY);

    if (conn->p.sd < 0) {
        conn_unref(conn);
        conn_put(conn);
        return;
    }

    ASSERT(conn->rmsg == NULL);
    ASSERT(conn->smsg == NULL);
    ASSERT(TAILQ_EMPTY(&conn->imsg_q));
    ASSERT(TAILQ_EMPTY(&conn->omsg_q));

    conn_unref(conn);

    status = close(conn->p.sd);
    if (status < 0) {
        log_error("close p %d failed, ignored: %s", conn->p.sd, strerror(errno));
    }
    conn->p.sd = -1;

    conn_put(conn);
}

rstatus_t
proxy_init(struct context *ctx)
{
    rstatus_t status;
    struct server_pool *pool = &ctx->pool;

    struct conn *p = conn_get_proxy(pool);
    if (!p) {
        return DN_ENOMEM;
    }

    status = conn_listen(pool->ctx, p);
    if (status != DN_OK) {
        conn_close(pool->ctx, p);
        return status;
    }

    char * log_datastore = "not selected data store";
    if (g_data_store == DATA_REDIS){
    	log_datastore = "redis";
    }
    else if (g_data_store == DATA_MEMCACHE){
    	log_datastore = "memcache";
    }

    log_debug(LOG_NOTICE, "p %d listening on '%.*s' in %s pool '%.*s'",
              p->p.sd, pool->proxy_endpoint.pname.len,
              pool->proxy_endpoint.pname.data,
			  log_datastore,
              pool->name.len, pool->name.data);

    return DN_OK;
}

void
proxy_deinit(struct context *ctx)
{
    struct server_pool *pool = &ctx->pool;
    struct conn *p = pool->p_conn;
    if (p != NULL) {
        conn_close(pool->ctx, p);
        pool->p_conn = NULL;
    }

    log_debug(LOG_VVERB, "deinit proxy");
}

static rstatus_t
proxy_accept(struct context *ctx, struct conn *p)
{
    rstatus_t status;
    struct conn *c;
    int sd;

    ASSERT(p->p.type == CONN_PROXY);
    ASSERT(p->p.sd > 0);
    ASSERT(p->p.recv_active && p->recv_ready);

    for (;;) {
        sd = accept(p->p.sd, NULL, NULL);
        if (sd < 0) {
            if (errno == EINTR) {
                log_warn("accept on %s %d not ready - eintr",
                         conn_get_type_string(p), p->p.sd);
                continue;
            }

            if (errno == EAGAIN || errno == EWOULDBLOCK) {
                p->recv_ready = 0;
                return DN_OK;
            }

            /*
             * FIXME: On EMFILE or ENFILE mask out IN event on the proxy; mask
             * it back in when some existing connection gets closed
             */

            log_error("accept on %s %d failed: %s",
                      conn_get_type_string(p), p->p.sd, strerror(errno));
            return DN_ERROR;
        }

        break;
    }

    c = conn_get(p->owner, true);
    if (c == NULL) {
        log_error("get conn for CLIENT %d from %s %d failed: %s", sd,
                  conn_get_type_string(p), p->p.sd, strerror(errno));
        status = close(sd);
        if (status < 0) {
            log_error("close c %d failed, ignored: %s", sd, strerror(errno));
        }
        return DN_ENOMEM;
    }
    c->p.sd = sd;

    stats_pool_incr(ctx, client_connections);

    status = dn_set_nonblocking(c->p.sd);
    if (status < 0) {
        log_error("set nonblock on %s %d from p %d failed: %s",
                  conn_get_type_string(c), c->p.sd, p->p.sd, strerror(errno));
        conn_close(ctx, c);
        return status;
    }

    if (p->family == AF_INET || p->family == AF_INET6) {
        status = dn_set_tcpnodelay(c->p.sd);
        if (status < 0) {
            log_warn("set tcpnodelay on %s %d from %s %d failed, ignored: %s",
                     conn_get_type_string(c), c->p.sd, conn_get_type_string(p),
                     p->p.sd, strerror(errno));
        }
    }

    status = thread_ctx_add_conn(c->ptctx, conn_get_pollable(c));
    if (status < 0) {
        log_error("event add conn from %s %d failed: %s",conn_get_type_string(p),
                  p->p.sd, strerror(errno));
        conn_close(ctx, c);
        return status;
    }

    log_notice("accepted %s %d on %s %d from '%s'", conn_get_type_string(c),
               c->p.sd, conn_get_type_string(p), p->p.sd, dn_unresolve_peer_desc(c->p.sd));

    return DN_OK;
}

static rstatus_t
proxy_recv(struct context *ctx, struct conn *conn)
{
    rstatus_t status;

    ASSERT(conn->p.type == CONN_PROXY);
    ASSERT(conn->p.recv_active);

    conn->recv_ready = 1;
    do {
        status = proxy_accept(ctx, conn);
        if (status != DN_OK) {
            return status;
        }
    } while (conn->recv_ready);

    return DN_OK;
}

struct conn_ops proxy_ops = {
    proxy_recv,
    NULL,
    NULL,
    NULL,
    NULL,
    NULL,
    proxy_close,
    NULL,
    proxy_ref,
    proxy_unref,
    // enqueue, dequeues
    NULL,
    NULL,
    NULL,
    NULL,
    conn_cant_handle_response
};

void
init_proxy_conn(struct conn *conn)
{
    conn->p.type = CONN_PROXY;
    conn->ops = &proxy_ops;
}

