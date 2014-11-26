/*
 * dyn_dnode_tls_server.c
 */

#include <sys/un.h>

#include "dyn_core.h"
#include "dyn_server.h"
#include "dyn_dnode_peer.h"
#include "dyn_dnode_tls_server.h"

static rstatus_t
dnode_tls_listen(struct context *ctx, struct conn *p);

rstatus_t
dnode_tls_each_init(void *elem, void *data)
{
    rstatus_t status;
    struct server_pool *pool = elem;
    struct conn *p;

    p = conn_get_dnode_tls(pool);

    if (p == NULL) {
        return DN_ENOMEM;
    }

    if (p->tls_ctx == NULL) {
        log_error("Could not allocate tls_ctx.");
        return DN_ENOMEM;
    }

    status = dyn_tls_init(p->tls_ctx);
    if (status != DN_OK) {
        return status;
    }
    status = dnode_tls_listen(pool->ctx, p);
    if (status != DN_OK) {
        p->close(pool->ctx, p);
        return status;
    }

    log_debug(LOG_NOTICE, "dyn: p %d listening on '%.*s' in %s pool %"PRIu32" '%.*s'"
              " with %"PRIu32" servers", p->sd, pool->ds_addrstr.len,
              pool->ds_addrstr.data, pool->redis ? "redis" : "memcache",
              pool->idx, pool->name.len, pool->name.data,
              array_n(&pool->server));
    return DN_OK;
}


void
dnode_tls_deinit(struct context *ctx)
{
    rstatus_t status;

    ASSERT(array_n(&ctx->pool) != 0);

    status = array_each(&ctx->pool, dnode_tls_each_deinit, NULL);
    if (status != DN_OK) {
        return;
    }

    log_debug(LOG_VVERB, "deinit dnode with %"PRIu32" pools",
              array_n(&ctx->pool));
}

rstatus_t
dnode_tls_init(struct context *ctx)
{
    rstatus_t status;

    ASSERT(array_n(&ctx->pool) != 0);

    status = array_each(&ctx->pool, dnode_tls_each_init, NULL);
    if (status != DN_OK) {
        dnode_tls_deinit(ctx);
        return status;
    }

    log_debug(LOG_VVERB, "init dnode with %"PRIu32" pools",
              array_n(&ctx->pool));

    return DN_OK;
}

rstatus_t
dnode_tls_each_deinit(void *elem, void *data)
{
    struct server_pool *pool = elem;
    struct conn *p;

    p = pool->d_conn;
    if (p != NULL) {
        dyn_tls_deinit(p);
        p->close(pool->ctx, p);
    }

    return DN_OK;
}

void
dnode_tls_ref(struct conn *conn, void *owner)
{
    struct server_pool *pool = owner;

    ASSERT(conn->dnode_tls_server);
    ASSERT(conn->owner == NULL);

    conn->family = pool->ds_family;
    conn->addrlen = pool->ds_addrlen;
    conn->addr = pool->ds_addr;

    pool->ds_conn = conn;

    /* owner of the dnode tls connection is the server pool */
    conn->owner = owner;

    log_debug(LOG_VVERB, "ref conn %p owner %p into pool %"PRIu32"", conn,
              pool, pool->idx);
}

void
dnode_tls_unref(struct conn *conn)
{
    struct server_pool *pool;

    ASSERT(conn->dnode_tls_server);
    ASSERT(conn->owner != NULL);

    pool = conn->owner;
    conn->owner = NULL;

    pool->d_conn = NULL;

    log_debug(LOG_VVERB, "unref conn %p owner %p from pool %"PRIu32"", conn,
              pool, pool->idx);
}

void
dnode_tls_close(struct context *ctx, struct conn *conn)
{
    rstatus_t status;

    ASSERT(conn->dnode_tls_server);

    if (conn->sd < 0) {
        conn->unref(conn);
        conn_put(conn);
        return;
    }

    ASSERT(conn->rmsg == NULL);
    ASSERT(conn->smsg == NULL);
    ASSERT(TAILQ_EMPTY(&conn->imsg_q));
    ASSERT(TAILQ_EMPTY(&conn->omsg_q));

    conn->unref(conn);

    status = close(conn->sd);
    if (status < 0) {
        log_error("close p %d failed, ignored: %s", conn->sd, strerror(errno));
    }
    conn->sd = -1;

    dyn_tls_close(conn);
    conn_put(conn);
}

static rstatus_t
dnode_tls_reuse(struct conn *p)
{
    rstatus_t status;
    struct sockaddr_un *un;

    switch (p->family) {
    case AF_INET:
    case AF_INET6:
        status = dn_set_reuseaddr(p->sd);
        break;

    case AF_UNIX:
        /*
         * bind() will fail if the pathname already exist. So, we call unlink()
         * to delete the pathname, in case it already exists. If it does not
         * exist, unlink() returns error, which we ignore
         */
        un = (struct sockaddr_un *) p->addr;
        unlink(un->sun_path);
        status = DN_OK;
        break;

    default:
        NOT_REACHED();
        status = DN_ERROR;
    }

    return status;
}

static rstatus_t
dnode_tls_listen(struct context *ctx, struct conn *p)
{
    rstatus_t status;
    struct server_pool *pool = p->owner;

    //TODOs: add this for testing
    //return DN_OK;


    ASSERT(p->dnode_tls_server);

    p->sd = socket(p->family, SOCK_STREAM, 0);
    if (p->sd < 0) {
        log_error("dyn: socket failed: %s", strerror(errno));
        return DN_ERROR;
    }

    status = dnode_tls_reuse(p);
    if (status < 0) {
        log_error("dyn: reuse of addr '%.*s' for listening on p %d failed: %s",
                  pool->ds_addrstr.len, pool->ds_addrstr.data, p->sd,
                  strerror(errno));
        return DN_ERROR;
    }

    status = bind(p->sd, p->addr, p->addrlen);
    if (status < 0) {
        log_error("dyn: bind on p %d to addr '%.*s' failed: %s", p->sd,
                  pool->ds_addrstr.len, pool->ds_addrstr.data, strerror(errno));
        return DN_ERROR;
    }

    status = listen(p->sd, pool->backlog);
    if (status < 0) {
        log_error("dyn: listen on p %d on addr '%.*s' failed: %s", p->sd,
                  pool->ds_addrstr.len, pool->ds_addrstr.data, strerror(errno));
        return DN_ERROR;
    }

    status = dn_set_nonblocking(p->sd);
    if (status < 0) {
        log_error("dyn: set nonblock on p %d on addr '%.*s' failed: %s", p->sd,
                  pool->ds_addrstr.len, pool->ds_addrstr.data, strerror(errno));
        return DN_ERROR;
    }

    log_debug(LOG_INFO, "dyn: e %d with nevent %d", ctx->evb->ep, ctx->evb->nevent);
    status = event_add_conn(ctx->evb, p);
    if (status < 0) {
        log_error("dyn: event add conn p %d on addr '%.*s' failed: %s",
                  p->sd, pool->ds_addrstr.len, pool->ds_addrstr.data,
                  strerror(errno));
        return DN_ERROR;
    }

    status = event_del_out(ctx->evb, p);
    if (status < 0) {
        log_error("dyn: event del out p %d on addr '%.*s' failed: %s",
                  strerror(errno));
        return DN_ERROR;
    }

    return DN_OK;
}

static rstatus_t
dnode_tls_accept(struct context *ctx, struct conn *p)
{
    rstatus_t status;
    struct conn *c;
    int sd;

    ASSERT(p->dnode_tls_server);
    ASSERT(p->sd > 0);
    ASSERT(p->recv_active && p->recv_ready);


    for (;;) {
        sd = accept(p->sd, NULL, NULL);
        if (sd < 0) {
            if (errno == EINTR) {
                log_debug(LOG_VERB, "dyn: accept on p %d not ready - eintr", p->sd);
                continue;
            }

            if (errno == EAGAIN || errno == EWOULDBLOCK) {
                log_debug(LOG_VERB, "dyn: accept on p %d not ready - eagain", p->sd);
                p->recv_ready = 0;
                return DN_OK;
            }

            /*
             * FIXME: On EMFILE or ENFILE mask out IN event on the proxy; mask
             * it back in when some existing connection gets closed
             */

            log_error("dyn: accept on p %d failed: %s", p->sd, strerror(errno));
            return DN_ERROR;
        }

        break;
    }

    log_debug(LOG_NOTICE, "dyn: accept on sd  %d", sd);
    c = conn_get_peer(p->owner, true, true, p->redis);

    log_debug(LOG_VVERB, "get dyn peer  conn %p client %d", c, c->dnode_client);

    if (c == NULL) {
        log_error("dyn: get conn client peer for c %d from p %d failed: %s", sd, p->sd,
                  strerror(errno));
        status = close(sd);
        if (status < 0) {
            log_error("dyn: close c %d failed, ignored: %s", sd, strerror(errno));
        }
        return DN_ENOMEM;
    }
    c->sd = sd;

    //TODO: Record tls client stats.
    //stats_pool_incr(ctx, c->owner, dnode_client_connections);

    status = dn_set_nonblocking(c->sd);
    if (status < 0) {
        log_error("dyn: set nonblock on c %d from p %d failed: %s", c->sd, p->sd,
                  strerror(errno));
        c->close(ctx, c);
        return status;
    }

    if (p->family == AF_INET || p->family == AF_INET6) {
        status = dn_set_tcpnodelay(c->sd);
        if (status < 0) {
            log_warn("dyn: set tcpnodelay on c %d from p %d failed, ignored: %s",
                     c->sd, p->sd, strerror(errno));
        }
    }

    if (dyn_tls_accept(p, c, sd) == DN_ERROR) {
        return DN_ERROR;
    }

    status = event_add_conn(ctx->evb, c);
    if (status < 0) {
        log_error("dyn: event add conn from p %d failed: %s", p->sd,
                  strerror(errno));
        c->close(ctx, c);
        return status;
    }

    log_debug(LOG_NOTICE, "dyn: 2q c %d on p %d from '%s'", c->sd, p->sd,
              dn_unresolve_peer_desc(c->sd));

    return DN_OK;
}

rstatus_t
dnode_tls_recv(struct context *ctx, struct conn *conn)
{
    rstatus_t status;

    //ASSERT(conn->dnode_tls_server && !conn->dnode_tls_client);
    ASSERT(conn->recv_active);

    conn->recv_ready = 1;
    do {
        status = dnode_tls_accept(ctx, conn);
        if (status != DN_OK) {
            return status;
        }
    } while (conn->recv_ready);

    return DN_OK;
}
