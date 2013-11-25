#include <stdlib.h>
#include <unistd.h>

#include <nc_core.h>
#include <nc_conf.h>
#include <dyn_peer.h>
#include <nc_server.h>
#include <dyn_peer.h>
#include <dyn_token.h>


void
dyn_peer_ref(struct conn *conn, void *owner)
{
    struct peer *pn = owner;

    ASSERT(!conn->dnode && !conn->dyn_client);
    ASSERT(conn->owner == NULL);

    conn->family = pn->family;
    conn->addrlen = pn->addrlen;
    conn->addr = pn->addr;

    pn->ns_conn_q++;
    TAILQ_INSERT_TAIL(&pn->s_conn_q, conn, conn_tqe);

    conn->owner = owner;

    log_debug(LOG_VVERB, "dyn: ref peer conn %p owner %p into '%.*s", conn, pn,
              pn->pname.len, pn->pname.data);
}

void
dyn_peer_unref(struct conn *conn)
{
    struct peer *pn;

    ASSERT(!conn->dnode && !conn->dyn_client);
    ASSERT(conn->owner != NULL);

    pn = conn->owner;
    conn->owner = NULL;

    ASSERT(pn->ns_conn_q != 0);
    pn->ns_conn_q--;
    TAILQ_REMOVE(&pn->s_conn_q, conn, conn_tqe);

    log_debug(LOG_VVERB, "dyn: unref peer conn %p owner %p from '%.*s'", conn, pn,
              pn->pname.len, pn->pname.data);
}

int
dyn_peer_timeout(struct conn *conn)
{
    struct peer *server;
    struct server_pool *pool;

    ASSERT(!conn->dnode && !conn->dyn_client);

    server = conn->owner;
    pool = server->owner;

    return pool->d_timeout;
}

bool
dyn_peer_active(struct conn *conn)
{
    ASSERT(!conn->dnode && !conn->dyn_client);

    if (!TAILQ_EMPTY(&conn->imsg_q)) {
        log_debug(LOG_VVERB, "dyn: s %d is active", conn->sd);
        return true;
    }

    if (!TAILQ_EMPTY(&conn->omsg_q)) {
        log_debug(LOG_VVERB, "dyn: s %d is active", conn->sd);
        return true;
    }

    if (conn->rmsg != NULL) {
        log_debug(LOG_VVERB, "dyn: s %d is active", conn->sd);
        return true;
    }

    if (conn->smsg != NULL) {
        log_debug(LOG_VVERB, "dyn: s %d is active", conn->sd);
        return true;
    }

    log_debug(LOG_VVERB, "dyn: s %d is inactive", conn->sd);

    return false;
}

static rstatus_t
dyn_peer_each_set_owner(void *elem, void *data)
{
    struct peer *s = elem;
    struct server_pool *sp = data;

    s->owner = sp;

    return NC_OK;
}

rstatus_t
dyn_peer_init(struct array *conf_seeds,
            struct server_pool *sp)
{
    struct array * seeds = &sp->seeds;
    struct array * peers = &sp->peers;
    rstatus_t status;
    uint32_t nseed, npeer;

    /* init seeds list */
    nseed = array_n(conf_seeds);
    ASSERT(nseed != 0);
    ASSERT(array_n(seeds) == 0);

    status = array_init(seeds, nseed, sizeof(struct peer));
    if (status != NC_OK) {
        return status;
    }

    /* transform conf seeds to seeds */
    status = array_each(conf_seeds, conf_seed_each_transform, seeds);
    if (status != NC_OK) {
        dyn_peer_deinit(seeds);
        return status;
    }
    ASSERT(array_n(seeds) == nseed);

    /* set seed owner */
    status = array_each(seeds, dyn_peer_each_set_owner, sp);
    if (status != NC_OK) {
        dyn_peer_deinit(seeds);
        return status;
    }

 
    /* initialize peers list = seeds list */
    ASSERT(array_n(peers) == 0); 
    
    status = array_init(peers, nseed, sizeof(struct peer));
    if (status != NC_OK) {
        return status;
    }
 
    status = array_each(conf_seeds, conf_seed_each_transform, peers);
    if (status != NC_OK) {
        dyn_peer_deinit(seeds);
        dyn_peer_deinit(peers);
        return status;
    }
    ASSERT(array_n(peers) == nseed);
   
    status = array_each(peers, dyn_peer_each_set_owner, sp);
    if (status != NC_OK) {
        dyn_peer_deinit(seeds);
        dyn_peer_deinit(peers);
        return status;
    }

    log_debug(LOG_DEBUG, "init %"PRIu32" seeds and peers in pool %"PRIu32" '%.*s'",
              nseed, sp->idx, sp->name.len, sp->name.data);

    return NC_OK;
}

void
dyn_peer_deinit(struct array *nodes)
{
    uint32_t i, nnode;

    for (i = 0, nnode = array_n(nodes); i < nnode; i++) {
        struct peer *s;

        s = array_pop(nodes);
        ASSERT(TAILQ_EMPTY(&s->s_conn_q) && s->ns_conn_q == 0);
    }
    array_deinit(nodes);
}


struct conn *
dyn_peer_conn(struct peer *server)
{
    struct server_pool *pool;
    struct conn *conn;

    pool = server->owner;

    if (server->ns_conn_q < pool->d_connections) {
        return conn_get_peer(server, false);
    }
    ASSERT(server->ns_conn_q == pool->d_connections);

    /*
     * Pick a server connection from the head of the queue and insert
     * it back into the tail of queue to maintain the lru order
     */
    conn = TAILQ_FIRST(&server->s_conn_q);
    ASSERT(!conn->dyn_client && !conn->dnode);

    TAILQ_REMOVE(&server->s_conn_q, conn, conn_tqe);
    TAILQ_INSERT_TAIL(&server->s_conn_q, conn, conn_tqe);

    return conn;
}


static rstatus_t
dyn_peer_each_preconnect(void *elem, void *data)
{
    rstatus_t status;
    struct peer *pn;
    struct server_pool *pool;
    struct conn *conn;

    pn = elem;
    pool = pn->owner;

    
    conn = dyn_peer_conn(pn);
    if (conn == NULL) {
        return NC_ENOMEM;
    }

    status = dyn_peer_connect(pool->ctx, pn, conn);
    if (status != NC_OK) {
        log_warn("dyn: connect to peer '%.*s' failed, ignored: %s",
                 pn->pname.len, pn->pname.data, strerror(errno));
        dyn_peer_close(pool->ctx, conn);
    }

    return NC_OK;
}

static rstatus_t
dyn_peer_each_disconnect(void *elem, void *data)
{
    struct peer *server;
    struct server_pool *pool;

    server = elem;
    pool = server->owner;

    //fixe me not to use s_conn_q
    while (!TAILQ_EMPTY(&server->s_conn_q)) {
        struct conn *conn;

        ASSERT(server->ns_conn_q > 0);

        conn = TAILQ_FIRST(&server->s_conn_q);
        conn->close(pool->ctx, conn);
    }

    return NC_OK;
}

static void
dyn_peer_failure(struct context *ctx, struct peer *server)
{
    struct server_pool *pool = server->owner;
    int64_t now, next;
    rstatus_t status;

    //fix me
    if (!pool->auto_eject_hosts) {
        return;
    }

    server->failure_count++;

    log_debug(LOG_VERB, "dyn: peer '%.*s' failure count %"PRIu32" limit %"PRIu32,
              server->pname.len, server->pname.data, server->failure_count,
              pool->server_failure_limit);

    if (server->failure_count < pool->server_failure_limit) {
        return;
    }

    now = nc_usec_now();
    if (now < 0) {
        return;
    }

    //fix me
    //stats_server_set_ts(ctx, server, server_ejected_at, now);

    //fix me
    next = now + pool->server_retry_timeout;

    log_debug(LOG_INFO, "dyn: update peer pool %"PRIu32" '%.*s' to delete peer '%.*s' "
              "for next %"PRIu32" secs", pool->idx, pool->name.len,
              pool->name.data, server->pname.len, server->pname.data,
              pool->server_retry_timeout / 1000 / 1000);

    //stats_pool_incr(ctx, pool, server_ejects);

    server->failure_count = 0;
    server->next_retry = next;

    status = dyn_peer_pool_run(pool);
    if (status != NC_OK) {
        log_error("dyn: updating peer pool %"PRIu32" '%.*s' failed: %s", pool->idx,
                  pool->name.len, pool->name.data, strerror(errno));
    }
}

static void
dyn_peer_close_stats(struct context *ctx, struct peer *server, err_t err,
                   unsigned eof, unsigned connected)
{
    if (connected) {
        //fix me
        //stats_server_decr(ctx, server, server_connections);
    }

    if (eof) {
        //fix me
        //stats_server_incr(ctx, server, server_eof);
        return;
    }

    switch (err) {
    case ETIMEDOUT:
        //stats_server_incr(ctx, server, server_timedout);
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
        //stats_server_incr(ctx, server, server_err);
        break;
    }
}

void
dyn_peer_close(struct context *ctx, struct conn *conn)
{
    rstatus_t status;
    struct msg *msg, *nmsg; /* current and next message */
    struct conn *c_conn;    /* peer client connection */

    ASSERT(!conn->dnode && !conn->dyn_client);    

    dyn_peer_close_stats(ctx, conn->owner, conn->err, conn->eof,
                       conn->connected);

    if (conn->sd < 0) {
        dyn_peer_failure(ctx, conn->owner);
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
            log_debug(LOG_INFO, "dyn: close s %d swallow req %"PRIu64" len %"PRIu32
                      " type %d", conn->sd, msg->id, msg->mlen, msg->type);
            req_put(msg);
        } else {
            c_conn = msg->owner;
            ASSERT(c_conn->dyn_client && !c_conn->dnode);

            msg->done = 1;
            msg->error = 1;
            msg->err = conn->err;

            if (req_done(c_conn, TAILQ_FIRST(&c_conn->omsg_q))) {
                event_add_out(ctx->evb, msg->owner);
            }

            log_debug(LOG_INFO, "dyn: close s %d schedule error for req %"PRIu64" "
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
            log_debug(LOG_INFO, "dyn: close s %d swallow req %"PRIu64" len %"PRIu32
                      " type %d", conn->sd, msg->id, msg->mlen, msg->type);
            req_put(msg);
        } else {
            c_conn = msg->owner;
            ASSERT(c_conn->dyn_client && !c_conn->dnode);

            msg->done = 1;
            msg->error = 1;
            msg->err = conn->err;

            if (req_done(c_conn, TAILQ_FIRST(&c_conn->omsg_q))) {
                event_add_out(ctx->evb, msg->owner);
            }

            log_debug(LOG_INFO, "dyn: close s %d schedule error for req %"PRIu64" "
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

        log_debug(LOG_INFO, "dyn: close s %d discarding rsp %"PRIu64" len %"PRIu32" "
                  "in error", conn->sd, msg->id, msg->mlen);
    }

    ASSERT(conn->smsg == NULL);

    dyn_peer_failure(ctx, conn->owner);

    conn->unref(conn);

    status = close(conn->sd);
    if (status < 0) {
        log_error("dyn: close s %d failed, ignored: %s", conn->sd, strerror(errno));
    }
    conn->sd = -1;

    conn_put(conn);
}

rstatus_t
dyn_peer_connect(struct context *ctx, struct peer *server, struct conn *conn)
{
    rstatus_t status;

    ASSERT(!conn->dnode && !conn->dyn_client);

    if (conn->sd > 0) {
        /* already connected on peer connection */
        return NC_OK;
    }

    log_debug(LOG_VVERB, "dyn: connect to peer '%.*s'", server->pname.len,
              server->pname.data);

    conn->sd = socket(conn->family, SOCK_STREAM, 0);
    if (conn->sd < 0) {
        log_error("dyn: socket for peer '%.*s' failed: %s", server->pname.len,
                  server->pname.data, strerror(errno));
        status = NC_ERROR;
        goto error;
    }

    status = nc_set_nonblocking(conn->sd);
    if (status != NC_OK) {
        log_error("dyn: set nonblock on s %d for peer '%.*s' failed: %s",
                  conn->sd,  server->pname.len, server->pname.data,
                  strerror(errno));
        goto error;
    }

    if (server->pname.data[0] != '/') {
        status = nc_set_tcpnodelay(conn->sd);
        if (status != NC_OK) {
            log_warn("dyn: set tcpnodelay on s %d for peer '%.*s' failed, ignored: %s",
                     conn->sd, server->pname.len, server->pname.data,
                     strerror(errno));
        }
    }

    status = event_add_conn(ctx->evb, conn);
    if (status != NC_OK) {
        log_error("dyn: event add conn s %d for peer '%.*s' failed: %s",
                  conn->sd, server->pname.len, server->pname.data,
                  strerror(errno));
        goto error;
    }

    ASSERT(!conn->connecting && !conn->connected);

    status = connect(conn->sd, conn->addr, conn->addrlen);
    if (status != NC_OK) {
        if (errno == EINPROGRESS) {
            conn->connecting = 1;
            log_debug(LOG_DEBUG, "dyn: connecting on s %d to peer '%.*s'",
                      conn->sd, server->pname.len, server->pname.data);
            return NC_OK;
        }

        log_error("dyn: connect on s %d to peer '%.*s' failed: %s", conn->sd,
                  server->pname.len, server->pname.data, strerror(errno));

        goto error;
    }

    ASSERT(!conn->connecting);
    conn->connected = 1;
    log_debug(LOG_INFO, "dyn: connected on s %d to peer '%.*s'", conn->sd,
              server->pname.len, server->pname.data);

    return NC_OK;

error:
    conn->err = errno;
    return status;
}

void
dyn_peer_connected(struct context *ctx, struct conn *conn)
{
    struct peer *server = conn->owner;

    ASSERT(!conn->dnode && !conn->dyn_client);
    ASSERT(conn->connecting && !conn->connected);

    //fix me
    //stats_server_incr(ctx, server, server_connections);

    conn->connecting = 0;
    conn->connected = 1;

    log_debug(LOG_INFO, "dyn: peer connected on s %d to server '%.*s'", conn->sd,
              server->pname.len, server->pname.data);
}

void
dyn_peer_ok(struct context *ctx, struct conn *conn)
{
    struct peer *server = conn->owner;

    ASSERT(!conn->dnode && !conn->dyn_client);
    ASSERT(conn->connected);

    if (server->failure_count != 0) {
        log_debug(LOG_VERB, "dyn: reset peer '%.*s' failure count from %"PRIu32
                  " to 0", server->pname.len, server->pname.data,
                  server->failure_count);
        server->failure_count = 0;
        server->next_retry = 0LL;
    }
}

static rstatus_t
dyn_peer_pool_update(struct server_pool *pool)
{
    rstatus_t status;
    int64_t now;
    uint32_t pnlive_server; /* prev # live server */

    //fix me
    if (!pool->auto_eject_hosts) {
        return NC_OK;
    }

    //fix me
    if (pool->next_rebuild == 0LL) {
        return NC_OK;
    }

    now = nc_usec_now();
    if (now < 0) {
        return NC_ERROR;
    }

    //fix me
    if (now <= pool->next_rebuild) {
        if (pool->nlive_server == 0) {
            errno = ECONNREFUSED;
            return NC_ERROR;
        }
        return NC_OK;
    }

    //fixe me to use anotehr variable
    pnlive_server = pool->nlive_server;

    status = dyn_peer_pool_run(pool);
    if (status != NC_OK) {
        log_error("dyn: updating peer pool %"PRIu32" with dist %d failed: %s", pool->idx,
                  pool->dist_type, strerror(errno));
        return status;
    }

    log_debug(LOG_INFO, "dyn: update peer pool %"PRIu32" '%.*s' to add %"PRIu32" servers",
              pool->idx, pool->name.len, pool->name.data,
              pool->nlive_server - pnlive_server);


    return NC_OK;
}

static struct dyn_token *
dyn_peer_pool_hash(struct server_pool *pool, uint8_t *key, uint32_t keylen)
{
    ASSERT(array_n(&pool->peers) != 0);

    if (array_n(&pool->peers) == 1) {
        return 0;
    }

    ASSERT(key != NULL && keylen != 0);
    struct dyn_token *token = nc_alloc(sizeof(struct dyn_token));
    if (token == NULL) {
        return NULL;
    }
    init_dyn_token(token);

    rstatus_t status = pool->key_hash((char *)key, keylen, token);
    if (status == 0) {
        return NULL;
    }

    return token;
}

static struct peer *
dyn_peer_pool_server(struct server_pool *pool, uint8_t *key, uint32_t keylen)
{
    struct peer *server;
    uint32_t hash, idx;
    struct dyn_token *token;

    ASSERT(array_n(&pool->peers) != 0);
    ASSERT(key != NULL && keylen != 0);

    switch (pool->dist_type) {
    case DIST_KETAMA:
        token = dyn_peer_pool_hash(pool, key, keylen);
        hash = token->mag[0];
        idx = ketama_dispatch(pool->continuum, pool->ncontinuum, hash);
        break;

    case DIST_VNODE:
        token = dyn_peer_pool_hash(pool, key, keylen);
        idx = vnode_dispatch(pool->continuum, pool->ncontinuum, token);
        break;

    case DIST_MODULA:
        token = dyn_peer_pool_hash(pool, key, keylen);
        hash = token->mag[0];
        idx = modula_dispatch(pool->continuum, pool->ncontinuum, hash);
        break;

    case DIST_RANDOM:
        idx = random_dispatch(pool->continuum, pool->ncontinuum, 0);
        break;

    default:
        NOT_REACHED();
        return NULL;
    }
    ASSERT(idx < array_n(&pool->peers));

    server = array_get(&pool->peers, idx);

    log_debug(LOG_VERB, "dyn: key '%.*s' on dist %d maps to server '%.*s'", keylen,
              key, pool->dist_type, server->pname.len, server->pname.data);

    return server;
}

struct conn *
dyn_peer_pool_conn(struct context *ctx, struct server_pool *pool, uint8_t *key,
                 uint32_t keylen)
{
    rstatus_t status;
    struct peer *server;
    struct conn *conn;

    status = dyn_peer_pool_update(pool);
    if (status != NC_OK) {
        return NULL;
    }

    /* from a given {key, keylen} pick a server from pool */
    server = dyn_peer_pool_server(pool, key, keylen);
    if (server == NULL) {
        return NULL;
    }

    /* pick a connection to a given server */
    conn = dyn_peer_conn(server);
    if (conn == NULL) {
        return NULL;
    }

    status = dyn_peer_connect(ctx, server, conn);
    if (status != NC_OK) {
        dyn_peer_close(ctx, conn);
        return NULL;
    }

    return conn;
}

static rstatus_t
dyn_peer_pool_each_preconnect(void *elem, void *data)
{
    rstatus_t status;
    struct server_pool *sp = elem;

    if (!sp->preconnect) {
        return NC_OK;
    }

    status = array_each(&sp->peers, dyn_peer_each_preconnect, NULL);
    if (status != NC_OK) {
        return status;
    }

    return NC_OK;
}

rstatus_t
dyn_peer_pool_preconnect(struct context *ctx)
{
    rstatus_t status;

    status = array_each(&ctx->pool, dyn_peer_pool_each_preconnect, NULL);
    if (status != NC_OK) {
        return status;
    }

    return NC_OK;
}

static rstatus_t
dyn_peer_pool_each_disconnect(void *elem, void *data)
{
    rstatus_t status;
    struct server_pool *sp = elem;

    status = array_each(&sp->peers, dyn_peer_each_disconnect, NULL);
    if (status != NC_OK) {
        return status;
    }

    return NC_OK;
}

void
dyn_peer_pool_disconnect(struct context *ctx)
{
    array_each(&ctx->pool, dyn_peer_pool_each_disconnect, NULL);
}

static rstatus_t
dyn_peer_pool_each_set_owner(void *elem, void *data)
{
    struct server_pool *sp = elem;
    struct context *ctx = data;

    sp->ctx = ctx;

    return NC_OK;
}

rstatus_t
dyn_peer_pool_run(struct server_pool *pool)
{
    ASSERT(array_n(&pool->peers) != 0);

    switch (pool->dist_type) {
    case DIST_KETAMA:
        return ketama_update(pool);

    case DIST_VNODE:
        return vnode_update(pool);

    case DIST_MODULA:
        return modula_update(pool);

    case DIST_RANDOM:
        return random_update(pool);

    default:
        NOT_REACHED();
        return NC_ERROR;
    }

    return NC_OK;
}

static rstatus_t
dyn_peer_pool_each_run(void *elem, void *data)
{
    return dyn_peer_pool_run(elem);
}

rstatus_t
dyn_peer_pool_init(struct array *server_pool, struct array *conf_pool,
                 struct context *ctx)
{
    rstatus_t status;
    uint32_t npool;

    npool = array_n(conf_pool);
    ASSERT(npool != 0);
    ASSERT(array_n(server_pool) == 0);

    status = array_init(server_pool, npool, sizeof(struct server_pool));
    if (status != NC_OK) {
        return status;
    }

    /* transform conf pool to server pool */
    status = array_each(conf_pool, conf_pool_each_transform, server_pool);
    if (status != NC_OK) {
        dyn_peer_pool_deinit(server_pool);
        return status;
    }
    ASSERT(array_n(server_pool) == npool);

    /* set ctx as the server pool owner */
    status = array_each(server_pool, dyn_peer_pool_each_set_owner, ctx);
    if (status != NC_OK) {
        dyn_peer_pool_deinit(server_pool);
        return status;
    }

    /* update server pool continuum */
    status = array_each(server_pool, dyn_peer_pool_each_run, NULL);
    if (status != NC_OK) {
        dyn_peer_pool_deinit(server_pool);
        return status;
    }

    log_debug(LOG_DEBUG, "dyn: init %"PRIu32" peer pools", npool);

    return NC_OK;
}

void
dyn_peer_pool_deinit(struct array *server_pool)
{
    uint32_t i, npool;

    for (i = 0, npool = array_n(server_pool); i < npool; i++) {
        struct server_pool *sp;

        sp = array_pop(server_pool);
        ASSERT(sp->p_conn == NULL);
        //fixe me to use different variables
        ASSERT(TAILQ_EMPTY(&sp->c_conn_q) && sp->nc_conn_q == 0);

        if (sp->continuum != NULL) {
            nc_free(sp->continuum);
            sp->ncontinuum = 0;
            sp->nserver_continuum = 0;
            sp->nlive_server = 0;
        }

        dyn_peer_deinit(&sp->peers);

        log_debug(LOG_DEBUG, "dyn: deinit peer pool %"PRIu32" '%.*s'", sp->idx,
                  sp->name.len, sp->name.data);
    }

    //array_deinit(server_pool);

    log_debug(LOG_DEBUG, "deinit %"PRIu32" peer pools", npool);
}
