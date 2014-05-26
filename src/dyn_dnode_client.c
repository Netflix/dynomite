/*
 * Dynomite - A thin, distributed replication layer for multi non-distributed storages.
 * Copyright (C) 2014 Netflix, Inc.
 */ 

#include "dyn_core.h"
#include "dyn_server.h"
#include "dyn_dnode_client.h"


void
dnode_client_ref(struct conn *conn, void *owner)
{
    struct server_pool *pool = owner;

    ASSERT(conn->dnode_client && !conn->dnode_server);
    ASSERT(conn->owner == NULL);

    /*
     * We use null pointer as the sockaddr argument in the accept() call as
     * we are not interested in the address of the peer for the accepted
     * connection
     */
    conn->family = 0;
    conn->addrlen = 0;
    conn->addr = NULL;

    pool->nc_conn_q++;
    TAILQ_INSERT_TAIL(&pool->c_conn_q, conn, conn_tqe);

    /* owner of the client connection is the server pool */
    conn->owner = owner;

    log_debug(LOG_VVERB, "dyn: ref conn %p owner %p into pool '%.*s'", conn, pool,
              pool->name.len, pool->name.data);
}

void
dnode_client_unref(struct conn *conn)
{
    struct server_pool *pool;

    ASSERT(conn->dnode_client && !conn->dnode_server);
    ASSERT(conn->owner != NULL);

    pool = conn->owner;
    conn->owner = NULL;

    ASSERT(pool->nc_conn_q != 0);
    pool->nc_conn_q--;
    TAILQ_REMOVE(&pool->c_conn_q, conn, conn_tqe);

    log_debug(LOG_VVERB, "dyn: unref conn %p owner %p from pool '%.*s'", conn,
              pool, pool->name.len, pool->name.data);
}

bool
dnode_client_active(struct conn *conn)
{
    ASSERT(conn->dnode_client && !conn->dnode_server);

    ASSERT(TAILQ_EMPTY(&conn->imsg_q));

    if (!TAILQ_EMPTY(&conn->omsg_q)) {
        log_debug(LOG_VVERB, "dyn: c %d is active", conn->sd);
        return true;
    }

    if (conn->rmsg != NULL) {
        log_debug(LOG_VVERB, "dyn: c %d is active", conn->sd);
        return true;
    }

    if (conn->smsg != NULL) {
        log_debug(LOG_VVERB, "dyn: c %d is active", conn->sd);
        return true;
    }

    log_debug(LOG_VVERB, "dyn: c %d is inactive", conn->sd);

    return false;
}

static void
dnode_client_close_stats(struct context *ctx, struct server_pool *pool, err_t err,
                   unsigned eof)
{
    //fix this for dnode_client_connections
    //stats_pool_decr(ctx, pool, client_connections);

    if (eof) {
        //fix this also
        //stats_pool_incr(ctx, pool, client_eof);
        return;
    }

    switch (err) {
    case EPIPE:
    case ETIMEDOUT:
    case ECONNRESET:
    case ECONNABORTED:
    case ENOTCONN:
    case ENETDOWN:
    case ENETUNREACH:
    case EHOSTDOWN:
    case EHOSTUNREACH:
    default:
        //fix this also
        //stats_pool_incr(ctx, pool, client_err);
        break;
    }
}

void
dnode_client_close(struct context *ctx, struct conn *conn)
{
    rstatus_t status;
    struct msg *msg, *nmsg; /* current and next message */

    ASSERT(conn->dnode_client && !conn->dnode_server);

    dnode_client_close_stats(ctx, conn->owner, conn->err, conn->eof);

    if (conn->sd < 0) {
        conn->unref(conn);
        conn_put(conn);
        return;
    }

    msg = conn->rmsg;
    if (msg != NULL) {
        conn->rmsg = NULL;

        ASSERT(msg->peer == NULL);
        ASSERT(msg->request && !msg->done);

        log_debug(LOG_INFO, "dyn: close c %d discarding pending req %"PRIu64" len "
                  "%"PRIu32" type %d", conn->sd, msg->id, msg->mlen,
                  msg->type);

        req_put(msg);
    }

    ASSERT(conn->smsg == NULL);
    ASSERT(TAILQ_EMPTY(&conn->imsg_q));

    for (msg = TAILQ_FIRST(&conn->omsg_q); msg != NULL; msg = nmsg) {
        nmsg = TAILQ_NEXT(msg, c_tqe);

        /* dequeue the message (request) from client outq */
        conn->dequeue_outq(ctx, conn, msg);

        if (msg->done) {
            log_debug(LOG_INFO, "dyn: close c %d discarding %s req %"PRIu64" len "
                      "%"PRIu32" type %d", conn->sd,
                      msg->error ? "error": "completed", msg->id, msg->mlen,
                      msg->type);
            req_put(msg);
        } else {
            msg->swallow = 1;

            ASSERT(msg->request);
            ASSERT(msg->peer == NULL);

            log_debug(LOG_INFO, "dyn: close c %d schedule swallow of req %"PRIu64" "
                      "len %"PRIu32" type %d", conn->sd, msg->id, msg->mlen,
                      msg->type);
        }
    }
    ASSERT(TAILQ_EMPTY(&conn->omsg_q));

    conn->unref(conn);

    status = close(conn->sd);
    if (status < 0) {
        log_error("dyn: close c %d failed, ignored: %s", conn->sd, strerror(errno));
    }
    conn->sd = -1;

    conn_put(conn);
}
