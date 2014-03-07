/*
 * Dynomite - A thin, distributed replication layer for multi non-distributed storages.
 * Copyright (C) 2014 Netflix, Inc.
 */ 

#include <dyn_core.h>
#include <dyn_dnode_peer.h>


struct msg *
dnode_rsp_get(struct conn *conn)
{
    struct msg *msg;

    ASSERT(!conn->dnode_client && !conn->dnode_server);

    msg = msg_get(conn, false, conn->redis);
    if (msg == NULL) {
        conn->err = errno;
    }

    return msg;
}

void
dnode_rsp_put(struct msg *msg)
{
    ASSERT(!msg->request);
    ASSERT(msg->peer == NULL);
    msg_put(msg);
}


struct msg *
dnode_rsp_recv_next(struct context *ctx, struct conn *conn, bool alloc)
{
    ASSERT(!conn->dnode_client && !conn->dnode_server);
    return rsp_recv_next(ctx, conn, alloc);
}

static bool
dnode_rsp_filter(struct context *ctx, struct conn *conn, struct msg *msg)
{
    struct msg *pmsg;

    ASSERT(!conn->dnode_client && !conn->dnode_server);

    if (msg_empty(msg)) {
        ASSERT(conn->rmsg == NULL);
        log_debug(LOG_VERB, "dyn: filter empty rsp %"PRIu64" on s %d", msg->id,
                  conn->sd);
        dnode_rsp_put(msg);
        return true;
    }

    pmsg = TAILQ_FIRST(&conn->omsg_q);
    if (pmsg == NULL) {
        log_debug(LOG_ERR, "dyn: filter stray rsp %"PRIu64" len %"PRIu32" on s %d",
                  msg->id, msg->mlen, conn->sd);
        dnode_rsp_put(msg);
        return true;
    }
    ASSERT(pmsg->peer == NULL);
    ASSERT(pmsg->request && !pmsg->done);

    if (pmsg->swallow) {
        conn->dequeue_outq(ctx, conn, pmsg);
        pmsg->done = 1;

        log_debug(LOG_INFO, "dyn: swallow rsp %"PRIu64" len %"PRIu32" of req "
                  "%"PRIu64" on s %d", msg->id, msg->mlen, pmsg->id,
                  conn->sd);

        dnode_rsp_put(msg);
        req_put(pmsg);
        return true;
    }

    return false;
}

static void
dnode_rsp_forward_stats(struct context *ctx, struct server *server, struct msg *msg)
{
    ASSERT(!msg->request);

    //fixme for dnode  stats    
    /* stats_server_incr(ctx, server, responses); */
    /* stats_server_incr_by(ctx, server, response_bytes, msg->mlen); */
}

static void
dnode_rsp_forward(struct context *ctx, struct conn *s_conn, struct msg *msg)
{
    rstatus_t status;
    struct msg *pmsg;
    struct conn *c_conn;

    ASSERT(!s_conn->dnode_client && !s_conn->dnode_server);

    /* response from server implies that server is ok and heartbeating */
    dnode_peer_ok(ctx, s_conn);

    /* dequeue peer message (request) from server */
    pmsg = TAILQ_FIRST(&s_conn->omsg_q);
    ASSERT(pmsg != NULL && pmsg->peer == NULL);
    ASSERT(pmsg->request && !pmsg->done);

    s_conn->dequeue_outq(ctx, s_conn, pmsg);
    pmsg->done = 1;

    /* establish msg <-> pmsg (response <-> request) link */
    pmsg->peer = msg;
    msg->peer = pmsg;

    msg->pre_coalesce(msg);

    c_conn = pmsg->owner;
    ASSERT(c_conn->client && !c_conn->proxy);

    if (TAILQ_FIRST(&c_conn->omsg_q) != NULL && dnode_req_done(c_conn, TAILQ_FIRST(&c_conn->omsg_q))) {
        status = event_add_out(ctx->evb, c_conn);
        if (status != NC_OK) {
            c_conn->err = errno;
        }
    }

    dnode_rsp_forward_stats(ctx, s_conn->owner, msg);
}

void
dnode_rsp_recv_done(struct context *ctx, struct conn *conn, struct msg *msg,
              struct msg *nmsg)
{
    ASSERT(!conn->dnode_client && !conn->dnode_server);
    ASSERT(msg != NULL && conn->rmsg == msg);
    ASSERT(!msg->request);
    ASSERT(msg->owner == conn);
    ASSERT(nmsg == NULL || !nmsg->request);

    /* enqueue next message (response), if any */
    conn->rmsg = nmsg;

    if (dnode_rsp_filter(ctx, conn, msg)) {
        return;
    }

    dnode_rsp_forward(ctx, conn, msg);
}

struct msg *
dnode_rsp_send_next(struct context *ctx, struct conn *conn)
{
    ASSERT(!conn->dnode_client && !conn->dnode_server);
    return rsp_send_next(ctx, conn);
}

void
dnode_rsp_send_done(struct context *ctx, struct conn *conn, struct msg *msg)
{
    struct msg *pmsg; /* peer message (request) */

    ASSERT(conn->dnode_client && !conn->dnode_server);
    ASSERT(conn->smsg == NULL);

    log_debug(LOG_VVERB, "dyn: send done rsp %"PRIu64" on c %d", msg->id, conn->sd);

    pmsg = msg->peer;

    ASSERT(!msg->request && pmsg->request);
    ASSERT(pmsg->peer == msg);
    ASSERT(pmsg->done && !pmsg->swallow);

    /* dequeue request from client outq */
    conn->dequeue_outq(ctx, conn, pmsg);

    req_put(pmsg);
}

