/*
 * Dynomite - A thin, distributed replication layer for multi non-distributed storages.
 * Copyright (C) 2014 Netflix, Inc.
 */ 

#include <dyn_core.h>
#include <dyn_dnode_peer.h>

struct msg *
dyn_rsp_get(struct conn *conn)
{
    struct msg *msg;

    ASSERT(!conn->dyn_client && !conn->dnode);

    msg = msg_get(conn, false, conn->redis);
    if (msg == NULL) {
        conn->err = errno;
    }

    return msg;
}

void
dyn_rsp_put(struct msg *msg)
{
    ASSERT(!msg->request);
    ASSERT(msg->peer == NULL);
    msg_put(msg);
}


struct msg *
dyn_rsp_recv_next(struct context *ctx, struct conn *conn, bool alloc)
{
    ASSERT(!conn->dyn_client && !conn->dnode);
    return rsp_recv_next(ctx, conn, alloc);
}

static bool
dyn_rsp_filter(struct context *ctx, struct conn *conn, struct msg *msg)
{
    struct msg *pmsg;

    ASSERT(!conn->dyn_client && !conn->dnode);

    if (msg_empty(msg)) {
        ASSERT(conn->rmsg == NULL);
        log_debug(LOG_VERB, "dyn: filter empty rsp %"PRIu64" on s %d", msg->id,
                  conn->sd);
        dyn_rsp_put(msg);
        return true;
    }

    pmsg = TAILQ_FIRST(&conn->omsg_q);
    if (pmsg == NULL) {
        log_debug(LOG_ERR, "dyn: filter stray rsp %"PRIu64" len %"PRIu32" on s %d",
                  msg->id, msg->mlen, conn->sd);
        dyn_rsp_put(msg);
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

        dyn_rsp_put(msg);
        req_put(pmsg);
        return true;
    }

    return false;
}

static void
dyn_rsp_forward_stats(struct context *ctx, struct server *server, struct msg *msg)
{
    ASSERT(!msg->request);

    /* stats_server_incr(ctx, server, responses); */
    /* stats_server_incr_by(ctx, server, response_bytes, msg->mlen); */
}

static void
dyn_rsp_forward(struct context *ctx, struct conn *s_conn, struct msg *msg)
{
    rstatus_t status;
    struct msg *pmsg;
    struct conn *c_conn;

    ASSERT(!s_conn->dyn_client && !s_conn->dnode);

    /* response from server implies that server is ok and heartbeating */
    dyn_peer_ok(ctx, s_conn);

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

    if (TAILQ_FIRST(&c_conn->omsg_q) != NULL && dyn_req_done(c_conn, TAILQ_FIRST(&c_conn->omsg_q))) {
        status = event_add_out(ctx->evb, c_conn);
        if (status != NC_OK) {
            c_conn->err = errno;
        }
    }

    dyn_rsp_forward_stats(ctx, s_conn->owner, msg);
}

void
dyn_rsp_recv_done(struct context *ctx, struct conn *conn, struct msg *msg,
              struct msg *nmsg)
{
    ASSERT(!conn->dyn_client && !conn->dnode);
    ASSERT(msg != NULL && conn->rmsg == msg);
    ASSERT(!msg->request);
    ASSERT(msg->owner == conn);
    ASSERT(nmsg == NULL || !nmsg->request);

    /* enqueue next message (response), if any */
    conn->rmsg = nmsg;

    if (dyn_rsp_filter(ctx, conn, msg)) {
        return;
    }

    dyn_rsp_forward(ctx, conn, msg);
}

struct msg *
dyn_rsp_send_next(struct context *ctx, struct conn *conn)
{
    ASSERT(!conn->dyn_client && !conn->dnode);
    return rsp_send_next(ctx, conn);
}

void
dyn_rsp_send_done(struct context *ctx, struct conn *conn, struct msg *msg)
{
    struct msg *pmsg; /* peer message (request) */

    ASSERT(conn->dyn_client && !conn->dnode);
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

