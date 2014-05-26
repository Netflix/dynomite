/*
 * Dynomite - A thin, distributed replication layer for multi non-distributed storages.
 * Copyright (C) 2014 Netflix, Inc.
 */ 

#include "dyn_core.h"
#include "dyn_server.h"
#include "dyn_dnode_peer.h"

struct msg *
dnode_req_get(struct conn *conn)
{
    struct msg *msg;

    ASSERT(conn->dnode_client && !conn->dnode_server);

    msg = msg_get(conn, true, conn->redis);
    if (msg == NULL) {
        conn->err = errno;
    }

    return msg;
}

void
dnode_req_put(struct msg *msg)
{
	req_put(msg);
}


bool
dnode_req_done(struct conn *conn, struct msg *msg)
{
    ASSERT(conn->dnode_client && !conn->dnode_server);
    return req_done(conn, msg);
}


bool
dnode_req_error(struct conn *conn, struct msg *msg)
{
    ASSERT(msg->request && dnode_req_done(conn, msg));
    return req_error(conn, msg);
}

void
dnode_req_peer_enqueue_imsgq(struct context *ctx, struct conn *conn, struct msg *msg)
{
    ASSERT(msg->request);
    ASSERT(!conn->dnode_client && !conn->dnode_server);
    
    req_server_enqueue_imsgq(ctx, conn, msg);
}

void
dnode_req_peer_dequeue_imsgq(struct context *ctx, struct conn *conn, struct msg *msg)
{
    ASSERT(msg->request);
    ASSERT(!conn->dnode_client && !conn->dnode_server);

    TAILQ_REMOVE(&conn->imsg_q, msg, s_tqe);

    //fixme for peer stats    
    /* stats_server_decr(ctx, conn->owner, in_queue); */
    /* stats_server_decr_by(ctx, conn->owner, in_queue_bytes, msg->mlen); */
}

void
dnode_req_client_enqueue_omsgq(struct context *ctx, struct conn *conn, struct msg *msg)
{
    ASSERT(msg->request);
    ASSERT(conn->dnode_client && !conn->dnode_server);

    TAILQ_INSERT_TAIL(&conn->omsg_q, msg, c_tqe);
}

void
dnode_req_peer_enqueue_omsgq(struct context *ctx, struct conn *conn, struct msg *msg)
{
    ASSERT(msg->request);
    ASSERT(!conn->dnode_client && !conn->dnode_server);

    TAILQ_INSERT_TAIL(&conn->omsg_q, msg, s_tqe);

    //fixme for peer stats
    /* stats_server_incr(ctx, conn->owner, out_queue); */
    /* stats_server_incr_by(ctx, conn->owner, out_queue_bytes, msg->mlen); */
}

void
dnode_req_client_dequeue_omsgq(struct context *ctx, struct conn *conn, struct msg *msg)
{
    ASSERT(msg->request);
    ASSERT(conn->dnode_client && !conn->dnode_server);

    TAILQ_REMOVE(&conn->omsg_q, msg, c_tqe);
}

void
dnode_req_peer_dequeue_omsgq(struct context *ctx, struct conn *conn, struct msg *msg)
{
    ASSERT(msg->request);
    ASSERT(!conn->dnode_client && !conn->dnode_server);

    msg_tmo_delete(msg);

    TAILQ_REMOVE(&conn->omsg_q, msg, s_tqe);

    //fixme for peer stats
    /* stats_server_decr(ctx, conn->owner, out_queue); */
    /* stats_server_decr_by(ctx, conn->owner, out_queue_bytes, msg->mlen); */
}

struct msg *
dnode_req_recv_next(struct context *ctx, struct conn *conn, bool alloc)
{
    struct msg *msg;

    ASSERT(conn->dnode_client && !conn->dnode_server);
    return req_recv_next(ctx, conn, alloc);
}


static void
dnode_req_gos_forward(struct context *ctx, struct conn *dc_conn, struct msg *msg)
{
    rstatus_t status;
    struct msg *pmsg;

    ASSERT(dc_conn->dnode_client && !dc_conn->dnode_server);

    //add messsage
    struct mbuf *nbuf = mbuf_get();
     if (nbuf == NULL) {
         loga("Error happened in calling mbuf_get");
         return;  //TODOs: need to address this further
     }

     dc_conn->enqueue_outq(ctx, dc_conn, msg);
     msg->done = 1;

     //TODOs: need to free the old msg object
     pmsg = msg_get(dc_conn, 1, 0);
     if (pmsg == NULL) {
         mbuf_put(nbuf);
         return;
     }

     //dyn message's meta data
     uint64_t msg_id = 1234;
     uint8_t type = GOSSIP_PING_REPLY;
     uint8_t version = 1;
     struct string data = string("PingReply");

     dmsg_write(nbuf, msg_id, type, version, &data);
     mbuf_insert(&pmsg->mhdr, nbuf);

     //should we do this?
     //s_conn->dequeue_outq(ctx, s_conn, pmsg);
     //pmsg->done = 1;

     /* establish msg <-> pmsg (response <-> request) link */
     msg->peer = pmsg;
     pmsg->peer = msg;

     //pmsg->pre_coalesce(pmsg);


    if (dnode_req_done(dc_conn, msg)) {
       status = event_add_out(ctx->evb, dc_conn);
       if (status != NC_OK) {
          dc_conn->err = errno;
       }
    }

    //dc_conn->enqueue_outq(ctx, dc_conn, pmsg);

    //dnode_rsp_forward_stats(ctx, s_conn->owner, msg);
}

static bool
dnode_req_filter(struct context *ctx, struct conn *conn, struct msg *msg)
{
    ASSERT(conn->dnode_client && !conn->dnode_server);

    if (msg_empty(msg)) {
        ASSERT(conn->rmsg == NULL);
        log_debug(LOG_VERB, "dyn: filter empty req %"PRIu64" from c %d", msg->id,
                  conn->sd);
        dnode_req_put(msg);
        return true;
    }


    /* dynomite hanlder */
    if (msg->dmsg != NULL) {
       if (dmsg_process(ctx, conn, msg->dmsg)) {
           //forward request
           dnode_req_gos_forward(ctx, conn, msg);
          return true;
       }
    }

    /*
     * Handle "quit\r\n", which is the protocol way of doing a
     * passive close
     */
    if (msg->quit) {
        ASSERT(conn->rmsg == NULL);
        log_debug(LOG_INFO, "dyn: filter quit req %"PRIu64" from c %d", msg->id,
                  conn->sd);
        conn->eof = 1;
        conn->recv_ready = 0;
        dnode_req_put(msg);
        return true;
    }

    return false;
}

static void
dnode_req_forward_error(struct context *ctx, struct conn *conn, struct msg *msg)
{
    rstatus_t status;

    ASSERT(conn->dnode_client && !conn->dnode_server);

    log_debug(LOG_INFO, "dyn: forward req %"PRIu64" len %"PRIu32" type %d from "
              "c %d failed: %s", msg->id, msg->mlen, msg->type, conn->sd,
              strerror(errno));

    msg->done = 1;
    msg->error = 1;
    msg->err = errno;

    /* noreply request don't expect any response */
    if (msg->noreply) {
        dnode_req_put(msg);
        return;
    }

    if (dnode_req_done(conn, TAILQ_FIRST(&conn->omsg_q))) {
        status = event_add_out(ctx->evb, conn);
        if (status != NC_OK) {
            conn->err = errno;
        }
    }
}

static void
dnode_req_forward_stats(struct context *ctx, struct server *server, struct msg *msg)
{
    ASSERT(msg->request);
     
    //fix me
    //stats_server_incr(ctx, server, requests);
    //stats_server_incr_by(ctx, server, request_bytes, msg->mlen);
}


static void
dnode_req_forward(struct context *ctx, struct conn *conn, struct msg *msg)
{
    struct server_pool *pool;
    uint8_t *key;
    uint32_t keylen;

    ASSERT(conn->dnode_client && !conn->dnode_server);

    pool = conn->owner;
    key = NULL;
    keylen = 0;

    if (!string_empty(&pool->hash_tag)) {
        struct string *tag = &pool->hash_tag;
        uint8_t *tag_start, *tag_end;

        tag_start = nc_strchr(msg->key_start, msg->key_end, tag->data[0]);
        if (tag_start != NULL) {
            tag_end = nc_strchr(tag_start + 1, msg->key_end, tag->data[1]);
            if (tag_end != NULL) {
                key = tag_start + 1;
                keylen = (uint32_t)(tag_end - key);
            }
        }
    }

    if (keylen == 0) {
        key = msg->key_start;
        keylen = (uint32_t)(msg->key_end - msg->key_start);
    }

    local_req_forward(ctx, conn, msg, key, keylen);
}


void
dnode_req_recv_done(struct context *ctx, struct conn *conn, struct msg *msg,
              struct msg *nmsg)
{
    ASSERT(conn->dnode_client && !conn->dnode_server);
    ASSERT(msg->request);
    ASSERT(msg->owner == conn);
    ASSERT(conn->rmsg == msg);
    ASSERT(nmsg == NULL || nmsg->request);

    /* enqueue next message (request), if any */
    conn->rmsg = nmsg;

    if (dnode_req_filter(ctx, conn, msg)) {
        return;
    }

    dnode_req_forward(ctx, conn, msg);
}

struct msg *
dnode_req_send_next(struct context *ctx, struct conn *conn)
{
    rstatus_t status;
    struct msg *msg, *nmsg; /* current and next message */

    ASSERT(!conn->dnode_client && !conn->dnode_server);

    return req_send_next(ctx, conn);
}

void
dnode_req_send_done(struct context *ctx, struct conn *conn, struct msg *msg)
{
    ASSERT(!conn->dnode_client && !conn->dnode_server);
    req_send_done(ctx, conn, msg);
}

