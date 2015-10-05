/*
 * Dynomite - A thin, distributed replication layer for multi non-distributed storages.
 * Copyright (C) 2014 Netflix, Inc.
 */ 

#include "dyn_core.h"
#include "dyn_dnode_peer.h"


struct msg *
dnode_rsp_get(struct conn *conn)
{
    struct msg *msg;

    ASSERT(conn->type == CONN_DNODE_PEER_SERVER);

    msg = msg_get(conn, false, conn->data_store);
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
    ASSERT(conn->type == CONN_DNODE_PEER_SERVER);

    conn->last_received = time(NULL);

    return rsp_recv_next(ctx, conn, alloc);
}

static bool
dnode_rsp_filter(struct context *ctx, struct conn *conn, struct msg *msg)
{
    struct msg *pmsg;

    ASSERT(conn->type == CONN_DNODE_PEER_SERVER);

    if (msg_empty(msg)) {
        ASSERT(conn->rmsg == NULL);
        log_debug(LOG_VERB, "dyn: filter empty rsp %"PRIu64" on s %d", msg->id,
                conn->sd);
        dnode_rsp_put(msg);
        return true;
    }

    pmsg = TAILQ_FIRST(&conn->omsg_q);
    if (pmsg == NULL) {
        log_debug(LOG_INFO, "dyn: filter stray rsp %"PRIu64" len %"PRIu32" on s %d noreply %d",
                msg->id, msg->mlen, conn->sd, msg->noreply);
        dnode_rsp_put(msg);
        return true;
    }
    ASSERT(pmsg->peer == NULL);
    ASSERT(pmsg->request && !pmsg->done);

    return false;
}

static void
dnode_rsp_forward_stats(struct context *ctx, struct server *server, struct msg *msg)
{
    ASSERT(!msg->request);
    stats_pool_incr(ctx, server->owner, peer_responses);
    stats_pool_incr_by(ctx, server->owner, peer_response_bytes, msg->mlen);
}

static void
dnode_rsp_swallow(struct context *ctx, struct conn *peer_conn,
                  struct msg *req, struct msg *rsp)
{
    conn_dequeue_outq(ctx, peer_conn, req);
    req->done = 1;
    log_debug(LOG_VERB, "conn %p swallow %p", peer_conn, req);
    if (rsp) {
        log_debug(LOG_INFO, "dyn: swallow rsp %"PRIu64" len %"PRIu32" of req "
                  "%"PRIu64" on s %d", rsp->id, rsp->mlen, req->id,
                  peer_conn->sd);
        dnode_rsp_put(rsp);
    }
    req_put(req);
}

/* Description: link data from a peer connection to a client-facing connection
 * peer_conn: a peer connection
 * msg      : msg with data from the peer connection after parsing
 */
static void
dnode_rsp_forward_match(struct context *ctx, struct conn *peer_conn, struct msg *rsp)
{
    rstatus_t status;
    struct msg *req;
    struct conn *c_conn;

    req = TAILQ_FIRST(&peer_conn->omsg_q);
    c_conn = req->owner;

    /* if client consistency is dc_one forward the response from only the
       local node. Since dyn_dnode_peer is always a remote node, drop the rsp */
    if (req->consistency == DC_ONE) {
        if (req->swallow) {
            dnode_rsp_swallow(ctx, peer_conn, req, rsp);
            return;
        }
        log_warn("req %d:%d with DC_ONE consistency is not being swallowed");
    }

    /* if client consistency is dc_quorum, forward the response from only the
       local region/DC. */
    if ((req->consistency == DC_QUORUM) && !peer_conn->same_dc) {
        if (req->swallow) {
            dnode_rsp_swallow(ctx, peer_conn, req, rsp);
            return;
        }
        log_warn("req %d:%d with DC_QUORUM consistency is not being swallowed");
    }

    log_debug(LOG_DEBUG, "DNODE RSP RECEIVED %s %d dmsg->id %u req %u:%u rsp %u:%u, ",
              conn_get_type_string(peer_conn),
              peer_conn->sd, rsp->dmsg->id,
              req->id, req->parent_id, rsp->id, rsp->parent_id);
    ASSERT(req != NULL && req->peer == NULL);
    ASSERT(req->request && !req->done);

    if (log_loggable(LOG_VVERB)) {
        loga("Dumping content for response:   ");
        msg_dump(rsp);

        loga("rsp id %d", rsp->id);

        loga("Dumping content for request:");
        msg_dump(req);

        loga("req id %d", req->id);
    }

    conn_dequeue_outq(ctx, peer_conn, req);
    req->done = 1;

    log_debug(LOG_VERB, "%p <-> %p", req, rsp);
    /* establish rsp <-> req (response <-> request) link */
    req->peer = rsp;
    rsp->peer = req;

    rsp->pre_coalesce(rsp);

    ASSERT_LOG((c_conn->type == CONN_CLIENT) ||
               (c_conn->type == CONN_DNODE_PEER_CLIENT),
               "c_conn type %s", conn_get_type_string(c_conn));

    dnode_rsp_forward_stats(ctx, peer_conn->owner, rsp);
    if (TAILQ_FIRST(&c_conn->omsg_q) != NULL && req_done(c_conn, req)) {
        log_debug(LOG_INFO, "handle rsp %d:%d for req %d:%d conn %p",
                rsp->id, rsp->parent_id, req->id, req->parent_id, c_conn);
        // c_conn owns respnse now
        rstatus_t status = conn_handle_response(c_conn,
                req->parent_id ? req->parent_id : req->id,
                rsp);
        if (req->swallow) {
            log_debug(LOG_INFO, "swallow request %d:%d", req->id, req->parent_id);
            req_put(req);
        }
    }
}

/* There are chances that the request to the remote peer or its response got dropped.
 * Hence we may not always receive a response to the request at the head of the FIFO.
 * Hence what we do is we mark that request as errored and move on the next one
 * in the outgoing queue. This works since we always have message ids in monotonically
 * increasing order.
 */
static void
dnode_rsp_forward(struct context *ctx, struct conn *peer_conn, struct msg *rsp)
{
    rstatus_t status;
    struct msg *req;
    struct conn *c_conn;

    ASSERT(peer_conn->type == CONN_DNODE_PEER_SERVER);

    /* response from a peer implies that peer is ok and heartbeating */
    dnode_peer_ok(ctx, peer_conn);

    /* dequeue peer message (request) from peer conn */
    while (true) {
        req = TAILQ_FIRST(&peer_conn->omsg_q);
        log_debug(LOG_VERB, "dnode_rsp_forward entering req %p rsp %p...", req, rsp);
        c_conn = req->owner;
        if (req->id == rsp->dmsg->id) {
            dnode_rsp_forward_match(ctx, peer_conn, rsp);
            return;
        }
        // Report a mismatch and try to rectify
        log_error("MISMATCH: dnode %s %d rsp_dmsg_id %u req %u:%u dnode rsp %u:%u",
                  conn_get_type_string(peer_conn),
                  peer_conn->sd, rsp->dmsg->id, req->id, req->parent_id, rsp->id,
                  rsp->parent_id);
        if (c_conn && conn_to_ctx(c_conn))
            stats_pool_incr(conn_to_ctx(c_conn), c_conn->owner,
                    peer_mismatch_requests);

        // TODO : should you be worried about message id getting wrapped around to 0?
        if (rsp->dmsg->id < req->id) {
            // We received a response from the past. This indeed proves out of order
            // responses. A blunder to the architecture. Log it and drop the response.
            log_error("MISMATCH: received response from the past. Dropping it");
            dnode_rsp_put(rsp);
            return;
        }

        if (req->consistency == DC_ONE) {
            if (req->swallow) {
                // swallow the request and move on the next one
                dnode_rsp_swallow(ctx, peer_conn, req, NULL);
                continue;
            }
            log_warn("req %d:%d with DC_ONE consistency is not being swallowed");
        }

        if ((req->consistency == DC_QUORUM) && !peer_conn->same_dc) {
            if (req->swallow) {
                // swallow the request and move on the next one
                dnode_rsp_swallow(ctx, peer_conn, req, NULL);
                continue;
            }
            log_warn("req %d:%d with DC_QUORUM consistency is not being swallowed");
        }

        log_error("MISMATCHED DNODE RSP RECEIVED %s %d dmsg->id %u req %u:%u rsp %u:%u, skipping....",
                  conn_get_type_string(peer_conn),
                 peer_conn->sd, rsp->dmsg->id,
                 req->id, req->parent_id, rsp->id, rsp->parent_id);
        ASSERT(req != NULL && req->peer == NULL);
        ASSERT(req->request && !req->done);

        if (log_loggable(LOG_VVERB)) {
            loga("skipping req:   ");
            msg_dump(req);
        }


        conn_dequeue_outq(ctx, peer_conn, req);
        req->done = 1;

        // Create an appropriate response for the request so its propagated up;
        struct msg *err_rsp = msg_get(peer_conn, false, peer_conn->data_store);
        err_rsp->error = req->error = 1;
        err_rsp->err = req->err = BAD_FORMAT;
        err_rsp->dyn_error = req->dyn_error = BAD_FORMAT;
        err_rsp->dmsg = dmsg_get();
        err_rsp->dmsg->id = req->id;
        log_debug(LOG_VERB, "%p <-> %p", req, err_rsp);
        /* establish err_rsp <-> req (response <-> request) link */
        req->peer = err_rsp;
        err_rsp->peer = req;

        log_error("Peer connection s %d skipping request %u:%u, dummy err_rsp %u:%u",
                 peer_conn->sd, req->id, req->parent_id, err_rsp->id, err_rsp->parent_id);
        rstatus_t status =
            conn_handle_response(c_conn, req->parent_id ? req->parent_id : req->id,
                                err_rsp);
        IGNORE_RET_VAL(status);
        if (req->swallow) {
                log_debug(LOG_INFO, "swallow request %d:%d", req->id, req->parent_id);
            req_put(req);
        }
    }
}



//TODOs: fix this in using dmsg_write with encrypted msgs
//         It is not in use now.
/*
void
dnode_rsp_gos_syn(struct context *ctx, struct conn *p_conn, struct msg *msg)
{
    rstatus_t status;
    struct msg *pmsg;

    //ASSERT(p_conn->type == CONN_DNODE_PEER_CLIENT);

    //add messsage
    struct mbuf *nbuf = mbuf_get();
    if (nbuf == NULL) {
        log_debug(LOG_ERR, "Error happened in calling mbuf_get");
        return;  //TODOs: need to address this further
    }

    msg->done = 1;

    //TODOs: need to free the old msg object
    pmsg = msg_get(p_conn, 0, msg->redis);
    if (pmsg == NULL) {
        mbuf_put(nbuf);
        return;
    }

    pmsg->done = 1;
    // establish msg <-> pmsg (response <-> request) link
    msg->peer = pmsg;
    pmsg->peer = msg;
    pmsg->pre_coalesce(pmsg);
    pmsg->owner = p_conn;

    //dyn message's meta data
    uint64_t msg_id = msg->dmsg->id;
    uint8_t type = GOSSIP_SYN_REPLY;
    struct string data = string("SYN_REPLY_OK");

    dmsg_write(nbuf, msg_id, type, p_conn, 0);
    mbuf_insert(&pmsg->mhdr, nbuf);

    //dnode_rsp_recv_done(ctx, p_conn, msg, pmsg);
    //should we do this?
    //conn_dequeue_outq(ctx, s_conn, pmsg);



     //p_conn->enqueue_outq(ctx, p_conn, pmsg);
     //if (TAILQ_FIRST(&p_conn->omsg_q) != NULL && req_done(p_conn, TAILQ_FIRST(&p_conn->omsg_q))) {
     //   status = event_add_out(ctx->evb, p_conn);
     //   if (status != DN_OK) {
     //      p_conn->err = errno;
     //   }
     //}


    if (TAILQ_FIRST(&p_conn->omsg_q) != NULL && req_done(p_conn, TAILQ_FIRST(&p_conn->omsg_q))) {
        status = event_add_out(ctx->evb, p_conn);
        if (status != DN_OK) {
            p_conn->err = errno;
        }
    }

    //dnode_rsp_forward_stats(ctx, s_conn->owner, msg);
}

*/

void
dnode_rsp_recv_done(struct context *ctx, struct conn *conn,
                    struct msg *msg, struct msg *nmsg)
{
    log_debug(LOG_VERB, "dnode_rsp_recv_done entering ...");

    ASSERT(conn->type == CONN_DNODE_PEER_SERVER);
    ASSERT(msg != NULL && conn->rmsg == msg);
    ASSERT(!msg->request);
    ASSERT(msg->owner == conn);
    ASSERT(nmsg == NULL || !nmsg->request);

    if (log_loggable(LOG_VVERB)) {
       loga("Dumping content for msg:   ");
       msg_dump(msg);

       if (nmsg != NULL) {
          loga("Dumping content for nmsg :");
          msg_dump(nmsg);
       }
    }

    /* enqueue next message (response), if any */
    conn->rmsg = nmsg;

    if (dnode_rsp_filter(ctx, conn, msg)) {
        return;
    }
    dnode_rsp_forward(ctx, conn, msg);
}


/* dnode sends a response back to a peer  */
struct msg *
dnode_rsp_send_next(struct context *ctx, struct conn *conn)
{
    rstatus_t status;

    ASSERT(conn->type == CONN_DNODE_PEER_CLIENT);

    struct msg *rsp = rsp_send_next(ctx, conn);

    if (rsp != NULL && conn->dyn_mode) {
        struct msg *pmsg = rsp->peer;

        //need to deal with multi-block later
        uint64_t msg_id = pmsg->dmsg->id;

        struct mbuf *header_buf = mbuf_get();
        if (header_buf == NULL) {
            loga("Unable to obtain an mbuf for header!");
            return NULL; //need to address error here properly
        }
        dmsg_type_t msg_type = DMSG_RES;
        //TODOs: need to set the outcoming conn to be secured too if the incoming conn is secured
        if (pmsg->owner->dnode_secured || conn->dnode_secured) {
            if (log_loggable(LOG_VVERB)) {
                log_debug(LOG_VVERB, "Encrypting response ...");
                loga("AES encryption key: %s\n", base64_encode(conn->aes_key, AES_KEYLEN));
            }

            if (ENCRYPTION) {
              status = dyn_aes_encrypt_msg(rsp, conn->aes_key);
              if (status == DN_ERROR) {
                    loga("OOM to obtain an mbuf for encryption!");
                    mbuf_put(header_buf);
                    req_put(rsp);
                    return NULL;
              }

              if (log_loggable(LOG_VVERB)) {
                   log_debug(LOG_VERB, "#encrypted bytes : %d", status);
              }

              dmsg_write(header_buf, msg_id, msg_type, conn, msg_length(rsp));
            } else {
                if (log_loggable(LOG_VVERB)) {
                   log_debug(LOG_VERB, "no encryption on the rsp payload");
                }
                dmsg_write(header_buf, msg_id, msg_type, conn, msg_length(rsp));
            }

        } else {
            //write dnode header
            log_info("sending dnode response with msg_id %u", msg_id);
            dmsg_write(header_buf, msg_id, msg_type, conn, msg_length(rsp));
        }

        mbuf_insert_head(&rsp->mhdr, header_buf);

        if (log_loggable(LOG_VVERB)) {
            log_hexdump(LOG_VVERB, header_buf->pos, mbuf_length(header_buf), "resp dyn message - header: ");
            msg_dump(rsp);
        }

    }

    return rsp;
}

void
dnode_rsp_send_done(struct context *ctx, struct conn *conn, struct msg *msg)
{
    if (log_loggable(LOG_VVERB)) {
       log_debug(LOG_VVERB, "dnode_rsp_send_done entering");
   }

    struct msg *pmsg; /* peer message (request) */

    ASSERT(conn->type == CONN_DNODE_PEER_CLIENT);
    ASSERT(conn->smsg == NULL);

    log_debug(LOG_VERB, "dyn: send done rsp %"PRIu64" on c %d", msg->id, conn->sd);

    pmsg = msg->peer;

    ASSERT(!msg->request && pmsg->request);
    ASSERT(pmsg->selected_rsp == msg);
    ASSERT(pmsg->done && !pmsg->swallow);
    log_debug(LOG_DEBUG, "DNODE RSP SENT %s %d dmsg->id %u",
              conn_get_type_string(conn),
             conn->sd, pmsg->dmsg->id);

    /* dequeue request from client outq */
    conn_dequeue_outq(ctx, conn, pmsg);

    req_put(pmsg);
}


