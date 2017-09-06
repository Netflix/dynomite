/*
 * Dynomite - A thin, distributed replication layer for multi non-distributed storages.
 * Copyright (C) 2014 Netflix, Inc.
 */ 

#include "dyn_core.h"
#include "dyn_server.h"
#include "dyn_dnode_client.h"
#include "dyn_dict_msg_id.h"
#include "dyn_response_mgr.h"

static void
dnode_client_ref(struct conn *conn, void *owner)
{
    struct server_pool *pool = owner;

    ASSERT(conn->type == CONN_DNODE_PEER_CLIENT);
    ASSERT(conn->owner == NULL);

    /*
     * We use null pointer as the sockaddr argument in the accept() call as
     * we are not interested in the address of the peer for the accepted
     * connection
     */
    conn->family = 0;
    conn->addrlen = 0;
    conn->addr = NULL;

    TAILQ_INSERT_TAIL(&pool->c_conn_q, conn, conn_tqe);

    /* owner of the client connection is the server pool */
    conn->owner = owner;
    conn->outstanding_msgs_dict = dictCreate(&msg_table_dict_type, NULL);
    log_debug(LOG_VVERB, "dyn: ref conn %p owner %p into pool '%.*s'", conn, pool,
              pool->name.len, pool->name.data);
}

static void
dnode_client_unref_internal_try_put(struct conn *conn)
{
    ASSERT(conn->waiting_to_unref);
    unsigned long msgs = dictSize(conn->outstanding_msgs_dict);
    if (msgs != 0) {
        log_warn("%M Waiting for %lu outstanding messages", conn, msgs);
        return;
    }
    struct server_pool *pool;
    ASSERT(conn->owner != NULL);
    conn_event_del_conn(conn);
    pool = conn->owner;
    conn->owner = NULL;
    dictRelease(conn->outstanding_msgs_dict);
    conn->outstanding_msgs_dict = NULL;
    conn->waiting_to_unref = 0;
    log_warn("unref %M owner %p from pool '%.*s'",
             conn, pool, pool->name.len, pool->name.data);
    conn_put(conn);
}

static void
dnode_client_unref_and_try_put(struct conn *conn)
{

    ASSERT(conn->type == CONN_DNODE_PEER_CLIENT);

    struct server_pool *pool;
    pool = conn->owner;
    ASSERT(conn->owner != NULL);
    ASSERT(TAILQ_COUNT(&pool->c_conn_q) != 0);
    TAILQ_REMOVE(&pool->c_conn_q, conn, conn_tqe);
    conn->waiting_to_unref = 1;
    dnode_client_unref_internal_try_put(conn);
    log_debug(LOG_VVERB, "dyn: unref conn %p owner %p from pool '%.*s'", conn,
              pool, pool->name.len, pool->name.data);
}

static void
dnode_client_unref(struct conn *conn)
{
    dnode_client_unref_and_try_put(conn);
}

static bool
dnode_client_active(struct conn *conn)
{
    ASSERT(conn->type == CONN_DNODE_PEER_CLIENT);

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
    stats_pool_decr(ctx, dnode_client_connections);

    if (eof) {
        //fix this also
        stats_pool_incr(ctx, dnode_client_eof);
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
        stats_pool_incr(ctx, dnode_client_err);
        break;
    }
}

static void
dnode_client_close(struct context *ctx, struct conn *conn)
{
    rstatus_t status;
    struct msg *req, *nmsg; /* current and next message */

    ASSERT(conn->type == CONN_DNODE_PEER_CLIENT);

    dnode_client_close_stats(ctx, conn->owner, conn->err, conn->eof);

    if (conn->sd < 0) {
        conn_unref(conn);
        return;
    }

    req = conn->rmsg;
    if (req != NULL) {
        conn->rmsg = NULL;

        ASSERT(req->selected_rsp == NULL);
        ASSERT(req->is_request && !req->done);

        if (log_loggable(LOG_INFO)) {
           log_debug(LOG_INFO, "dyn: close c %d discarding pending req %"PRIu64" len "
                  "%"PRIu32" type %d", conn->sd, req->id, req->mlen,
                  req->type);
        }

        dictDelete(conn->outstanding_msgs_dict, &req->id);
        req_put(req);
    }

    ASSERT(conn->smsg == NULL);
    ASSERT(TAILQ_EMPTY(&conn->imsg_q));

    for (req = TAILQ_FIRST(&conn->omsg_q); req != NULL; req = nmsg) {
        nmsg = TAILQ_NEXT(req, c_tqe);

        /* dequeue the message (request) from client outq */
        conn_dequeue_outq(ctx, conn, req);

        if (req->done || req->selected_rsp) {
            if (log_loggable(LOG_INFO)) {
               log_debug(LOG_INFO, "dyn: close c %d discarding %s req %"PRIu64" len "
                         "%"PRIu32" type %d", conn->sd,
                         req->is_error ? "error": "completed", req->id, req->mlen,
                         req->type);
            }
            dictDelete(conn->outstanding_msgs_dict, &req->id);
            req_put(req);
        } else {
            req->swallow = 1;

            ASSERT(req->is_request);

            if (log_loggable(LOG_INFO)) {
               log_debug(LOG_INFO, "dyn: close c %d schedule swallow of req %"PRIu64" "
                         "len %"PRIu32" type %d", conn->sd, req->id, req->mlen,
                         req->type);
            }
        }
    }
    ASSERT(TAILQ_EMPTY(&conn->omsg_q));


    status = close(conn->sd);
    if (status < 0) {
        log_error("dyn: close c %d failed, ignored: %s", conn->sd, strerror(errno));
    }
    conn->sd = -1;
    conn_unref(conn);
}

static rstatus_t
dnode_client_handle_response(struct conn *conn, msgid_t reqid, struct msg *rsp)
{
    // Forward the response to the caller which is client connection.
    rstatus_t status = DN_OK;

    ASSERT(conn->type == CONN_DNODE_PEER_CLIENT);
    // Fetch the original request
    struct msg *req = dictFetchValue(conn->outstanding_msgs_dict, &reqid);
    if (!req) {
        log_notice("looks like we already cleanedup the request for %d", reqid);
        rsp_put(rsp);
        return DN_OK;
    }

    // dnode client has no extra logic of coalescing etc like the client/coordinator.
    // Hence all work for this request is done at this time
    ASSERT_LOG(!req->selected_rsp, "req %lu:%lu has selected_rsp set", req->id, req->parent_id);
    status = msg_handle_response(req, rsp);
    if (conn->waiting_to_unref) {
        dictDelete(conn->outstanding_msgs_dict, &reqid);
        log_info("Putting %M", req);
        req_put(req);
        dnode_client_unref_internal_try_put(conn);
        return DN_OK;
    }

    // Remove the message from the hash table. 
    dictDelete(conn->outstanding_msgs_dict, &reqid);

    // If this request is first in the out queue, then the connection is ready,
    // add the connection to epoll for writing
    if (conn_is_req_first_in_outqueue(conn, req)) {
        status = conn_event_add_out(conn);
        if (status != DN_OK) {
            conn->err = errno;
        }
    }
    return status;
}

static bool
dnode_req_filter(struct context *ctx, struct conn *conn, struct msg *req)
{
    ASSERT(conn->type == CONN_DNODE_PEER_CLIENT);

    if (msg_empty(req)) {
        ASSERT(conn->rmsg == NULL);
        if (log_loggable(LOG_VERB)) {
           log_debug(LOG_VERB, "dyn: filter empty req %"PRIu64" from c %d", req->id,
                       conn->sd);
        }
        req_put(req);
        return true;
    }

    /* dynomite handler */
    if (req->dmsg != NULL) {
        if (dmsg_process(ctx, conn, req->dmsg)) {
            req_put(req);
            return true;
        }

    }

    return false;
}

static void
dnode_req_forward(struct context *ctx, struct conn *conn, struct msg *req)
{
    struct server_pool *pool;

    log_debug(LOG_DEBUG, "%M DNODE REQ RECEIVED dmsg->id %u", conn, req->dmsg->id);

    ASSERT(conn->type == CONN_DNODE_PEER_CLIENT);

    pool = conn->owner;

    log_debug(LOG_DEBUG, "%M adding message %d:%d", conn, req->id, req->parent_id);
    dictAdd(conn->outstanding_msgs_dict, &req->id, req);

    uint32_t keylen = 0;
    uint8_t *key = msg_get_tagged_key(req, 0, &keylen);

    ASSERT(req->dmsg != NULL);
    /* enqueue message (request) into client outq, if response is expected
     * and its not marked for swallow */
    if (req->expect_datastore_reply  && !req->swallow) {
        conn_enqueue_outq(ctx, conn, req);
        req->rsp_handler = msg_local_one_rsp_handler;
    }
    if (req->dmsg->type == DMSG_REQ) {
        // This is a request received from a peer rack in the same DC, just forward
        // it to the local datastore
        dyn_error_t dyn_error_code = DN_OK;
        rstatus_t s = local_req_forward(ctx, conn, req, key, keylen, &dyn_error_code);
        if (s != DN_OK) {
            req_forward_error(ctx, conn, req, s, dyn_error_code);
        }
    } else if (req->dmsg->type == DMSG_REQ_FORWARD) {
        // This is a request received from a remote DC. Forward it to all local racks
        struct mbuf *orig_mbuf = STAILQ_FIRST(&req->mhdr);
        struct datacenter *dc = server_get_dc(pool, &pool->dc);
        req_forward_all_local_racks(ctx, conn, req, orig_mbuf, key, keylen, dc);
    }
}

static struct msg *
dnode_req_recv_next(struct context *ctx, struct conn *conn, bool alloc)
{
    ASSERT(conn->type == CONN_DNODE_PEER_CLIENT);
    return req_recv_next(ctx, conn, alloc);
}

static void
dnode_req_recv_done(struct context *ctx, struct conn *conn,
                    struct msg *req, struct msg *nmsg)
{
    ASSERT(conn->type == CONN_DNODE_PEER_CLIENT);
    ASSERT(req->is_request);
    ASSERT(req->owner == conn);
    ASSERT(conn->rmsg == req);
    ASSERT(nmsg == NULL || nmsg->is_request);

    /* enqueue next message (request), if any */
    conn->rmsg = nmsg;

    if (dnode_req_filter(ctx, conn, req)) {
        return;
    }

    log_debug(LOG_VERB, "received req %d:%d", req->id, req->parent_id);
    dnode_req_forward(ctx, conn, req);
}

static void
dnode_req_client_enqueue_omsgq(struct context *ctx, struct conn *conn, struct msg *req)
{
    ASSERT(req->is_request);
    ASSERT(conn->type == CONN_DNODE_PEER_CLIENT);

    log_debug(LOG_VERB, "conn %p enqueue outq %p", conn, req);
    TAILQ_INSERT_TAIL(&conn->omsg_q, req, c_tqe);

    histo_add(&ctx->stats->dnode_client_out_queue, TAILQ_COUNT(&conn->omsg_q));
    stats_pool_incr(ctx, dnode_client_out_queue);
    stats_pool_incr_by(ctx, dnode_client_out_queue_bytes, req->mlen);
}

static void
dnode_req_client_dequeue_omsgq(struct context *ctx, struct conn *conn, struct msg *req)
{
    ASSERT(req->is_request);
    ASSERT(conn->type == CONN_DNODE_PEER_CLIENT);

    TAILQ_REMOVE(&conn->omsg_q, req, c_tqe);
    log_debug(LOG_VERB, "conn %p dequeue outq %p", conn, req);

    histo_add(&ctx->stats->dnode_client_out_queue, TAILQ_COUNT(&conn->omsg_q));
    stats_pool_decr(ctx, dnode_client_out_queue);
    stats_pool_decr_by(ctx, dnode_client_out_queue_bytes, req->mlen);
}

/* dnode sends a response back to a peer  */
static struct msg *
dnode_rsp_send_next(struct context *ctx, struct conn *conn)
{
    rstatus_t status;

    // SMB: There is some non trivial thing happening here. And I think it is very
    // important to read this before anything is changed in here. There is also a
    // bug that exists which I will mention briefly:
    // A message is a structure that has a list of mbufs which hold the actual data.
    // Each mbuf has start, pos, last as pointers (amongst others) which indicate start of the
    // buffer, current read position and end of the buffer respectively.
    //
    // Every time a message is sent to a peer within dynomite, a DNODE header is
    // prepended which is created using dmsg_write. A message remembers this case
    // in dnode_header_prepended, so that if the messsage is sent in parts, the
    // header is not prepended again for the subsequent parts.
    //
    // Like I said earlier there is a pos pointer in mbuf. If a message is sent
    // partially (or it is parsed partially too I think) the pos reflects that
    // case such that things can be resumed where it left off.
    //
    // dmsg_write has a parameter which reflects the payload length following the
    // dnode header calculated by msg_length. msg_length is a summation of all
    // mbuf sizes (last - start). Which I think is wrong.
    //
    // +------------+           +---------------+
    // |    DC1N1   +---------> |     DC2N1     |
    // +------------+           +-------+-------+
    //                                  |
    //                                  |
    //                                  |
    //                                  |
    //                          +-------v-------+
    //                          |    DC2N2      |
    //                          +---------------+
    //
    // Consider the case where
    // a node DC1N1 in region DC1 sends a request to DC2N1 which forwards it to
    // to local token owner DC2N2. Now DC2N1 receives a response from DC2N2 which
    // has to be relayed back to DC1N1. This response from DC2N2 already has a
    // dnode header but for the link between DC2N1 and DC2N2. DC2N1 should strip
    // this header and prepend its own header for sending it back to DC1N1. This
    // gets handled in encryption case since we overwrite all mbufs in the response
    // However if the encryption is off, the message length sent to dmsg_write
    // consists of the header from DC2N2 also which is wrong. So this relaying
    // of responses will not work for the case where encryption is disabled.
    //
    // So msg_length should really be from mbuf->pos and not mbuf->start. This
    // is a problem only with remote region replication since that is the only
    // case where we CAN have 2 hops to send the request/response. This is also
    // not a problem if encryption is ON.
    ASSERT(conn->type == CONN_DNODE_PEER_CLIENT);

    struct msg *rsp = rsp_send_next(ctx, conn);

    if (rsp != NULL && conn->dyn_mode) {
        struct msg *req = rsp->peer;

        //need to deal with multi-block later
        uint64_t msg_id = req->dmsg->id;
        if (rsp->dnode_header_prepended) {
            return rsp;
        }

        struct mbuf *header_buf = mbuf_get();
        if (header_buf == NULL) {
            loga("Unable to obtain an mbuf for header!");
            return NULL; //need to address error here properly
        }
        dmsg_type_t msg_type = DMSG_RES;
        //TODOs: need to set the outcoming conn to be secured too if the incoming conn is secured
        if (req->owner->dnode_secured || conn->dnode_secured) {
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
            log_debug(LOG_VERB, "sending dnode response with msg_id %u", msg_id);
            dmsg_write(header_buf, msg_id, msg_type, conn, msg_length(rsp));
        }

        rsp->dnode_header_prepended = 1;
        mbuf_insert_head(&rsp->mhdr, header_buf);

        if (log_loggable(LOG_VVERB)) {
            log_hexdump(LOG_VVERB, header_buf->pos, mbuf_length(header_buf), "resp dyn message - header: ");
            msg_dump(LOG_VVERB, rsp);
        }

    }

    return rsp;
}

static void
dnode_rsp_send_done(struct context *ctx, struct conn *conn, struct msg *rsp)
{
    if (log_loggable(LOG_VVERB)) {
       log_debug(LOG_VVERB, "dnode_rsp_send_done entering");
   }

    struct msg *req; /* peer message (request) */

    ASSERT(conn->type == CONN_DNODE_PEER_CLIENT);
    ASSERT(conn->smsg == NULL);

    log_debug(LOG_VERB, "dyn: send done rsp %"PRIu64" on c %d", rsp->id, conn->sd);

    req = rsp->peer;

    ASSERT(!rsp->is_request && req->is_request);
    ASSERT(req->selected_rsp == rsp);
    log_debug(LOG_DEBUG, "%M DNODE RSP SENT dmsg->id %u", conn, req->dmsg->id);

    /* dequeue request from client outq */
    conn_dequeue_outq(ctx, conn, req);

    req_put(req);
}

struct conn_ops dnode_client_ops = {
    msg_recv,
    dnode_req_recv_next,
    dnode_req_recv_done,
    msg_send,
    dnode_rsp_send_next,
    dnode_rsp_send_done,
    dnode_client_close,
    dnode_client_active,
    dnode_client_ref,
    dnode_client_unref,
    NULL,
    NULL,
    dnode_req_client_enqueue_omsgq,
    dnode_req_client_dequeue_omsgq,
    dnode_client_handle_response 
};

void
init_dnode_client_conn(struct conn *conn)
{
    conn->dyn_mode = 1;
    conn->type = CONN_DNODE_PEER_CLIENT;
    conn->ops = &dnode_client_ops;
}
