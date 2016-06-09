/*
 * Dynomite - A thin, distributed replication layer for multi non-distributed storages.
 * Copyright (C) 2014 Netflix, Inc.
 */ 

#include "dyn_core.h"
#include "dyn_topology.h"
#include "dyn_server.h"
#include "dyn_dnode_client.h"
#include "dyn_thread_ctx.h"

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

    pool->dn_conn_q++;
    TAILQ_INSERT_TAIL(&pool->c_conn_q, conn, conn_tqe);

    /* owner of the client connection is the server pool */
    conn->owner = owner;
    conn->ptctx = core_get_ptctx_for_conn(pool->ctx, conn->type);
    log_debug(LOG_VVERB, "dyn: ref conn %p owner %p into pool '%.*s'", conn, pool,
              pool->name.len, pool->name.data);
}

static void
dnode_client_unref(struct conn *conn)
{
    struct server_pool *pool;

    ASSERT(conn->type == CONN_DNODE_PEER_CLIENT);
    ASSERT(conn->owner != NULL);

    pool = conn->owner;
    conn->owner = NULL;

    ASSERT(pool->dn_conn_q != 0);
    pool->dn_conn_q--;
    TAILQ_REMOVE(&pool->c_conn_q, conn, conn_tqe);

    log_debug(LOG_VVERB, "dyn: unref conn %p owner %p from pool '%.*s'", conn,
              pool, pool->name.len, pool->name.data);
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
    struct msg *msg, *nmsg; /* current and next message */

    ASSERT(conn->type == CONN_DNODE_PEER_CLIENT);

    dnode_client_close_stats(ctx, conn->owner, conn->err, conn->eof);

    if (conn->sd < 0) {
        conn_unref(conn);
        conn_put(conn);
        return;
    }

    msg = conn->rmsg;
    if (msg != NULL) {
        conn->rmsg = NULL;

        ASSERT(msg->peer == NULL);
        ASSERT(msg->request && !msg->done);

        if (log_loggable(LOG_INFO)) {
           log_debug(LOG_INFO, "dyn: close c %d discarding pending req %"PRIu64" len "
                  "%"PRIu32" type %d", conn->sd, msg->id, msg->mlen,
                  msg->type);
        }

        req_put(msg);
    }

    ASSERT(conn->smsg == NULL);
    ASSERT(TAILQ_EMPTY(&conn->imsg_q));

    for (msg = TAILQ_FIRST(&conn->omsg_q); msg != NULL; msg = nmsg) {
        nmsg = TAILQ_NEXT(msg, c_tqe);

        /* dequeue the message (request) from client outq */
        conn_dequeue_outq(ctx, conn, msg);

        if (msg->done) {
            if (log_loggable(LOG_INFO)) {
               log_debug(LOG_INFO, "dyn: close c %d discarding %s req %"PRIu64" len "
                      "%"PRIu32" type %d", conn->sd,
                      msg->error ? "error": "completed", msg->id, msg->mlen,
                      msg->type);
            }
            req_put(msg);
        } else {
            msg->swallow = 1;

            ASSERT(msg->request);
            ASSERT(msg->peer == NULL);

            if (log_loggable(LOG_INFO)) {
               log_debug(LOG_INFO, "dyn: close c %d schedule swallow of req %"PRIu64" "
                      "len %"PRIu32" type %d", conn->sd, msg->id, msg->mlen,
                      msg->type);
            }
        }
    }
    ASSERT(TAILQ_EMPTY(&conn->omsg_q));

    conn_unref(conn);

    status = close(conn->sd);
    if (status < 0) {
        log_error("dyn: close c %d failed, ignored: %s", conn->sd, strerror(errno));
    }
    conn->sd = -1;

    conn_put(conn);
}

static rstatus_t
dnode_client_handle_response(struct conn *conn, msgid_t msgid, struct msg *rsp)
{
    // Forward the response to the caller which is client connection.
    rstatus_t status = DN_OK;
    /* There is no hash table on the dnode client side. So we rely on rsp->peer
       to get the corresponding request */
    ASSERT_LOG(rsp->peer, "rsp %d:%d does not have a peer", rsp->id, rsp->parent_id);
    struct msg *req = rsp->peer;
    req->peer = NULL;
    req->selected_rsp = rsp;
    status = thread_ctx_add_out(conn->ptctx, conn);
    if (status != DN_OK) {
        conn->err = errno;
    }
    return status;
}

static bool
dnode_req_filter(struct context *ctx, struct conn *conn, struct msg *msg)
{
    ASSERT(conn->type == CONN_DNODE_PEER_CLIENT);

    if (msg_empty(msg)) {
        ASSERT(conn->rmsg == NULL);
        if (log_loggable(LOG_VERB)) {
           log_debug(LOG_VERB, "dyn: filter empty req %"PRIu64" from c %d", msg->id,
                       conn->sd);
        }
        req_put(msg);
        return true;
    }

    /* dynomite handler */
    if (msg->dmsg != NULL) {
        if (dmsg_process(ctx, conn, msg->dmsg)) {
            req_put(msg);
            return true;
        }

    }

    return false;
}

static void
dnode_req_forward(struct context *ctx, struct conn *conn, struct msg *msg)
{
    struct server_pool *pool;
    uint8_t *key;
    uint32_t keylen;

    if (log_loggable(LOG_DEBUG)) {
       log_debug(LOG_DEBUG, "dnode_req_forward entering ");
    }
    log_debug(LOG_DEBUG, "DNODE REQ RECEIVED %s %d dmsg->id %u",
              conn_get_type_string(conn), conn->sd, msg->dmsg->id);

    ASSERT(conn->type == CONN_DNODE_PEER_CLIENT);

    pool = conn->owner;
    key = NULL;
    keylen = 0;

    if (!string_empty(&pool->hash_tag)) {
        struct string *tag = &pool->hash_tag;
        uint8_t *tag_start, *tag_end;

        tag_start = dn_strchr(msg->key_start, msg->key_end, tag->data[0]);
        if (tag_start != NULL) {
            tag_end = dn_strchr(tag_start + 1, msg->key_end, tag->data[1]);
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

    ASSERT(msg->dmsg != NULL);
    if (msg->dmsg->type == DMSG_REQ) {
        if (msg->expect_datastore_reply) {
            conn_enqueue_outq(ctx, conn, msg);
        }
       datastore_req_forward(conn, msg, key, keylen);
    } else if (msg->dmsg->type == DMSG_REQ_FORWARD) {
        struct mbuf *orig_mbuf = STAILQ_FIRST(&msg->mhdr);
        uint32_t rack_cnt = array_n(&pool->my_dc->racks);
        uint32_t rack_index;
        for(rack_index = 0; rack_index < rack_cnt; rack_index++) {
            struct rack *rack = array_get(&pool->my_dc->racks, rack_index);
            //log_debug(LOG_DEBUG, "forwarding to rack  '%.*s'",
            //            rack->name->len, rack->name->data);
            struct msg *rack_msg;
            // TODO: SHAILESH Not sure why using this line causes data corruption.
            //if (rack == pool->my_rack) {
            if (string_compare(rack->name, &pool->rack_name) == 0 ) {
                rack_msg = msg;
            } else {
                rack_msg = msg_get(conn, msg->request, __FUNCTION__);
                if (rack_msg == NULL) {
                    log_debug(LOG_VERB, "whelp, looks like yer screwed now, buddy. no inter-rack messages for you!");
                    continue;
                }

                if (msg_clone(msg, orig_mbuf, rack_msg) != DN_OK) {
                    msg_put(rack_msg);
                    continue;
                }
                rack_msg->swallow = true;
            }

            if (log_loggable(LOG_DEBUG)) {
               log_debug(LOG_DEBUG, "forwarding request from conn '%s' to rack '%.*s' dc '%.*s' ",
                           dn_unresolve_peer_desc(conn->sd), rack->name->len, rack->name->data, rack->dc->len, rack->dc->data);
            }

            remote_req_forward(ctx, conn, rack_msg, rack, key, keylen);
        }
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
                    struct msg *msg, struct msg *nmsg)
{
    ASSERT(conn->type == CONN_DNODE_PEER_CLIENT);
    ASSERT(msg->request);
    ASSERT(msg->owner == conn);
    ASSERT(conn->rmsg == msg);
    ASSERT(nmsg == NULL || nmsg->request);

    /* enqueue next message (request), if any */
    conn->rmsg = nmsg;

    if (dnode_req_filter(ctx, conn, msg)) {
        return;
    }

    log_debug(LOG_VERB, "received msg: %d:%d", msg->id, msg->parent_id);
    dnode_req_forward(ctx, conn, msg);
}

static void
dnode_req_client_enqueue_omsgq(struct context *ctx, struct conn *conn, struct msg *msg)
{
    ASSERT(msg->request);
    ASSERT(conn->type == CONN_DNODE_PEER_CLIENT);

    log_debug(LOG_VERB, "conn %p enqueue outq %p", conn, msg);
    TAILQ_INSERT_TAIL(&conn->omsg_q, msg, c_tqe);

    //use only the 1st pool
    conn->omsg_count++;
    histo_add(&ctx->stats->dnode_client_out_queue, conn->omsg_count);
    stats_pool_incr(ctx, dnode_client_out_queue);
    stats_pool_incr_by(ctx, dnode_client_out_queue_bytes, msg->mlen);
}

static void
dnode_req_client_dequeue_omsgq(struct context *ctx, struct conn *conn, struct msg *msg)
{
    ASSERT(msg->request);
    ASSERT(conn->type == CONN_DNODE_PEER_CLIENT);

    TAILQ_REMOVE(&conn->omsg_q, msg, c_tqe);
    log_debug(LOG_VERB, "conn %p dequeue outq %p", conn, msg);

    //use the 1st pool
    conn->omsg_count--;
    histo_add(&ctx->stats->dnode_client_out_queue, conn->omsg_count);
    stats_pool_decr(ctx, dnode_client_out_queue);
    stats_pool_decr_by(ctx, dnode_client_out_queue_bytes, msg->mlen);
}

/* dnode sends a response back to a peer  */
static struct msg *
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
            log_debug(LOG_VERB, "sending dnode response with msg_id %u", msg_id);
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

    ASSERT(!rsp->request && req->request);
    ASSERT(req->selected_rsp == rsp);
    ASSERT(req->done && !req->swallow);
    log_debug(LOG_DEBUG, "DNODE RSP SENT %s %d dmsg->id %u",
              conn_get_type_string(conn),
             conn->sd, req->dmsg->id);

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
    conn->type = CONN_DNODE_PEER_CLIENT;
    conn->ops = &dnode_client_ops;
}
