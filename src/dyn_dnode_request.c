/*
 * Dynomite - A thin, distributed replication layer for multi non-distributed storages.
 * Copyright (C) 2014 Netflix, Inc.
 */ 

#include "dyn_core.h"
#include "dyn_dnode_peer.h"
#include "dyn_mbuf.h"
#include "dyn_server.h"


//static struct string client_request_dyn_msg = string("Client_request");
static uint64_t peer_msg_id = 0;


struct msg *
dnode_req_get(struct conn *conn)
{
    struct msg *msg;

    ASSERT(conn->dnode_client && !conn->dnode_server);

    msg = msg_get(conn, true, conn->data_store);
    if (msg == NULL) {
        conn->err = errno;
    }

    return msg;
}

static void
dnode_req_put(struct msg *msg)
{
    req_put(msg);
}


bool
dnode_req_done(struct conn *conn, struct msg *msg)
{
    //ASSERT(!conn->dnode_client && !conn->dnode_server );
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

    log_debug(LOG_VERB, "conn %p enqueue inq %d:%d calling req_server_enqueue_imsgq",
              conn, msg->id, msg->parent_id);
    req_server_enqueue_imsgq(ctx, conn, msg);
}

void
dnode_req_peer_dequeue_imsgq(struct context *ctx, struct conn *conn, struct msg *msg)
{
    ASSERT(msg->request);
    ASSERT(!conn->dnode_client && !conn->dnode_server);

    TAILQ_REMOVE(&conn->imsg_q, msg, s_tqe);
    log_debug(LOG_VERB, "conn %p dequeue inq %d:%d", conn, msg->id, msg->parent_id);

    struct server_pool *pool = (struct server_pool *) array_get(&ctx->pool, 0);
    if (conn->same_dc)
        stats_pool_decr(ctx, pool, peer_in_queue);
    else
        stats_pool_decr(ctx, pool, remote_peer_in_queue);
    stats_pool_decr_by(ctx, pool, peer_in_queue_bytes, msg->mlen);
}

void
dnode_req_client_enqueue_omsgq(struct context *ctx, struct conn *conn, struct msg *msg)
{
    ASSERT(msg->request);
    ASSERT(conn->dnode_client && !conn->dnode_server);

    log_debug(LOG_VERB, "conn %p enqueue outq %p", conn, msg);
    TAILQ_INSERT_TAIL(&conn->omsg_q, msg, c_tqe);

    //use only the 1st pool
    struct server_pool *pool = (struct server_pool *) array_get(&ctx->pool, 0);
    stats_pool_incr(ctx, pool, dnode_client_out_queue);
    stats_pool_incr_by(ctx, pool, dnode_client_out_queue_bytes, msg->mlen);
}

void
dnode_req_peer_enqueue_omsgq(struct context *ctx, struct conn *conn, struct msg *msg)
{
    ASSERT(msg->request);
    ASSERT(!conn->dnode_client && !conn->dnode_server);

    TAILQ_INSERT_TAIL(&conn->omsg_q, msg, s_tqe);
    log_debug(LOG_VERB, "conn %p enqueue outq %d:%d", conn, msg->id, msg->parent_id);

    //use only the 1st pool
    struct server_pool *pool = (struct server_pool *) array_get(&ctx->pool, 0);
    if (conn->same_dc)
        stats_pool_incr(ctx, pool, peer_out_queue);
    else
        stats_pool_incr(ctx, pool, remote_peer_out_queue);
   stats_pool_incr_by(ctx, pool, peer_out_queue_bytes, msg->mlen);
}

void
dnode_req_client_dequeue_omsgq(struct context *ctx, struct conn *conn, struct msg *msg)
{
    ASSERT(msg->request);
    ASSERT(conn->dnode_client && !conn->dnode_server);

    TAILQ_REMOVE(&conn->omsg_q, msg, c_tqe);
    log_debug(LOG_VERB, "conn %p dequeue outq %p", conn, msg);

    //use the 1st pool
    struct server_pool *pool = (struct server_pool *) array_get(&ctx->pool, 0);
    stats_pool_decr(ctx, pool, dnode_client_out_queue);
    stats_pool_decr_by(ctx, pool, dnode_client_out_queue_bytes, msg->mlen);
}

void
dnode_req_peer_dequeue_omsgq(struct context *ctx, struct conn *conn, struct msg *msg)
{
    ASSERT(msg->request);
    ASSERT(!conn->dnode_client && !conn->dnode_server);

    msg_tmo_delete(msg);

    TAILQ_REMOVE(&conn->omsg_q, msg, s_tqe);
    log_debug(LOG_VVERB, "conn %p dequeue outq %p", conn, msg);

    //use the 1st pool
    struct server_pool *pool = (struct server_pool *) array_get(&ctx->pool, 0);
    stats_pool_decr(ctx, pool, peer_out_queue);
    stats_pool_decr_by(ctx, pool, peer_out_queue_bytes, msg->mlen);
}

struct msg *
dnode_req_recv_next(struct context *ctx, struct conn *conn, bool alloc)
{
    ASSERT(conn->dnode_client && !conn->dnode_server);
    return req_recv_next(ctx, conn, alloc);
}

static bool
dnode_req_filter(struct context *ctx, struct conn *conn, struct msg *msg)
{
    ASSERT(conn->dnode_client && !conn->dnode_server);

    if (msg_empty(msg)) {
        ASSERT(conn->rmsg == NULL);
        if (log_loggable(LOG_VERB)) {
           log_debug(LOG_VERB, "dyn: filter empty req %"PRIu64" from c %d", msg->id,
                       conn->sd);
        }
        dnode_req_put(msg);
        return true;
    }

    /* dynomite handler */
    if (msg->dmsg != NULL) {
        if (dmsg_process(ctx, conn, msg->dmsg)) {
            dnode_req_put(msg);
            return true;
        }

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
    if (msg->noreply || msg->swallow) {
        dnode_req_put(msg);
        return;
    }

    if (dnode_req_done(conn, TAILQ_FIRST(&conn->omsg_q))) {
        status = event_add_out(ctx->evb, conn);
        if (status != DN_OK) {
            conn->err = errno;
        }
    }

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
    log_debug(LOG_DEBUG, "DNODE REQ RECEIVED %c %d dmsg->id %u",
             conn->dnode_client ? 'c' : (conn->dnode_server ? 's' : 'p'),
             conn->sd, msg->dmsg->id);

    ASSERT(conn->dnode_client && !conn->dnode_server);

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
       local_req_forward(ctx, conn, msg, key, keylen);
    } else if (msg->dmsg->type == DMSG_REQ_FORWARD) {
        struct mbuf *orig_mbuf = STAILQ_FIRST(&msg->mhdr);
        struct datacenter *dc = server_get_dc(pool, &pool->dc);
        uint32_t rack_cnt = array_n(&dc->racks);
        uint32_t rack_index;
        for(rack_index = 0; rack_index < rack_cnt; rack_index++) {
            struct rack *rack = array_get(&dc->racks, rack_index);
            //log_debug(LOG_DEBUG, "forwarding to rack  '%.*s'",
            //            rack->name->len, rack->name->data);
            struct msg *rack_msg;
            if (string_compare(rack->name, &pool->rack) == 0 ) {
                rack_msg = msg;
            } else {
                rack_msg = msg_get(conn, msg->request, msg->data_store);
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


void
dnode_req_recv_done(struct context *ctx, struct conn *conn,
                    struct msg *msg, struct msg *nmsg)
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

    log_debug(LOG_VERB, "received msg: %d:%d", msg->id, msg->parent_id);
    dnode_req_forward(ctx, conn, msg);
}


struct msg *
dnode_req_send_next(struct context *ctx, struct conn *conn)
{
    rstatus_t status;

    ASSERT(!conn->dnode_client && !conn->dnode_server);

    uint32_t now = time(NULL);
    //throttling the sending traffics here
    if (!conn->same_dc) {
        if (conn->last_sent != 0) {
            uint32_t elapsed_time = now - conn->last_sent;
            uint32_t earned_tokens = elapsed_time * msgs_per_sec();
            conn->avail_tokens = (conn->avail_tokens + earned_tokens) < msgs_per_sec()?
                    conn->avail_tokens + earned_tokens : msgs_per_sec();

        }

        conn->last_sent = now;
        if (conn->avail_tokens > 0) {
            conn->avail_tokens--;
            return req_send_next(ctx, conn);
        }

        //requeue
        status = event_add_out(ctx->evb, conn);
        IGNORE_RET_VAL(status);

        return NULL;
    }

    conn->last_sent = now;
    return req_send_next(ctx, conn);
}

void
dnode_req_send_done(struct context *ctx, struct conn *conn, struct msg *msg)
{
    if (log_loggable(LOG_DEBUG)) {
       log_debug(LOG_VERB, "dnode_req_send_done entering!!!");
    }
    ASSERT(!conn->dnode_client && !conn->dnode_server);
    log_debug(LOG_DEBUG, "DNODE REQ SEND %c %d dmsg->id %u",
             conn->dnode_client ? 'c' : (conn->dnode_server ? 's' : 'p'),
             conn->sd, msg->dmsg ? msg->dmsg->id : 0);
    req_send_done(ctx, conn, msg);
}


static void
dnode_peer_req_forward_stats(struct context *ctx, struct server *server, struct msg *msg)
{
    ASSERT(msg->request);
    //use only the 1st pool
    //struct server_pool *pool = (struct server_pool *) array_get(&ctx->pool, 0);
    struct server_pool *pool = server->owner;
    stats_pool_incr(ctx, pool, peer_requests);
    stats_pool_incr_by(ctx, pool, peer_request_bytes, msg->mlen);
}


/* Forward a client request over to a peer */
void dnode_peer_req_forward(struct context *ctx, struct conn *c_conn,
                            struct conn *p_conn, struct msg *msg,
                            struct rack *rack, uint8_t *key, uint32_t keylen)
{

    struct server *server = p_conn->owner;
    log_debug(LOG_DEBUG, "forwarding request from client conn '%s' to peer conn '%s' on rack '%.*s' dc '%.*s' ",
              dn_unresolve_peer_desc(c_conn->sd), dn_unresolve_peer_desc(p_conn->sd),
              rack->name->len, rack->name->data,
              server->dc.len, server->dc.data);

    struct string *dc = rack->dc;
    rstatus_t status;
    /* enqueue message (request) into client outq, if response is expected */
    if (!msg->noreply && !msg->swallow) {
        c_conn->enqueue_outq(ctx, c_conn, msg);
    }

    ASSERT(!p_conn->dnode_client && !p_conn->dnode_server);
    ASSERT(c_conn->client || c_conn->dnode_client);

    /* enqueue the message (request) into peer inq */
    status = event_add_out(ctx->evb, p_conn);
    if (status != DN_OK) {
        dnode_req_forward_error(ctx, p_conn, msg);
        p_conn->err = errno;
        return;
    }

    struct mbuf *header_buf = mbuf_get();
    if (header_buf == NULL) {
        loga("Unable to obtain an mbuf for dnode msg's header!");
        req_put(msg);
        return;
    }

    struct server_pool *pool = c_conn->owner;
    dmsg_type_t msg_type = (string_compare(&pool->dc, dc) != 0)? DMSG_REQ_FORWARD : DMSG_REQ;

    if (p_conn->dnode_secured) {
        //Encrypting and adding header for a request
        if (log_loggable(LOG_VVERB)) {
           log_debug(LOG_VERB, "AES encryption key: %s\n", base64_encode(p_conn->aes_key, AES_KEYLEN));
        }

        //write dnode header
        if (ENCRYPTION) {
            status = dyn_aes_encrypt_msg(msg, p_conn->aes_key);
            if (status == DN_ERROR) {
                loga("OOM to obtain an mbuf for encryption!");
                mbuf_put(header_buf);
                req_put(msg);
                return;
            }

            if (log_loggable(LOG_VVERB)) {
               log_debug(LOG_VERB, "#encrypted bytes : %d", status);
            }

            dmsg_write(header_buf, msg->id, msg_type, p_conn, msg_length(msg));
        } else {
            if (log_loggable(LOG_VVERB)) {
               log_debug(LOG_VERB, "no encryption on the msg payload");
            }
            dmsg_write(header_buf, msg->id, msg_type, p_conn, msg_length(msg));
        }

    } else {
        //write dnode header
        dmsg_write(header_buf, msg->id, msg_type, p_conn, msg_length(msg));
    }

    mbuf_insert_head(&msg->mhdr, header_buf);

    if (log_loggable(LOG_VVERB)) {
        log_hexdump(LOG_VVERB, header_buf->pos, mbuf_length(header_buf), "dyn message header: ");
        msg_dump(msg);
    }

    p_conn->enqueue_inq(ctx, p_conn, msg);

    dnode_peer_req_forward_stats(ctx, p_conn->owner, msg);

    if (log_loggable(LOG_VVERB)) {
       log_debug(LOG_VVERB, "remote forward from c %d to s %d req %"PRIu64" len %"PRIu32
                   " type %d with key '%.*s'", c_conn->sd, p_conn->sd, msg->id,
                   msg->mlen, msg->type, keylen, key);
    }

}


/*
 //for sending a string over to a peer
void
peer_gossip_forward1(struct context *ctx, struct conn *conn, bool redis, struct string *data)
{
    rstatus_t status;
    struct msg *msg = msg_get(conn, 1, redis);

    if (msg == NULL) {
        log_debug(LOG_DEBUG, "Unable to obtain a msg");
        return;
    }

    struct mbuf *nbuf = mbuf_get();
    if (nbuf == NULL) {
        log_debug(LOG_DEBUG, "Unable to obtain a mbuf");
        msg_put(msg);
        return;
    }

    uint64_t msg_id = peer_msg_id++;

    dmsg_write(nbuf, msg_id, GOSSIP_SYN, version, data);
    mbuf_insert_head(&msg->mhdr, nbuf);

    if (TAILQ_EMPTY(&conn->imsg_q)) {
        status = event_add_out(ctx->evb, conn);
        if (status != DN_OK) {
            dnode_req_forward_error(ctx, conn, msg);
            conn->err = errno;
            return;
        }
    }

    conn->enqueue_inq(ctx, conn, msg);


    log_debug(LOG_VERB, "gossip to peer %d with msg_id %"PRIu64" '%.*s'", conn->sd, msg->id,
                         data->len, data->data);

}
 */

/*
 * Sending a mbuf of gossip data over the wire to a peer
 */
void
dnode_peer_gossip_forward(struct context *ctx, struct conn *conn, int data_store, struct mbuf *data_buf)
{
    rstatus_t status;
    struct msg *msg = msg_get(conn, 1, data_store);

    if (msg == NULL) {
        log_debug(LOG_DEBUG, "Unable to obtain a msg");
        return;
    }

    struct mbuf *header_buf = mbuf_get();
    if (header_buf == NULL) {
        log_debug(LOG_DEBUG, "Unable to obtain a data_buf");
        dnode_rsp_put(msg);
        return;
    }

    uint64_t msg_id = peer_msg_id++;

    if (conn->dnode_secured) {
        if (log_loggable(LOG_VERB)) {
           log_debug(LOG_VERB, "Assemble a secured msg to send");
           log_debug(LOG_VERB, "AES encryption key: %s\n", base64_encode(conn->aes_key, AES_KEYLEN));
        }

        if (ENCRYPTION) {
            struct mbuf *encrypted_buf = mbuf_get();
            if (encrypted_buf == NULL) {
                loga("Unable to obtain an data_buf for encryption!");
                return; //TODOs: need to clean up
            }

            status = dyn_aes_encrypt(data_buf->pos, mbuf_length(data_buf), encrypted_buf, conn->aes_key);
            if (log_loggable(LOG_VERB)) {
               log_debug(LOG_VERB, "#encrypted bytes : %d", status);
            }

            //write dnode header
            dmsg_write(header_buf, msg_id, GOSSIP_SYN, conn, mbuf_length(encrypted_buf));

            if (log_loggable(LOG_VVERB)) {
                log_hexdump(LOG_VVERB, data_buf->pos, mbuf_length(data_buf), "dyn message original payload: ");
                log_hexdump(LOG_VVERB, encrypted_buf->pos, mbuf_length(encrypted_buf), "dyn message encrypted payload: ");
            }

            mbuf_remove(&msg->mhdr, data_buf);
            mbuf_insert(&msg->mhdr, encrypted_buf);
            //free data_buf as no one will need it again
            mbuf_put(data_buf);  //TODOS: need to remove this from the msg->mhdr as in the other method

        } else {
            if (log_loggable(LOG_VVERB)) {
               log_debug(LOG_VVERB, "No encryption");
            }
            dmsg_write_mbuf(header_buf, msg_id, GOSSIP_SYN, conn, mbuf_length(data_buf));
            mbuf_insert(&msg->mhdr, data_buf);
        }

    } else {
        if (log_loggable(LOG_VVERB)) {
           log_debug(LOG_VVERB, "Assemble a non-secured msg to send");
        }
        dmsg_write_mbuf(header_buf, msg_id, GOSSIP_SYN, conn, mbuf_length(data_buf));
        mbuf_insert(&msg->mhdr, data_buf);
    }

    mbuf_insert_head(&msg->mhdr, header_buf);

    if (log_loggable(LOG_VVERB)) {
        log_hexdump(LOG_VVERB, header_buf->pos, mbuf_length(header_buf), "dyn gossip message header: ");
        msg_dump(msg);
    }

    /* enqueue the message (request) into peer inq */
    if (TAILQ_EMPTY(&conn->imsg_q)) {
        status = event_add_out(ctx->evb, conn);
        if (status != DN_OK) {
            dnode_req_forward_error(ctx, conn, msg);
            conn->err = errno;
            return;
        }
    }

    //need to handle a reply
    //conn->enqueue_outq(ctx, conn, msg);

    msg->noreply = 1;
    conn->enqueue_inq(ctx, conn, msg);
}
