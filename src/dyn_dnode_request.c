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

static void
dnode_req_forward_error(struct context *ctx, struct conn *conn, struct msg *msg)
{
    rstatus_t status;

    ASSERT(conn->type == CONN_DNODE_PEER_CLIENT);

    log_debug(LOG_INFO, "dyn: forward req %"PRIu64" len %"PRIu32" type %d from "
            "c %d failed: %s", msg->id, msg->mlen, msg->type, conn->sd,
            strerror(errno));

    msg->done = 1;
    msg->error = 1;
    msg->err = errno;

    /* noreply request don't expect any response */
    if (msg->noreply || msg->swallow) {
        req_put(msg);
        return;
    }

    if (req_done(conn, TAILQ_FIRST(&conn->omsg_q))) {
        status = event_add_out(ctx->evb, conn);
        if (status != DN_OK) {
            conn->err = errno;
        }
    }

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
void
dnode_peer_req_forward(struct context *ctx, struct conn *c_conn,
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
        conn_enqueue_outq(ctx, c_conn, msg);
    }

    ASSERT(p_conn->type == CONN_DNODE_PEER_SERVER);
    ASSERT((c_conn->type == CONN_CLIENT) ||
           (c_conn->type == CONN_DNODE_PEER_CLIENT));

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

    conn_enqueue_inq(ctx, p_conn, msg);

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
    struct msg *msg = msg_get(conn, 1, data_store, __FUNCTION__);

    if (msg == NULL) {
        log_debug(LOG_DEBUG, "Unable to obtain a msg");
        return;
    }

    struct mbuf *header_buf = mbuf_get();
    if (header_buf == NULL) {
        log_debug(LOG_DEBUG, "Unable to obtain a data_buf");
        rsp_put(msg);
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

            status = dyn_aes_encrypt(data_buf->pos, (int)mbuf_length(data_buf), encrypted_buf, conn->aes_key);
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
    conn_enqueue_inq(ctx, conn, msg);
}
