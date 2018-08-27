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
dnode_req_forward_error(struct context *ctx, struct conn *conn, struct msg *req)
{
    rstatus_t status;

    ASSERT(conn->type == CONN_DNODE_PEER_CLIENT);

    log_debug(LOG_INFO, "dyn: forward req %"PRIu64" len %"PRIu32" type %d from "
            "c %d failed: %s", req->id, req->mlen, req->type, conn->sd,
            strerror(errno));

    req->done = 1;
    req->is_error = 1;
    req->error_code = errno;

    if (!req->expect_datastore_reply || req->swallow) {
        req_put(req);
        return;
    }

    if (req_done(conn, TAILQ_FIRST(&conn->omsg_q))) {
        status = conn_event_add_out(conn);
        if (status != DN_OK) {
            conn->err = errno;
        }
    }

}

static void
dnode_peer_req_forward_stats(struct context *ctx, struct node *server, struct msg *req)
{
    ASSERT(req->is_request);
    stats_pool_incr(ctx, peer_requests);
    stats_pool_incr_by(ctx, peer_request_bytes, req->mlen);
}


/* Forward a client request over to a peer */
rstatus_t
dnode_peer_req_forward(struct context *ctx, struct conn *c_conn,
                       struct conn *p_conn, struct msg *req,
                       struct rack *rack, uint8_t *key, uint32_t keylen,
                       dyn_error_t *dyn_error_code)
{

    struct node *server = p_conn->owner;
    log_info("%s FORWARD %s to peer %s on rack '%.*s' dc '%.*s' ",
             print_obj(c_conn), print_obj(req), print_obj(p_conn), rack->name->len, rack->name->data,
             server->dc.len, server->dc.data);

    struct string *dc = rack->dc;
    rstatus_t status;

    ASSERT(p_conn->type == CONN_DNODE_PEER_SERVER);
    ASSERT((c_conn->type == CONN_CLIENT) ||
           (c_conn->type == CONN_DNODE_PEER_CLIENT));

    /* enqueue the message (request) into peer inq */
    status = conn_event_add_out(p_conn);
    if (status != DN_OK) {
        *dyn_error_code = DYNOMITE_UNKNOWN_ERROR;
        p_conn->err = errno;
        return DN_ERROR;
    }

    struct mbuf *header_buf = mbuf_get();
    if (header_buf == NULL) {
        loga("Unable to obtain an mbuf for dnode msg's header!");
        *dyn_error_code = DYNOMITE_OK;
        return DN_ENOMEM;
    }

    struct server_pool *pool = c_conn->owner;
    dmsg_type_t msg_type = (string_compare(&pool->dc, dc) != 0)? DMSG_REQ_FORWARD : DMSG_REQ;

    // SMB: THere is some non trivial business happening here. Better refer to the
    // comment in dnode_rsp_send_next to understand the stuff here.
    // Note: THere MIGHT BE A NEED TO PORT THE dnode_header_prepended FIX FROM THERE
    // TO HERE. especially when a message is being sent in parts
    if (p_conn->dnode_secured) {
        //Encrypting and adding header for a request
        if (log_loggable(LOG_VVERB)) {
            SCOPED_CHARPTR(encoded_aes_key) = base64_encode(p_conn->aes_key, AES_KEYLEN);
            if (encoded_aes_key)
                log_debug(LOG_VVERB, "AES encryption key: %s\n", encoded_aes_key);
        }

        //write dnode header
        if (ENCRYPTION) {
            size_t encrypted_bytes;
            status = dyn_aes_encrypt_msg(req, p_conn->aes_key, &encrypted_bytes);
            if (status != DN_OK) {
                if (status == DN_ENOMEM) {
                    loga("OOM to obtain an mbuf for encryption!");
                } else if (status == DN_ERROR) {
                    loga("Encryption failed: Empty message");
                }
                *dyn_error_code = status;
                mbuf_put(header_buf);
                return status;
            }

            log_debug(LOG_VVERB, "#encrypted bytes : %d", encrypted_bytes);

            dmsg_write(header_buf, req->id, msg_type, p_conn, msg_length(req));
        } else {
            log_debug(LOG_VVERB, "no encryption on the msg payload");
            dmsg_write(header_buf, req->id, msg_type, p_conn, msg_length(req));
        }

    } else {
        //write dnode header
        dmsg_write(header_buf, req->id, msg_type, p_conn, msg_length(req));
    }

    mbuf_insert_head(&req->mhdr, header_buf);

    if (log_loggable(LOG_VVERB)) {
        log_hexdump(LOG_VVERB, header_buf->pos, mbuf_length(header_buf), "dyn message header: ");
        msg_dump(LOG_VVERB, req);
    }

    conn_enqueue_inq(ctx, p_conn, req);

    dnode_peer_req_forward_stats(ctx, p_conn->owner, req);

    log_debug(LOG_VVERB, "remote forward from c %d to s %d req %"PRIu64" len %"PRIu32
              " type %d with key '%.*s'", c_conn->sd, p_conn->sd, req->id,
              req->mlen, req->type, keylen, key);
    return DN_OK;
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
        status = conn_event_add_out(conn);
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
dnode_peer_gossip_forward(struct context *ctx, struct conn *conn, struct mbuf *data_buf)
{
    rstatus_t status;
    struct msg *msg = msg_get(conn, 1, __FUNCTION__);

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
            SCOPED_CHARPTR(encoded_aes_key) = base64_encode(conn->aes_key, AES_KEYLEN);
            if (encoded_aes_key)
                log_debug(LOG_VERB, "AES encryption key: %s\n", encoded_aes_key);
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
        msg_dump(LOG_VVERB, msg);
    }

    /* enqueue the message (request) into peer inq */
    if (TAILQ_EMPTY(&conn->imsg_q)) {
        status = conn_event_add_out(conn);
        if (status != DN_OK) {
            dnode_req_forward_error(ctx, conn, msg);
            conn->err = errno;
            return;
        }
    }

    //need to handle a reply
    //conn->enqueue_outq(ctx, conn, msg);

    msg->expect_datastore_reply = 0;
    conn_enqueue_inq(ctx, conn, msg);
}
