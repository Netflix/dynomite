/*
 * Dynomite - A thin, distributed replication layer for multi non-distributed storages.
 * Copyright (C) 2014 Netflix, Inc.
 */ 

/*
 * twemproxy - A fast and lightweight proxy for memcached protocol.
 * Copyright (C) 2011 Twitter, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/**
 * This is the client connection. It receives requests from the client, and
 * forwards it to the corresponding peers and local data store server (if this
 * node owns the token).
 * There is fair amount of machinery involved here mainly for consistency feature
 * It acts more of a co-ordinator than a mere client connection handler.
 * - outstanding_msgs_dict : This is a hash table (HT) of request id to request
 *   mapping. When it receives a request, it adds the message to the HT, and
 *   removes it when it finished responding. We need a hash table mainly for
 *   implementing consistency. When a response is received from a peer, it is 
 *   handed over to the client connection. It uses this HT to get the request &
 *   calls the request's response handler.
 * - waiting_to_unref: Now that we distribute messages to multiple nodes and that
 *   we have consistency, there is a need for the responses to refer back to the
 *   original requests. This makes cleaning up and connection tear down fairly
 *   complex. The client connection has to wait for all responses (either a good
 *   response or a error response due to timeout). Hence the client connection
 *   should wait for the above HT outstanding_msgs_dict to get empty. This flag
 *   waiting_to_unref indicates that the client connection is ready to close and
 *   just waiting for the outstanding messages to finish.
 */

#include "dyn_core.h"
#include "dyn_topology.h"
#include "dyn_server.h"
#include "dyn_client.h"
#include "dyn_dnode_peer.h"
#include "dyn_thread_ctx.h"

static rstatus_t msg_quorum_rsp_handler(struct msg *req, struct msg *rsp);
static rstatus_t msg_local_one_rsp_handler(struct msg *req, struct msg *rsp);
static msg_response_handler_t msg_get_rsp_handler(struct msg *req);

static unsigned int
dict_msg_id_hash(const void *key)
{
    msgid_t id = *(msgid_t*)key;
    return dictGenHashFunction(key, sizeof(id));
}

static int
dict_msg_id_cmp(void *privdata, const void *key1, const void *key2)
{
    msgid_t id1 = *(msgid_t*)key1;
    msgid_t id2 = *(msgid_t*)key2;
    return id1 == id2;
}

dictType msg_table_dict_type = {
    dict_msg_id_hash,            /* hash function */
    NULL,                        /* key dup */
    NULL,                        /* val dup */
    dict_msg_id_cmp,             /* key compare */
    NULL,                        /* key destructor */
    NULL                         /* val destructor */
};


static void
client_ref(struct conn *conn, void *owner)
{
    struct server_pool *pool = owner;

    ASSERT(conn->type == CONN_CLIENT);
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
    conn->outstanding_msgs_dict = dictCreate(&msg_table_dict_type, NULL);
    conn->waiting_to_unref = 0;

    log_debug(LOG_VVERB, "ref conn %p owner %p into pool '%.*s'", conn, pool,
              pool->name.len, pool->name.data);
}

static void
client_unref_internal_try_put(struct conn *conn)
{
    ASSERT(conn->waiting_to_unref);
    unsigned long msgs = dictSize(conn->outstanding_msgs_dict);
    if (msgs != 0) {
        log_warn("conn %p Waiting for %lu outstanding messages", conn, msgs);
        return;
    }
    struct server_pool *pool;
    ASSERT(conn->owner != NULL);
    pool = conn->owner;
    conn->owner = NULL;
    dictRelease(conn->outstanding_msgs_dict);
    conn->waiting_to_unref = 0;
    log_warn("unref conn %p owner %p from pool '%.*s'", conn,
             pool, pool->name.len, pool->name.data);
    conn_put(conn);
}

static void
client_unref_and_try_put(struct conn *conn)
{
    ASSERT(conn->type == CONN_CLIENT);

    struct server_pool *pool;
    pool = conn->owner;
    ASSERT(conn->owner != NULL);
    ASSERT(pool->dn_conn_q != 0);
    pool->dn_conn_q--;
    TAILQ_REMOVE(&pool->c_conn_q, conn, conn_tqe);
    conn->waiting_to_unref = 1;
    client_unref_internal_try_put(conn);
}

static void
client_unref(struct conn *conn)
{
    client_unref_and_try_put(conn);
}

static bool
client_active(struct conn *conn)
{
    ASSERT(conn->type == CONN_CLIENT);

    ASSERT(TAILQ_EMPTY(&conn->imsg_q));

    if (!TAILQ_EMPTY(&conn->omsg_q)) {
        log_debug(LOG_VVERB, "c %d is active", conn->sd);
        return true;
    }

    if (conn->rmsg != NULL) {
        log_debug(LOG_VVERB, "c %d is active", conn->sd);
        return true;
    }

    if (conn->smsg != NULL) {
        log_debug(LOG_VVERB, "c %d is active", conn->sd);
        return true;
    }

    log_debug(LOG_VVERB, "c %d is inactive", conn->sd);

    return false;
}

static void
client_close_stats(struct context *ctx, struct server_pool *pool, err_t err,
                   unsigned eof)
{
    stats_pool_decr(ctx, client_connections);

    if (eof) {
        stats_pool_incr(ctx, client_eof);
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
        stats_pool_incr(ctx, client_err);
        break;
    }
}

static void
client_close(struct context *ctx, struct conn *conn)
{
    rstatus_t status;
    struct msg *msg, *nmsg; /* current and next message */

    ASSERT(conn->type == CONN_CLIENT);

    client_close_stats(ctx, conn->owner, conn->err, conn->eof);

    if (conn->sd < 0) {
        client_unref(conn);
        return;
    }

    msg = conn->rmsg;
    if (msg != NULL) {
        conn->rmsg = NULL;

        ASSERT(msg->peer == NULL);
        ASSERT(msg->request && !msg->done);

        log_debug(LOG_INFO, "close c %d discarding pending req %"PRIu64" len "
                  "%"PRIu32" type %d", conn->sd, msg->id, msg->mlen,
                  msg->type);

        req_put(msg);
    }

    ASSERT(conn->smsg == NULL);
    ASSERT(TAILQ_EMPTY(&conn->imsg_q));

    for (msg = TAILQ_FIRST(&conn->omsg_q); msg != NULL; msg = nmsg) {
        nmsg = TAILQ_NEXT(msg, c_tqe);

        /* dequeue the message (request) from client outq */
        conn_dequeue_outq(ctx, conn, msg);

        if (msg->done) {
            log_debug(LOG_INFO, "close c %d discarding %s req %"PRIu64" len "
                      "%"PRIu32" type %d", conn->sd,
                      msg->error ? "error": "completed", msg->id, msg->mlen,
                      msg->type);
            req_put(msg);
        } else {
            msg->swallow = 1;

            ASSERT(msg->request);
            ASSERT(msg->peer == NULL);

            log_debug(LOG_INFO, "close c %d schedule swallow of req %"PRIu64" "
                      "len %"PRIu32" type %d", conn->sd, msg->id, msg->mlen,
                      msg->type);
        }

        stats_pool_incr(ctx, client_dropped_requests);
    }
    ASSERT(TAILQ_EMPTY(&conn->omsg_q));

    status = close(conn->sd);
    if (status < 0) {
        log_error("close c %d failed, ignored: %s", conn->sd, strerror(errno));
    }
    conn->sd = -1;
    client_unref(conn);
}

// A response handler first deletes the link between the response and the
// request. This request can be a request clone at the dnode_server connection.
static rstatus_t
client_handle_response(struct conn *conn, msgid_t reqid, struct msg *rsp)
{
    // First lets delink the response and message that earlier code did
    if (rsp->peer) {
        rsp->peer->peer = NULL;
    }
    rsp->peer = NULL;
    // now the handler owns the response. the caller owns the request
    ASSERT(conn->type == CONN_CLIENT);
    // Fetch the original request
    struct msg *req = dictFetchValue(conn->outstanding_msgs_dict, &reqid);
    if (!req) {
        log_notice("looks like we already cleanedup the request for %d", reqid);
        rsp_put(rsp);
        return DN_OK;
    }
    // we have to submit the response irrespective of the unref status.
    rstatus_t status = msg_handle_response(req, rsp);
    if (conn->waiting_to_unref) {
        // dont care about the status.
        if (req->awaiting_rsps)
            return DN_OK;
        // all responses received
        dictDelete(conn->outstanding_msgs_dict, &reqid);
        log_info("Putting req %d", req->id);
        req_put(req);
        client_unref_internal_try_put(conn);
        return DN_OK;
    }
    if (status == DN_NOOPS) {
        // by now the response is dropped
        if (!req->awaiting_rsps) {
            // if we have sent the response for this request or the connection
            // is closed and we are just waiting to drain off the messages.
            if (req->rsp_sent) {
                dictDelete(conn->outstanding_msgs_dict, &reqid);
                log_info("Putting req %d", req->id);
                req_put(req);
            }
        }
    } else if (status == DN_OK) {
        status = thread_ctx_add_out(conn->ptctx, conn);
        if (status != DN_OK) {
            conn->err = errno;
        }
    }
    return status;
}

struct msg *
req_recv_next(struct context *ctx, struct conn *conn, bool alloc)
{
    struct msg *msg;

    ASSERT((conn->type == CONN_DNODE_PEER_CLIENT) ||
           (conn->type = CONN_CLIENT));

    if (conn->eof) {
        msg = conn->rmsg;

        //if (conn->dyn_mode) {
        //    if (conn->non_bytes_recv > MAX_CONN_ALLOWABLE_NON_RECV) {
        //        conn->err = EPIPE;
        //        return NULL;
        //    }
        //    conn->eof = 0;
        //    return msg;
        //}

        /* client sent eof before sending the entire request */
        if (msg != NULL) {
            conn->rmsg = NULL;

            ASSERT(msg->peer == NULL);
            ASSERT(msg->request && !msg->done);

            log_error("eof c %d discarding incomplete req %"PRIu64" len "
                      "%"PRIu32"", conn->sd, msg->id, msg->mlen);

            req_put(msg);
        }

        /*
         * TCP half-close enables the client to terminate its half of the
         * connection (i.e. the client no longer sends data), but it still
         * is able to receive data from the proxy. The proxy closes its
         * half (by sending the second FIN) when the client has no
         * outstanding requests
         */
        if (!conn_active(conn)) {
            conn->done = 1;
            log_debug(LOG_INFO, "c %d is done", conn->sd);
        }

        return NULL;
    }

    msg = conn->rmsg;
    if (msg != NULL) {
        ASSERT(msg->request);
        return msg;
    }

    if (!alloc) {
        return NULL;
    }

    msg = req_get(conn);
    if (msg != NULL) {
        conn->rmsg = msg;
    }

    return msg;
}

static bool
req_filter(struct context *ctx, struct conn *conn, struct msg *msg)
{
    ASSERT(conn->type == CONN_CLIENT);

    if (msg_empty(msg)) {
        ASSERT(conn->rmsg == NULL);
        log_debug(LOG_VERB, "filter empty req %"PRIu64" from c %d", msg->id,
                  conn->sd);
        req_put(msg);
        return true;
    }

    /*
     * Handle "quit\r\n", which is the protocol way of doing a
     * passive close
     */
    if (msg->quit) {
        ASSERT(conn->rmsg == NULL);
        log_debug(LOG_INFO, "filter quit req %"PRIu64" from c %d", msg->id,
                  conn->sd);
        conn->eof = 1;
        conn->recv_ready = 0;
        req_put(msg);
        return true;
    }

    return false;
}

static void
send_rsp_integer(struct context *ctx, struct conn *c_conn, struct msg *req)
{
    //do nothing
    struct msg *rsp = msg_get_rsp_integer(c_conn);
    if (req->expect_datastore_reply)
        conn_enqueue_outq(ctx, c_conn, req);
    req->peer = rsp;
    rsp->peer = req;
    req->selected_rsp = rsp;

    req->done = 1;
    //req->pre_coalesce(req);
    rstatus_t status = thread_ctx_add_out(c_conn->ptctx, c_conn);
    IGNORE_RET_VAL(status);
}

void
client_forward_error(struct conn *conn, struct msg *msg,
                  err_t err)
{
    rstatus_t status;

    if (log_loggable(LOG_INFO)) {
       log_debug(LOG_INFO, "forward req %"PRIu64" len %"PRIu32" type %d from "
                 "c %d failed: %s", msg->id, msg->mlen, msg->type, conn->sd,
                 dn_strerror(err));
    }

    msg->done = 1;
    msg->error = 1;
    msg->err = err;

    if (!msg->expect_datastore_reply) {
        req_put(msg);
        return;
    }

    if (req_done(conn, TAILQ_FIRST(&conn->omsg_q))) {
        status = thread_ctx_add_out(conn->ptctx, conn);
        if (status != DN_OK) {
            conn->err = status;
        }
    }

}

static void
admin_local_req_forward(struct context *ctx, struct conn *c_conn, struct msg *msg,
                        struct rack *rack, uint8_t *key, uint32_t keylen)
{
    ASSERT((c_conn->type == CONN_CLIENT) ||
           (c_conn->type == CONN_DNODE_PEER_CLIENT));

    struct peer *peer = topo_get_peer_in_rack_for_key(ctx_get_topology(ctx), rack,
                                                      key, keylen, msg->msg_type);
    if (peer == NULL) {
        c_conn->err = EHOSTDOWN;
        client_forward_error(c_conn, msg, DN_ENOHOST);
        return;
    }

    if (!peer->is_local) {
        send_rsp_integer(ctx, c_conn, msg);
    } else {
        log_debug(LOG_NOTICE, "Need to delete [%.*s] ", keylen, key);
        if (msg->expect_datastore_reply) {
            conn_enqueue_outq(ctx, c_conn, msg);
        }
        datastore_req_forward(c_conn, msg, key, keylen);
    }
}

void
remote_req_forward(struct context *ctx, struct conn *c_conn, struct msg *msg, 
                        struct rack *rack, uint8_t *key, uint32_t keylen)
{
    ASSERT((c_conn->type == CONN_CLIENT) ||
           (c_conn->type == CONN_DNODE_PEER_CLIENT));

    struct peer *peer = topo_get_peer_in_rack_for_key(ctx_get_topology(ctx), rack, key,
                                         keylen, msg->msg_type);
    if (peer == NULL) {
        client_forward_error(c_conn, msg, DN_ENOHOST);
        return;
    }

    if (peer->is_local) {
        log_debug(LOG_VERB, "c_conn: %p forwarding %d:%d is local", c_conn,
                msg->id, msg->parent_id);
        if (msg->expect_datastore_reply) {
            conn_enqueue_outq(ctx, c_conn, msg);
        }
        datastore_req_forward(c_conn, msg, key, keylen);
    } else {
        log_debug(LOG_VERB, "c_conn: %p forwarding %d:%d to peer %p", c_conn,
                msg->id, msg->parent_id, peer);
        if (msg->expect_datastore_reply && !msg->swallow) {
            conn_enqueue_outq(ctx, c_conn, msg);
        }
        dnode_peer_req_forward(ctx, c_conn, peer, msg, key, keylen);
    }
}

static void
req_forward_all_local_racks(struct context *ctx, struct conn *c_conn,
                            struct msg *msg, struct mbuf *orig_mbuf,
                            uint8_t *key, uint32_t keylen, struct datacenter *dc)
{
    uint8_t rack_cnt = (uint8_t)array_n(&dc->racks);
    uint8_t rack_index;
    msg->rsp_handler = msg_get_rsp_handler(msg);
    init_response_mgr(&msg->rspmgr, msg, msg->is_read, rack_cnt, c_conn);
    log_info("msg %d:%d same DC racks:%d expect replies %d",
             msg->id, msg->parent_id, rack_cnt, msg->rspmgr.quorum_responses);
    for(rack_index = 0; rack_index < rack_cnt; rack_index++)
    {
        struct rack *rack = array_get(&dc->racks, rack_index);
        struct msg *rack_msg;
        struct server_pool *pool = c_conn->owner;
        if (string_compare(rack->name, &pool->rack_name) == 0 ) {
            rack_msg = msg;
        } else {
            rack_msg = msg_get(c_conn, msg->request, __FUNCTION__);
            if (rack_msg == NULL) {
                log_error("whelp, looks like yer screwed "
                          "now, buddy. no inter-rack messages for you!");
                continue;
            }

            msg_clone(msg, orig_mbuf, rack_msg);
            log_info("msg (%d:%d) clone to rack msg (%d:%d)",
                     msg->id, msg->parent_id, rack_msg->id, rack_msg->parent_id);
            rack_msg->swallow = true;
        }

        if (log_loggable(LOG_DEBUG)) {
            log_debug(LOG_DEBUG, "forwarding request to conn '%s' on rack '%.*s'",
                    dn_unresolve_peer_desc(c_conn->sd), rack->name->len, rack->name->data);
        }
        log_debug(LOG_VERB, "c_conn: %p forwarding (%d:%d)",
                c_conn, rack_msg->id, rack_msg->parent_id);
        remote_req_forward(ctx, c_conn, rack_msg, rack, key, keylen);
    }
}

static bool
request_send_to_all_dcs(struct msg *msg)
{
    if (!msg->is_read)
        return true;
    return false;
}

static bool
request_send_to_all_local_racks(struct msg *msg)
{
    if (!msg->is_read)
        return true;
    if ((msg->type == MSG_REQ_REDIS_PING) ||
        (msg->type == MSG_REQ_REDIS_INFO))
        return false;
    if (msg->consistency == DC_QUORUM)
        return true;
    return false;
}

static void
req_forward_remote_dc(struct context *ctx, struct conn *c_conn, struct msg *msg,
                      struct mbuf *orig_mbuf, uint8_t *key, uint32_t keylen,
                      struct datacenter *dc)
{
    uint32_t rack_cnt = array_n(&dc->racks);
    if (rack_cnt == 0)
        return;

    struct rack *rack = dc->preselected_rack_for_replication;
    if (rack == NULL)
        rack = array_get(&dc->racks, 0);

    struct msg *rack_msg = msg_get(c_conn, msg->request, __FUNCTION__);
    if (rack_msg == NULL) {
        log_debug(LOG_VERB, "whelp, looks like yer screwed now, buddy. no inter-rack messages for you!");
        msg_put(rack_msg);
        return;
    }

    msg_clone(msg, orig_mbuf, rack_msg);
    rack_msg->swallow = true;

    if (log_loggable(LOG_DEBUG)) {
        log_debug(LOG_DEBUG, "forwarding request to conn '%s' on rack '%.*s'",
                dn_unresolve_peer_desc(c_conn->sd), rack->name->len, rack->name->data);
    }
    remote_req_forward(ctx, c_conn, rack_msg, rack, key, keylen);
}

static void
req_forward_local_dc(struct context *ctx, struct conn *c_conn, struct msg *msg,
                     struct mbuf *orig_mbuf, uint8_t *key, uint32_t keylen,
                     struct datacenter *dc)
{
    struct server_pool *pool = c_conn->owner;
    if (request_send_to_all_local_racks(msg)) {
        // send request to all local racks
        req_forward_all_local_racks(ctx, c_conn, msg, orig_mbuf, key, keylen, dc);
    } else {
        // send request to only local token owner
        ASSERT(msg->is_read);
        msg->rsp_handler = msg_get_rsp_handler(msg);
        remote_req_forward(ctx, c_conn, msg, pool->my_rack, key, keylen);
    }

}

static void
incr_client_stats(struct context *ctx, struct msg *msg)
{
    if (msg->is_read)
        stats_pool_incr(ctx, client_read_requests);
    else
        stats_pool_incr(ctx, client_write_requests);
}

static void
req_forward(struct context *ctx, struct conn *c_conn, struct msg *msg)
{
    struct server_pool *pool = c_conn->owner;

    ASSERT(c_conn->type == CONN_CLIENT);
    incr_client_stats(ctx, msg);

    // add the message to the dict
    log_debug(LOG_DEBUG, "conn %p adding message %d:%d", c_conn, msg->id, msg->parent_id);
    dictAdd(c_conn->outstanding_msgs_dict, &msg->id, msg);

    // extract key for msg, either using the hash_tag what the parser got
    uint8_t *key = NULL;
    uint32_t keylen = 0;
    key = g_msg_get_key(msg, &pool->topo->hash_tag, &keylen);

    // need to capture the initial mbuf location as once we add in the dynomite
    // headers (as mbufs to the src msg), that will bork the request sent to
    // secondary racks
    struct mbuf *orig_mbuf = STAILQ_FIRST(&msg->mhdr);

    if (ctx->admin_opt == 1) {
        if (msg->type == MSG_REQ_REDIS_DEL || msg->type == MSG_REQ_MC_DELETE) {
            admin_local_req_forward(ctx, c_conn, msg, pool->my_rack, key, keylen);
            return;
        }
    }

    msg->consistency = conn_get_consisteny(c_conn, msg->is_read);

    /* forward the request */
    struct topology *topo = pool->topo;
    uint32_t dc_cnt = array_n(&topo->datacenters);
    uint32_t dc_index;

    for(dc_index = 0; dc_index < dc_cnt; dc_index++) {
        struct datacenter *dc = array_get(&topo->datacenters, dc_index);
        if (dc == NULL) {
            log_error("Wow, this is very bad, dc is NULL");
            return;
        }

        if (pool->my_dc == dc)
            req_forward_local_dc(ctx, c_conn, msg, orig_mbuf, key, keylen, dc);
        else if (request_send_to_all_dcs(msg)) {
            req_forward_remote_dc(ctx, c_conn, msg, orig_mbuf, key, keylen, dc);
        }
    }
}


void
req_recv_done(struct context *ctx, struct conn *conn,
              struct msg *msg, struct msg *nmsg)
{
    ASSERT(conn->type == CONN_CLIENT);
    ASSERT(msg->request);
    ASSERT(msg->owner == conn);
    ASSERT(conn->rmsg == msg);
    ASSERT(nmsg == NULL || nmsg->request);

    if (!msg->is_read)
        stats_histo_add_payloadsize(ctx, msg->mlen);

    /* enqueue next message (request), if any */
    conn->rmsg = nmsg;

    if (req_filter(ctx, conn, msg)) {
        return;
    }

    msg->stime_in_microsec = dn_usec_now();
    req_forward(ctx, conn, msg);
}

static msg_response_handler_t 
msg_get_rsp_handler(struct msg *req)
{
    if ((req->consistency == DC_ONE) ||
        (req->type == MSG_REQ_REDIS_PING) ||
        (req->type == MSG_REQ_REDIS_INFO))
        return msg_local_one_rsp_handler;
    return msg_quorum_rsp_handler;
}

static rstatus_t
msg_local_one_rsp_handler(struct msg *req, struct msg *rsp)
{
    ASSERT_LOG(!req->selected_rsp, "req %d already has a rsp %d, adding new rsp %d",
               req->id, req->selected_rsp->id, rsp->id);
    req->awaiting_rsps = 0;
    if (req->peer)
        log_warn("Received more than one response for dc_one. req: %d:%d \
                 prev rsp %d:%d new rsp %d:%d", req->id, req->parent_id,
                 req->peer->id, req->peer->parent_id, rsp->id, rsp->parent_id);
    req->peer = NULL;
    rsp->peer = req;
    req->selected_rsp = rsp;
    return DN_OK;
}

static rstatus_t
swallow_extra_rsp(struct msg *req, struct msg *rsp)
{
    log_info("req %d swallowing response %d", req->id, rsp->id);
    ASSERT_LOG(req->awaiting_rsps, "Req %d:%d already has no awaiting rsps, rsp %d",
               req->id, req->parent_id, rsp->id);
    // drop this response.
    rsp_put(rsp);
    msg_decr_awaiting_rsps(req);
    return DN_NOOPS;
}

static rstatus_t
msg_quorum_rsp_handler(struct msg *req, struct msg *rsp)
{
    if (req->rspmgr.done)
        return swallow_extra_rsp(req, rsp);
    rspmgr_submit_response(&req->rspmgr, rsp);
    if (!rspmgr_check_is_done(&req->rspmgr))
        return DN_EAGAIN;
    // rsp is absorbed by rspmgr. so we can use that variable
    rsp = rspmgr_get_response(&req->rspmgr);
    ASSERT(rsp);
    rspmgr_free_other_responses(&req->rspmgr, rsp);
    req->peer = NULL;
    rsp->peer = req;
    req->selected_rsp = rsp;
    req->err = rsp->err;
    req->error = rsp->error;
    req->dyn_error = rsp->dyn_error;
    return DN_OK;
}

static void
req_client_enqueue_omsgq(struct context *ctx, struct conn *conn, struct msg *msg)
{
    ASSERT(msg->request);
    ASSERT(conn->type == CONN_CLIENT);

    conn->omsg_count++;
    histo_add(&ctx->stats->client_out_queue, conn->omsg_count);
    TAILQ_INSERT_TAIL(&conn->omsg_q, msg, c_tqe);
    log_debug(LOG_VERB, "conn %p enqueue outq %d:%d", conn, msg->id, msg->parent_id);
}

static void
req_client_dequeue_omsgq(struct context *ctx, struct conn *conn, struct msg *msg)
{
    ASSERT(msg->request);
    ASSERT(conn->type == CONN_CLIENT);

    if (msg->stime_in_microsec) {
        usec_t latency = dn_usec_now() - msg->stime_in_microsec;
        stats_histo_add_latency(ctx, latency);
    }
    conn->omsg_count--;
    histo_add(&ctx->stats->client_out_queue, conn->omsg_count);
    TAILQ_REMOVE(&conn->omsg_q, msg, c_tqe);
    log_debug(LOG_VERB, "conn %p dequeue outq %p", conn, msg);
}

static void
client_rsp_send_done(struct context *ctx, struct conn *conn, struct msg *rsp)
{
    ASSERT(conn->type == CONN_CLIENT);
    ASSERT(conn->smsg == NULL);

    log_debug(LOG_VERB, "conn %p rsp %p done", conn, rsp);
    struct msg *req = rsp->peer;
    ASSERT_LOG(req, "response %d does not have a corresponding request", rsp->id);
    ASSERT_LOG(!req->rsp_sent, "request %d:%d already had a response sent",
               req->id, req->parent_id);

    ASSERT(!rsp->request && req->request);
    ASSERT(req->selected_rsp == rsp);
    req->rsp_sent = 1;

    /* dequeue request from client outq */
    conn_dequeue_outq(ctx, conn, req);

    // Remove it from the dict
    if (!req->awaiting_rsps) {
        log_debug(LOG_VERB, "conn %p removing message %d:%d", conn, req->id, req->parent_id);
        dictDelete(conn->outstanding_msgs_dict, &req->id);
        req_put(req);
    } else {
        log_info("req %d:%d still awaiting rsps %d", req->id, req->parent_id,
                  req->awaiting_rsps);
    }
}

struct conn_ops client_ops = {
    msg_recv,
    req_recv_next,
    req_recv_done,
    msg_send,
    rsp_send_next,
    client_rsp_send_done,
    client_close,
    client_active,
    client_ref,
    client_unref,
    NULL,
    NULL,
    req_client_enqueue_omsgq,
    req_client_dequeue_omsgq,
    client_handle_response 
};

void
init_client_conn(struct conn *conn)
{
    conn->type = CONN_CLIENT;
    conn->ops = &client_ops;
}
