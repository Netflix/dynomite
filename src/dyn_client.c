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
#include "dyn_server.h"
#include "dyn_client.h"
#include "dyn_dnode_peer.h"

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
    stats_pool_decr(ctx, pool, client_connections);

    if (eof) {
        stats_pool_incr(ctx, pool, client_eof);
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
        stats_pool_incr(ctx, pool, client_err);
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
        client_unref_and_try_put(conn);
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

        stats_pool_incr(ctx, conn->owner, client_dropped_requests);
    }
    ASSERT(TAILQ_EMPTY(&conn->omsg_q));

    status = close(conn->sd);
    if (status < 0) {
        log_error("close c %d failed, ignored: %s", conn->sd, strerror(errno));
    }
    conn->sd = -1;
    client_unref_and_try_put(conn);
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
        struct context *ctx = conn_to_ctx(conn);
        status = event_add_out(ctx->evb, conn);
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
    rstatus_t status = event_add_out(ctx->evb, c_conn);
    IGNORE_RET_VAL(status);
}

static void
req_forward_error(struct context *ctx, struct conn *conn, struct msg *msg,
                  err_t err)
{
    rstatus_t status;

    if (log_loggable(LOG_INFO)) {
       log_debug(LOG_INFO, "forward req %"PRIu64" len %"PRIu32" type %d from "
                 "c %d failed: %s", msg->id, msg->mlen, msg->type, conn->sd,
                 strerror(err));
    }

    msg->done = 1;
    msg->error = 1;
    msg->err = err;

    if (!msg->expect_datastore_reply) {
        req_put(msg);
        return;
    }

    if (req_done(conn, TAILQ_FIRST(&conn->omsg_q))) {
        status = event_add_out(ctx->evb, conn);
        if (status != DN_OK) {
            conn->err = err;
        }
    }

}

static void
req_redis_stats(struct context *ctx, struct server *server, struct msg *msg)
{

    switch (msg->type) {

    case MSG_REQ_REDIS_GET:
    	 stats_server_incr(ctx, server, redis_req_get);
    	 break;
    case MSG_REQ_REDIS_SET:
    	 stats_server_incr(ctx, server, redis_req_set);
         break;
    case MSG_REQ_REDIS_DEL:
         stats_server_incr(ctx, server, redis_req_del);
    	 break;
    case MSG_REQ_REDIS_INCR:
    case MSG_REQ_REDIS_DECR:
         stats_server_incr(ctx, server, redis_req_incr_decr);
         break;
    case MSG_REQ_REDIS_KEYS:
         stats_server_incr(ctx, server, redis_req_keys);
         break;
    case MSG_REQ_REDIS_MGET:
         stats_server_incr(ctx, server, redis_req_mget);
         break;
    case MSG_REQ_REDIS_SCAN:
         stats_server_incr(ctx, server, redis_req_scan);
         break;
    case MSG_REQ_REDIS_SORT:
          stats_server_incr(ctx, server, redis_req_sort);
          break;
    case MSG_REQ_REDIS_PING:
         stats_server_incr(ctx, server, redis_req_ping);
         break;
    case MSG_REQ_REDIS_LREM:
          stats_server_incr(ctx, server, redis_req_lreqm);
          /* do not break as this is a list operation as the following.
           * We count twice the LREM because it is an intensive operation/
           *  */
    case MSG_REQ_REDIS_LRANGE:
    case MSG_REQ_REDIS_LSET:
    case MSG_REQ_REDIS_LTRIM:
    case MSG_REQ_REDIS_LINDEX:
    case MSG_REQ_REDIS_LPUSHX:
    	 stats_server_incr(ctx, server, redis_req_lists);
    	 break;
    case MSG_REQ_REDIS_SUNION:
         stats_server_incr(ctx, server, redis_req_sunion);
         /* do not break as this is a set operation as the following.
          * We count twice the SUNION because it is an intensive operation/
          *  */
    case MSG_REQ_REDIS_SETBIT:
    case MSG_REQ_REDIS_SETEX:
    case MSG_REQ_REDIS_SETRANGE:
    case MSG_REQ_REDIS_SADD:
    case MSG_REQ_REDIS_SDIFF:
    case MSG_REQ_REDIS_SDIFFSTORE:
    case MSG_REQ_REDIS_SINTER:
    case MSG_REQ_REDIS_SINTERSTORE:
    case MSG_REQ_REDIS_SREM:
    case MSG_REQ_REDIS_SUNIONSTORE:
    case MSG_REQ_REDIS_SSCAN:
    	stats_server_incr(ctx, server, redis_req_set);
    	break;
    case MSG_REQ_REDIS_ZADD:
    case MSG_REQ_REDIS_ZINTERSTORE:
    case MSG_REQ_REDIS_ZRANGE:
    case MSG_REQ_REDIS_ZRANGEBYSCORE:
    case MSG_REQ_REDIS_ZREM:
    case MSG_REQ_REDIS_ZREVRANGE:
    case MSG_REQ_REDIS_ZREVRANGEBYSCORE:
    case MSG_REQ_REDIS_ZUNIONSTORE:
    case MSG_REQ_REDIS_ZSCAN:
    case MSG_REQ_REDIS_ZCOUNT:
    case MSG_REQ_REDIS_ZINCRBY:
    case MSG_REQ_REDIS_ZREMRANGEBYRANK:
    case MSG_REQ_REDIS_ZREMRANGEBYSCORE:
    	stats_server_incr(ctx, server, redis_req_sortedsets);
    	break;
    case MSG_REQ_REDIS_HINCRBY:
    case MSG_REQ_REDIS_HINCRBYFLOAT:
    case MSG_REQ_REDIS_HSET:
    case MSG_REQ_REDIS_HSETNX:
    	stats_server_incr(ctx, server, redis_req_hashes);
    	break;
    default:
        stats_server_incr(ctx, server, redis_req_other);
        break;
    }
}

static void
req_forward_stats(struct context *ctx, struct server *server, struct msg *msg)
{
    ASSERT(msg->request);

    if (msg->is_read) {
       stats_server_incr(ctx, server, read_requests);
       stats_server_incr_by(ctx, server, read_request_bytes, msg->mlen);
    } else {
       stats_server_incr(ctx, server, write_requests);
       stats_server_incr_by(ctx, server, write_request_bytes, msg->mlen);
    }
}

void
local_req_forward(struct context *ctx, struct conn *c_conn, struct msg *msg,
                  uint8_t *key, uint32_t keylen)
{
    rstatus_t status;
    struct conn *s_conn;

    if (log_loggable(LOG_VVERB)) {
       loga("local_req_forward entering ............");
    }

    ASSERT((c_conn->type == CONN_CLIENT) ||
           (c_conn->type == CONN_DNODE_PEER_CLIENT));

    /* enqueue message (request) into client outq, if response is expected */
    if (msg->expect_datastore_reply) {
        conn_enqueue_outq(ctx, c_conn, msg);
    }

    s_conn = server_pool_conn(ctx, c_conn->owner, key, keylen);
    log_debug(LOG_VERB, "c_conn %p got server conn %p", c_conn, s_conn);
    if (s_conn == NULL) {
        req_forward_error(ctx, c_conn, msg, errno);
        return;
    }
    ASSERT(s_conn->type == CONN_SERVER);

    if (log_loggable(LOG_DEBUG)) {
       log_debug(LOG_DEBUG, "forwarding request from client conn '%s' to storage conn '%s'",
                    dn_unresolve_peer_desc(c_conn->sd), dn_unresolve_peer_desc(s_conn->sd));
    }

    if (ctx->dyn_state == NORMAL) {
        /* enqueue the message (request) into server inq */
        if (TAILQ_EMPTY(&s_conn->imsg_q)) {
            status = event_add_out(ctx->evb, s_conn);

            if (status != DN_OK) {
                req_forward_error(ctx, c_conn, msg, errno);
                s_conn->err = errno;
                return;
            }
        }
    } else if (ctx->dyn_state == STANDBY) {  //no reads/writes from peers/clients
        log_debug(LOG_INFO, "Node is in STANDBY state. Drop write/read requests");
        req_forward_error(ctx, c_conn, msg, errno);
        return;
    } else if (ctx->dyn_state == WRITES_ONLY && msg->is_read) {
        //no reads from peers/clients but allow writes from peers/clients
        log_debug(LOG_INFO, "Node is in WRITES_ONLY state. Drop read requests");
        req_forward_error(ctx, c_conn, msg, errno);
        return;
    } else if (ctx->dyn_state == RESUMING) {
        log_debug(LOG_INFO, "Node is in RESUMING state. Still drop read requests and flush out all the queued writes");
        if (msg->is_read) {
            req_forward_error(ctx, c_conn, msg, errno);
            return;
        }

        status = event_add_out(ctx->evb, s_conn);

        if (status != DN_OK) {
            req_forward_error(ctx, c_conn, msg, errno);
            s_conn->err = errno;
            return;
        }
    }

    conn_enqueue_inq(ctx, s_conn, msg);
    req_forward_stats(ctx, s_conn->owner, msg);
    if(msg->data_store==DATA_REDIS){
    	req_redis_stats(ctx, s_conn->owner, msg);
	}


    if (log_loggable(LOG_VERB)) {
       log_debug(LOG_VERB, "local forward from c %d to s %d req %"PRIu64" len %"PRIu32
                " type %d with key '%.*s'", c_conn->sd, s_conn->sd, msg->id,
                msg->mlen, msg->type, keylen, key);
    }
}


static void
admin_local_req_forward(struct context *ctx, struct conn *c_conn, struct msg *msg,
                        struct rack *rack, uint8_t *key, uint32_t keylen)
{
    struct conn *p_conn;

    ASSERT((c_conn->type == CONN_CLIENT) ||
           (c_conn->type == CONN_DNODE_PEER_CLIENT));

    p_conn = dnode_peer_pool_conn(ctx, c_conn->owner, rack, key, keylen, msg->msg_type);
    if (p_conn == NULL) {
        c_conn->err = EHOSTDOWN;
        req_forward_error(ctx, c_conn, msg, c_conn->err);
        return;
    }

    struct server *peer = p_conn->owner;

    if (peer->is_local) {
        send_rsp_integer(ctx, c_conn, msg);
    } else {
        log_debug(LOG_NOTICE, "Need to delete [%.*s] ", keylen, key);
        local_req_forward(ctx, c_conn, msg, key, keylen);
    }
}

void
remote_req_forward(struct context *ctx, struct conn *c_conn, struct msg *msg, 
                        struct rack *rack, uint8_t *key, uint32_t keylen)
{
    struct conn *p_conn;

    ASSERT((c_conn->type == CONN_CLIENT) ||
           (c_conn->type == CONN_DNODE_PEER_CLIENT));

    p_conn = dnode_peer_pool_conn(ctx, c_conn->owner, rack, key, keylen, msg->msg_type);
    if (p_conn == NULL) {
        c_conn->err = EHOSTDOWN;
        req_forward_error(ctx, c_conn, msg, c_conn->err);
        return;
    }

    //jeb - check if s_conn is _this_ node, and if so, get conn from server_pool_conn instead
    struct server *peer = p_conn->owner;

    if (peer->is_local) {
        log_debug(LOG_VERB, "c_conn: %p forwarding %d:%d is local", c_conn,
                  msg->id, msg->parent_id);
        local_req_forward(ctx, c_conn, msg, key, keylen);
        return;
    } else {
        log_debug(LOG_VERB, "c_conn: %p forwarding %d:%d to p_conn %p", c_conn,
                  msg->id, msg->parent_id, p_conn);
        dnode_peer_req_forward(ctx, c_conn, p_conn, msg, rack, key, keylen);
    }
}

static void
req_forward_all_local_racks(struct context *ctx, struct conn *c_conn,
                            struct msg *msg, struct mbuf *orig_mbuf,
                            uint8_t *key, uint32_t keylen, struct datacenter *dc)
{
    //log_debug(LOG_DEBUG, "dc name  '%.*s'",
    //            dc->name->len, dc->name->data);
    uint8_t rack_cnt = (uint8_t)array_n(&dc->racks);
    uint8_t rack_index;
    msg->rsp_handler = msg_get_rsp_handler(msg);
    init_response_mgr(&msg->rspmgr, msg, msg->is_read, rack_cnt, c_conn);
    log_info("msg %d:%d same DC racks:%d expect replies %d",
             msg->id, msg->parent_id, rack_cnt, msg->rspmgr.max_responses);
    for(rack_index = 0; rack_index < rack_cnt; rack_index++) {
        struct rack *rack = array_get(&dc->racks, rack_index);
        //log_debug(LOG_DEBUG, "rack name '%.*s'",
        //            rack->name->len, rack->name->data);
        struct msg *rack_msg;
        // clone message even for local node
        struct server_pool *pool = c_conn->owner;
        if (string_compare(rack->name, &pool->rack) == 0 ) {
            rack_msg = msg;
        } else {
            rack_msg = msg_get(c_conn, msg->request, msg->data_store,
                               __FUNCTION__);
            if (rack_msg == NULL) {
                log_debug(LOG_VERB, "whelp, looks like yer screwed "
                        "now, buddy. no inter-rack messages for "
                        "you!");
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

    struct msg *rack_msg = msg_get(c_conn, msg->request, msg->data_store, __FUNCTION__);
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
        struct rack * rack = server_get_rack_by_dc_rack(pool, &pool->rack,
                                                        &pool->dc);
        remote_req_forward(ctx, c_conn, msg, rack, key, keylen);
    }

}

static void
req_forward(struct context *ctx, struct conn *c_conn, struct msg *msg)
{
    struct server_pool *pool = c_conn->owner;
    uint8_t *key;
    uint32_t keylen;

    ASSERT(c_conn->type == CONN_CLIENT);

    if (msg->is_read)
        stats_pool_incr(ctx, pool, client_read_requests);
    else
        stats_pool_incr(ctx, pool, client_write_requests);

    key = NULL;
    keylen = 0;

    // add the message to the dict
    log_debug(LOG_DEBUG, "conn %p adding message %d:%d", c_conn, msg->id, msg->parent_id);
    dictAdd(c_conn->outstanding_msgs_dict, &msg->id, msg);

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

    // need to capture the initial mbuf location as once we add in the dynomite
    // headers (as mbufs to the src msg), that will bork the request sent to
    // secondary racks
    struct mbuf *orig_mbuf = STAILQ_FIRST(&msg->mhdr);

    if (ctx->admin_opt == 1) {
        if (msg->type == MSG_REQ_REDIS_DEL || msg->type == MSG_REQ_MC_DELETE) {
          struct rack * rack = server_get_rack_by_dc_rack(pool, &pool->rack, &pool->dc);
          admin_local_req_forward(ctx, c_conn, msg, rack, key, keylen);
          return;
        }
    }

    if (msg->is_read) {
        msg->consistency = conn_get_read_consistency(c_conn);
    } else {
        msg->consistency = conn_get_write_consistency(c_conn);
    }

    /* forward the request */
    uint32_t dc_cnt = array_n(&pool->datacenters);
    uint32_t dc_index;

    for(dc_index = 0; dc_index < dc_cnt; dc_index++) {

        struct datacenter *dc = array_get(&pool->datacenters, dc_index);
        if (dc == NULL) {
            log_error("Wow, this is very bad, dc is NULL");
            return;
        }

        if (string_compare(dc->name, &pool->dc) == 0)
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

struct conn_ops client_ops = {
    msg_recv,
    req_recv_next,
    req_recv_done,
    msg_send,
    rsp_send_next,
    rsp_send_done,
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
