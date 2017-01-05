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
#include "dyn_dict_msg_id.h"

static rstatus_t msg_quorum_rsp_handler(struct msg *req, struct msg *rsp);
static msg_response_handler_t msg_get_rsp_handler(struct msg *req);


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
        log_warn("conn %s %d Waiting for %lu outstanding messages",
                 conn_get_type_string(conn), conn->sd, msgs);
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
    struct msg *req, *nreq; /* current and next message */

    ASSERT(conn->type == CONN_CLIENT);

    client_close_stats(ctx, conn->owner, conn->err, conn->eof);

    if (conn->sd < 0) {
        client_unref(conn);
        return;
    }

    req = conn->rmsg;
    if (req != NULL) {
        conn->rmsg = NULL;

        ASSERT(req->selected_rsp == NULL);
        ASSERT(req->is_request && !req->done);

        log_debug(LOG_INFO, "close c %d discarding pending req %"PRIu64" len "
                  "%"PRIu32" type %d", conn->sd, req->id, req->mlen,
                  req->type);

        req_put(req);
    }

    ASSERT(conn->smsg == NULL);
    ASSERT(TAILQ_EMPTY(&conn->imsg_q));

    for (req = TAILQ_FIRST(&conn->omsg_q); req != NULL; req = nreq) {
        nreq = TAILQ_NEXT(req, c_tqe);

        /* dequeue the message (request) from client outq */
        conn_dequeue_outq(ctx, conn, req);

        if (req->done || req->selected_rsp) {
            log_debug(LOG_INFO, "close c %d discarding %s req %"PRIu64" len "
                      "%"PRIu32" type %d", conn->sd,
                      req->is_error ? "error": "completed", req->id, req->mlen,
                      req->type);
            req_put(req);
        } else {
            req->swallow = 1;

            ASSERT(req->is_request);
            ASSERT(req->selected_rsp == NULL);

            log_debug(LOG_INFO, "close c %d schedule swallow of req %"PRIu64" "
                      "len %"PRIu32" type %d", conn->sd, req->id, req->mlen,
                      req->type);
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

/* Handle a response to a given request. if this is a quorum setting, choose the
 * right response. Then make sure all the requests are satisfied in a fragmented
 * request scenario and then use the post coalesce logic to cook up a combined
 * response
 */
static rstatus_t
client_handle_response(struct conn *conn, msgid_t reqid, struct msg *rsp)
{
    // now the handler owns the response.
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
        log_info("Putting req %lu:%lu", req->id, req->parent_id);
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
                log_info("Putting req %lu:%lu", req->id, req->parent_id);
                req_put(req);
            }
        }
    } else if (status == DN_OK) {
        g_pre_coalesce(req->selected_rsp);
        if (req_done(conn, req)) {
            status = conn_event_add_out(conn);
            if (status != DN_OK) {
                conn->err = errno;
            }
        }
    }
    return status;
}

struct msg *
req_recv_next(struct context *ctx, struct conn *conn, bool alloc)
{
    struct msg *req;

    ASSERT((conn->type == CONN_DNODE_PEER_CLIENT) ||
           (conn->type = CONN_CLIENT));

    if (conn->eof) {
        req = conn->rmsg;

        //if (conn->dyn_mode) {
        //    if (conn->non_bytes_recv > MAX_CONN_ALLOWABLE_NON_RECV) {
        //        conn->err = EPIPE;
        //        return NULL;
        //    }
        //    conn->eof = 0;
        //    return req;
        //}

        /* client sent eof before sending the entire request */
        if (req != NULL) {
            conn->rmsg = NULL;

            ASSERT(req->selected_rsp == NULL);
            ASSERT(req->is_request && !req->done);

            log_error("eof c %d discarding incomplete req %"PRIu64" len "
                      "%"PRIu32"", conn->sd, req->id, req->mlen);

            req_put(req);
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

    req = conn->rmsg;
    if (req != NULL) {
        ASSERT(req->is_request);
        return req;
    }

    if (!alloc) {
        return NULL;
    }

    req = req_get(conn);
    if (req != NULL) {
        conn->rmsg = req;
    }

    return req;
}

static bool
req_filter(struct context *ctx, struct conn *conn, struct msg *req)
{
    ASSERT(conn->type == CONN_CLIENT);

    if (msg_empty(req)) {
        ASSERT(conn->rmsg == NULL);
        log_debug(LOG_VERB, "filter empty req %"PRIu64" from c %d", req->id,
                  conn->sd);
        req_put(req);
        return true;
    }

    /*
     * Handle "quit\r\n", which is the protocol way of doing a
     * passive close
     */
    if (req->quit) {
        ASSERT(conn->rmsg == NULL);
        log_debug(LOG_INFO, "filter quit req %"PRIu64" from c %d", req->id,
                  conn->sd);
        conn->eof = 1;
        conn->recv_ready = 0;
        req_put(req);
        return true;
    }

    return false;
}

static void
send_rsp_integer(struct context *ctx, struct conn *c_conn, struct msg *req)
{
    //do nothing
    struct msg *rsp = msg_get_rsp_integer(c_conn);
    req->selected_rsp = rsp;
    rsp->peer = req;
    req->selected_rsp = rsp;

    req->done = 1;
    //req->pre_coalesce(req);
    rstatus_t status = conn_event_add_out(c_conn);
    IGNORE_RET_VAL(status);
}

/* Expects req to be in the conn's outq already */
void
req_forward_error(struct context *ctx, struct conn *conn, struct msg *req,
                  err_t error_code, err_t dyn_error_code)
{
    log_info("forward req %lu:%lu len %"PRIu32" type %d from "
             "%s c %d failed: %s", req->id, req->parent_id, req->mlen, req->type,
             conn_get_type_string(conn), conn->sd, strerror(error_code));

    // Nothing to do if request is not expecting a reply.
    // The higher layer will take care of freeing the request
    if (!req->expect_datastore_reply) {
        return;
    }

    // Create an appropriate response for the request so its propagated up;
    // This response gets dropped in rsp_make_error anyways. But since this is
    // an error path its ok with the overhead.
    struct msg *rsp = msg_get(conn, false, __FUNCTION__);
    rsp->type = MSG_RSP_REDIS_ERROR;
    rsp->peer = req;
    rsp->is_error = 1;
    rsp->error_code = error_code;
    rsp->dyn_error_code = dyn_error_code;
    //TODO: Check if this is required in response
    rsp->dmsg = dmsg_get();
    rsp->dmsg->id =  req->id;

    rstatus_t status =
        conn_handle_response(conn, req->parent_id ? req->parent_id : req->id,
                             rsp);
    IGNORE_RET_VAL(status);
}

static void
req_redis_stats(struct context *ctx, struct msg *req)
{

    switch (req->type) {

    case MSG_REQ_REDIS_GET:
         stats_server_incr(ctx, redis_req_get);
         break;
    case MSG_REQ_REDIS_SET:
         stats_server_incr(ctx, redis_req_set);
         break;
    case MSG_REQ_REDIS_DEL:
         stats_server_incr(ctx, redis_req_del);
         break;
    case MSG_REQ_REDIS_INCR:
    case MSG_REQ_REDIS_DECR:
         stats_server_incr(ctx, redis_req_incr_decr);
         break;
    case MSG_REQ_REDIS_KEYS:
         stats_server_incr(ctx, redis_req_keys);
         break;
    case MSG_REQ_REDIS_MGET:
         stats_server_incr(ctx, redis_req_mget);
         break;
    case MSG_REQ_REDIS_SCAN:
         stats_server_incr(ctx, redis_req_scan);
         break;
    case MSG_REQ_REDIS_SORT:
          stats_server_incr(ctx, redis_req_sort);
          break;
    case MSG_REQ_REDIS_PING:
         stats_server_incr(ctx, redis_req_ping);
         break;
    case MSG_REQ_REDIS_LREM:
          stats_server_incr(ctx, redis_req_lreqm);
          /* do not break as this is a list operation as the following.
           * We count twice the LREM because it is an intensive operation/
           *  */
    case MSG_REQ_REDIS_LRANGE:
    case MSG_REQ_REDIS_LSET:
    case MSG_REQ_REDIS_LTRIM:
    case MSG_REQ_REDIS_LINDEX:
    case MSG_REQ_REDIS_LPUSHX:
         stats_server_incr(ctx, redis_req_lists);
         break;
    case MSG_REQ_REDIS_SUNION:
         stats_server_incr(ctx, redis_req_sunion);
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
        stats_server_incr(ctx, redis_req_set);
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
        stats_server_incr(ctx, redis_req_sortedsets);
        break;
    case MSG_REQ_REDIS_HINCRBY:
    case MSG_REQ_REDIS_HINCRBYFLOAT:
    case MSG_REQ_REDIS_HSET:
    case MSG_REQ_REDIS_HSETNX:
        stats_server_incr(ctx, redis_req_hashes);
        break;
    default:
        stats_server_incr(ctx, redis_req_other);
        break;
    }
}

static void
req_forward_stats(struct context *ctx, struct msg *req)
{
    ASSERT(req->is_request);

    if (req->is_read) {
       stats_server_incr(ctx, read_requests);
       stats_server_incr_by(ctx, read_request_bytes, req->mlen);
    } else {
       stats_server_incr(ctx, write_requests);
       stats_server_incr_by(ctx, write_request_bytes, req->mlen);
    }
}

rstatus_t
local_req_forward(struct context *ctx, struct conn *c_conn, struct msg *req,
                  uint8_t *key, uint32_t keylen, dyn_error_t *dyn_error_code)
{
    rstatus_t status;
    struct conn *s_conn;

    ASSERT((c_conn->type == CONN_CLIENT) ||
           (c_conn->type == CONN_DNODE_PEER_CLIENT));

    s_conn = get_datastore_conn(ctx, c_conn->owner);
    log_debug(LOG_VERB, "c_conn %p got server conn %p", c_conn, s_conn);
    if (s_conn == NULL) {
        *dyn_error_code = STORAGE_CONNECTION_REFUSE;
        return errno;
    }
    ASSERT(s_conn->type == CONN_SERVER);

    if (log_loggable(LOG_DEBUG)) {
       log_debug(LOG_DEBUG, "forwarding request from client conn '%s' to storage conn '%s'",
                    dn_unresolve_peer_desc(c_conn->sd), dn_unresolve_peer_desc(s_conn->sd));
    }

    if (ctx->dyn_state == NORMAL) {
        /* enqueue the message (request) into server inq */
        if (TAILQ_EMPTY(&s_conn->imsg_q)) {
            status = conn_event_add_out(s_conn);

            if (status != DN_OK) {
                *dyn_error_code = DYNOMITE_UNKNOWN_ERROR;
                s_conn->err = errno;
                return DN_ERROR;
            }
        }
    } else if (ctx->dyn_state == STANDBY) {  //no reads/writes from peers/clients
        log_debug(LOG_INFO, "Node is in STANDBY state. Drop write/read requests");
        *dyn_error_code = DYNOMITE_INVALID_STATE;
        return DN_ERROR;
    } else if (ctx->dyn_state == WRITES_ONLY && req->is_read) {
        //no reads from peers/clients but allow writes from peers/clients
        log_debug(LOG_INFO, "Node is in WRITES_ONLY state. Drop read requests");
        *dyn_error_code = DYNOMITE_INVALID_STATE;
        return DN_ERROR;
    } else if (ctx->dyn_state == RESUMING) {
        log_debug(LOG_INFO, "Node is in RESUMING state. Still drop read requests and flush out all the queued writes");
        if (req->is_read) {
            *dyn_error_code = DYNOMITE_INVALID_STATE;
            return DN_ERROR;
        }

        status = conn_event_add_out(s_conn);

        if (status != DN_OK) {
            *dyn_error_code = DYNOMITE_UNKNOWN_ERROR;
            s_conn->err = errno;
            return DN_ERROR;
        }
    }

    conn_enqueue_inq(ctx, s_conn, req);
    req_forward_stats(ctx, req);
    if(g_data_store == DATA_REDIS){
        req_redis_stats(ctx, req);
    }


    if (log_loggable(LOG_VERB)) {
       log_debug(LOG_VERB, "local forward from c %d to s %d req %"PRIu64" len %"PRIu32
                " type %d with key '%.*s'", c_conn->sd, s_conn->sd, req->id,
                req->mlen, req->type, keylen, key);
    }
    *dyn_error_code = 0;
    return DN_OK;
}


static rstatus_t
admin_local_req_forward(struct context *ctx, struct conn *c_conn, struct msg *req,
                        struct rack *rack, uint8_t *key, uint32_t keylen,
                        dyn_error_t *dyn_error_code)
{
    ASSERT((c_conn->type == CONN_CLIENT) ||
           (c_conn->type == CONN_DNODE_PEER_CLIENT));

    struct node *peer = dnode_peer_pool_server(ctx, c_conn->owner, rack, key, keylen, req->msg_routing);
    if (!peer->is_local) {
        *dyn_error_code = DYNOMITE_INVALID_ADMIN_REQ;
        return DN_ERROR;
    }

    log_debug(LOG_NOTICE, "Need to delete [%.*s] ", keylen, key);
    return local_req_forward(ctx, c_conn, req, key, keylen, dyn_error_code);
}

/* On Success, the request is placed in the other connection's inq. Otherwise
 * it is the caller's responsibility to take care of freeing it.
 */
rstatus_t
remote_req_forward(struct context *ctx, struct conn *c_conn, struct msg *req, 
                   struct rack *rack, uint8_t *key, uint32_t keylen,
                   dyn_error_t *dyn_error_code)
{
    ASSERT((c_conn->type == CONN_CLIENT) ||
           (c_conn->type == CONN_DNODE_PEER_CLIENT));

    struct node * peer = dnode_peer_pool_server(ctx, c_conn->owner, rack, key,
                                                keylen, req->msg_routing);
    if (peer->is_local) {
        log_debug(LOG_VERB, "c_conn: %p forwarding %d:%d is local", c_conn,
                  req->id, req->parent_id);
        return local_req_forward(ctx, c_conn, req, key, keylen, dyn_error_code);
    }

    if (peer->state == DOWN) {
        *dyn_error_code = PEER_HOST_DOWN;
        return DN_ERROR;
    }

    // now get a peer connection
    struct conn *p_conn = dnode_peer_pool_server_conn(ctx, peer);
    if (!p_conn) {
        // No active connection. return error
        *dyn_error_code = PEER_HOST_NOT_CONNECTED;
        return DN_ERROR;
    }

    log_debug(LOG_VERB, "c_conn: %p forwarding %d:%d to p_conn %p", c_conn,
            req->id, req->parent_id, p_conn);
    return dnode_peer_req_forward(ctx, c_conn, p_conn, req, rack, key, keylen, dyn_error_code);
}

void
req_forward_all_local_racks(struct context *ctx, struct conn *c_conn,
                            struct msg *req, struct mbuf *orig_mbuf,
                            uint8_t *key, uint32_t keylen, struct datacenter *dc)
{
    uint8_t rack_cnt = (uint8_t)array_n(&dc->racks);
    uint8_t rack_index;
    init_response_mgr(&req->rspmgr, req, req->is_read, rack_cnt, c_conn);
    log_info("req %d:%d same DC racks:%d expect replies %d",
             req->id, req->parent_id, rack_cnt, req->rspmgr.max_responses);
    for(rack_index = 0; rack_index < rack_cnt; rack_index++) {

        struct rack *rack = array_get(&dc->racks, rack_index);
        struct server_pool *pool = c_conn->owner;
        dyn_error_t dyn_error_code = 0;
        rstatus_t s = DN_OK;

        if (string_compare(rack->name, &pool->rack) == 0 ) {

            // Local Rack
            s = remote_req_forward(ctx, c_conn, req, rack, key,
                                   keylen, &dyn_error_code);
            if (s != DN_OK) {
                req_forward_error(ctx, c_conn, req, s, dyn_error_code);
            }

        } else {
            // Remote Rack
            struct msg *rack_msg = msg_get(c_conn, req->is_request, __FUNCTION__);
            if (rack_msg == NULL) {
                log_debug(LOG_VERB, "whelp, looks like yer screwed "
                        "now, buddy. no inter-rack messages for you!");
                if (req->consistency != DC_ONE) {
                    // expecting a reply to form a quorum
                    req_forward_error(ctx, c_conn, req, DN_ENOMEM, DYNOMITE_UNKNOWN_ERROR);
                }
                continue;
            }

            msg_clone(req, orig_mbuf, rack_msg);
            rack_msg->swallow = true;
            log_info("conn: %p(%d) %s req (%lu:%lu) clone to rack req (%lu:%lu) for rack '%.*s'",
                     c_conn, c_conn->sd, conn_get_type_string(c_conn), req->id, req->parent_id, rack_msg->id,
                     rack_msg->parent_id, rack->name->len, rack->name->data);

            s = remote_req_forward(ctx, c_conn, rack_msg, rack, key, keylen, &dyn_error_code);
            if (s != DN_OK) {
                if (req->consistency != DC_ONE) {
                    // expecting a reply to form a quorum
                    req_forward_error(ctx, c_conn, rack_msg, s, dyn_error_code);
                }
                req_put(rack_msg);
                continue;
            }
        }
    }
}

static bool
request_send_to_all_dcs(struct msg *req)
{
    // There is a routing override
    if (req->msg_routing != ROUTING_NORMAL)
        return false;

    // Reads are not propagated
    if (req->is_read)
        return false;

    return true;
}

/**
 * Determine if a request should be forwarded to all replicas within the local
 * DC.
 * @param[in] req Message.
 * @return bool True if message should be forwarded to all local replicas, else false
 */
static bool
request_send_to_all_local_racks(struct msg *req)
{
    /* There is a routing override set by the parser on this message. Do not
     * propagate it to other racks irrespective of the consistency setting */
    if (req->msg_routing != ROUTING_NORMAL)
        return false;

    // A write should go to all racks
    if (!req->is_read)
        return true;

    if ((req->consistency == DC_QUORUM) ||
        (req->consistency == DC_SAFE_QUORUM))
        return true;
    return false;
}

static void
req_forward_remote_dc(struct context *ctx, struct conn *c_conn, struct msg *req,
                      struct mbuf *orig_mbuf, uint8_t *key, uint32_t keylen,
                      struct datacenter *dc)
{
    const uint32_t rack_cnt = array_n(&dc->racks);
    if (rack_cnt == 0)
        return;

    struct rack *rack = dc->preselected_rack_for_replication;
    if (rack == NULL)
        rack = array_get(&dc->racks, 0);

    struct msg *rack_msg = msg_get(c_conn, req->is_request, __FUNCTION__);
    if (rack_msg == NULL) {
        log_debug(LOG_VERB, "whelp, looks like yer screwed now, buddy. no inter-rack messages for you!");
        return;
    }

    msg_clone(req, orig_mbuf, rack_msg);
    log_info("req (%d:%d) clone to remote dc rack req (%d:%d)",
             req->id, req->parent_id, rack_msg->id, rack_msg->parent_id);
    rack_msg->swallow = true;

    log_debug(LOG_DEBUG, "forwarding request from conn '%s' on rack '%.*s'",
              dn_unresolve_peer_desc(c_conn->sd), rack->name->len, rack->name->data);

    dyn_error_t dyn_error_code = 0;
    rstatus_t s = remote_req_forward(ctx, c_conn, rack_msg, rack, key, keylen,
                                     &dyn_error_code);
    if (s == DN_OK) {
        return;
    }
    req_put(rack_msg);
    // Start over with another rack.
    uint8_t rack_index;
    for(rack_index = 0; rack_index < rack_cnt; rack_index++) {
        rack = array_get(&dc->racks, rack_index);

        if (rack == dc->preselected_rack_for_replication)
            continue;
        rack_msg = msg_get(c_conn, req->is_request, __FUNCTION__);
        if (rack_msg == NULL) {
            log_debug(LOG_VERB, "whelp, looks like yer screwed now, buddy. no inter-rack messages for you!");
            return;
        }

        msg_clone(req, orig_mbuf, rack_msg);
        log_info("req (%d:%d) clone to remote dc rack req (%d:%d)",
                req->id, req->parent_id, rack_msg->id, rack_msg->parent_id);
        rack_msg->swallow = true;

        log_debug(LOG_DEBUG, "forwarding request from conn '%s' on rack '%.*s'",
                  dn_unresolve_peer_desc(c_conn->sd), rack->name->len, rack->name->data);

        dyn_error_code = DYNOMITE_OK;
        s = remote_req_forward(ctx, c_conn, rack_msg, rack, key, keylen,
                               &dyn_error_code);
        if (s == DN_OK) {
            stats_pool_incr(ctx, remote_peer_failover_requests);
            return;
        }
        req_put(rack_msg);
    }
    stats_pool_incr(ctx, remote_peer_dropped_requests);
}

static void
req_forward_local_dc(struct context *ctx, struct conn *c_conn, struct msg *req,
                     struct mbuf *orig_mbuf, uint8_t *key, uint32_t keylen,
                     struct datacenter *dc)
{
    struct server_pool *pool = c_conn->owner;
    req->rsp_handler = msg_get_rsp_handler(req);
    if (request_send_to_all_local_racks(req)) {
        // send request to all local racks
        req_forward_all_local_racks(ctx, c_conn, req, orig_mbuf, key, keylen, dc);
    } else {
        // send request to only local token owner
        struct rack * rack = server_get_rack_by_dc_rack(pool, &pool->rack,
                                                        &pool->dc);
        dyn_error_t dyn_error_code = 0;
        rstatus_t s = remote_req_forward(ctx, c_conn, req, rack, key, keylen,
                                         &dyn_error_code);
        if (s != DN_OK) {
            req_forward_error(ctx, c_conn, req, s, dyn_error_code);
        }
    }
}

static void
req_forward(struct context *ctx, struct conn *c_conn, struct msg *req)
{
    struct server_pool *pool = c_conn->owner;
    dyn_error_t dyn_error_code = DYNOMITE_OK;
    rstatus_t s = DN_OK;

    ASSERT(c_conn->type == CONN_CLIENT);

    if (req->is_read) {
        if (req->type != MSG_REQ_REDIS_PING)
            stats_pool_incr(ctx, client_read_requests);
    } else
        stats_pool_incr(ctx, client_write_requests);

    uint32_t keylen = 0;
    uint8_t *key = msg_get_key(req, &pool->hash_tag, &keylen);

    log_debug(LOG_DEBUG, "conn %p received message %d:%d key '%.*s' adding to dict", c_conn,
              req->id, req->parent_id, keylen, key);
    // add the message to the dict
    dictAdd(c_conn->outstanding_msgs_dict, &req->id, req);


    // need to capture the initial mbuf location as once we add in the dynomite
    // headers (as mbufs to the src req), that will bork the request sent to
    // secondary racks
    struct mbuf *orig_mbuf = STAILQ_FIRST(&req->mhdr);

    /* enqueue message (request) into client outq, if response is expected */
    if (req->expect_datastore_reply) {
        conn_enqueue_outq(ctx, c_conn, req);
    }

    if (ctx->admin_opt == 1) {
        if (req->type == MSG_REQ_REDIS_DEL || req->type == MSG_REQ_MC_DELETE) {
            struct rack * rack = server_get_rack_by_dc_rack(pool, &pool->rack, &pool->dc);
            s = admin_local_req_forward(ctx, c_conn, req, rack, key,
                                        keylen, &dyn_error_code);
            if (s != DN_OK) {
                req_forward_error(ctx, c_conn, req, s, dyn_error_code);
            }
            return;
        }
    }

    if (req->msg_routing == ROUTING_LOCAL_NODE_ONLY) {
        // Strictly local host only
        req->consistency = DC_ONE;
        req->rsp_handler = msg_local_one_rsp_handler;

        s = local_req_forward(ctx, c_conn, req, key, keylen, &dyn_error_code);
        if (s != DN_OK) {
            req_forward_error(ctx, c_conn, req, s, dyn_error_code);
        }
        return;
    }

    req->consistency = req->is_read ? conn_get_read_consistency(c_conn) :
                                      conn_get_write_consistency(c_conn);

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
            req_forward_local_dc(ctx, c_conn, req, orig_mbuf, key, keylen, dc);
        else if (request_send_to_all_dcs(req)) {
            req_forward_remote_dc(ctx, c_conn, req, orig_mbuf, key, keylen, dc);
        }
    }
}


void
req_recv_done(struct context *ctx, struct conn *conn,
              struct msg *req, struct msg *nreq)
{
    ASSERT(conn->type == CONN_CLIENT);
    ASSERT(req->is_request);
    ASSERT(req->owner == conn);
    ASSERT(conn->rmsg == req);
    ASSERT(nreq == NULL || nreq->is_request);

    if (!req->is_read)
        stats_histo_add_payloadsize(ctx, req->mlen);

    /* enqueue next message (request), if any */
    conn->rmsg = nreq;

    if (req_filter(ctx, conn, req)) {
        return;
    }

    req->stime_in_microsec = dn_usec_now();
    req_forward(ctx, conn, req);
}

static msg_response_handler_t 
msg_get_rsp_handler(struct msg *req)
{
    if (request_send_to_all_local_racks(req)) {
        // Request is being braoadcasted
        // Check if its quorum
        if ((req->consistency == DC_QUORUM) ||
            (req->consistency == DC_SAFE_QUORUM))
            return msg_quorum_rsp_handler;
    }
    return msg_local_one_rsp_handler;
}

rstatus_t
msg_local_one_rsp_handler(struct msg *req, struct msg *rsp)
{
    ASSERT_LOG(!req->selected_rsp, "Received more than one response for dc_one.\
               req: %d:%d prev rsp %d:%d new rsp %d:%d", req->id, req->parent_id,
               req->selected_rsp->id, req->selected_rsp->parent_id, rsp->id,
               rsp->parent_id);
    req->awaiting_rsps = 0;
    rsp->peer = req;
    req->is_error = rsp->is_error;
    req->error_code = rsp->error_code;
    req->dyn_error_code = rsp->dyn_error_code;
    req->selected_rsp = rsp;
    log_info("Req %lu:%lu selected_rsp %lu:%lu", req->id, req->parent_id,
             rsp->id, rsp->parent_id);
    return DN_OK;
}

static rstatus_t
swallow_extra_rsp(struct msg *req, struct msg *rsp)
{
    log_info("req %d swallowing response %d awaiting %d",
             req->id, rsp->id, req->awaiting_rsps);
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
    rsp->peer = req;
    req->selected_rsp = rsp;
    req->error_code = rsp->error_code;
    req->is_error = rsp->is_error;
    req->dyn_error_code = rsp->dyn_error_code;
    return DN_OK;
}

static void
req_client_enqueue_omsgq(struct context *ctx, struct conn *conn, struct msg *req)
{
    ASSERT(req->is_request);
    ASSERT(conn->type == CONN_CLIENT);

    conn->omsg_count++;
    histo_add(&ctx->stats->client_out_queue, conn->omsg_count);
    TAILQ_INSERT_TAIL(&conn->omsg_q, req, c_tqe);
    log_debug(LOG_VERB, "conn %p enqueue outq %lu:%lu", conn, req->id, req->parent_id);
}

static void
req_client_dequeue_omsgq(struct context *ctx, struct conn *conn, struct msg *req)
{
    ASSERT(req->is_request);
    ASSERT(conn->type == CONN_CLIENT);

    if (req->stime_in_microsec) {
        usec_t latency = dn_usec_now() - req->stime_in_microsec;
        stats_histo_add_latency(ctx, latency);
    }
    conn->omsg_count--;
    histo_add(&ctx->stats->client_out_queue, conn->omsg_count);
    TAILQ_REMOVE(&conn->omsg_q, req, c_tqe);
    log_debug(LOG_VERB, "conn %p dequeue outq %lu:%lu", conn, req->id, req->parent_id);
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
