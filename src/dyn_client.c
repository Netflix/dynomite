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


void
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

void
client_unref(struct conn *conn)
{
    client_unref_and_try_put(conn);
}

bool
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

void
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
rstatus_t
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
            return;
        // all responses received
        dictDelete(conn->outstanding_msgs_dict, &reqid);
        log_info("Putting req %d", req->id);
        req_put(req);
        client_unref_internal_try_put(conn);
        return;
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

